import logging
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import pandas as pd
import schedule
from sqlalchemy import create_engine

from config import config
from constants import leaderboards_info
from utilities.steam_helpers import get_leaderboard, get_username


def compute_leaderboard(leaderboard_id: int,
                        course_name: str,
                        dt: datetime) -> list[tuple[int, int, int, int, str, datetime]]:
    """

    :param leaderboard_id: Fly Dangerous steam leaderboards web api id
    :param course_name: Fly Dangerous course name
    :param dt: timestamp for when the job run kicked off
    :return: list of entries for a leaderboard
    """
    logging.info(f"{datetime.now(timezone.utc)}: Beginning compute_leaderboard job for course: {course_name}")

    leaderboard_data = []
    xml_bytes = get_leaderboard(leaderboard_id)

    # Converting the byte response from the xml steamapi response using xml etrees
    data = ET.fromstring(xml_bytes)
    entries = data.find("entries")

    # Checking if there are less than 200 entries to determine what the max points available will be
    entry_count = len(entries)
    if entry_count < 200:
        points_total = entry_count
    else:
        points_total = 200

    # Iterating over the entries for entry indexes [0,199) - this may change in the future to support more entries
    for i, entry in enumerate(entries):
        # Getting values out of xml tree nodes
        steam_id = int(entry.find("steamid").text)
        rank = int(entry.find("rank").text)
        score_time = int(entry.find("score").text)
        points = points_total - rank

        leaderboard_data.append((rank, steam_id, score_time, points, course_name, dt))

    logging.info(
        f"{datetime.now(timezone.utc)}: Completed compute_leaderboard for course: {course_name}: Entries: {len(leaderboard_data)}")

    return leaderboard_data


# Done daily
def sink_top_200(leaderboards: list[list[tuple[int, int, int, int, str, datetime]]],
                 engine) -> None:
    """

    :param leaderboards: list of list of leaderboard entries
    :param engine: SQLAlchemy engine used for sinking data to database
    :return:
    """
    frames: list[pd.DataFrame] = []

    # Iterating over ledaerboard objects
    for leaderboard in leaderboards:
        leaderboard_200_df = pd.DataFrame(data=leaderboard,
                                          columns=["rank", "steam_id", "time", "points", "course", "timestamp"])

        leaderboard_200_df = leaderboard_200_df[leaderboard_200_df["rank"] <= 200]

        # Adding this leaderboard's top 200 entries to the frames which will be turned into our sinkable df
        frames.append(leaderboard_200_df)

    # Constructing the sinkable df of top 200 times per course
    top_200_per_course_df = pd.concat(frames)

    logging.info(f"{datetime.now(timezone.utc)}: Started sink_top_200")

    # Sink to database
    top_200_per_course_df.to_sql("top_score_history", con=engine, if_exists="append", index=False)
    logging.info(f"{datetime.now(timezone.utc)}: Successfully appended to top_score_history table")


# Done every run
def sink_top_scores(leaderboard_data: list[tuple[int, int, int, int, str, datetime]],
                    engine) -> None:
    """

    :param leaderboard_data: list of leaderboard entries
    :param engine: SQLAlchemy engine used for sinking data to database
    :return:
    """
    logging.info(f"{datetime.now(timezone.utc)}: Started sink_top_scores")

    leaderboard_df = pd.DataFrame(data=leaderboard_data,
                                  columns=["rank", "steam_id", "time", "points", "course", "timestamp"])

    # Sink to database
    leaderboard_df.to_sql("top_score", con=engine, if_exists="replace", index=False)
    logging.info(f"{datetime.now(timezone.utc)}: Successfully appended to top_score table")


# Done every run
def sink_leaders(full_leaderboard: list[tuple[int, int, int, int, str, datetime]],
                 dt: datetime,
                 engine) -> None:
    """

    :param full_leaderboard: list of entries for all Fly Dangerous leaderboard entries
    :param dt: timestamp of when the job run kicked off
    :param engine: SQLAlchemy engine used for sinking data to database
    :return:
    """
    logging.info(
        f"{datetime.now(timezone.utc)}: Beginning sink_leaders")

    top_scores_df = pd.DataFrame(data=full_leaderboard,
                                 columns=["rank", "steam_id", "time", "points", "course", "timestamp"])

    leader_df = top_scores_df.groupby("steam_id")["points"].sum().sort_values(ascending=False)
    leader_df = leader_df.reset_index()

    # Appending the run datetime to all rows under the new column timestamp
    leader_df["timestamp"] = dt

    # Iterating over all username entries in the top 200 and resolving them to their steam usernames
    usernames = []
    for i, val in enumerate(leader_df["steam_id"]):
        if i > 199:
            break
        usernames.append(get_username(val))
        time.sleep(1)

    leader_df["steam_username"] = pd.Series(usernames)

    # Sinking to the database
    leader_df.to_sql("leader", con=engine, if_exists="replace", index=False)
    logging.info(
        f"{datetime.now(timezone.utc)}: Completed sink_leaders: Length of leaders: {len(leader_df)}")


def run_leaderboards_job() -> None:
    """
    Job definition for the every 15 minute definition that executes the drop/create top_score and leader tables
    :return:
    """
    # Getting a datetime to append to the current run (used for historical tracking)
    dt = datetime.now(timezone.utc)

    logging.info(f"{dt}: Started run_leadboards_job")

    # Creating a SQLAlchemy engine
    engine = create_engine(config.DB_URL)

    # Creating list to hold tuple of top_score entries and resetting on subsequent runs
    full_leaderboard = []

    logging.info(
        f"{datetime.now(timezone.utc)}: Beginning to loop over courses")

    for leaderboard_id, course_name in leaderboards_info.items():
        logging.info(f"{datetime.now(timezone.utc)}: Beginning loop for course: {course_name}")
        leaderboard_data = compute_leaderboard(leaderboard_id, course_name, dt)

        full_leaderboard.extend(leaderboard_data)
        logging.info(f"{datetime.now(timezone.utc)}: Finished loop for course: {course_name}")
        time.sleep(1)

    logging.info(
        f"{datetime.now(timezone.utc)}: Finished iterating over courses: Raw length of entries: {len(full_leaderboard)}"
    )

    sink_top_scores(full_leaderboard, engine)
    sink_leaders(full_leaderboard, dt, engine)

    engine.dispose()


def run_daily_job() -> None:
    """
    Job definition for the daily job that appends the top 200 course times per course in the top_score_history table
    :return:
    """
    # Getting a datetime to append to the current run (used for historical tracking)
    dt = datetime.now(timezone.utc)

    logging.info(f"{dt}: Started run_daily_job")

    # Creating a SQLAlchemy engine
    engine = create_engine(config.DB_URL)

    # Creating list to hold tuple of top_score entries and resetting on subsequent runs
    leaderboards = []

    logging.info(
        f"{datetime.now(timezone.utc)}: Beginning to loop over courses")

    for leaderboard_id, course_name in leaderboards_info.items():
        logging.info(f"{datetime.now(timezone.utc)}: Beginning loop for course: {course_name}")
        leaderboard_data = compute_leaderboard(leaderboard_id, course_name, dt)
        leaderboards.append(leaderboard_data)
        time.sleep(1)

    logging.info(
        f"{datetime.now(timezone.utc)}: Finished iterating over courses"
    )

    sink_top_200(leaderboards, engine)

    engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Since we want this running in a docker container, this is how we schedule updates (the underlying mechanism of
    # this is a cron job) .do accepts a callable that will be executed during schedule.run_pending()
    schedule.every(config.UPDATE_FREQUENCY_MIN).minutes.do(run_leaderboards_job)
    schedule.every(1).day.do(run_daily_job)

    # Infinite loop to sleep for 3 minutes, check if there is a pending job, and then execute if so
    while True:
        schedule.run_pending()
        time.sleep(180)
