import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import pandas as pd
import schedule
from sqlalchemy import create_engine

from config import config
from constants import leaderboards_info
from utilities.steam_helpers import get_leaderboard, get_username


def run_leaderboards_job() -> None:
    """

    :return:
    """
    # Getting a datetime to append to the current run (used for historical tracking)
    dt = datetime.now(timezone.utc)

    # Creating a SQLAlchemy engine
    engine = create_engine(config.DB_URL)

    # Creating list to hold tuple of top_score entries and resetting on subsequent runs
    full_leaderboard = []

    for leaderboard_id, course_name in leaderboards_info.items():
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
            if i > 199:
                break

            # Getting values out of xml tree nodes
            steam_id = int(entry.find("steamid").text)
            rank = int(entry.find("rank").text)
            time = int(entry.find("score").text)
            points = points_total - rank

            full_leaderboard.append((rank, steam_id, time, points, course_name, dt))

    # Creating the pandas dataframe we'll use to manipulate and sink data
    scores_df = pd.DataFrame(data=full_leaderboard,
                             columns=["rank", "steam_id", "time", "points", "course", "timestamp"])
    # Sinking the data to the database- this is an append-only table
    scores_df.to_sql("top_score", con=engine, if_exists="append", index=False)

    # ToDo nail down actual strategy for rankings- current is completely arbitrary
    # Calculating all scores for users: sum(points) per course where points is 200 or len(entries) - leaderboard
    # position
    top_df = scores_df.groupby("steam_id")["points"].sum().sort_values(ascending=False)

    # Grabbing the top 50 entries from the top_df and pushing the index (steam_id) back to being a column
    top_50 = top_df.head(50)
    top_50 = top_50.reset_index()

    # Appending the run datetime to all rows under the new column timestamp
    top_50["timestamp"] = dt

    # Iterating over all username entries in the top 50 and resolving them to their steam usernames
    usernames = []
    for val in top_50["steam_id"]:
        usernames.append(get_username(val))

    top_50["steam_username"] = usernames

    # Sinking to the database
    top_50.to_sql("leader", con=engine, if_exists="append", index=False)

    # Disposing of our engine as the configurable sleep time warrants this not being a long-lasting connection
    engine.dispose()


if __name__ == "__main__":
    # Since we want this running in a docker container, this is how we schedule updates (the underlying mechanism of
    # this is a cron job) .do accepts a callable that will be executed during schedule.run_pending()
    schedule.every(config.UPDATE_FREQUENCY_MIN).minutes.do(run_leaderboards_job)

    # Infinite loop to sleep for 3 minutes, check if there is a pending job, and then execute if so
    while True:
        schedule.run_pending()
        time.sleep(30)
