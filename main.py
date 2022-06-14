import os
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from sqlalchemy import create_engine
from constants import STEAM_APPID, leaderboards_info
import schedule
import time


def get_leaderboard(leaderboard_id: id) -> str:
    url = f"https://steamcommunity.com/stats/{STEAM_APPID}/leaderboards/{leaderboard_id}?xml=1&start=1&end=200"
    resp = requests.get(url)

    return resp.content


def get_username(user_id: int) -> str:
    url = f"https://steamcommunity.com/profiles/{user_id}/?xml=1"
    resp = requests.get(url)

    xml_str = resp.content
    tree = ET.fromstring(xml_str)

    return tree.find("steamID").text


def run_analytics_job() -> None:
    dt = datetime.now(timezone.utc)

    db_string = os.getenv("DB_URL")
    engine = create_engine(db_string)

    full_leaderboard = []

    for leaderboard_id, course_name in leaderboards_info.items():
        xml_str = get_leaderboard(leaderboard_id)
        tree = ET.fromstring(xml_str)

        entries = tree.find("entries")
        entry_count = len(entries)

        if entry_count < 200:
            points_total = entry_count
        else:
            points_total = 200

        for i, entry in enumerate(entries):
            if i > 199:
                break

            # Getting values of xml tree nodes
            steam_id = int(entry.find("steamid").text)
            rank = int(entry.find("rank").text)
            time = int(entry.find("score").text)
            points = points_total - rank

            full_leaderboard.append((rank, steam_id, time, points, course_name, dt))

    scores_df = pd.DataFrame(data=full_leaderboard,
                             columns=["rank", "steam_id", "time", "points", "course", "timestamp"])
    scores_df.to_sql("top_score", con=engine, if_exists="append", index=False)

    top_df = scores_df.groupby("steam_id")["points"].sum().sort_values(ascending=False)

    top_50 = top_df.head(50)
    top_50 = top_50.reset_index()

    top_50["timestamp"] = dt

    usernames = []
    for val in top_50["steam_id"]:
        usernames.append(get_username(val))

    top_50["steam_username"] = usernames
    top_50.to_sql("leader", con=engine, if_exists="append", index=False)

    engine.dispose()


if __name__ == "__main__":
    schedule.every(15).minutes.do(run_analytics_job)

    while True:
        schedule.run_pending()
        time.sleep(10)
