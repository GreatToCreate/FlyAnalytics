import xml.etree.ElementTree as ET

import requests

from constants import STEAM_APPID


def get_leaderboard(leaderboard_id: id) -> bytes:
    """
    Used to get leaderboard data for the top 200 entries on a particular Steam Fly Dangerous leaderboard
    :param leaderboard_id: int representing the leaderboard id for a Fly Dangerous Steam leaderboard
    :return: bytes containing the response data from the steam leaderboards api
    """
    # Currently we only care about the top 200 entries, but this should probably be turned into a configurable option
    # as users outside the top 200 likely want to know their global rankings. Has DB size (table top_score)
    # considerations as it's an append-only run-time-stamped table for historical tracking.
    url = f"https://steamcommunity.com/stats/{STEAM_APPID}/leaderboards/{leaderboard_id}?xml=1&start=1&end=200"
    resp = requests.get(url)

    return resp.content


def get_username(user_id: int) -> str:
    """
    Used to get a steam user's username given their steam id
    :param user_id: int steam user id
    :return: str steam user username
    """
    url = f"https://steamcommunity.com/profiles/{user_id}/?xml=1"
    resp = requests.get(url)

    xml_str = resp.content
    tree = ET.fromstring(xml_str)

    return tree.find("steamID").text
