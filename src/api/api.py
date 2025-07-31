"""Main API dispatcher for collecting games and user data from chess.com or lichess.org."""

import os
import logging
from typing import Optional, Union, Dict, Any, List

import src.api.chesscom_api as chesscom_api
import src.api.lichess_api as lichess_api

logger = logging.getLogger(__name__)

TOKEN = os.getenv("lichess_token")
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/x-ndjson"
}

def get_games(
    username: str,
    max_games: int,
    perf_type: Optional[str],
    platform: str
) -> Optional[Union[List[Dict[str, Any]], Dict[str, Any]]]:
    """
    Retrieve games for a given user from the selected platform.

    Args:
        username (str): The username of the player.
        max_games (int): Maximum number of games to retrieve.
        perf_type (Optional[str]): Type of performance (e.g., blitz, rapid).
        platform (str): Either 'chess.com' or 'lichess.org'.

    Returns:
        Optional[Union[List[Dict[str, Any]], Dict[str, Any]]]: Game data, or None
                                                               if platform is unsupported.
    """
    if platform == 'chess.com':
        return chesscom_api.get_games(username, max_games, perf_type)
    if platform == 'lichess.org':
        return lichess_api.get_games(username, max_games, perf_type)

    logger.warning("Unsupported platform '%s' in get_games()", platform)
    return None

def collect_user_data(
    username: str,
    platform: str
) -> Optional[Dict[str, Any]]:
    """
    Collect user profile data from the selected platform.

    Args:
        username (str): The username of the player.
        platform (str): Either 'chess.com' or 'lichess.org'.

    Returns:
        Optional[Dict[str, Any]]: User data, or None if platform is unsupported.
    """
    if platform == 'chess.com':
        return chesscom_api.collect_user_data(username)
    if platform == 'lichess.org':
        return lichess_api.collect_user_data(username)

    logger.warning("Unsupported platform '%s' in collect_user_data()", platform)
    return None
