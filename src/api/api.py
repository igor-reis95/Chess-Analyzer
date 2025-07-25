import os
import logging
import src.api.chesscom_api as chesscom_api
import src.api.lichess_api as lichess_api

logger = logging.getLogger(__name__)

TOKEN = os.getenv("lichess_token")
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/x-ndjson"
}

def get_games(username, max_games, perf_type, platform):
    if platform == 'chess.com':
        return chesscom_api.get_games(username, max_games, perf_type)
    if platform == 'lichess.org':
        return lichess_api.get_games(username, max_games, perf_type)
    logger.warning(f"Unsupported platform '{platform}' in get_games()")
    return None

def collect_user_data(username, platform):
    if platform == 'chess.com':
        return chesscom_api.collect_user_data(username)
    if platform == 'lichess.org':
        return lichess_api.collect_user_data(username)
    logger.warning(f"Unsupported platform '{platform}' in collect_user_data()")
    return None
