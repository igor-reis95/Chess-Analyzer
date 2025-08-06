"""Chess.com API Client Module.

This module provides functions to fetch and process user data and game data from the Chess.com API.
It includes conversion utilities, PGN parsing, and game transformation logic.

Features:
- User profile and performance statistics
- Game history fetching and filtering
- PGN to structured data transformation
- ECO code to opening name mapping
"""

import io
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import chess
import chess.pgn
import pandas as pd
import requests

from src.api.chesscom_opening_resolver import get_opening_name

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def fetch_user_profile(username: str) -> Dict[str, Any]:
    """
    Fetch basic profile information of a Chess.com user.

    :param username: Username of the player.
    :return: Dictionary containing profile data.
    :raises requests.exceptions.RequestException: On HTTP error.
    """
    url = f"https://api.chess.com/pub/player/{username}"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.json()


def fetch_user_stats(username: str) -> Dict[str, Any]:
    """
    Fetch performance statistics of a Chess.com user.

    :param username: Username of the player.
    :return: Dictionary with stats data.
    :raises requests.exceptions.RequestException: On HTTP error.
    """
    url = f"https://api.chess.com/pub/player/{username}/stats"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.json()


def extract_rating_data(stats: Dict[str, Any], mode: str) -> Dict[str, int]:
    """
    Extract rating and game count from stats.

    :param stats: Stats dictionary.
    :param mode: Game mode (bullet, blitz, rapid).
    :return: Dictionary with rating and game count.
    """
    mode_data = stats.get(f"chess_{mode}", {})
    last = mode_data.get("last", {})
    record = mode_data.get("record", {})
    rating = last.get("rating", 0)
    games = sum(record.get(k, 0) for k in ["win", "draw", "loss"])
    return {"rating": rating, "games": games, "prog": 0}


def convert_to_milliseconds(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert timestamp columns from seconds to milliseconds.

    :param df: DataFrame with timestamp columns.
    :return: Modified DataFrame.
    """
    df["createdAt"] = df["createdAt"] * 1000
    df["lastMoveAt"] = df["lastMoveAt"] * 1000
    return df


def build_user_data(profile: Dict[str, Any], stats: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build normalized user profile data.

    :param profile: Raw profile data.
    :param stats: Raw stats data.
    :return: Normalized user dictionary.
    """
    return {
        "id": profile.get("player_id"),
        "username": profile.get("username"),
        "perfs": {
            mode: extract_rating_data(stats, mode)
            for mode in ["bullet", "blitz", "rapid"]
        } | {
            "classical": {"rating": 0, "games": 0, "prog": 0},
            "puzzle": {"rating": 0, "games": 0, "prog": 0},
        },
        "createdAt": profile.get("joined") * 1000,
        "seenAt": profile.get("last_online") * 1000,
        "playTime": {"total": None, "tv": None},
        "url": profile.get("url"),
    }


def collect_user_data(username: str) -> Dict[str, Any]:
    """
    Fetch and normalize all user data.

    :param username: Username of the player.
    :return: Dictionary with structured user data.
    """
    profile = fetch_user_profile(username)
    stats = fetch_user_stats(username)
    return build_user_data(profile, stats)


def fetch_games_chesscom(
    username: str,
    target_count: int,
    time_class: Optional[str]
) -> List[Dict[str, Any]]:
    """
    Fetch rated standard chess games from Chess.com archives.

    :param username: Username of the player.
    :param target_count: Max number of games to retrieve.
    :param time_class: Game type filter (blitz, rapid, etc.).
    :return: List of raw game dictionaries.
    """
    archives_url = f"https://api.chess.com/pub/player/{username}/games/archives"
    resp = requests.get(archives_url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    archives = resp.json().get("archives", [])

    selected = []
    for url in reversed(archives):
        if len(selected) >= target_count:
            break
        r = requests.get(url, headers=HEADERS, timeout=60)
        r.raise_for_status()
        for game in r.json().get("games", []):
            if time_class and game.get("time_class") != time_class:
                continue
            if not game.get("rated", False) or game.get("rules") != "chess":
                continue
            selected.append(game)
            if len(selected) >= target_count:
                break
    return selected


def translate_result(game_status: Optional[str]) -> Optional[str]:
    """
    Translate raw PGN result into simplified label.

    :param game_status: Raw PGN result string.
    :return: Normalized result label.
    """
    if not game_status:
        return None
    status = game_status.lower()
    if "resignation" in status or "abandoned" in status:
        return "resignation"
    if "on time" in status:
        return "outoftime"
    if "checkmate" in status:
        return "mate"
    if "drawn" in status:
        return "draw"
    return None


def pgn_str_to_json(pgn_str: str) -> Dict[str, Any]:
    """
    Convert PGN string to structured dictionary.

    :param pgn_str: PGN string.
    :return: Dictionary with metadata and moves.
    """
    pgn = io.StringIO(pgn_str)
    game = chess.pgn.read_game(pgn)
    metadata = dict(game.headers)
    moves = []
    node = game
    while not node.is_end():
        next_node = node.variation(0)
        moves.append(node.board().san(next_node.move))
        node = next_node
    return {
        "metadata": metadata,
        "moves": moves,
        "result": metadata.get("Result", "")
    }


def datetime_str_to_unix(date_str: str, time_str: str) -> Optional[int]:
    """
    Convert PGN date/time strings to Unix timestamp in milliseconds.

    :param date_str: Date string in format %Y.%m.%d.
    :param time_str: Time string in format %H:%M:%S.
    :return: Timestamp in milliseconds or None.
    """
    try:
        dt_string = f"{date_str} {time_str}"
        dt_object = datetime.strptime(dt_string, "%Y.%m.%d %H:%M:%S")
        return int(dt_object.timestamp()) * 1000
    except Exception as e: # pylint: disable=broad-exception-caught
        logger.warning("Failed to parse timestamp: %s", e)
        return None


def convert_moves(move_string: str) -> str:
    """
    Convert UCI moves to SAN format.

    :param move_string: Space-separated UCI moves.
    :return: Space-separated SAN moves.
    """
    if not move_string:
        return ""

    board = chess.Board()
    san_moves = []

    for uci_move in move_string.split():
        try:
            move = chess.Move.from_uci(uci_move)
            san_moves.append(board.san(move))
            board.push(move)
        except ValueError:
            san_moves.append(uci_move)

    return " ".join(san_moves)


def eco_to_opening(eco_code: str) -> str:
    """
    Get opening name from ECO code.

    :param eco_code: ECO code (e.g., C20).
    :return: Opening name.
    """
    eco_df = pd.read_csv("data/opening_ecos.csv")
    eco_mapping = eco_df.drop_duplicates("eco").set_index("eco")["name"].to_dict()
    return eco_mapping.get(eco_code, "Unknown Opening")


def transform_game(game: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Transform raw game JSON into normalized format.

    :param game: Raw game data.
    :return: Normalized game dictionary or None if invalid.
    """
    try:
        pgn_data = pgn_str_to_json(game["pgn"])

        game_id = game["url"].split("/")[-1]
        created_at = datetime_str_to_unix(
            pgn_data["metadata"].get("UTCDate", ""),
            pgn_data["metadata"].get("UTCTime", "")
        )
        last_move_at = datetime_str_to_unix(
            pgn_data["metadata"].get("EndDate", ""),
            pgn_data["metadata"].get("EndTime", "")
        )

        game_status = translate_result(pgn_data["metadata"].get("Termination", ""))
        raw_moves = " ".join(pgn_data["moves"])
        moves = convert_moves(raw_moves)

        result = pgn_data["metadata"].get("Result", "")
        game_winner = "white" if result == "1-0" else "black" if result == "0-1" else None

        opening_eco = pgn_data["metadata"].get("ECO", "")
        opening_name = get_opening_name(opening_eco, moves)

        time_control = pgn_data["metadata"].get("TimeControl", "")
        clock_initial = time_control.split("+")[0] if "+" in time_control else time_control
        clock_increment = time_control.split("+")[1] if "+" in time_control else 0

        return {
            "id": game_id,
            "rated": game.get("rated"),
            "variant": "standard",
            "speed": game.get("time_class"),
            "time_control_with_increment": game.get("time_control"),
            "perf": game.get("time_class"),
            "createdAt": created_at,
            "lastMoveAt": last_move_at,
            "status": game_status,
            "source": "chess.com",
            "players": {
                "white": {
                    "user": {
                        "name": pgn_data["metadata"].get("White"),
                        "id": game["white"]["@id"].split("player/")[-1]
                    },
                    "rating": pgn_data["metadata"].get("WhiteElo"),
                    "ratingDiff": None
                },
                "black": {
                    "user": {
                        "name": pgn_data["metadata"].get("Black"),
                        "id": game["black"]["@id"].split("player/")[-1]
                    },
                    "rating": pgn_data["metadata"].get("BlackElo"),
                    "ratingDiff": None
                }
            },
            "fullId": None,
            "winner": game_winner,
            "opening": {
                "eco": opening_eco,
                "name": opening_name,
                "ply": None
            },
            "moves": moves,
            "clocks": [],
            "clock": {
                "initial": clock_initial,
                "increment": clock_increment,
                "TotalTime": None
            },
            "division": {
                "middle": None,
                "end": None
            }
        }
    except Exception as e: # pylint: disable=broad-exception-caught
        logger.error("Error transforming game: %s", e)
        return None


def get_games(username: str, target_count: int, time_class: Optional[str]) -> List[Dict[str, Any]]:
    """
    Fetch and normalize recent games from Chess.com.

    :param username: Player username.
    :param target_count: Max number of games to retrieve.
    :param time_class: Filter by game speed.
    :return: List of normalized games.
    """
    raw_games = fetch_games_chesscom(username, target_count, time_class)
    return [game for game in (transform_game(g) for g in raw_games) if game is not None]
