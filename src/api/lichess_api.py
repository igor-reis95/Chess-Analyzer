"""Lichess API Client Module.

This module provides functionality to fetch chess game data from the Lichess API.
It handles authentication, request construction, response streaming, and error handling.

Key Features:
- Secure token handling via environment variables
- Configurable game fetching with performance and color filters
- Streaming NDJSON response processing
- Comprehensive logging for requests, responses, and errors
"""

import json
import logging
import os
from typing import Any, Dict, List

import requests

logger = logging.getLogger(__name__)

TOKEN = os.getenv("lichess_token")
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/x-ndjson",
}


def get_games(
    username: str,
    max_games: int,
    perf_type: str,
) -> List[Dict[str, Any]]:
    """
    Fetch chess games from Lichess API for a specific user with given filters.

    :param username: Lichess username to fetch games for.
    :param max_games: Maximum number of games to retrieve.
    :param perf_type: Time control type. Options:
        - Specific types: 'bullet', 'blitz', 'rapid', 'classical'
        - 'all': Includes all types
    :return: A list of games as dictionaries.
    :raises requests.exceptions.RequestException: On HTTP failure.
    :raises json.JSONDecodeError: On invalid JSON in response.
    """
    if perf_type == "all":
        perf_type = "bullet,blitz,rapid,classical"

    params = {
        "max": max_games,
        "perfType": perf_type,
        "color": None,
        "rated": True,
        "accuracy": True,
        "division": True,
        "opening": True,
        "clocks": True,
        "evals": True,
    }

    url = f"https://lichess.org/api/games/user/{username}"
    logger.info("Starting request to Lichess API for user %s with params %s", username, params)

    try:
        response = requests.get(
            url,
            headers=HEADERS,
            params=params,
            stream=True,
            timeout=40,
        )
        response.raise_for_status()
        logger.info("Received response from Lichess API with status code %s", response.status_code)
    except requests.exceptions.RequestException as e:
        logger.error("Request to Lichess API failed: %s", e)
        raise

    games_list: List[Dict[str, Any]] = []
    for line_number, line in enumerate(response.iter_lines(), start=1):
        if line:
            try:
                game = json.loads(line.decode("utf-8"))
                games_list.append(game)
            except json.JSONDecodeError as e:
                logger.warning("JSON decode error on line %s: %s. Skipping line.", line_number, e)

    logger.info("Successfully fetched %s games for user %s", len(games_list), username)
    return games_list


def collect_user_data(username: str) -> Dict[str, Any]:
    """
    Fetch user profile data from the Lichess API.

    :param username: The Lichess username to retrieve data for.
    :return: A dictionary containing user profile data.
    :raises requests.exceptions.HTTPError: If the request fails.
    """
    url = f"https://lichess.org/api/user/{username}"
    try:
        response = requests.get(
            url,
            headers=HEADERS,
            stream=True,
            timeout=10,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error("Failed to fetch user data for %s: %s", username, e)
        raise

    return response.json()
