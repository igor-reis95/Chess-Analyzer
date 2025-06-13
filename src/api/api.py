"""Lichess API Client Module

This module provides functionality to fetch chess game data from the Lichess API.
It handles authentication, request construction, response streaming, and error handling.

Key Features:
- Secure token handling via environment variables
- Configurable game fetching with performance and color filters
- Streaming NDJSON response processing
- Comprehensive logging for requests, responses, and errors
"""

import os
import json
import logging
import requests

logger = logging.getLogger(__name__)

TOKEN = os.getenv("lichess_token")
headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/x-ndjson"
}

def get_games(username, max_games, perf_type, color):
    """
    Fetch chess games from Lichess API for a specific user with given filters.

    Args:
        username (str): Lichess username to fetch games for.
        max_games (int): Maximum number of games to retrieve (API limit applies).
        perf_type (str): Game time control type. Options:
            - Specific type: 'ultraBullet', 'bullet', 'blitz', 'rapid', 'classical'
            - 'all': Includes all time controls.
        color (str): Player color to filter. Options:
            - 'white': Only games where player was white.
            - 'black': Only games where player was black.
            - 'Both': Includes games for both colors (no filter).

    Returns:
        list: A list of dictionaries, each representing a game with complete metadata.

    Raises:
        requests.exceptions.RequestException: For HTTP request failures.
        json.JSONDecodeError: If response contains invalid JSON data.
    """
    if perf_type == 'all':
        perf_type = 'ultraBullet,bullet,blitz,rapid,classical'

    if color == 'Both':
        color = None

    params = {
        "max": max_games,
        "perfType": perf_type,
        "color": color,
        "rated": True,
        "accuracy": True,
        "division": True,
        "opening": True,
        "clocks": True,
        "evals": True
    }

    url = f"https://lichess.org/api/games/user/{username}"

    logger.info("Starting request to Lichess API for user %s with params %s", username, params)

    try:
        response = requests.get(
            url,
            headers=headers,
            params=params,
            stream=True,
            timeout=30
        )
        response.raise_for_status()
        logger.info("Received response from Lichess API with status code %s", response.status_code)
    except requests.exceptions.RequestException as e:
        logger.error("Request to Lichess API failed: %s", e)
        raise

    games_list = []
    for line_number, line in enumerate(response.iter_lines(), start=1):
        if line:
            try:
                game = json.loads(line.decode("utf-8"))
                games_list.append(game)
            except json.JSONDecodeError as e:
                logger.warning("JSON decode error on line %s: %s. Skipping line.", line_number, e)
                continue

    logger.info("Successfully fetched %s games for user %s", len(games_list), username)
    return games_list

def collect_user_data(username):
    """
    Fetches user profile data from the Lichess API.

    Parameters:
        username (str): The Lichess username to retrieve data for.

    Returns:
        dict: A dictionary containing user profile data from the Lichess API.

    Raises:
        requests.exceptions.HTTPError: If the API request returns an unsuccessful status code.
    """
    url = f"https://lichess.org/api/user/{username}"
    response = requests.get(
        url,
        headers=headers,
        stream=True,
        timeout=10
    )
    response.raise_for_status()
    return response.json()
