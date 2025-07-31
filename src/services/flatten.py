"""Game Data Processing Module

This module provides utilities for processing and flattening chess game data from
Lichess API responses. It extracts nested game information into a flat structure
suitable for analysis and DataFrame operations.

Key Features:
- Safely extracts nested dictionary data with graceful handling of missing keys
- Processes player-specific features (ratings, accuracy metrics) for both colors
- Extracts clock, game stage, and opening information
- Combines all features into a flattened pandas DataFrame
"""

from typing import Any, Dict, List, Optional
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def _safe_get(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    """
    Safely retrieve nested dictionary values with graceful failure.

    Args:
        d (Dict[str, Any]): The dictionary to search through.
        *keys (str): Variable length argument of keys to traverse.

    Returns:
        Optional[Any]: The value at the nested key location or None if any key is missing.
    """
    for key in keys:
        if isinstance(d, dict) and key in d:
            d = d[key]
        else:
            logger.debug("Missing key '%s' in dictionary.", key)
            return None
    return d

def extract_player_features(game: Dict[str, Any], color: str) -> Dict[str, Any]:
    """
    Extract player-specific features from game data.

    Args:
        game (Dict[str, Any]): Raw game data dictionary from Lichess API.
        color (str): Either 'white' or 'black'.

    Returns:
        Dict[str, Any]: Extracted features with keys prefixed by color.
    """
    logger.debug("Extracting player features for %s.", color)
    return {
        f"{color}_name": _safe_get(game, "players", color, "user", "name"),
        f"{color}_rating": _safe_get(game, "players", color, "rating"),
        f"{color}_ratingDiff": _safe_get(game, "players", color, "ratingDiff"),
        f"{color}_inaccuracy": _safe_get(game, "players", color, "analysis", "inaccuracy"),
        f"{color}_mistake": _safe_get(game, "players", color, "analysis", "mistake"),
        f"{color}_blunder": _safe_get(game, "players", color, "analysis", "blunder"),
        f"{color}_acpl": _safe_get(game, "players", color, "analysis", "acpl"),
        f"{color}_accuracy": _safe_get(game, "players", color, "analysis", "accuracy"),
    }

def extract_clock_features(game: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract time control and clock information.

    Args:
        game (Dict[str, Any]): Game data.

    Returns:
        Dict[str, Any]: Clock-related features.
    """
    logger.debug("Extracting clock features.")
    return {
        "clock_time_control": _safe_get(game, "clock", "initial"),
        "clock_increment": _safe_get(game, "clock", "increment"),
        "clock_total_time": _safe_get(game, "clock", "total_time"),
        "clock_time_per_move": _safe_get(game, "clocks"),
    }

def extract_division_features(game: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract division transition move numbers.

    Args:
        game (Dict[str, Any]): Game data.

    Returns:
        Dict[str, Any]: Game stage transitions.
    """
    logger.debug("Extracting division features.")
    return {
        "division_middle": _safe_get(game, "division", "middle"),
        "division_end": _safe_get(game, "division", "end"),
    }

def extract_opening_features(game: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract chess opening metadata.

    Args:
        game (Dict[str, Any]): Game data.

    Returns:
        Dict[str, Any]: Opening-related information.
    """
    logger.debug("Extracting opening features.")
    return {
        "opening_eco": _safe_get(game, "opening", "eco"),
        "opening_name": _safe_get(game, "opening", "name"),
        "opening_ply": _safe_get(game, "opening", "ply"),
    }

def extract_flattened_features(game: Dict[str, Any]) -> Dict[str, Any]:
    """
    Combine all extracted features into a single flat dictionary.

    Args:
        game (Dict[str, Any]): Game data.

    Returns:
        Dict[str, Any]: Flattened representation.
    """
    logger.debug("Extracting all features from game.")
    return {
        **extract_player_features(game, "white"),
        **extract_player_features(game, "black"),
        **extract_clock_features(game),
        **extract_division_features(game),
        **extract_opening_features(game),
    }

def flatten_game_data(games_list: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert list of nested game dictionaries into a flat pandas DataFrame.

    Args:
        games_list (List[Dict[str, Any]]): List of raw games from Lichess API.

    Returns:
        pd.DataFrame: DataFrame with flattened and original game metadata.
    """
    logger.info("Flattening %d games.", len(games_list))
    flattened_rows = [extract_flattened_features(game) for game in games_list]
    df_flattened = pd.DataFrame(flattened_rows)
    df_original = pd.DataFrame(games_list)

    cols_to_drop = [
        col for col in ['players', 'clock', 'division', 'opening']
        if col in df_original.columns
    ]
    df_original_cleaned = df_original.drop(columns=cols_to_drop)

    logger.debug("Merging flattened features with original top-level data.")
    return pd.concat([
        df_original_cleaned.reset_index(drop=True),
        df_flattened.reset_index(drop=True)
    ], axis=1)
