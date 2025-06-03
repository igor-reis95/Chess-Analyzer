"""Game Data Processing Module

This module provides utilities for processing and flattening chess game data from
Lichess API responses.
It extracts nested game information into a flat structure suitable for analysis and
DataFrame operations.

Key Features:
- Safely extracts nested dictionary data with graceful handling of missing keys
- Processes player-specific features (ratings, accuracy metrics) for both colors
- Extracts clock, game stage, and opening information
- Combines all features into a flattened pandas DataFrame
"""

import pandas as pd

def _safe_get(d, *keys):
    """Safely retrieve nested dictionary values with graceful failure.
    
    Args:
        d (dict): The dictionary to search through
        *keys: Variable length argument of keys to traverse
        
    Returns:
        The value at the nested key location or None if any key is missing
    """
    for key in keys:
        if isinstance(d, dict) and key in d:
            d = d[key]
        else:
            return None
    return d

def extract_player_features(game, color):
    """Extract player-specific features from game data.
    
    Args:
        game (dict): Raw game data dictionary from Lichess API
        color (str): Either 'white' or 'black' to specify which player
        
    Returns:
        dict: Dictionary containing all extracted features for the specified player,
              with keys prefixed by the player color (e.g., 'white_rating')
    """
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

def extract_clock_features(game):
    """Extract time control and clock information from game data.
    
    Args:
        game (dict): Raw game data dictionary
        
    Returns:
        dict: Dictionary containing:
            - clock_time_control: Initial clock time in seconds
            - clock_increment: Clock increment per move
            - clock_total_time: Total time available (initial + increment * estimated moves)
    """
    return {
        "clock_time_control": _safe_get(game, "clock", "initial"),
        "clock_increment": _safe_get(game, "clock", "increment"),
        "clock_total_time": _safe_get(game, "clock", "total_time")
    }

def extract_division_features(game):
    """Extract game stage division information.
    
    Args:
        game (dict): Raw game data
        
    Returns:
        dict: Dictionary containing move numbers where:
            - division_middle: Game transitions to middlegame
            - division_end: Game transitions to endgame
    """
    return {
        "division_middle": _safe_get(game, "division", "middle"),
        "division_end": _safe_get(game, "division", "end")
    }

def extract_opening_features(game):
    """Extract chess opening information.
    
    Args:
        game (dict): Raw game data
        
    Returns:
        dict: Dictionary containing:
            - opening_eco: Encyclopedia of Chess Openings code
            - opening_name: Common name of the opening
            - opening_ply: Number of moves in the opening phase
    """
    return {
        "opening_eco": _safe_get(game, "opening", "eco"),
        "opening_name": _safe_get(game, "opening", "name"),
        "opening_ply": _safe_get(game, "opening", "ply")
    }

def extract_flattened_features(game):
    """Combine all feature extractors into a single flat dictionary.
    
    Args:
        game (dict): Raw game data
        
    Returns:
        dict: Combined dictionary with all features from:
            - Both players (white and black)
            - Clock information
            - Game stage divisions
            - Opening information
    """
    return {
        **extract_player_features(game, "white"),
        **extract_player_features(game, "black"),
        **extract_clock_features(game),
        **extract_division_features(game),
        **extract_opening_features(game)
    }

def flatten_game_data(games_list):
    """Convert list of nested game dictionaries into a flat pandas DataFrame.
    
    Args:
        games_list (list): List of raw game dictionaries from Lichess API
        
    Returns:
        pd.DataFrame: Combined DataFrame containing:
            - Original top-level game fields (with nested columns removed)
            - All flattened features from extract_flattened_features()
            
    Note:
        Drops original nested columns ('players', 'clock', 'division', 'opening')
        to avoid duplication with the flattened versions.
    """
    flattened_rows = [extract_flattened_features(game) for game in games_list]
    df_flattened = pd.DataFrame(flattened_rows)
    df_original = pd.DataFrame(games_list)

    # Drop nested fields to avoid duplication
    cols_to_drop = [
        col for col in ['players', 'clock', 'division', 'opening']
        if col in df_original.columns
    ]
    df_original_cleaned = df_original.drop(columns=cols_to_drop)

    df_combined = pd.concat([
            df_original_cleaned.reset_index(drop=True),
            df_flattened.reset_index(drop=True)
        ], axis=1)
    return df_combined
