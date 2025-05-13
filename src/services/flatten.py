import json
import os
import pandas as pd

def _safe_get(d, *keys):
    """Safely get nested values in a dict."""
    for key in keys:
        if isinstance(d, dict) and key in d:
            d = d[key]
        else:
            return None
    return d

def extract_player_features(game, color):
    """Extract features related to the given player's color ('white' or 'black')"""
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
    """Extract clock-related features"""
    return {
        "clock_time_control": _safe_get(game, "clock", "initial"),
        "clock_increment": _safe_get(game, "clock", "increment"),
        "clock_total_time": _safe_get(game, "clock", "total_time")
    }

def extract_division_features(game):
    """Extract division of game stage information"""
    return {
        "division_middle": _safe_get(game, "division", "middle"),
        "division_end": _safe_get(game, "division", "end")
    }

def extract_opening_features(game):
    """Extract opening information"""
    return {
        "opening_eco": _safe_get(game, "opening", "eco"),
        "opening_name": _safe_get(game, "opening", "name"),
        "opening_ply": _safe_get(game, "opening", "ply")
    }

def extract_flattened_features(game):
    return {
        **extract_player_features(game, "white"),
        **extract_player_features(game, "black"),
        **extract_clock_features(game),
        **extract_division_features(game),
        **extract_opening_features(game)
    }

def flatten_game_data(games_list):
    """Return a DataFrame with both original and flattened game data."""
    flattened_rows = [extract_flattened_features(game) for game in games_list]
    df_flattened = pd.DataFrame(flattened_rows)
    df_original = pd.DataFrame(games_list)

    # Drop nested fields to avoid duplication
    cols_to_drop = [col for col in ['players', 'clock', 'division', 'opening'] if col in df_original.columns]
    df_original_cleaned = df_original.drop(columns=cols_to_drop)

    df_combined = pd.concat([df_original_cleaned.reset_index(drop=True), df_flattened.reset_index(drop=True)], axis=1)
    return df_combined