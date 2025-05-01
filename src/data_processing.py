import json
import os
import pandas as pd

def save_games_to_json(games_list, username, folder="data"):
    """Save the raw games list as a JSON file."""
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, f"{username}_games.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(games_list, f, indent=2, ensure_ascii=False)
    print(f"Saved games to {filepath}")

def save_df_to_csv(df, username, folder="data"):
    """Save the processed games DataFrame as a CSV file."""
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, f"{username}_games.csv")
    df.to_csv(filepath, index=False)
    print(f"Saved processed games to {filepath}")

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
    """Extract tournament division information"""
    return {
        "division_middle": _safe_get(game, "division", "middle"),
        "division_end": _safe_get(game, "division", "end")
    }

def flatten_game_data(games_list):
    """Convert list of game dicts into a flat, usable DataFrame."""
    extracted = []
    for game in games_list:
        row = {
            **extract_player_features(game, "white"),
            **extract_player_features(game, "black"),
            **extract_clock_features(game),
            **extract_division_features(game)
        }
        extracted.append(row)
    return pd.DataFrame(extracted)
