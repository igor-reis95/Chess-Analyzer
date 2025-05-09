import os
import json
import pandas as pd

def save_games_to_json(games_list, username, folder="data/raw"):
    """Save the raw games list as a JSON file."""
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, f"{username}_games.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(games_list, f, indent=2, ensure_ascii=False)
    print(f"Saved games to {filepath}")

def save_df_to_csv(df, username, folder="data/processed"):
    """Save the processed games DataFrame as a CSV file."""
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, f"{username}_games.csv")
    df.to_csv(filepath, index=False)
    print(f"Saved processed games to {filepath}")