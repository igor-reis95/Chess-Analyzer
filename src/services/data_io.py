"""Utilities for saving chess game data to disk.

This module provides functions to persist raw and processed game data:
- `save_games_to_json`: saves a list of raw games to a JSON file.
- `save_df_to_csv`: saves a DataFrame of processed games to a CSV file.

Files are stored under configurable folders (default: data/raw and data/processed).
"""

import os
import json
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("local_database_url")

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

def save_processed_game_data(df, table = 'games_processed_data'):
    """Save processed data from games into a postgreSQL database managed by Render"""
    # Create SQLAlchemy engine
    try:
        engine = create_engine(DATABASE_URL)
    except:
        print("Database not found. Running in no-db mode.")

    # Get existing IDs from database
    existing_ids = pd.read_sql("SELECT id FROM games_processed_data", engine)['id'].tolist()

    # Filter DataFrame to exclude existing IDs
    df_to_insert = df[~df['id'].isin(existing_ids)]

    # Write to SQL table in the public schema
    df_to_insert.to_sql(
        name=table,
        con=engine,
        schema='public',
        if_exists='replace',
        index=False
    )
