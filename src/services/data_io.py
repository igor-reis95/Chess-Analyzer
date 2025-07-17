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
from sqlalchemy.exc import SQLAlchemyError, OperationalError


DATABASE_URL = os.getenv("database_url")

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

def save_processed_game_data(df, table='games_processed_data'):
    """Save processed data from games into a PostgreSQL database"""
    try:
        # Create engine
        engine = create_engine(DATABASE_URL)

        # Get a list of repeating ids to not insert duplicate ids
        existing_ids = pd.read_sql("SELECT match_id FROM games_processed_data", engine)['match_id'].tolist()

        # Filter DataFrame
        df_to_insert = df[~df['match_id'].isin(existing_ids)]

        # Save filtered data
        df_to_insert.to_sql(
            name=table,
            con=engine,
            schema='public',
            if_exists='append',
            index=False
        )
    except OperationalError as oe:
        print(f"Database connection error: {oe}")
    except SQLAlchemyError as se:
        print(f"SQLAlchemy error: {se}")
    except ValueError as ve:
        print(f"Value error while processing DataFrame: {ve}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def save_processed_user_data(df):
    """Save processed user data into the database"""
    # Create engine and delete the row being inserted
    engine = create_engine(DATABASE_URL)

    # Append the updated rows
    df.to_sql(
        name='user_processed_data',
        con=engine,
        schema='public',
        if_exists='append',
        index=False
    )

def get_user_data(username) -> pd.Series:
        """
        Retrieve the row of user data with the highest ID for the current username
        from the database using SQLAlchemy.

        Returns:
            pd.Series: A row from the database representing the latest user data.
        """

        query = """
            SELECT * FROM user_processed_data
            WHERE username = %s
            ORDER BY id DESC
            LIMIT 1;
        """

        try:
            # Create SQLAlchemy engine
            engine = create_engine(DATABASE_URL)
            
            # Use with engine.connect() for proper connection management
            with engine.connect() as conn:
                df = pd.read_sql(query, conn, params=(username,))
                
        except Exception as e:
            raise RuntimeError(f"Database error: {e}")
        finally:
            engine.dispose()  # Clean up engine resources

        if df.empty:
            raise ValueError(f"No data found for username: {username}")

        return df.iloc[0].to_dict()