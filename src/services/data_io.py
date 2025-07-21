"""Utilities for saving chess game data to disk.

This module provides functions to persist raw and processed game data:
- `save_games_to_json`: saves a list of raw games to a JSON file.
- `save_df_to_csv`: saves a DataFrame of processed games to a CSV file.

Files are stored under configurable folders (default: data/raw and data/processed).
"""

import os
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

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

def save_processed_game_data(conn, df, table='games_processed_data'):
    """Append all rows in df to the table using fast psycopg2 bulk insert"""

    try:
        with conn.cursor() as cur:
            if df.empty:
                print("DataFrame is empty. Nothing to insert.")
                return

            # Prepare data for insertion
            df = df.astype(object).where(pd.notnull(df), None)
            values = [tuple(row) for row in df.to_numpy()]
            columns = ', '.join(df.columns)
            insert_sql = f"INSERT INTO {table} ({columns}) VALUES %s"
            
            execute_values(cur, insert_sql, values)
            conn.commit()
            print(f"Inserted {len(values)} rows into {table}.")

    except psycopg2.OperationalError as oe:
        print(f"Database connection error: {oe}")
    except psycopg2.DatabaseError as de:
        conn.rollback()
        print(f"Database error: {de} at save_processed_game_data")
    except Exception as e:
        print(f"Unexpected error: {e}")

def save_processed_user_data(conn, df):
    """Fast insert using psycopg2 + execute_values"""

    # Convert DataFrame to list of tuples
    values = [tuple(row) for row in df.to_numpy()]
    columns = ', '.join(df.columns)

    # Build SQL statement (parameterized)
    insert_sql = f"INSERT INTO users_processed_data ({columns}) VALUES %s"

    # Connect and insert
    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values)
            conn.commit()

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"psycopg2 insert error: {e} at save_processed_user_data")

def get_user_data(conn, username) -> dict:
    query = """
        SELECT * FROM users_processed_data
        WHERE username = %s
        ORDER BY id DESC
        LIMIT 1;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(query, (username,))
            row = cur.fetchone()

            if not row:
                raise ValueError(f"No data found for username: {username}")

            # Convert to dict using cursor description
            colnames = [desc[0] for desc in cur.description]
            return dict(zip(colnames, row))

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Database error: {e} at get_user_data")
    
def save_report_data(conn, username, number_of_games, time_control, slug) -> dict:
    try:
        with conn.cursor() as cur:
            insert_sql = """
                INSERT INTO reports (username, number_of_games, time_control, public_id)
                VALUES (%s, %s, %s, %s)
                RETURNING id;
            """
            cur.execute(insert_sql, (username, number_of_games, time_control, slug))
            report_id = cur.fetchone()[0]
            return report_id

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Database error: {e} at save_report_data")
    
def get_report_by_slug(conn, slug: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, username, number_of_games, time_control, public_id
            FROM reports
            WHERE public_id = %s
        """, (slug,))
        row = cur.fetchone()

    if row:
        return {
            "id": row[0],
            "username": row[1],
            "number_of_games": row[2],
            "time_control": row[3],
            "public_id": row[4]
        }
    return None

def get_games_by_report_id(conn, report_id: int):
    query = "SELECT * FROM games_processed_data WHERE reports_id = %s"
    return pd.read_sql(query, conn, params=(report_id,))

def get_user_by_report_id(conn, report_id: int) -> dict:
    query = "SELECT * FROM users_processed_data WHERE reports_id = %s"
    df = pd.read_sql(query, conn, params=(report_id,))
    
    if df.empty:
        return {}

    return df.iloc[0].to_dict()
