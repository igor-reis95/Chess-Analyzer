"""
Utilities for saving and retrieving chess game data to/from disk and database.

Provides functions to persist raw and processed game data, user data, reports, 
and to fetch data by keys such as username or report ID.

Data is stored in configurable folders and PostgreSQL tables.
"""

import logging
import os
from typing import Optional, Union, Dict, Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("database_url")


def save_processed_game_data(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame,
    table: str = "games_processed_data"
) -> None:
    """
    Append all rows in the DataFrame to the specified table using fast bulk insert.

    Args:
        conn: psycopg2 connection object.
        df: DataFrame with processed game data.
        table: Target database table name.

    Returns:
        None

    Raises:
        psycopg2.DatabaseError on failure.
    """
    try:
        with conn.cursor() as cur:
            if df.empty:
                logger.info("DataFrame is empty. Nothing to insert.")
                return

            # Replace NaNs with None for psycopg2 compatibility
            df_clean = df.astype(object).where(pd.notnull(df), None)
            values = [tuple(row) for row in df_clean.to_numpy()]
            columns = ', '.join(df.columns)
            insert_sql = f"INSERT INTO {table} ({columns}) VALUES %s"

            execute_values(cur, insert_sql, values)
            conn.commit()
            logger.info("Inserted %d rows into %s.", len(values), table)

    except psycopg2.OperationalError as oe:
        logger.error("Database connection error: %s", oe)
        raise
    except psycopg2.DatabaseError as de:
        conn.rollback()
        logger.error("Database error at save_processed_game_data: %s", de)
        raise
    except Exception as e:
        logger.error("Unexpected error at save_processed_game_data: %s", e)
        raise


def save_processed_user_data(
    conn: psycopg2.extensions.connection,
    df: pd.DataFrame
) -> None:
    """
    Insert user processed data using psycopg2 execute_values for bulk insert.

    Args:
        conn: psycopg2 connection object.
        df: DataFrame containing processed user data.

    Raises:
        RuntimeError on insertion failure.
    """
    values = [tuple(row) for row in df.to_numpy()]
    columns = ', '.join(df.columns)
    insert_sql = f"INSERT INTO users_processed_data ({columns}) VALUES %s"

    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values)
            conn.commit()
            logger.info("Inserted %d user rows.", len(values))

    except Exception as e:
        conn.rollback()
        logger.error("psycopg2 insert error at save_processed_user_data: %s", e)
        raise RuntimeError(f"psycopg2 insert error: {e} at save_processed_user_data") from e


def get_user_data(
    conn: psycopg2.extensions.connection,
    username: str
) -> Dict[str, Any]:
    """
    Fetch the latest processed user data for a given username.

    Args:
        conn: psycopg2 connection object.
        username: User's username string.

    Returns:
        Dictionary with user data columns and values.

    Raises:
        RuntimeError if query fails or no data found.
    """
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

            colnames = [desc[0] for desc in cur.description]
            return dict(zip(colnames, row))

    except Exception as e:
        conn.rollback()
        logger.error("Database error at get_user_data: %s", e)
        raise RuntimeError(f"Database error: {e} at get_user_data") from e


def save_report_data(
    conn: psycopg2.extensions.connection,
    username: str,
    number_of_games: int,
    time_control: str,
    platform: str,
    slug: str
) -> int:
    """
    Insert a new report entry and return its generated ID.

    Args:
        conn: psycopg2 connection object.
        username: User's username.
        number_of_games: Number of games in the report.
        time_control: Time control string.
        platform: Platform name (e.g., "lichess").
        slug: Public ID slug for the report.

    Returns:
        The newly inserted report's ID.

    Raises:
        RuntimeError on failure.
    """
    insert_sql = """
        INSERT INTO reports (username, number_of_games, time_control, public_id, platform)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(insert_sql, (username, number_of_games, time_control, slug, platform))
            report_id = cur.fetchone()[0]
            conn.commit()
            logger.info("Inserted report for user %s with id %d", username, report_id)
            return report_id

    except Exception as e:
        conn.rollback()
        logger.error("Database error at save_report_data: %s", e)
        raise RuntimeError(f"Database error: {e} at save_report_data") from e

def save_report_execution_time(
    conn: psycopg2.extensions.connection,
    report_id: int,
    execution_time: float
) -> None:
    """
    Update the execution time for a given report.

    Args:
        conn: psycopg2 connection object.
        report_id: Report ID to update.
        execution_time: Execution time in seconds.

    Raises:
        RuntimeError on failure.
    """
    update_sql = """
        UPDATE reports
        SET execution_time = %s
        WHERE id = %s;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(update_sql, (execution_time, report_id))
            conn.commit()
            logger.info("Updated execution time for report id %d", report_id)

    except Exception as e:
        conn.rollback()
        logger.error("Database error at save_report_execution_time: %s", e)
        raise RuntimeError("psycopg2 insert error at save_processed_user_data") from e


def get_report_by_slug(
    conn: psycopg2.extensions.connection,
    slug: str
) -> Optional[Dict[str, Union[int, str]]]:
    """
    Retrieve report metadata by public slug.

    Args:
        conn: psycopg2 connection object.
        slug: Public slug identifier for the report.

    Returns:
        Dictionary of report info if found, else None.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, username, number_of_games, time_control, public_id, platform
            FROM reports
            WHERE public_id = %s
            """,
            (slug,)
        )
        row = cur.fetchone()

    if row:
        logger.debug("Report found for slug %s", slug)
        return {
            "id": row[0],
            "username": row[1],
            "number_of_games": row[2],
            "time_control": row[3],
            "public_id": row[4],
            "platform": row[5]
        }
    logger.info("No report found for slug %s", slug)
    return None


def get_games_by_report_id(
    conn: psycopg2.extensions.connection,
    report_id: int
) -> pd.DataFrame:
    """
    Retrieve all games associated with a report ID as a DataFrame.

    Args:
        conn: psycopg2 connection object.
        report_id: Report ID to fetch games for.

    Returns:
        DataFrame containing all games for the report.
    """
    query = "SELECT * FROM games_processed_data WHERE report_id = %s"
    with conn.cursor() as cur:
        cur.execute(query, (report_id,))
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

    logger.debug("Fetched %d games for report id %d", len(rows), report_id)
    return pd.DataFrame(rows, columns=colnames)


def get_user_by_report_id(
    conn: psycopg2.extensions.connection,
    report_id: int
) -> Dict[str, Any]:
    """
    Retrieve user data associated with a given report ID.

    Args:
        conn: psycopg2 connection object.
        report_id: Report ID.

    Returns:
        Dictionary of user data columns and values, or empty dict if none found.
    """
    query = "SELECT * FROM users_processed_data WHERE report_id = %s"
    with conn.cursor() as cur:
        cur.execute(query, (report_id,))
        row = cur.fetchone()
        if not row:
            logger.info("No user data found for report id %d", report_id)
            return {}

        colnames = [desc[0] for desc in cur.description]

    logger.debug("User data fetched for report id %d", report_id)
    return dict(zip(colnames, row))
