"""Data processing utilities for normalizing and enriching chess game data.

This module provides functions to transform raw game data from chess platforms (Lichess/Chess.com)
into a cleaned, player-centric format with derived metrics and formatted timestamps.

Functions:
    extract_perspective: Extract data from viewpoint of given player color.
    normalize_perspective: Combine white/black perspectives into one DataFrame.
    post_process: Main pipeline to clean and enrich raw data.
    calculate_derived_metrics: Compute rating difference, move counts, time control.
    process_datetime_columns: Convert and localize timestamps.
    format_columns: Apply final formatting and sorting.
    select_final_columns: Reorder and filter final output columns.
    normalize_opening_name: Simplify opening names by removing variations.
    format_play_time: Convert timedelta to human-readable string.
    get_final_clocks: Extract final clock times for both players.
    get_avg_time_per_move: Calculate average move times.
    process_user_data: Process raw user profile data from API.
"""

import logging
import math
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def extract_perspective(df: pd.DataFrame, username: str, color: str) -> pd.DataFrame:
    """Extract game data from perspective of specific player color.

    Transforms raw game data to show metrics from either white or black player's viewpoint,
    including ratings, accuracy metrics, and game results.

    Args:
        df: Raw game DataFrame from Lichess/Chess.com API.
        username: Player's username to filter by.
        color: Either 'white' or 'black' specifying perspective.

    Returns:
        DataFrame filtered and transformed to represent player's perspective.

    Raises:
        ValueError: If invalid color is provided (not 'white' or 'black').
    """
    if color not in ('white', 'black'):
        logger.error('Invalid color %s provided to extract_perspective', color)
        raise ValueError("Color must be either 'white' or 'black'")

    logger.debug('Extracting %s perspective for user %s', color, username)
    opp_color = 'white' if color == 'black' else 'black'

    perspective = df.copy()
    perspective = perspective[df[f'{color}_name'].str.lower() == username.lower()]

    # Player/opponent metadata
    perspective['player_name'] = perspective[f'{color}_name']
    perspective['opponent_name'] = perspective[f'{opp_color}_name']
    perspective['player_color'] = color

    # Ratings
    perspective['player_rating'] = perspective[f'{color}_rating'].astype(int)
    perspective['player_rating_diff'] = perspective[f'{color}_ratingDiff']
    perspective['opponent_rating'] = perspective[f'{opp_color}_rating'].astype(int)
    perspective['opponent_rating_diff'] = perspective[f'{opp_color}_ratingDiff']

    # Accuracy metrics
    accuracy_metrics = ['inaccuracy', 'mistake', 'blunder', 'accuracy']
    for metric in accuracy_metrics:
        perspective[f'player_{metric}'] = perspective[f'{color}_{metric}']
        perspective[f'opponent_{metric}'] = perspective[f'{opp_color}_{metric}']

    # Time data
    time_cols = {
        'player_final_clock': f'{color}_final_clock',
        'opponent_final_clock': f'{opp_color}_final_clock',
        'player_avg_time_per_move': f'{color}_avg_time',
        'opponent_avg_time_per_move': f'{opp_color}_avg_time'
    }
    for new_col, old_col in time_cols.items():
        perspective[new_col] = perspective[old_col]

    # Result from player's perspective
    perspective['result'] = perspective['winner'].map(
        lambda w: 'win' if w == color else 'loss' if w == opp_color else 'draw'
    )

    logger.info('Extracted %s games from %s perspective', len(perspective), color)
    return perspective


def normalize_perspective(df: pd.DataFrame, username: str) -> pd.DataFrame:
    """Normalize DataFrame to have all games from player's perspective.

    Combines white and black perspective DataFrames into single unified view.

    Args:
        df: Raw game DataFrame from API.
        username: Player's username to normalize around.

    Returns:
        Concatenated DataFrame with unified player perspective columns.
    """
    logger.debug('Normalizing perspective for user %s', username)
    white = extract_perspective(df, username, 'white')
    black = extract_perspective(df, username, 'black')
    combined = pd.concat([white, black], ignore_index=True)

    logger.info('Combined %s white and %s black perspectives', len(white), len(black))
    return combined


def calculate_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate derived metrics from game data.

    Computes:
        - Rating difference between players
        - Move counts (half-moves and full moves)
        - Time spent playing
        - Formatted time control strings

    Args:
        df: DataFrame after perspective normalization.

    Returns:
        DataFrame with additional derived metric columns.
    """
    logger.debug('Calculating derived metrics')
    # Rating difference
    df['rating_difference'] = df['player_rating'] - df['opponent_rating']

    # Move counts
    df['half_moves'] = df['moves'].apply(lambda x: len(x.split()))
    df['full_moves'] = df['half_moves'].apply(lambda x: math.ceil(x / 2))

    # Time spent playing
    df['time_spent_playing'] = (
        df['last_move_at'] - df['created_at']
    ).dt.total_seconds()

    # Time control formatting
    def format_time_control(time_control: int, increment: int) -> str:
        """Format time control string with special cases for bullet variants."""
        if time_control == 30:  # ½ minute
            return f'½+{increment}'
        if time_control == 15:  # ¼ minute
            return f'¼+{increment}'
        return f'{time_control // 60}+{increment}'

    if df['source'][0] != 'chess.com':
        df['clock_time_control'] = pd.to_numeric(
            df['clock_time_control'],
            errors='coerce'
        )
        df['clock_increment'] = pd.to_numeric(df['clock_increment'], errors='coerce')
        df['time_control_with_increment'] = df.apply(
            lambda row: format_time_control(
                row['clock_time_control'],
                row['clock_increment']
            ),
            axis=1
        )
    else:
        df['source'] = 'lichess.org'

    logger.debug('Added %s derived metrics', len(df))
    return df


def process_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert millisecond timestamps to localized datetime objects.

    Args:
        df: DataFrame with raw timestamp columns ('createdAt', 'lastMoveAt') in ms.

    Returns:
        DataFrame with new datetime columns localized to America/Sao_Paulo timezone.
    """
    logger.debug('Processing datetime columns')
    df['created_at'] = (
        pd.to_datetime(df['createdAt'], unit='ms')
        .dt.tz_localize('UTC')
        .dt.tz_convert('America/Sao_Paulo')
    )

    df['last_move_at'] = (
        pd.to_datetime(df['lastMoveAt'], unit='ms')
        .dt.tz_localize('UTC')
        .dt.tz_convert('America/Sao_Paulo')
    )
    logger.debug('Converted datetime columns to datetime with timezone')

    return df


def format_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Format datetime columns and sort DataFrame.

    Args:
        df: DataFrame with datetime columns.

    Returns:
        DataFrame with formatted datetime strings and sorted by creation date.
    """
    logger.debug('Formatting columns')
    df.sort_values(by=['created_at'], ascending=False, inplace=True)
    df['created_at'] = df['created_at'].dt.strftime('%d/%m/%y %H:%M')
    return df


def normalize_opening_name(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize opening names by removing variation details.

    Creates new column 'normalized_opening_name' containing only main opening name.

    Args:
        df: DataFrame containing 'opening_name' column.

    Returns:
        DataFrame with additional normalized opening name column.
    """
    logger.debug('Normalizing opening names')

    def get_main_opening(opening_name: str) -> Optional[str]:
        """Extract main opening name before colon."""
        if isinstance(opening_name, str):
            return opening_name.split(":")[0].strip()
        return None

    df['normalized_opening_name'] = df['opening_name'].apply(get_main_opening)
    return df


def select_final_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Select and order final columns for output.

    Args:
        df: Fully processed DataFrame.

    Returns:
        DataFrame filtered to selected columns in specified order.
    """
    logger.debug('Selecting final columns')
    df.rename(columns={'id': 'match_id'}, inplace=True)

    column_order = [
        'match_id', 'player_color', 'player_name', 'opponent_name', 'result', 'status',
        'player_rating', 'opponent_rating', 'rating_difference',
        'variant', 'speed', 'perf', 'clock_time_control', 'clock_increment',
        'time_control_with_increment', 'source', 'division_middle', 'opening_eval',
        'division_end', 'middlegame_eval', 'created_at', 'last_move_at', 'time_spent_playing',
        'opening_eco', 'opening_name', 'normalized_opening_name', 'opening_ply',
        'player_rating_diff', 'player_final_clock', 'player_avg_time_per_move',
        'player_inaccuracy', 'player_mistake', 'player_blunder', 'player_accuracy',
        'opponent_rating_diff', 'opponent_final_clock', 'opponent_avg_time_per_move',
        'opponent_inaccuracy', 'opponent_mistake', 'opponent_blunder', 'opponent_accuracy',
        'half_moves', 'full_moves', 'moves', 'clocks'
    ]

    existing_cols = [col for col in column_order if col in df.columns]
    return df[existing_cols]


def format_play_time(x: pd.Timedelta) -> str:
    """Convert timedelta to human-readable string.

    Args:
        x: Duration to format.

    Returns:
        Formatted string like '12 hours and 34 minutes'.
    """
    total_seconds = int(x.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    return f"{hours} hours and {minutes} minutes"


def get_final_clocks(df: pd.DataFrame) -> pd.DataFrame:
    """Extract final clock times for both players.

    Args:
        df: DataFrame containing 'clocks' column with list of clock times.

    Returns:
        DataFrame with added 'white_final_clock' and 'black_final_clock' columns.
    """
    logger.debug('Extracting final clock times')

    def _extract_clocks(clock_array: list) -> Tuple[Optional[float], Optional[float]]:
        """Helper to extract final clock times for white and black."""
        if not isinstance(clock_array, list) or len(clock_array) < 2:
            return None, None

        last_two = clock_array[-2:]

        # Determine whose turn it was last
        if len(clock_array) % 2 == 1:  # White just moved
            white = last_two[1] if len(last_two) > 1 else None
            black = last_two[0]
        else:  # Black just moved
            white = last_two[0]
            black = last_two[1] if len(last_two) > 1 else None

        return white, black

    clocks = df['clocks'].apply(_extract_clocks)
    df['white_final_clock'] = clocks.str[0]
    df['black_final_clock'] = clocks.str[1]

    return df


def get_avg_time_per_move(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate average time per move for both players.

    Args:
        df: DataFrame containing 'clocks' column with move times.

    Returns:
        DataFrame with added 'white_avg_time' and 'black_avg_time' columns.
    """
    logger.debug('Calculating average time per move')

    def _calculate_avg_times(clock_array: list) -> Tuple[Optional[float], Optional[float]]:
        """Helper to calculate average move times."""
        if not isinstance(clock_array, list) or len(clock_array) < 2:
            return None, None

        white_times = []
        black_times = []

        # Split times by player
        for i, time in enumerate(clock_array):
            if i % 2 == 0:  # White's moves are at even indices (0, 2, 4...)
                white_times.append(time)
            else:  # Black's moves are at odd indices (1, 3, 5...)
                black_times.append(time)

        # Initialize defaults
        white_avg = None
        black_avg = None

        # Calculate white average (need at least 2 moves)
        if len(white_times) >= 2:
            try:
                white_total = white_times[0] - white_times[-1]
                white_avg = round((white_total / (len(white_times) - 1)) / 100, 2)
            except (IndexError, ZeroDivisionError):
                pass

        # Calculate black average (need at least 2 moves)
        if len(black_times) >= 2:
            try:
                black_total = black_times[0] - black_times[-1]
                black_avg = round((black_total / (len(black_times) - 1)) / 100, 2)
            except (IndexError, ZeroDivisionError):
                pass

        return white_avg, black_avg

    # Apply the helper function
    avg_times = df['clocks'].apply(_calculate_avg_times)

    # Add new columns
    df['white_avg_time'] = avg_times.str[0]
    df['black_avg_time'] = avg_times.str[1]

    return df


def process_user_data(
    data: Dict[str, Any],
    platform: str,
    perfs_to_include: Optional[List[str]] = None
) -> pd.DataFrame:
    """Process raw user data from chess platform API.

    Args:
        data: JSON-like response from user API.
        platform: 'lichess' or 'chess.com'.
        perfs_to_include: List of performance types to include.
                         Defaults to ['bullet', 'blitz', 'rapid', 'classical', 'puzzle'].

    Returns:
        Processed DataFrame with user profile and performance stats.
    """
    logger.debug('Processing user data for platform %s', platform)
    perfs_to_include = perfs_to_include or ['bullet', 'blitz', 'rapid', 'classical', 'puzzle']

    # Basic user info
    user_data = {
        'username': data.get('username'),
        'created_at': data.get('createdAt'),
        'last_seen': data.get('seenAt'),
        'play_time': data.get('playTime', {}).get('total'),
        'url': data.get('url'),
        'platform': platform
    }

    # Performance stats
    for perf in perfs_to_include:
        perf_data = data.get('perfs', {}).get(perf, {})
        user_data.update({
            f'{perf}_games': perf_data.get('games'),
            f'{perf}_rating': perf_data.get('rating'),
            f'{perf}_prog': perf_data.get('prog')
        })

    user_df = pd.DataFrame([user_data])

    # Datetime conversions
    user_df['created_at_datetime'] = pd.to_datetime(user_df['created_at'], unit='ms')
    user_df['last_seen_datetime'] = pd.to_datetime(user_df['last_seen'], unit='ms')
    user_df['report_created_at'] = pd.Timestamp.now()

    # Formatting
    user_df["created_at"] = user_df["created_at_datetime"].dt.strftime("%d/%m/%y")
    user_df["last_seen"] = user_df["last_seen_datetime"].dt.strftime("%d/%m/%y")

    def safe_format_play_time(seconds: Optional[float]) -> Optional[str]:
        """Safely format play time handling None values."""
        if pd.isna(seconds):
            return None
        try:
            return format_play_time(pd.to_timedelta(seconds, unit='s'))
        except Exception as e: # pylint: disable=broad-exception-caught
            logger.warning('Failed to format play time: %s', e)
            return None

    user_df['play_time'] = user_df['play_time'].apply(safe_format_play_time)

    logger.info('Processed user data for %s', user_data['username'])
    return user_df


def post_process(df: pd.DataFrame, username: str) -> pd.DataFrame:
    """Full post-processing pipeline for chess game data.

    Processing steps:
        1. Extract final clock times
        2. Calculate average move times
        3. Normalize player perspective
        4. Process datetime columns
        5. Calculate derived metrics
        6. Normalize opening names
        7. Format columns
        8. Select final columns

    Args:
        df: Raw game DataFrame from API.
        username: Player username to normalize perspective around.

    Returns:
        Fully processed DataFrame ready for analysis.
    """
    logger.info('Starting post-processing for user %s', username)

    processing_steps = [
        ('Extracting final clocks', get_final_clocks),
        ('Calculating move times', get_avg_time_per_move),
        ('Normalizing perspective', lambda d: normalize_perspective(d, username)),
        ('Processing datetimes', process_datetime_columns),
        ('Calculating metrics', calculate_derived_metrics),
        ('Normalizing openings', normalize_opening_name),
        ('Formatting columns', format_columns),
        ('Selecting columns', select_final_columns)
    ]

    for step_name, step_func in processing_steps:
        logger.debug('Processing step: %s', step_name)
        df = step_func(df)

    logger.info('Completed processing %s games', len(df))
    return df
