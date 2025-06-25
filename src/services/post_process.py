"""
Data processing utilities for normalizing and enriching chess game data.

This module provides functions to transform raw game data from Lichess API into
a cleaned, player-centric format with additional derived metrics and formatted timestamps.

Key functions:
- extract_perspective: extract data from the viewpoint of a given player color.
- normalize_perspective: combine white and black perspectives into one DataFrame.
- post_process: main pipeline to clean and enrich raw data.
- calculate_derived_metrics: compute new columns like rating difference, move counts,
time control format.
- process_datetime_columns: convert and localize timestamps.
- format_columns: apply final formatting and sorting.
- select_final_columns: reorder and filter the final columns for output.
"""

from typing import Optional, Tuple
import math
import pandas as pd



def extract_perspective(df: pd.DataFrame, username: str, color: str) -> pd.DataFrame:
    """
    Extracts game data from the perspective of a specific player color.

    Args:
        df: Raw game DataFrame.
        username: The player's username.
        color: 'white' or 'black' specifying the player color perspective.

    Returns:
        A DataFrame filtered and transformed to represent the player's perspective,
        including player/opponent names, ratings, accuracy, and result.
    """
    opp_color = 'white' if color == 'black' else 'black'

    perspective = df.copy()
    perspective = perspective[df[f'{color}_name'].str.lower() == username.lower()]

    perspective['player_name'] = perspective[f'{color}_name']
    perspective['opponent_name'] = perspective[f'{opp_color}_name']
    perspective['player_color'] = color

    # Ratings
    perspective['player_rating'] = perspective[f'{color}_rating']
    perspective['player_rating_diff'] = perspective[f'{color}_ratingDiff']
    perspective['opponent_rating'] = perspective[f'{opp_color}_rating']
    perspective['opponent_rating_diff'] = perspective[f'{opp_color}_ratingDiff']

    # Accuracy and errors
    perspective['player_inaccuracy'] = perspective[f'{color}_inaccuracy']
    perspective['player_mistake'] = perspective[f'{color}_mistake']
    perspective['player_blunder'] = perspective[f'{color}_blunder']
    perspective['player_accuracy'] = perspective[f'{color}_accuracy']
    perspective['opponent_inaccuracy'] = perspective[f'{opp_color}_inaccuracy']
    perspective['opponent_mistake'] = perspective[f'{opp_color}_mistake']
    perspective['opponent_blunder'] = perspective[f'{opp_color}_blunder']
    perspective['opponent_accuracy'] = perspective[f'{opp_color}_accuracy']

    # Time related data
    perspective['player_final_clock'] = perspective[f'{color}_final_clock']
    perspective['opponent_final_clock'] = perspective[f'{opp_color}_final_clock']
    perspective['player_avg_time_per_move'] = perspective[f'{color}_avg_time']
    perspective['opponent_avg_time_per_move'] = perspective[f'{opp_color}_avg_time']

    # Result mapping: 'win', 'loss', or 'draw' from player's perspective
    perspective['result'] = perspective['winner'].map(
        lambda w: 'win' if w == color else 'loss' if w == opp_color else 'draw'
    )

    return perspective


def normalize_perspective(df: pd.DataFrame, username: str) -> pd.DataFrame:
    """
    Normalize the DataFrame to have all games from the player's perspective.

    Args:
        df: Raw game DataFrame.
        username: Player's username.

    Returns:
        Concatenated DataFrame with player perspective for both white and black games.
    """
    white = extract_perspective(df, username, 'white')
    black = extract_perspective(df, username, 'black')
    return pd.concat([white, black], ignore_index=True)

def calculate_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate derived metrics such as rating difference, move counts, time spent playing,
    and formats the time control with increment.

    Args:
        df: DataFrame after perspective normalization and datetime processing.

    Returns:
        DataFrame with new columns added.
    """
    # Rating difference between player and opponent
    df['rating_difference'] = df['player_rating'] - df['opponent_rating']

    # Count half-moves and full moves in the game
    df['half_moves'] = df['moves'].apply(lambda x: len(x.split()))
    df['full_moves'] = df['half_moves'].apply(lambda x: math.ceil(x / 2))

    # Calculate total time spent playing in seconds
    df['time_spent_playing'] = (df['last_move_at'] - df['created_at']).dt.total_seconds()

    def format_time_control(time_control: int, increment: int) -> str:
        """
        Format the time control string with special cases for bullet variants.

        Args:
            time_control: Base time control in seconds.
            increment: Increment in seconds per move.

        Returns:
            Formatted time control string.
        """
        if time_control == 30:  # ½ minute
            return f'½+{increment}'
        elif time_control == 15:  # ¼ minute
            return f'¼+{increment}'
        return f'{time_control // 60}+{increment}'

    df['time_control_with_increment'] = df.apply(
        lambda row: format_time_control(row['clock_time_control'], row['clock_increment']),
        axis=1
    )

    return df


def process_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert millisecond timestamps to localized datetime objects.

    Args:
        df: DataFrame with raw timestamp columns ('createdAt', 'lastMoveAt') in ms.

    Returns:
        DataFrame with new datetime columns 'created_at' and 'last_move_at' localized to 
        America/Sao_Paulo timezone.
    """
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

    return df


def format_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Format datetime columns and sort DataFrame.

    Args:
        df: DataFrame with datetime columns.

    Returns:
        DataFrame with formatted 'created_at' string column and sorted by creation date descending.
    """
    df.sort_values(by=['created_at'], ascending=False, inplace=True)
    df['created_at'] = df['created_at'].dt.strftime('%d/%m/%y %H:%M')
    return df

def normalize_opening_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the names of the openings so it only shows the main opening, without variation
    
    - 'normalized_opening_name': the main opening name without sub-variation details.
    
    Args:
        df : A DataFrame containing at least 'opening_name' and 'opening_eco' columns.
    
    Returns:
        The original DataFrame with two new columns added.
    """
    def get_main_opening(opening_name):
        if isinstance(opening_name, str):
            return opening_name.split(":")[0].strip()
        return None

    df['normalized_opening_name'] = df['opening_name'].apply(get_main_opening)
    return df

def select_final_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and order the final columns for presentation or further use.

    Args:
        df: DataFrame with all processed columns.

    Returns:
        DataFrame filtered to only the selected columns in the specified order.
    """
    df.rename(columns={'id': 'match_id'}, inplace = True) # Necessary for no conflict with db id
    column_order = [
        'match_id', 'player_color', 'player_name', 'opponent_name', 'result', 'status',
        'player_rating', 'opponent_rating', 'rating_difference',
        'variant', 'speed', 'perf', 'clock_time_control', 'clock_increment',
        'time_control_with_increment', 'source', 'tournament', 'division_middle', 'opening_eval',
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

def format_play_time(x):
    """
    Converts a timedelta to a human-readable string format.

    Parameters:
        x (pd.Timedelta): Duration to format.

    Returns:
        str: Formatted string like '12 hours and 34 minutes'.
    """
    total_seconds = int(x.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    return f"{hours} hours and {minutes} minutes"

def get_final_clocks(df: pd.DataFrame) -> pd.DataFrame:
    """Adds white_final_clock and black_final_clock columns to the DataFrame.
    
    Args:
        df: DataFrame containing a 'clocks' column with list of clock times
        
    Returns:
        Modified DataFrame with two new columns
    """
    def _extract_clocks(clock_array: list) -> Tuple[Optional[float], Optional[float]]:
        """Helper function to extract the final clock times for white and black."""
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
        # TODO: Transform from centiseconds to mm:ss.ms

        return white, black

    # Apply the helper function to each row
    clocks = df['clocks'].apply(_extract_clocks)

    # Assign results to new columns
    df['white_final_clock'] = clocks.str[0]
    df['black_final_clock'] = clocks.str[1]

    return df

def get_avg_time_per_move(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate average time per move for white and black players.
    
    Args:
        df: DataFrame containing a 'clocks' column with list of clock times
        
    Returns:
        DataFrame with two new columns: white_avg_time, black_avg_time
    """
    def _calculate_avg_times(clock_array: list) -> Tuple[Optional[float], Optional[float]]:
        """Helper function to calculate average time per move for each player."""
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

        # Calculate time differences and averages
        if len(white_times) > 1:
            white_total = white_times[0] - white_times[-1]
            white_avg = white_total / (len(white_times) - 1)
            white_avg = round(white_avg / 100, 2)

        if len(black_times) > 1:
            black_total = black_times[0] - black_times[-1]
            black_avg = black_total / (len(black_times) - 1)
            black_avg = round(black_avg / 100, 2)

        return white_avg, black_avg

    # Apply the helper function
    avg_times = df['clocks'].apply(_calculate_avg_times)

    # Add new columns
    df['white_avg_time'] = avg_times.str[0]
    df['black_avg_time'] = avg_times.str[1]

    return df

def process_user_data(data, perfs_to_include=None):
    """
    Processes raw user data from Lichess API and returns a structured DataFrame.

    Parameters:
        data (dict): JSON-like response from the Lichess user API.
        perfs_to_include (list, optional): List of performance types to include
                                           (e.g., ['bullet', 'blitz']). Defaults to common types.

    Returns:
        pd.DataFrame: Processed data with user profile and performance stats.
    """
    if perfs_to_include is None:
        perfs_to_include = ['bullet', 'blitz', 'rapid', 'classical', 'puzzle']

    # Basic user info
    user_data = {
        'username': data.get('username'),
        'created_at': data.get('createdAt'),
        'last_seen': data.get('seenAt'),
        'play_time': data.get('playTime', {}).get('total'),  # in seconds
        'url': data.get('url')
    }

    # Extract performance-related stats
    for perf in perfs_to_include:
        perf_data = data.get('perfs', {}).get(perf, {})
        user_data[f'{perf}_games'] = perf_data.get('games')
        user_data[f'{perf}_rating'] = perf_data.get('rating')
        user_data[f'{perf}_prog'] = perf_data.get('prog')

    # Convert to DataFrame (single-row)
    user_df = pd.DataFrame([user_data])

    # Convert timestamps to datetime/timedelta
    user_df['created_at_datetime'] = pd.to_datetime(user_df['created_at'], unit='ms')
    user_df['last_seen_datetime'] = pd.to_datetime(user_df['last_seen'], unit='ms')

    # Add timestamp of report creation
    user_df['report_created_at'] = pd.Timestamp.now()

    # Format datetime fields to readable strings
    user_df["created_at"] = user_df["created_at_datetime"].dt.strftime("%d/%m/%y")
    user_df["last_seen"] = user_df["last_seen_datetime"].dt.strftime("%d/%m/%y")

    # Format play time as 'X hours and Y minutes'
    user_df['play_time'] = pd.to_timedelta(user_df['play_time'], unit='s').apply(format_play_time)

    return user_df

def post_process(df: pd.DataFrame, username: str) -> pd.DataFrame:
    """
    Full post-processing pipeline to clean and enrich raw chess game data.

    Args:
        df: Raw DataFrame from Lichess API.
        username: Player's username to normalize perspective.

    Returns:
        Processed DataFrame with standardized columns, formatting, and derived metrics.
    """
    # 1. Retrieve final clock time for both players
    df = get_final_clocks(df)

    # 2. Calculate average time per move and save it to the dataframe
    df = get_avg_time_per_move(df)

    # 3. Normalize player perspective
    df = normalize_perspective(df, username)

    # 4. Process datetime columns (convert and localize timestamps)
    df = process_datetime_columns(df)

    # 5. Calculate derived metrics (rating diff, moves, time control)
    df = calculate_derived_metrics(df)

    # 6. Format opening columns with a normalized name and also a link to the eco
    df = normalize_opening_name(df)

    # 7. Apply final formatting and sorting
    df = format_columns(df)

    # 8. Select and order final columns
    return select_final_columns(df)
