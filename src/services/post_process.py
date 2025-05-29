# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import math
import pandas as pd


def extract_perspective(df, username, color):
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
    perspective['player_blunder'] = perspective[f'{color}_acpl']
    perspective['player_accuracy'] = perspective[f'{color}_accuracy']
    perspective['opponent_inaccuracy'] = perspective[f'{opp_color}_inaccuracy']
    perspective['opponent_mistake'] = perspective[f'{opp_color}_mistake']
    perspective['opponent_blunder'] = perspective[f'{opp_color}_acpl']
    perspective['opponent_accuracy'] = perspective[f'{opp_color}_accuracy']

    # Resultado
    perspective['result'] = perspective['winner'].map(
        lambda w: 'win' if w == color else 'loss' if w == opp_color else 'draw'
    )

    return perspective

def normalize_perspective(df, username):
    white = extract_perspective(df, username, 'white')
    black = extract_perspective(df, username, 'black')
    return pd.concat([white, black], ignore_index=True)

def post_process(df, username):
    """Process and clean raw game data from Lichess API.
    
    Args:
        df: Raw DataFrame from Lichess API
        username: Player's username to normalize perspective
        
    Returns:
        Processed DataFrame with standardized columns and formatting
    """
    # 1. Normalize player perspective
    df = normalize_perspective(df, username)

    # 2. Process datetime columns
    df = process_datetime_columns(df)

    # 3. Calculate derived metrics
    df = calculate_derived_metrics(df)

    # 4. Format and clean columns
    df = format_columns(df)

    # 5. Select and order final columns
    return select_final_columns(df)


def calculate_derived_metrics(df):
    """Calculate all derived metrics and new columns."""
    # Main calculations
    df['rating_difference'] = df['player_rating'] - df['opponent_rating']
    df['half_moves'] = df['moves'].apply(lambda x: len(x.split()))
    df['full_moves'] = df['half_moves'].apply(lambda x: math.ceil(x / 2))
    df['time_spent_playing'] = (df['last_move_at'] - df['created_at']).dt.total_seconds()

    def format_time_control(time_control, increment):
        """Helper function: Formats time control with special bullet cases."""
        if time_control == 30:  # ½ minute
            return f'½+{increment}'
        elif time_control == 15:  # ¼ minute
            return f'¼+{increment}'
        return f'{time_control//60}+{increment}'

    # Use the helper function for time control formatting
    df['time_control_with_increment'] = df.apply(
        lambda row: format_time_control(row['clock_time_control'], row['clock_increment']),
        axis=1
    )

    return df


def process_datetime_columns(df):
    """Process all datetime-related columns."""
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


def format_columns(df):
    """Apply final formatting to columns."""
    df['created_at'] = df['created_at'].dt.strftime('%d/%m/%y %H:%M')
    df.sort_values(by=['created_at'], ascending=False, inplace=True)
    return df


def select_final_columns(df):
    """Select and order the final columns to return."""
    column_order = [
        'id', 'player_color', 'player_name', 'opponent_name', 'result', 'status',
        'player_rating', 'opponent_rating', 'rating_difference',
        'variant', 'speed', 'perf', 'clock_time_control', 'clock_increment',
        'time_control_with_increment', 'source', 'tournament', 'division_middle', 'division_end',
        'created_at', 'last_move_at', 'time_spent_playing',
        'opening_eco', 'opening_name', 'opening_ply',
        'player_rating_diff', 'player_inaccuracy', 'player_mistake',
        'player_blunder', 'player_accuracy',
        'opponent_rating_diff', 'opponent_inaccuracy', 'opponent_mistake',
        'opponent_blunder', 'opponent_accuracy', 'half_moves', 'full_moves', 'moves'
    ]

    # Only keep columns that exist in the DataFrame
    existing_cols = [col for col in column_order if col in df.columns]
    return df[existing_cols]
