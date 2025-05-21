# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
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
    df = normalize_perspective(df, username)
    df['rating_difference'] = df['player_rating'] - df['opponent_rating']
    df['created_at'] = pd.to_datetime(df['createdAt'], unit='ms')
    df['last_move_at'] = pd.to_datetime(df['lastMoveAt'], unit='ms')
    df['time_spent_playing'] = (df['last_move_at'] - df['created_at']).dt.total_seconds()
    df.sort_values(by=['created_at'], ascending = False, inplace = True)

    keep_cols = [
        'id', 'player_color', 'player_name', 'opponent_name', 'result', 'status',
        'player_rating', 'opponent_rating', 'rating_difference',
        'variant', 'speed', 'perf', 'clock_time_control', 'clock_increment',
        'source', 'tournament', 'division_middle', 'division_end',
        'created_at', 'last_move_at', 'time_spent_playing',
        'opening_eco', 'opening_name', 'opening_ply',
        'player_rating_diff', 'player_inaccuracy', 'player_mistake',
        'player_blunder', 'player_accuracy',
        'opponent_rating_diff', 'opponent_inaccuracy', 'opponent_mistake',
        'opponent_blunder', 'opponent_accuracy', 'moves'
    ]
    existing_cols = [col for col in keep_cols if col in df.columns]
    return df[existing_cols]
