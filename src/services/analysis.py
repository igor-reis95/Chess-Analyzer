import pandas as pd

def get_rating_diff(df, username, color):
    return df[df[f'{color}_name'] == username][f'{color}_ratingDiff'].sum()

def get_opening_counts(df):
    return df['opening_eco'].value_counts(dropna=False).head()

def get_opening_counts_by_result(df, color):
    return df[df['winner'] == color]['opening_eco'].value_counts(dropna=False).head()

def get_rating_range(df, username, color):
    min_rating = df[df[f'{color}_name'] == username][f'{color}_rating'].min()
    max_rating = df[df[f'{color}_name'] == username][f'{color}_rating'].max()
    return min_rating, max_rating

def count_results(df, color):
    wins = len(df[df['winner'] == color])
    losses = len(df[(df['winner'] != color) & df['winner'].notna()])
    draws = len(df[df['winner'].isna()])

    return wins, losses, draws

def get_common_opponents(df, username):
    white_opponents = df['white_name'][df['white_name'] != username]
    black_opponents = df['black_name'][df['black_name'] != username]
    common_opponents = pd.concat([white_opponents, black_opponents]).value_counts().head()
    return common_opponents

def get_accuracy_stats(df,color):
    all_games = df[f'{color}_accuracy'].mean()
    wins = df[df['winner'] == color][f'{color}_accuracy'].mean()
    losses = df[(df['winner'] != color) & (df['winner'].notna())][f'{color}_accuracy'].mean()
    draws = df[df['winner'].isna()][f'{color}_accuracy'].mean()
    return {
        'overall': all_games,
        'wins': wins,
        'losses': losses,
        'draws': draws
    }

def analysis_per_color(df, username, color):
    filtered_df = df[df[f'{color}_name'].str.lower() == username.lower()]
    results = {
        "rating_diff": get_rating_diff(filtered_df, username, color),
        "opening_counts": get_opening_counts(filtered_df),
        "opening_wins": get_opening_counts_by_result(filtered_df, color),
        "opening_losses": get_opening_counts_by_result(filtered_df, color),
        "rating_range": get_rating_range(filtered_df, username, color),
        "results": count_results(filtered_df, color),
        "common_opponents": get_common_opponents(filtered_df, username),
        "accuracy": get_accuracy_stats(filtered_df, color),
    }

    return results
