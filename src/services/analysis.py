# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import pandas as pd

def filter_by_color(df, color=None):
    return df if color is None else df[df['player_color'] == color]

def get_rating_diff(df):
    return df['player_rating_diff'].sum()

def get_top_openings(df):
    return df['opening_eco'].value_counts(dropna=False).head()

def get_top_openings_by_result(df, color=None):
    df = filter_by_color(df, color)

    # Filter games by result and color first
    wins = df[(df['result'] == 'win') & (df['player_color'] == color)]
    losses = df[(df['result'] == 'loss') & (df['player_color'] == color)]
    draws = df[(df['result'] == 'draw') & (df['player_color'] == color)]

    # Count top openings for each result
    ecos_for_wins = wins['opening_eco'].value_counts().head()
    ecos_for_losses = losses['opening_eco'].value_counts().head()
    ecos_for_draws = draws['opening_eco'].value_counts().head()

    return ecos_for_wins, ecos_for_losses, ecos_for_draws

def get_rating_range(df):
    min_rating = df['player_rating'].min()
    max_rating = df['player_rating'].max()
    return min_rating, max_rating

def count_results(df):
    counts = df['result'].value_counts()
    return counts.get('win', 0), counts.get('loss', 0), counts.get('draw', 0)

def get_common_opponents(df):
    return df['opponent_name'].value_counts().head()

def get_accuracy_stats(df):
    overall = round(df['player_accuracy'].mean(), 2)
    wins = round(df[df['result'] == 'win']['player_accuracy'].mean(), 2)
    losses = round(df[df['result'] == 'loss']['player_accuracy'].mean(), 2)
    draws = round(df[df['result'] == 'draw']['player_accuracy'].mean(), 2)
    return {
        'overall': overall,
        'wins': wins,
        'losses': losses,
        'draws': draws
    }

def basic_analysis(df, color=None):
    df = filter_by_color(df, color)
    opening_by_result = get_top_openings_by_result(df, color)
    wins, losses, draws = count_results(df)
    results = {
        "rating_diff": get_rating_diff(df),
        "opening_counts": get_top_openings(df),
        "opening_wins": opening_by_result[0],
        "opening_losses": opening_by_result[1],
        "opening_draws": opening_by_result[2],
        "rating_range": get_rating_range(df),
        "results": {"wins": wins, "losses": losses, "draws": draws},
        "common_opponents": get_common_opponents(df),
        "accuracy": get_accuracy_stats(df)
    }
    return results

def prepare_winrate_data(df):
    results = ['win', 'draw', 'loss']

    def get_percentages(subset):
        counts = subset['result'].value_counts(normalize=True) * 100
        return {r: round(counts.get(r, 0), 2) for r in results}

    total = get_percentages(df)
    white = get_percentages(df[df['player_color'] == 'white'])
    black = get_percentages(df[df['player_color'] == 'black'])

    return {
        'White': white,
        'Black': black,
        'Both': total
    }