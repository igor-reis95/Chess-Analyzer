"""
Visualization utilities for chess game statistics and evaluations.

This module provides functions to generate matplotlib charts and return them as
base64-encoded PNG images. It includes visualizations for win rates, opening
evaluations, conversion comparisons, and popular/successful openings based on
Lichess analysis data.
"""

import io
import base64
import logging
from typing import Dict
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt # pylint: disable=wrong-import-position
from src.services.analysis import train_logistic_regression_model, get_player_rating_bracket_evaluation, get_opening_stats # pylint: disable=wrong-import-position

logger = logging.getLogger(__name__)


def winrate_bar_graph(data: Dict[str, Dict[str, float]]) -> str:
    """
    Generate a base64-encoded stacked bar chart for win/draw/loss percentages by color.

    Args:
        data (Dict[str, Dict[str, float]]): Nested dict with 'win', 'draw', 'loss' percentages
                                            for each color key (e.g., "White", "Black", "Both").

    Returns:
        str: Base64-encoded PNG image of the generated bar chart.
    """
    logger.debug("Generating winrate bar graph for data: %s", data)

    labels = list(data.keys())
    wins = [data[color]['win'] for color in labels]
    draws = [data[color]['draw'] for color in labels]
    losses = [data[color]['loss'] for color in labels]

    x = np.arange(len(labels))
    bar_width = 0.5

    _, ax = plt.subplots(figsize=(10, 5))

    ax.bar(x, wins, bar_width, label='win', color='#92b76f')
    ax.bar(x, draws, bar_width, bottom=wins, label='draw', color='#d59c4d')
    ax.bar(x, losses, bar_width,
           bottom=[i + j for i, j in zip(wins, draws)],
           label='loss',
           color='#db6f72')

    ax.set_ylabel('Percentage')
    ax.set_title('Win Rates by Player Perspective')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylim(0, 100)
    ax.legend()

    for idx, (w, d, l) in enumerate(zip(wins, draws, losses)):
        ax.text(idx, w / 2, f'{w:.0f}%', ha='center', va='center', color='white')
        ax.text(idx, w + d / 2, f'{d:.0f}%', ha='center', va='center', color='black')
        ax.text(idx, w + d + l / 2, f'{l:.0f}%', ha='center', va='center', color='white')

    plt.tight_layout()

    img_stream = io.BytesIO()
    plt.savefig(img_stream, format='png')
    img_stream.seek(0)
    img_base64 = base64.b64encode(img_stream.read()).decode('utf-8')
    plt.close()

    logger.debug("Winrate bar graph successfully generated.")
    return img_base64


def plot_eval_on_opening(df: pd.DataFrame, lichess_stats: Dict, user_data: Dict) -> str:
    """
    Generate a base64-encoded bar chart for average adjusted opening evaluation by player color.

    Args:
        df (pd.DataFrame): DataFrame with columns 'opening_eval' and 'player_color'.

    Returns:
        str: Base64-encoded PNG image of the bar chart.
    """
    df.loc[:, 'opening_eval'] = pd.to_numeric(df['opening_eval'], errors='coerce')
    df["adjusted_eval"] = df.apply(
        lambda row: -row["opening_eval"] if row["player_color"] == "black" else row["opening_eval"],
        axis=1
    )

    overall_avg = df["adjusted_eval"].mean()
    white_avg = df[df["player_color"] == "white"]["adjusted_eval"].mean()
    black_avg = df[df["player_color"] == "black"]["adjusted_eval"].mean()

    perf_type = df['perf'].loc[0]
    rating_avg_eval, _ = get_player_rating_bracket_evaluation(
        perf_type, lichess_stats, user_data
    )
    averages = {
        "White": white_avg,
        "Black": black_avg,
        "Overall": overall_avg,
        "Lichess Avg": rating_avg_eval
    }

    # Create custom colors for each bar
    colors = []
    for key, val in averages.items():
        if key == "Lichess Avg":
            colors.append("#d49b54")
        elif val >= 0:
            colors.append("#93b674")
        else:
            colors.append("#da6f73")

    plt.figure(figsize=(8, 6))
    bars = plt.bar(averages.keys(), averages.values(), color=colors)
    plt.axhline(0, color="black", linestyle="--", alpha=0.5)

    for b in bars:
        height = b.get_height()
        plt.text(b.get_x() + b.get_width() / 2., height,
                 f'{height:.2f}',
                 ha='center', va='bottom')

    plt.title("Average Evaluation by Player Perspective")
    plt.ylabel("Adjusted Evaluation")
    plt.xlabel("Player Color")

    plt.tight_layout()

    img_stream = io.BytesIO()
    plt.savefig(img_stream, format='png')
    img_stream.seek(0)
    img_base64 = base64.b64encode(img_stream.read()).decode('utf-8')
    plt.close()

    logger.debug("Opening evaluation bar graph generated.")
    return img_base64

def plot_opening_stats(df: pd.DataFrame, color: str = "Overall") -> str:
    """
    Generate a base64-encoded horizontal bar chart showing opening performance.

    Args:
        df (pd.DataFrame): DataFrame with opening stats.
        color (str, optional): Player color filter ('overall', 'white', 'black').
                               Defaults to 'overall'.

    Returns:
        str: Base64-encoded PNG image of the horizontal bar chart.
    """
    if color == "Overall":
        df = get_opening_stats(df)
    else:
        df = get_opening_stats(df[df['player_color'] == color.lower()])

    if len(df) == 0 or df['avg_eval'].isna().all():
        plt.figure(figsize=(10, 7))
        plt.text(0.5, 0.5,
                 f"No opening data available for {color}\n"
                 "(Need at least one opening played thrice)",
                 ha='center', va='center')
        plt.axis('off')
    else:
        df = df.sort_values('count', ascending=True)
        colors = ["#93b674" if x >= 0 else "#da6f73" for x in df['avg_eval']]

        plt.figure(figsize=(10, 7))
        bars = plt.barh(df['opening_label'], df['avg_eval'], color=colors)

        for b in bars:
            width = b.get_width()
            label_x_pos = width
            ha = 'left' if width >= 0 else 'right'
            plt.text(label_x_pos, b.get_y() + b.get_height() / 2,
                     f'{width:.2f}',
                     va='center', ha=ha,
                     color='black', fontsize=8)

        plt.axvline(0, color="black", linestyle="--", alpha=0.5)
        plt.title(f"Opening Performance ({color})")
        plt.xlabel("Average Evaluation")
        plt.ylabel("Opening (Count)")

        min_eval = df['avg_eval'].replace([np.inf, -np.inf], np.nan).min()
        max_eval = df['avg_eval'].replace([np.inf, -np.inf], np.nan).max()

        if pd.isna(min_eval) or pd.isna(max_eval):
            min_eval, max_eval = -1, 1
        elif min_eval == max_eval:
            min_eval, max_eval = min_eval - 1, max_eval + 1

        padding = (max_eval - min_eval) * 0.1
        plt.xlim(min_eval - padding, max_eval + padding)

    plt.tight_layout()
    img = io.BytesIO()
    plt.savefig(img, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    img.seek(0)
    img_base64 = base64.b64encode(img.getvalue()).decode()

    logger.debug("Opening stats horizontal bar chart generated for color: %s", color)
    return img_base64


def plot_conversion_comparison(
    player_stats: dict,
    lichess_stats: dict,
    stat_key: str,
    title: str
) -> str:
    """
    Plot a comparison bar chart for a given conversion statistic between player and Lichess data.

    Args:
        player_stats (dict): Player's statistics.
        lichess_stats (dict): Lichess reference statistics.
        stat_key (str): Key to extract the statistic from both dicts.
        title (str): Plot title.

    Returns:
        str: Base64-encoded PNG image of the comparison bar chart.
    """
    player_value = player_stats[stat_key]
    lichess_value = lichess_stats['conversion_stats'][stat_key]

    plt.figure(figsize=(10, 5))
    metrics = ['You', 'Lichess Playerbase']
    values = [player_value, lichess_value]

    bars = plt.bar(metrics, values, color=('#93b674', '#d49b54'))
    plt.title(title)
    plt.ylabel('Percentage (%)')
    plt.ylim(0, 100)

    for b in bars:
        height = b.get_height()
        plt.text(b.get_x() + b.get_width() / 2., height,
                 f'{height:.1f}%',
                 ha='center', va='bottom')

    plt.grid(axis='y', linestyle='--', alpha=0.7)

    img = io.BytesIO()
    plt.savefig(img, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    img.seek(0)
    img_base64 = base64.b64encode(img.getvalue()).decode()

    logger.debug("Conversion comparison chart generated for stat key: %s", stat_key)
    return img_base64


def lichess_popular_openings(lichess_analysis_data: dict) -> str:
    """
    Generate a horizontal bar chart for the most popular chess openings by ECO code.

    Args:
        lichess_analysis_data (dict): Lichess analysis data containing 'popular_openings'.

    Returns:
        str: Base64-encoded PNG image of the popular openings chart.
    """
    popular_openings_df = pd.DataFrame(lichess_analysis_data["popular_openings"]).head()
    popular_openings_df = popular_openings_df.sort_values('percentage')

    plt.figure(figsize=(8, 5))
    bars = plt.barh(popular_openings_df['ECO'], popular_openings_df['percentage'], color='#d49b54')

    plt.title('Most Popular Chess Openings by ECO Code')
    plt.xlabel('Percentage of Games')
    plt.ylabel('ECO Code')

    plt.xticks([0, 0.02, 0.04, 0.06], ['0%', '2%', '4%', '6%'])

    for b in bars:
        width = b.get_width()
        percentage = width * 100
        x_pos = width - (width * 0.005)

        plt.text(
            x_pos,
            b.get_y() + b.get_height() / 2,
            f'{percentage:.2f}%',
            ha='right',
            va='center',
            color='black',
            fontweight='bold'
        )

    plt.tight_layout()

    img = io.BytesIO()
    plt.savefig(img, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    img.seek(0)
    img_base64 = base64.b64encode(img.getvalue()).decode()

    logger.debug("Popular openings chart generated.")
    return img_base64


def lichess_successful_openings(lichess_analysis_data: dict, color: str) -> str:
    """
    Generate a horizontal bar chart of the most successful chess openings by ECO code.

    Args:
        lichess_analysis_data (dict): Lichess analysis data containing 'opening_eval_per_eco'.
        color (str): Player color filter ('white' or other).

    Returns:
        str: Base64-encoded PNG image of the successful openings chart.
    """
    if color == 'White':
        popular_openings_df = pd.DataFrame(lichess_analysis_data["opening_eval_per_eco"]).tail()
    else:
        popular_openings_df = pd.DataFrame(lichess_analysis_data["opening_eval_per_eco"]) \
            .head().sort_values(by='evaluation', ascending=False)
        popular_openings_df['evaluation'] = popular_openings_df['evaluation'].abs()

    color_text = color.capitalize()

    plt.figure(figsize=(8, 5))
    bars = plt.barh(popular_openings_df['ECO'], popular_openings_df['evaluation'], color='#d49b54')

    plt.title(f'Most Successful Chess Openings for {color_text} by ECO Code')
    plt.xlabel('Evaluation of Games')
    plt.ylabel('ECO Code')

    for b in bars:
        width = b.get_width()
        if width >= 0:
            x_pos = width - 0.05
            ha = 'right'
        else:
            x_pos = width + 0.05
            ha = 'left'

        plt.text(
            x_pos,
            b.get_y() + b.get_height() / 2,
            f'{width:.2f}',
            ha=ha,
            va='center',
            color='black',
            fontweight='bold'
        )

    plt.tight_layout()

    img = io.BytesIO()
    plt.savefig(img, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    img.seek(0)
    img_base64 = base64.b64encode(img.getvalue()).decode()

    logger.debug("Successful openings chart generated for color: %s", color)
    return img_base64

def logistic_regression_graph(df):
    """
    Generate a horizontal bar chart of logistic regression feature coefficients.
    
    Args:
        df: Input DataFrame containing chess game data.
        
    Returns:
        Base64 encoded string of the generated chart image.
    """
    importance_df = train_logistic_regression_model(df)

    # Renaming features
    importance_df['feature'] = importance_df['feature'].replace({
        'opening_group_': 'Opening: ',
        'is_white': 'Is white?',
        'rating_difference': 'Rating Difference',
        'full_moves': 'Amount of moves'
    }, regex=True)
    

    # Set color based on coefficient sign
    colors = [
        '#94b578' if coef > 0 else '#d96f74' 
        for coef in importance_df['coefficient']
    ]

    # Plot the most influential features
    plt.figure(figsize=(10, 8))
    plt.barh(importance_df['feature'], importance_df['coefficient'], color=colors)
    plt.axvline(0, color='gray', linestyle='--')

    plt.title(
        'Feature Influence on Win Probability '
        '(Logistic Regression Coefficients)'
    )
    plt.xlabel('Coefficient Value')
    plt.ylabel('Feature')
    plt.gca().invert_yaxis()
    plt.tight_layout()

    # Save to base64 for Flask
    img = io.BytesIO()
    plt.savefig(img, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    img.seek(0)
    img_base64 = base64.b64encode(img.getvalue()).decode()

    logger.debug("Successfully generated logistic regression chart")
    return img_base64
