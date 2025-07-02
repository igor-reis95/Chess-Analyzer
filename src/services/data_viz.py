"""
This module provides utility functions to generate base64-encoded visualizations 
from chess match data.

Functions:
- winrate_bar_graph: Generates a stacked bar chart of win/draw/loss percentages by color.
- plot_game_status_distribution: Generates a donut chart showing game outcome distributions.

These functions are designed for integration in data pipelines or web apps where
image output needs to be embedded or transmitted via text (e.g., HTML or JSON).
"""

import io
import base64
import logging
from typing import Dict
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt # pylint: disable=wrong-import-position
import seaborn as sns

# Create logger
logger = logging.getLogger(__name__)

def winrate_bar_graph(data: Dict[str, Dict[str, float]]) -> str:
    """
    Generate a base64-encoded stacked bar chart for win/draw/loss percentages by color.

    Args:
        data (Dict[str, Dict[str, float]]): Nested dictionary with win, draw, and loss
                                            percentages for each color ("White", "Black", "Both").

    Returns:
        str: Base64-encoded PNG image of the generated chart.
    """
    logger.debug("Generating winrate bar graph for data: %s", data)

    labels = list(data.keys())
    wins = [data[color]['win'] for color in labels]
    draws = [data[color]['draw'] for color in labels]
    losses = [data[color]['loss'] for color in labels]

    x = np.arange(len(labels))
    bar_width = 0.5

    _, ax = plt.subplots(figsize=(8,5))

    ax.bar(x, wins, bar_width, label='win', color='#92b76f')
    ax.bar(x, draws, bar_width, bottom=wins, label='draw', color='#d59c4d')
    ax.bar(
        x,
        losses,
        bar_width,
        bottom=[i + j for i, j in zip(wins, draws)],
        label='loss',
        color='#db6f72'
    )

    ax.set_ylabel('Percentage')
    ax.set_title('Win Rates by Color')
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


def plot_game_status_distribution(df: pd.DataFrame) -> str:
    """
    Generate a base64-encoded donut chart for game status distribution.

    Args:
        df (pd.DataFrame): DataFrame containing a 'status' column.

    Returns:
        str: Base64-encoded PNG image of the chart.
    """
    logger.debug("Generating game status distribution chart.")

    # Count and compute relative frequency
    status_counts = df['status'].value_counts()
    total = status_counts.sum()
    status_percent = status_counts / total

    # Group small categories into 'other'
    threshold = 0.10
    major_statuses = status_percent[status_percent >= threshold]
    other_count = status_counts[status_percent < threshold].sum() / 100

    # Final data
    final_counts = major_statuses.copy()
    if other_count > 0:
        final_counts['other'] = other_count

    # Define color mapping
    color_map = {
        'resign': '#93b674',
        'mate': '#da6f73',
        'draw': '#d49b54',
        'outoftime': '#3288d1',
        'other': '#6c757d'
    }

    # Assign colors based on final labels
    custom_colors = [color_map.get(status, '#6c757d') for status in final_counts.index]

    # Plot
    fig, ax = plt.subplots()
    wedges, _, _ = ax.pie(
        final_counts,
        labels=final_counts.index,
        autopct='%1.1f%%',
        startangle=90,
        wedgeprops={'width': 0.4},
        colors=custom_colors
    )

    centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    fig.gca().add_artist(centre_circle)

    ax.axis('equal')
    plt.title("Game Status Distribution")
    ax.legend(
        wedges,
        final_counts.index,
        title="Status",
        loc="center left",
        bbox_to_anchor=(1, 0.5)
    )

    plt.tight_layout()

    img_stream = io.BytesIO()
    plt.savefig(img_stream, format='png')
    img_stream.seek(0)
    img_base64 = base64.b64encode(img_stream.read()).decode('utf-8')
    plt.close()

    logger.debug("Game status distribution chart successfully generated.")
    return img_base64

def plot_eval_per_opening(df):
    df['opening_eval'] = pd.to_numeric(df['opening_eval'], errors='coerce')
    df["adjusted_eval"] = df.apply(
        lambda row: -row["opening_eval"] if row["player_color"] == "black" else row["opening_eval"],
        axis=1
    )

    # Overall average (all games)
    overall_avg = df["adjusted_eval"].mean()

    # Averages by player color
    white_avg = df[df["player_color"] == "white"]["adjusted_eval"].mean()
    black_avg = df[df["player_color"] == "black"]["adjusted_eval"].mean()

    # Data for plotting
    averages = {
        "Overall": overall_avg,
        "White": white_avg,
        "Black": black_avg
    }

    # Define colors based on value
    colors = []
    for value in averages.values():
        if value >= 0:
            colors.append("#93b674")
        else:
            colors.append("#da6f73")

    # Plot
    plt.figure(figsize=(8, 5))
    bars = plt.bar(averages.keys(), averages.values(), color=colors)
    plt.axhline(0, color="black", linestyle="--", alpha=0.5)

    # Add value labels on top of each bar
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
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
    return img_base64

def get_opening_stats(df):
    df['opening_eval'] = pd.to_numeric(df['opening_eval'], errors='coerce')
    df["adjusted_eval"] = df.apply(
        lambda row: -row["opening_eval"] if row["player_color"] == "black" else row["opening_eval"],
        axis=1
    )
    df = df.groupby("normalized_opening_name").agg(
        count=("adjusted_eval", "size"),
        avg_eval=("adjusted_eval", "mean")
    ).reset_index()
    df = df[df["count"] > 2].sort_values("count", ascending=False)
    df["opening_label"] = df.apply(lambda x: f"{x['normalized_opening_name']} ({x['count']})",axis=1)
    return df

def plot_opening_stats(df, color="Overall"):
    if color == "Overall":
        df = get_opening_stats(df)
    else:
        df = get_opening_stats(df[df['player_color'] == color])

    # Sort by frequency (assuming 'count' exists in your stats)
    df = df.sort_values('count', ascending=True)

    # Create color list based on evaluation values
    colors = ["#93b674" if x >= 0 else "#da6f73" for x in df['avg_eval']]

    plt.figure(figsize=(8, 5))

    # Create the barplot with our custom colors
    bars = plt.barh(df['opening_label'], df['avg_eval'], color=colors)

    # Add value labels
    for bar_rect in bars:
        width = bar_rect.get_width()
        label_x_pos = width if width >= 0 else width
        plt.text(label_x_pos, bar_rect.get_y() + bar_rect.get_height()/2,
                 f'{width:.2f}',
                 va='center', ha='left' if width >= 0 else 'right',
                 color='black', fontsize=8)

    plt.axvline(0, color="black", linestyle="--", alpha=0.5)
    plt.title(f"Opening Performance ({color})")
    plt.xlabel("Average Evaluation")
    plt.ylabel("Opening (Count)")

    # Add some padding to prevent label cutoff
    plt.xlim(min(df['avg_eval']) * 1.1, max(df['avg_eval']) * 1.1)

    plt.tight_layout()

    # Save plot to base64 string
    img = io.BytesIO()
    plt.savefig(img, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    img.seek(0)
    return base64.b64encode(img.getvalue()).decode()
