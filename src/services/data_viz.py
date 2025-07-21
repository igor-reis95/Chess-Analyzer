import io
import base64
import logging
from typing import Dict
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt # pylint: disable=wrong-import-position

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

    _, ax = plt.subplots(figsize=(10,5))

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

def plot_eval_on_opening(df):
    df.loc[:, 'opening_eval'] = pd.to_numeric(df['opening_eval'], errors='coerce')
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
    for b in bars:
        height = b.get_height()
        plt.text(b.get_x() + b.get_width()/2., height,
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
    df.loc[:, 'opening_eval'] = pd.to_numeric(df['opening_eval'], errors='coerce')
    df.loc[:, "adjusted_eval"] = df.apply(
        lambda row: -row["opening_eval"] if row["player_color"] == "black" else row["opening_eval"],
        axis=1
    )
    df = df.groupby("normalized_opening_name").agg(
        count=("adjusted_eval", "size"),
        avg_eval=("adjusted_eval", "mean")
    ).reset_index()
    df = df[df["count"] > 2].sort_values("count", ascending=False)
    df["opening_label"] = df.apply(lambda x: f"{x['normalized_opening_name']} ({x['count']})",axis=1)
    return df.head() # .head() to not bring every value and create a huge graph

def plot_opening_stats(df, color="overall"):
    if color == "overall":
        df = get_opening_stats(df)
    else:
        df = get_opening_stats(df[df['player_color'] == color])

    # Check if we have any valid data to plot
    if len(df) == 0 or df['avg_eval'].isna().all():
        # Create an empty plot with a message
        plt.figure(figsize=(10, 7))
        plt.text(0.5, 0.5, 
                f"No opening data available for {color}\n(Need at least 3 games per opening)",
                ha='center', va='center')
        plt.axis('off')
    else:
        # Sort by frequency
        df = df.sort_values('count', ascending=True)

        # Create color list based on evaluation values
        colors = ["#93b674" if x >= 0 else "#da6f73" for x in df['avg_eval']]

        plt.figure(figsize=(10, 7))
        bars = plt.barh(df['opening_label'], df['avg_eval'], color=colors)

        # Add value labels
        for b in bars:
            width = b.get_width()
            label_x_pos = width if width >= 0 else width
            plt.text(label_x_pos, b.get_y() + b.get_height()/2,
                    f'{width:.2f}',
                    va='center', ha='left' if width >= 0 else 'right',
                    color='black', fontsize=8)

        plt.axvline(0, color="black", linestyle="--", alpha=0.5)
        plt.title(f"Opening Performance ({color})")
        plt.xlabel("Average Evaluation")
        plt.ylabel("Opening (Count)")

        # Calculate min/max with fallback values
        min_eval = df['avg_eval'].replace([np.inf, -np.inf], np.nan).min()
        max_eval = df['avg_eval'].replace([np.inf, -np.inf], np.nan).max()
        
        # Handle case where all values are the same or NaN
        if pd.isna(min_eval) or pd.isna(max_eval):
            min_eval, max_eval = -1, 1  # Default range when no valid data
        elif min_eval == max_eval:
            min_eval, max_eval = min_eval - 1, max_eval + 1

        padding = (max_eval - min_eval) * 0.1  # 10% padding
        plt.xlim(min_eval - padding, max_eval + padding)

    plt.tight_layout()
    img = io.BytesIO()
    plt.savefig(img, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    img.seek(0)
    return base64.b64encode(img.getvalue()).decode()

def lichess_popular_openings(lichess_analysis_data):
    # Retrieve popular openings data, turn it into a dataframe and return the top 5
    popular_openings_df = pd.DataFrame(lichess_analysis_data["popular_openings"]).head()
    popular_openings_df = popular_openings_df.sort_values('percentage')

    plt.figure(figsize=(8, 5))
    bars = plt.barh(popular_openings_df['ECO'], popular_openings_df['percentage'], color='#1E90FF')

    plt.title('Most Popular Chess Openings by ECO Code')
    plt.xlabel('Percentage of Games')
    plt.ylabel('ECO Code')

    # Manually format as percentages by multiplying by 100 (x-axis legend)
    plt.xticks([0, 0.02, 0.04, 0.06], ['0%', '2%', '4%', '6%'])

    # Add percentage labels
    for b in bars:
        width = b.get_width()
        percentage = width * 100  # Convert to percentage

        # Calculate the position relative to bar width
        x_pos = width - (width * 0.005)

        plt.text(
            x_pos,  # Convert back to decimal for positioning
            b.get_y() + b.get_height()/2,
            f'{percentage:.2f}%',
            ha='right',
            va='center',
            color='black',
            fontweight='bold'
        )

    plt.tight_layout()
    # Save plot to base64 string
    img = io.BytesIO()
    plt.savefig(img, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    img.seek(0)
    return base64.b64encode(img.getvalue()).decode()

def lichess_successful_openings(lichess_analysis_data, color):
    # Retrieve popular openings data, turn it into a dataframe and return the top 5
    if color == 'white':
        popular_openings_df = pd.DataFrame(lichess_analysis_data["opening_eval_per_eco"]).tail()
    else:
        popular_openings_df = pd.DataFrame(lichess_analysis_data["opening_eval_per_eco"]).head().sort_values(by='evaluation', ascending=False)

    color_text = color[0].upper() + color[1:] # To make the first letter uppercase

    plt.figure(figsize=(8, 5))
    bars = plt.barh(popular_openings_df['ECO'], popular_openings_df['evaluation'], color='#1E90FF')

    plt.title(f'Most Successful Chess Openings for {color_text} by ECO Code')
    plt.xlabel('Evaluation of Games')
    plt.ylabel('ECO Code')

    # Add value labels at bar ends
    for b in bars:
        width = b.get_width()

        # Position text at the end of the bar
        if width >= 0:
            x_pos = width - 0.05  # Slightly inside from the end for positive values
            ha = 'right'  # Right-align for positive bars
        else:
            x_pos = width + 0.05  # Slightly inside from the end for negative values
            ha = 'left'  # Left-align for negative bars

        plt.text(
            x_pos,
            b.get_y() + b.get_height()/2,
            f'{width:.2f}',
            ha=ha,
            va='center',
            color='black',
            fontweight='bold'
        )

    plt.tight_layout()

    # Save plot to base64 string
    img = io.BytesIO()
    plt.savefig(img, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    img.seek(0)
    return base64.b64encode(img.getvalue()).decode()

def plot_conversion_comparison(player_stats, lichess_stats, 
                             stat_key, title):
    """
    Plot comparison between player and Lichess playerbase for any conversion stat
    
    Parameters:
        player_stats: dict with player's statistics
        lichess_stats: dict with Lichess reference stats
        stat_key: key to extract from both stats dictionaries
        title: plot title
        colors: tuple of two colors for the bars
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
    
    # Add value labels
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}%',
                ha='center', va='bottom')
    
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Save plot to base64 string
    img = io.BytesIO()
    plt.savefig(img, format='png', dpi=100, bbox_inches='tight')
    plt.close()
    img.seek(0)
    return base64.b64encode(img.getvalue()).decode()