# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import io
import base64
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def winrate_bar_graph(data):
    labels = list(data.keys())
    wins = [data[color]['win'] for color in labels]
    draws = [data[color]['draw'] for color in labels]
    losses = [data[color]['loss'] for color in labels]

    x = np.arange(len(labels))  # [0, 1, 2]
    bar_width = 0.5

    fig, ax = plt.subplots()

    # Stack the bars
    p1 = ax.bar(x, wins, bar_width, label='win', color='#4CAF50')
    p2 = ax.bar(x, draws, bar_width, bottom=wins, label='draw', color='#FFC107')
    p3 = ax.bar(x, losses, bar_width,
                bottom=[i + j for i, j in zip(wins, draws)], label='loss', color='#F44336')

    # Labels
    ax.set_ylabel('Percentage')
    ax.set_title('Win Rates by Color')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylim(0, 100)
    ax.legend()

    # Optional: Add percentage labels on top
    for idx, (w, d, l) in enumerate(zip(wins, draws, losses)):
        ax.text(idx, w / 2, f'{w:.0f}%', ha='center', va='center', color='white')
        ax.text(idx, w + d / 2, f'{d:.0f}%', ha='center', va='center', color='black')
        ax.text(idx, w + d + l / 2, f'{l:.0f}%', ha='center', va='center', color='white')

    plt.tight_layout()
    # Save the plot to a BytesIO object
    img_stream = io.BytesIO()
    plt.savefig(img_stream, format='png')
    img_stream.seek(0)

    # Encode the image in base64
    img_base64 = base64.b64encode(img_stream.read()).decode('utf-8')

    # Return the base64 string
    plt.close()
    return img_base64

def plot_game_status_distribution(df):
    # Count the occurrences of each status
    status_counts = df['status'].value_counts()

    # Create the pie chart
    fig, ax = plt.subplots()
    wedges, texts, autotexts = ax.pie(
        status_counts,
        labels=status_counts.index,
        autopct='%1.1f%%',
        startangle=90,
        wedgeprops={'width': 0.4}
    )

    # Add a white circle in the center to make it a donut chart
    centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    fig.gca().add_artist(centre_circle)

    # Set the aspect ratio to be equal for the pie chart to be circular
    ax.axis('equal')
    plt.title("Game Status Distribution")
    ax.legend(
        wedges,
        status_counts.index,
        title="Status",
        loc="center left",
        bbox_to_anchor=(1, 0.5)
    )

    # Adjust layout
    plt.tight_layout()

    # Save the plot to a BytesIO object
    img_stream = io.BytesIO()
    plt.savefig(img_stream, format='png')
    img_stream.seek(0)

    # Encode the image in base64
    img_base64 = base64.b64encode(img_stream.read()).decode('utf-8')

    # Return the base64 string
    plt.close()
    return img_base64