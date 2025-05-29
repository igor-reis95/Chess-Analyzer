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

    fig = plt.figure()
    ax = fig.add_subplot(111)

    # Stack the bars
    ax.bar(x, wins, bar_width, label='win', color='#92b76f')
    ax.bar(x, draws, bar_width, bottom=wins, label='draw', color='#d59c4d')
    ax.bar(x, losses, bar_width,
        bottom=[i + j for i, j in zip(wins, draws)], label='loss', color='#db6f72')

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
    status_counts = df['status'].value_counts()

    custom_colors = []
    for status in status_counts.index:
        if status == 'resign':
            custom_colors.append('#93b674')
        elif status == 'mate':
            custom_colors.append('#da6f73')
        elif status == 'draw':
            custom_colors.append('#d49b54')
        elif status == 'outoftime':
            custom_colors.append('#3288d1')
        else:
            custom_colors.append('#6c757d')

    fig, ax = plt.subplots()
    wedges, _, _ = ax.pie(
        status_counts,
        labels=status_counts.index,
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
        status_counts.index,
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
    return img_base64
