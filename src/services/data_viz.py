# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import io
import base64
import matplotlib.pyplot as plt

def count_game_outcomes(df, username):
    username = username.lower()

    is_white = df['white_name'].str.lower() == username
    is_black = df['black_name'].str.lower() == username

    win_as_white = is_white & (df['winner'] == 'white')
    win_as_black = is_black & (df['winner'] == 'black')

    loss_as_white = is_white & (df['winner'] == 'black')
    loss_as_black = is_black & (df['winner'] == 'white')

    draws = (is_white | is_black) & df['winner'].isna()

    num_wins = (win_as_white | win_as_black).sum()
    num_losses = (loss_as_white | loss_as_black).sum()
    num_draws = draws.sum()

    return num_wins, num_losses, num_draws

def status_distribution(df):
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


def openings_bar_graph(df):
    opening_counts = df['opening_name'].value_counts().head(5)

    fig, ax = plt.subplots()
    opening_counts.plot(kind='bar', ax=ax, color='skyblue', edgecolor='black')

    ax.set_title('Top 5 Openings')
    ax.set_xlabel('Opening')
    ax.set_ylabel('Number of Games')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    img_io = io.BytesIO()
    plt.savefig(img_io, format='png')
    img_io.seek(0)
    plt.show()

def games_outcome_distribution(df, username):
    # Data
    username = 'Totallyweakplayer39'
    wins, losses, draws = count_game_outcomes(df, username)
    values = [wins, losses, draws]
    labels = ['Wins', 'Losses', 'Draws']

    # Plot
    fig, ax = plt.subplots()
    wedges, texts, autotexts = ax.pie(
        values,
        labels=labels,
        autopct='%1.1f%%',
        startangle=90,
        wedgeprops={'width': 0.4}
    )

    # Add center circle
    centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    fig.gca().add_artist(centre_circle)

    # Equal aspect ratio ensures the donut is a circle
    ax.axis('equal')
    plt.title(f"Game Outcome Distribution for {username}")

    # Optional legend (though labels already shown)
    plt.legend(wedges, labels, title="Outcome", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))

    plt.tight_layout()
    plt.show()  # or plt.savefig(...) if you're saving it
