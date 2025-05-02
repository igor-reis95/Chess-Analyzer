from flask import request, render_template
from src.webapp import app
from src.api.api import get_games
from src.data_scripts.data_processing import save_games_to_json, save_df_to_csv, flatten_game_data

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        username = request.form["username"]
        max_games = int(request.form.get("max_games", 50))  # Default is 50
        perf_type = request.form.get("perf_type", "blitz")  # Default is "blitz"
        color = request.form.get("color", None)  # Default is None

        try:
            # Fetching the games based on user input
            games = get_games(username, max_games, perf_type, color)

            # Save the games to a JSON file
            save_games_to_json(games, username)

            # Save the games to a CSV file
            df = flatten_game_data(games)
            save_df_to_csv(df, username)
            
            # Pass the games data to the results page
            return render_template("result.html", username=username, count=len(games), games=games)
        except Exception as e:
            # Handle any error that happens during fetching the games
            return f"An error occurred: {str(e)}"

    return render_template("form.html")