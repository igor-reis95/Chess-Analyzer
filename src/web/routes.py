# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import traceback
from flask import render_template, request
from src.services.analysis import analysis_per_color
from src.services.data_viz import status_distribution
from src.services.game_processor import GameProcessor
from ..webapp import app


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        username = request.form["username"]
        max_games = int(request.form.get("max_games", 30))  # Default is 50
        perf_type = request.form.get("perf_type", "blitz")  # Default is "blitz"
        color = request.form.get("color", None)  # Default is None

        try:
            # Create the GameProcessor instance
            processor = GameProcessor(username, max_games, perf_type, color)

            # Run the processing steps
            processor.run_all()
            df = processor.get_dataframe()

            # Run the data analysis
            analysis_for_white = analysis_per_color(df, 'white')
            analysis_for_black = analysis_per_color(df, 'black')

            # Generate graphs
            status_distribution_graph = status_distribution(df)

            # Pass the results to the results page
            return render_template("result.html",
                                   username=username,
                                   count=len(processor.games),
                                   games = processor.games,
                                   analysis_for_white = analysis_for_white,
                                   analysis_for_black = analysis_for_black,
                                   status_distribution_graph=status_distribution_graph)
        except Exception as e:
            # Handle any error that happens during fetching the games
            tb = traceback.format_exc()
            return f"<pre>An error occurred:\n{str(e)}\n\n{tb}</pre>"

    return render_template("form.html")
