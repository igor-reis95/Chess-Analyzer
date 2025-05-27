# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import traceback
from flask import render_template, request
from src.services.analysis import basic_analysis
from src.services.data_viz import status_distribution
from src.services.game_processor import GameProcessor
from ..webapp import app


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        username = request.form["username"]
        max_games = int(request.form.get("max_games"))
        perf_type = request.form.get("perf_type")
        color = request.form.get("color")

        try:
            # Create the GameProcessor instance
            processor = GameProcessor(username, max_games, perf_type, color)

            # Run the processing steps
            processor.run_all()
            df = processor.get_dataframe()
            game_records = df.to_dict(orient='records')
            game_records_for_table = df.head(30).to_dict(orient='records')

            # Run the data analysis
            overall_analysis = basic_analysis(df)
            analysis_for_white = basic_analysis(df, 'white')
            analysis_for_black = basic_analysis(df, 'black')
            common_opponents_html = overall_analysis['common_opponents'].to_frame().to_html(
                classes="table table-striped",  # Bootstrap classes (optional)
                header=True, 
                index=False
            )

            # Generate graphs
            status_distribution_graph = status_distribution(df)

            # Pass the results to the results page
            return render_template("result.html",
                                   username=username,
                                   count=len(game_records),
                                   games_table = game_records_for_table,
                                   overall_analysis = overall_analysis,
                                   analysis_for_white = analysis_for_white,
                                   analysis_for_black = analysis_for_black,
                                   common_opponents = common_opponents_html,
                                   status_distribution_graph=status_distribution_graph)
        except Exception as e:
            # Handle any error that happens during fetching the games
            tb = traceback.format_exc()
            return f"<pre>An error occurred:\n{str(e)}\n\n{tb}</pre>"

    return render_template("form.html")
