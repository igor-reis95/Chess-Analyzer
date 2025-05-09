from flask import request, render_template
from ..webapp import app
from src.services.game_processor import GameProcessor

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        username = request.form["username"]
        max_games = int(request.form.get("max_games", 50))  # Default is 50
        perf_type = request.form.get("perf_type", "blitz")  # Default is "blitz"
        color = request.form.get("color", None)  # Default is None

        try:
            # Create the GameProcessor instance
            processor = GameProcessor(username, max_games, perf_type, color)

            # Run the processing steps
            processor.run_all()

            # Pass the results to the results page
            return render_template("result.html", 
                                   username=username, 
                                   count=len(processor.games), 
                                   summary=processor.get_summary_stats())
        except Exception as e:
            # Handle any error that happens during fetching the games
            return f"An error occurred: {str(e)}"

    return render_template("form.html")
