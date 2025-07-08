"""Chess Game Analysis Web Application.

This module provides the main Flask route for analyzing chess game data.
It handles user submissions, processes game data through various services,
and renders visualizations of the analysis results.
"""

import traceback
import re
import logging
import io
from io import BytesIO
import csv
import json
from weasyprint import HTML
from flask import render_template, request, Response, make_response, send_file
from src.services.analysis import prepare_winrate_data, calculate_advantage_stats
import src.services.data_viz as viz
import src.services.data_insights as insights
from src.services.game_processor import GameProcessor
from src.services.user_processor import UserProcessor
from ..webapp import app

# Constant values and logger creation
MAX_GAMES_LIMIT = 900
GAMES_TABLE_PREVIEW = 30

logger = logging.getLogger(__name__)

@app.route("/", methods=["GET", "POST"])
def index():
    """Handle root route with minimal logic."""
    if request.method == "GET":
        return _show_form()
    return _handle_form_submission(request.form)

# ----------------------
# Helper Functions
# ----------------------
def _show_form() -> str:
    """Render the empty input form."""
    return render_template("form.html")

def _handle_form_submission(form_data: dict) -> str:
    """Process submitted form data and return results or errors."""
    try:
        params = _validate_inputs(form_data)
        df, user_data = _fetch_and_prepare_data(params)
        return _render_results(params, df, user_data)
    except ValueError as e:
        logger.warning("Validation failed: %s", e)
        return _render_error(f"Invalid input: {str(e)}", status_code=400)
    # pylint: disable=broad-exception-caught
    except Exception as e:
        logger.exception("Unexpected error during form submission")
        return _render_error(f"Processing error: {str(e)}")

# ----------------------
# Data Layer
# ----------------------
def _validate_inputs(form_data: dict) -> dict:
    """Strict server-side validation."""
    username = form_data.get("username", "").strip()
    max_games = int(form_data.get("max_games", 0))

    # Format checks
    if not re.match(r"^[\w-]{3,20}$", username):
        raise ValueError("Username: 3-20 chars (letters, numbers, _-)")

    # Business logic
    if max_games > MAX_GAMES_LIMIT:
        raise ValueError("Maximum 900 games allowed")

    return {
        "username": username,
        "max_games": min(max_games, MAX_GAMES_LIMIT),  # Force compliance
        "perf_type": form_data.get("perf_type", "blitz"),
        "color": form_data.get("color", "both")
    }

def _fetch_and_prepare_data(params: dict) -> tuple:
    # Fetch game and user data from Lichess
    #Fetch and process chess game data
    game_processor = GameProcessor(
        username=params["username"],
        max_games=params["max_games"],
        perf_type=params["perf_type"],
        color=params["color"]
    )
    game_processor.run_all()

    # Fetch and process chess user data
    user_processor = UserProcessor(params["username"])
    user_processor.run_all()
    user_data = user_processor.get_user_data()

    return game_processor.get_dataframe().head(params["max_games"]), user_data

# ----------------------
# Presentation Layer
# ----------------------
def _render_results(params: dict, df, user_data) -> str:
    """Render analysis results template."""
    return render_template(
        "result.html",
        **_generate_template_context(params, df, user_data)
    )

def _generate_template_context(params: dict, df, user_data) -> dict:
    """Prepare all data needed for the results template."""

    # Retrieve player stats data and Lichess stats data
    player_data = calculate_advantage_stats(df)
    with open("data/lichess_analysis_snapshot.json", "r") as f:
        lichess_data = json.load(f)

    return {
        **params,
        "count": len(df),
        "games_table": df.head(GAMES_TABLE_PREVIEW).to_dict(orient="records"),
        "form_data": params,
        **_get_visualizations(df, player_data, lichess_data),
        "user_data": user_data
    }

def _get_visualizations(df, player_data, lichess_data) -> dict:
    """Generate all visualization outputs."""
    return {
        "status_distribution_graph": viz.plot_game_status_distribution(df),
        "winrate_graph": viz.winrate_bar_graph(prepare_winrate_data(df)),
        "eval_per_opening": viz.plot_eval_per_opening(df),
        "openings": {
            "overall": viz.plot_opening_stats(df, "Overall"),
            "white": viz.plot_opening_stats(df, "white"),
            "black": viz.plot_opening_stats(df, "black"),
        },
        "lichess_openings": {
            "popular": viz.lichess_popular_openings(lichess_data),
            "successful_white": viz.lichess_successful_openings(lichess_data, "white"),
            "successful_black": viz.lichess_successful_openings(lichess_data, "black"),
        },
        "conversion": {
            "when_ahead": viz.plot_conversion_comparison(
                player_data, lichess_data,
                stat_key='pct_won_when_ahead',
                title='% Wins when ahead after opening'
            ),
            "when_behind": viz.plot_conversion_comparison(
                player_data, lichess_data,
                stat_key='pct_won_or_drawn_when_behind',
                title='% Wins/Draws when behind after opening'
            )
        }
    }

def _render_error(message: str, status_code: int = 500) -> str:
    """Render error message with traceback."""
    tb = traceback.format_exc()
    return Response(f"<pre>{message}\n\n{tb}</pre>", status=status_code, mimetype="text/html")
