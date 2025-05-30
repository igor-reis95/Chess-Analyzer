"""Chess Game Analysis Web Application.

This module provides the main Flask route for analyzing chess game data.
It handles user submissions, processes game data through various services,
and renders visualizations of the analysis results.
"""

import traceback
import re
import logging
from typing import Any
from flask import render_template, request, Response
from src.services.analysis import basic_analysis, prepare_winrate_data
from src.services.data_viz import plot_game_status_distribution, winrate_bar_graph
from src.services.game_processor import GameProcessor
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
        df = _fetch_and_prepare_data(params)
        return _render_results(params, df)
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

def _fetch_and_prepare_data(params: dict) -> dict:
    """Fetch and process chess game data."""
    processor = GameProcessor(
        username=params["username"],
        max_games=params["max_games"],
        perf_type=params["perf_type"],
        color=params["color"]
    )
    processor.run_all()
    return processor.get_dataframe().head(params["max_games"])

# ----------------------
# Presentation Layer
# ----------------------
def _render_results(params: dict, df) -> str:
    """Render analysis results template."""
    return render_template(
        "result.html",
        **_generate_template_context(params, df)
    )

def _generate_template_context(params: dict, df) -> dict:
    """Prepare all data needed for the results template."""
    return {
        **params,
        "count": len(df),
        "games_table": df.head(GAMES_TABLE_PREVIEW).to_dict(orient='records'),
        **_get_analysis_data(df),
        **_get_visualizations(df)
    }

def _get_analysis_data(df) -> dict[str, Any]:
    """Generate all analysis metrics."""
    if df.empty:
        raise ValueError("No games found. Try with different parameters.")
    overall = basic_analysis(df)
    return {
        "overall_analysis": overall,
        "analysis_for_white": basic_analysis(df, 'white'),
        "analysis_for_black": basic_analysis(df, 'black'),
        "common_opponents": overall['common_opponents'].to_frame().to_html(
            classes="table table-striped",
            header=True,
            index=False
        )
    }

def _get_visualizations(df) -> dict:
    """Generate all visualization outputs."""
    return {
        "status_distribution_graph": plot_game_status_distribution(df),
        "winrate_graph": winrate_bar_graph(prepare_winrate_data(df))
    }

def _render_error(message: str, status_code: int = 500) -> str:
    """Render error message with traceback."""
    tb = traceback.format_exc()
    return Response(f"<pre>{message}\n\n{tb}</pre>", status=status_code, mimetype="text/html")
