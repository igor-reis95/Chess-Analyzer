"""Chess Game Analysis Web Application.

This module provides the main Flask route for analyzing chess game data.
It handles user submissions, processes game data through various services,
and renders visualizations of the analysis results.
"""
# pylint: skip-file
import os
import re
import logging
import json
import time
import uuid
import psycopg2
from functools import wraps
from flask import render_template, request, redirect, url_for
from src.services.analysis import prepare_winrate_data, calculate_advantage_stats
import src.services.data_viz as viz
import src.services.data_io as io
import src.services.data_insights as insights
from src.services.game_processor import GameProcessor
from src.services.user_processor import UserProcessor
from ..webapp import app

# Constant values and logger creation
MAX_GAMES_LIMIT = 900
GAMES_TABLE_PREVIEW = 30
DATABASE_URL = os.getenv("database_url")

logger = logging.getLogger(__name__)

def log_execution_time(func):
    """Decorator to log function execution time"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        start_time = time.perf_counter()
        
        try:
            result = func(*args, **kwargs)
            elapsed = time.perf_counter() - start_time
            logger.info(
                f"{func.__name__} executed in {elapsed:.4f}s | "
                f"Args: {str(args)[:100]}... | "
                f"Kwargs: {str(kwargs)[:100]}..."
            )
            return result
        except Exception as e:
            logger.error(
                f"Error in {func.__name__} after {time.perf_counter()-start_time:.4f}s: {str(e)}",
                exc_info=True
            )
            raise

    return wrapper

@app.route("/", methods=["GET", "POST"])
def index():
    """Handle root route with minimal logic."""
    if request.method == "GET":
        return _show_form()
    return _handle_form_submission(request.form)

@app.route("/report/<slug>")
def report_view(slug):
    conn = psycopg2.connect(DATABASE_URL)
    try:
        report = io.get_report_by_slug(conn, slug)
        if report is None:
            return _render_error("Report not found", status_code=404)

        report_id = report["id"]
        params = {
            "username": report["username"],
            "max_games": report["number_of_games"],
            "perf_type": report["time_control"]
        }
        games_df = io.get_games_by_report_id(conn, report_id)
        user_data = io.get_user_by_report_id(conn, report_id)

        context = _generate_template_context(params, games_df, user_data)

        return render_template(
            "result.html",
            **context
        )
    finally:
        conn.close()

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
        slug = create_and_store_report(params)
        return _redirect_to_report(slug)

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
        "perf_type": form_data.get("perf_type", "blitz")
    }

@log_execution_time
def _fetch_and_prepare_data(params: dict) -> tuple:
    """Fetch and process data. No DB actions."""
    timings = {}

    # GameProcessor
    game_start = time.perf_counter()
    game_processor = GameProcessor(
        username=params["username"],
        max_games=params["max_games"],
        perf_type=params["perf_type"]
    )
    game_processor.run_all()
    timings["game_processing"] = time.perf_counter() - game_start

    # UserProcessor
    user_start = time.perf_counter()
    user_processor = UserProcessor(username=params["username"])
    user_processor.fetch_user_data()
    user_processor.process_user_data()
    timings["user_processing"] = time.perf_counter() - user_start

    # Log
    logger.info(
        f"Performance Breakdown:\n"
        f"Game Processing: {timings['game_processing']:.2f}s\n"
        f"User Processing: {timings['user_processing']:.2f}s\n"
        f"Game/User Ratio: {timings['game_processing']/timings['user_processing']:.1f}x"
    )

    return game_processor, user_processor

def create_and_store_report(params: dict) -> str:

    conn = psycopg2.connect(DATABASE_URL)

    slug = uuid.uuid4().hex[:8]

    # Process data
    game_processor, user_processor = _fetch_and_prepare_data(params)

    # Insert report and get ID
    report_id = io.save_report_data(
        conn,
        username=params["username"],
        number_of_games=params["max_games"],
        time_control=params["perf_type"],
        slug=slug
    )

    # Add report_id to the DataFrames
    game_df = game_processor.get_dataframe()
    game_df["reports_id"] = report_id

    user_df = user_processor.get_dataframe()
    user_df["reports_id"] = report_id

    # Store data
    io.save_processed_game_data(conn, game_df)
    io.save_df_to_csv(game_df, params["username"])
    io.save_processed_user_data(conn, user_df)

    return slug  # Used to redirect to /report/{slug}


# ----------------------
# Presentation Layer
# ----------------------
def _redirect_to_report(slug: str):
    """Redirect user to their report page."""
    return redirect(url_for("report_view", slug=slug))


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
        **_get_insights(df, player_data, lichess_data),
        "user_data": user_data
    }

@log_execution_time
def _get_visualizations(df, player_data, lichess_data) -> dict:
    """Generate all visualization outputs."""
    return {
        "winrate_graph_viz": viz.winrate_bar_graph(prepare_winrate_data(df)),
        "eval_on_opening_viz": viz.plot_eval_on_opening(df),
        "openings_viz": {
            "overall": viz.plot_opening_stats(df, "overall"),
            "white": viz.plot_opening_stats(df, "white"),
            "black": viz.plot_opening_stats(df, "black"),
        },
        "lichess_openings_viz": {
            "popular": viz.lichess_popular_openings(lichess_data),
            "successful_white": viz.lichess_successful_openings(lichess_data, "white"),
            "successful_black": viz.lichess_successful_openings(lichess_data, "black"),
        },
        "conversion_viz": {
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

def _get_insights(df, player_data, lichess_data) -> dict:
    """Generate all visualization outputs."""
    winrate_data = prepare_winrate_data(df)
    return {
        "winrate_graph_insights": {
            "overall": insights.winrate_graph_insights(winrate_data, "overall"),
            "white": insights.winrate_graph_insights(winrate_data, "white"),
            "black": insights.winrate_graph_insights(winrate_data, "black")
        },
        "openings_insights": {
            "overall": insights.opening_stats_insights(df, "overall"),
            "white": insights.opening_stats_insights(df, "white"),
            "black": insights.opening_stats_insights(df, "black")
        },
        "eval_on_opening_insights": {
            "overall": insights.eval_per_opening_insights(df, "overall"),
            "white": insights.eval_per_opening_insights(df, "white"),
            "black": insights.eval_per_opening_insights(df, "black")
        },
        "lichess_openings_insights": {
            "popular_insights": insights.lichess_popular_openings_insights(),
            "successful_white": insights.lichess_successful_openings_insights("white"),
            "successful_black": insights.lichess_successful_openings_insights("black")
        },
        "conversion_insights": {
            "when_ahead": insights.insight_conversion_stat(player_data, lichess_data, "pct_won_when_ahead"),
            "when_behind": insights.insight_conversion_stat(player_data, lichess_data, "pct_won_or_drawn_when_behind"),
        }
    }

def _render_error(error_message: str, status_code: int = 400) -> str:
    """Render error page with consistent styling."""
    logger.error(f"Rendering error page: {error_message}")
    return render_template(
        'error.html',
        error_message=error_message
    ), status_code

# Example usage in your route:
@app.errorhandler(404)
def page_not_found():
    return _render_error("Page not found", 404)

@app.errorhandler(500)
def internal_server_error():
    return _render_error("Internal server error", 500)

@app.route('/error')
def show_error():
    message = request.args.get('message', 'An unknown error occurred')
    return _render_error(message)