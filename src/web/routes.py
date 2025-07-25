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
from flask import render_template, request, redirect, url_for, g
from src.services.analysis import prepare_winrate_data, calculate_advantage_stats
import src.services.data_viz as viz
import src.services.data_io as io
import src.services.data_insights as insights
from src.services.game_processor import GameProcessor
from src.services.user_processor import UserProcessor
from ..webapp import app

# Constant values and logger creation
MAX_GAMES_LIMIT = 1000
GAMES_TABLE_PREVIEW = 30
DATABASE_URL = os.getenv("database_url")
REPORT_CONTEXT_CACHE = {} # Used to not reconnected to the database when data is in memory

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

# How much time it takes to execute the code
@app.before_request
def before_request():
    request.start_time = time.time()

@app.after_request
def after_request(response):
    execution_time = time.time() - request.start_time
    app.logger.info(f"Request took {execution_time:.2f} seconds")
    return response

@app.route("/", methods=["GET", "POST"])
def index():
    """Handle root route with minimal logic."""
    if request.method == "GET":
        return _show_form()
    return _handle_form_submission(request.form)

@app.route("/report/<slug>")
def report_view(slug):
    try:
        # If accessing from form.html, use data in memory
        if slug in REPORT_CONTEXT_CACHE:
            context = REPORT_CONTEXT_CACHE.pop(slug)  # Use once and clear
        else:
            # Accessing report directly from DB
            try:
                conn = psycopg2.connect(DATABASE_URL)
            except:
                return _render_error("Database connection failed", 500)

            try:
                report = io.get_report_by_slug(conn, slug)
                if report is None:
                    return _render_error("Report not found", 404)

                report_id = report["id"]
                params = {
                    "username": report["username"],
                    "max_games": report["number_of_games"],
                    "perf_type": report["time_control"],
                    "platform": report["platform"]
                }

                games_data = io.get_games_by_report_id(conn, report_id)
                user_data = io.get_user_by_report_id(conn, report_id)

                context = _generate_template_context(params, games_data, user_data)
            finally:
                conn.close()

        return render_template("result.html", **context)

    except Exception:
        logger.exception("Unexpected error in report_view")
        return _render_error("An unexpected error occurred", 500)


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
    username = form_data.get("username", "").strip()
    max_games = int(form_data.get("max_games", 0))
    platform = form_data.get("platform", "lichess").lower()

    # Format checks
    if not re.match(r"^[\w-]{3,20}$", username):
        raise ValueError("Username: 3-20 chars (letters, numbers, _-)")

    if platform not in ["lichess.org", "chess.com"]:
        raise ValueError("Platform must be 'lichess.org' or 'chess.com'")

    # Business logic
    if max_games > MAX_GAMES_LIMIT:
        raise ValueError("Maximum 900 games allowed")

    return {
        "username": username,
        "max_games": min(max_games, MAX_GAMES_LIMIT),
        "perf_type": form_data.get("perf_type", "blitz"),
        "platform": platform
    }

def _fetch_and_prepare_data(params: dict) -> tuple:
    """Fetch and process data. No DB actions."""
    timings = {}

    # GameProcessor
    game_start = time.perf_counter()
    game_processor = GameProcessor(
        username=params["username"],
        max_games=params["max_games"],
        perf_type=params["perf_type"],
        platform=params["platform"]
    )
    game_processor.run_all()
    timings["game_processing"] = time.perf_counter() - game_start

    # UserProcessor
    user_start = time.perf_counter()
    user_processor = UserProcessor(username=params["username"], platform=params["platform"])
    user_processor.fetch_user_data()
    user_processor.process_user_data()
    timings["user_processing"] = time.perf_counter() - user_start

    # Log
    logger.info(
        f"Performance Breakdown:\n"
        f"Game Processing: {timings['game_processing']:.2f}s\n"
        f"User Processing: {timings['user_processing']:.2f}s\n"
    )

    return game_processor, user_processor

@log_execution_time
def create_and_store_report(params: dict) -> str:
    """Create report with detailed step timing"""
    step_timings = {}
    logger = logging.getLogger(__name__)
    
    # Start total timer
    total_start = time.perf_counter()
    
    # 1. Database connection
    step_start = time.perf_counter()
    conn = psycopg2.connect(DATABASE_URL)
    step_timings["db_connection"] = time.perf_counter() - step_start
    
    # Generate slug
    slug = uuid.uuid4().hex[:8]
    
    # 2. Data processing
    step_start = time.perf_counter()
    game_processor, user_processor = _fetch_and_prepare_data(params)
    step_timings["data_processing"] = time.perf_counter() - step_start
    
    # 3. Save report metadata
    step_start = time.perf_counter()
    report_id = io.save_report_data(
        conn,
        username=params["username"],
        number_of_games=params["max_games"],
        time_control=params["perf_type"],
        platform=params["platform"],
        slug=slug
    )
    step_timings["save_report_metadata"] = time.perf_counter() - step_start
    
    # 4. Prepare DataFrames
    step_start = time.perf_counter()
    game_df = game_processor.get_dataframe()
    game_df["report_id"] = report_id
    user_df = user_processor.get_dataframe()
    user_df["report_id"] = report_id
    step_timings["prepare_dataframes"] = time.perf_counter() - step_start
    
    # 5. Store data
    step_start = time.perf_counter()
    io.save_processed_game_data(conn, game_df)
    io.save_df_to_csv(game_df, params["username"])
    io.save_processed_user_data(conn, user_df)
    step_timings["data_storage"] = time.perf_counter() - step_start
    
    # 6. Create context
    step_start = time.perf_counter()
    user_data = user_df.iloc[0].to_dict()
    context = _generate_template_context(params, game_df, user_data)
    REPORT_CONTEXT_CACHE[slug] = context
    step_timings["context_creation"] = time.perf_counter() - step_start
    
    # Calculate total time
    total_time = time.perf_counter() - total_start
    
    # Log detailed breakdown
    logger.info(
        f"Report creation breakdown for {params['username']}:\n"
        f"1. DB Connection: {step_timings['db_connection']:.3f}s\n"
        f"2. Data Processing: {step_timings['data_processing']:.3f}s\n"
        f"3. Save Metadata: {step_timings['save_report_metadata']:.3f}s\n"
        f"4. Prepare DataFrames: {step_timings['prepare_dataframes']:.3f}s\n"
        f"5. Data Storage: {step_timings['data_storage']:.3f}s\n"
        f"6. Context Creation: {step_timings['context_creation']:.3f}s\n"
        f"Total Execution: {total_time:.3f}s"
    )

    # Save execution_time in the reports table
    io.save_report_execution_time(conn, report_id, round(total_time,3))
    
    return slug


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