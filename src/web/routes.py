"""Chess Game Analysis Web Application.

This module provides the main Flask application for analyzing chess game data.
It handles user submissions, processes game data through various services,
and renders visualizations of the analysis results.

Key Components:
- Routes for form submission, report viewing, and CSV downloads
- Data processing pipeline for chess games and user stats
- Visualization and insight generation
- Error handling and logging
"""

import os
import re
import io
import logging
import json
import time
import uuid
from functools import wraps
from typing import Any, Dict, Tuple, Union

import pandas as pd
import psycopg2
from flask import make_response, redirect, render_template, request, url_for

from src.services.analysis import calculate_advantage_stats, prepare_winrate_data
import src.services.data_insights as insights
import src.services.data_io as data_io
import src.services.data_viz as viz
from src.services.game_processor import GameProcessor
from src.services.user_processor import UserProcessor
from ..webapp import app

# Constants
MAX_GAMES_LIMIT = 1000
GAMES_TABLE_PREVIEW = 30
DATABASE_URL = os.getenv("database_url")
REPORT_CONTEXT_CACHE = {}  # Used to cache report data in memory

logger: logging.Logger = logging.getLogger(__name__)


def log_execution_time(func):
    """Decorator to log function execution time with arguments and result status.
    
    Args:
        func: The function to be decorated
        
    Returns:
        The wrapped function with logging capability
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time = time.perf_counter()

        try:
            result = func(*args, **kwargs)
            elapsed = time.perf_counter() - start_time
            logger.info(
                "%s executed in %.4fs | Args: %s... | Kwargs: %s...",
                func.__name__,
                elapsed,
                str(args)[:100],
                str(kwargs)[:100]
            )
            return result
        except Exception as e:
            logger.error(
                "Error in %s after %.4fs: %s",
                func.__name__,
                time.perf_counter()-start_time,
                str(e),
                exc_info=True
            )
            raise

    return wrapper


@app.before_request
def before_request() -> None:
    """Start timer before each request to measure processing time."""
    request.start_time = time.time()


@app.after_request
def after_request(response) -> Any:
    """Log request processing time after each request completes.
    
    Args:
        response: The Flask response object
        
    Returns:
        The original response with timing header
    """
    execution_time = time.time() - request.start_time
    app.logger.info("Request took %.2f seconds", execution_time)
    response.headers["X-Execution-Time"] = f"{execution_time:.2f}s"
    return response


@app.route("/", methods=["GET", "POST"])
def index() -> Union[str, Any]:
    """Handle root route with form submission and display.
    
    Returns:
        Rendered template or redirect response
    """
    if request.method == "GET":
        return _show_form()
    return _handle_form_submission(request.form)


@app.route("/report/<slug>")
def report_view(slug: str) -> Union[str, Any]:
    """Display analysis report for a given slug.
    
    Args:
        slug: Unique identifier for the report
        
    Returns:
        Rendered report template or error page
    """
    try:
        # Try to get cached data first
        if slug in REPORT_CONTEXT_CACHE:
            context = REPORT_CONTEXT_CACHE.pop(slug)  # Use once and clear
            return render_template("result.html", **context, report_slug=slug)

        # Fall back to database lookup
        try:
            conn = psycopg2.connect(DATABASE_URL)
            report = data_io.get_report_by_slug(conn, slug)

            if report is None:
                return _render_error("Report not found", 404)

            games_data = data_io.get_games_by_report_id(conn, report["id"])
            user_data = data_io.get_user_by_report_id(conn, report["id"])

            params = {
                "username": report["username"],
                "max_games": report["number_of_games"],
                "perf_type": report["time_control"],
                "platform": report["platform"]
            }

            context = _generate_template_context(params, games_data, user_data)
            return render_template("result.html", **context, report_slug=slug)

        except psycopg2.Error as e:
            logger.error("Database error: %s", str(e))
            return _render_error("Database connection failed", 500)
        finally:
            if 'conn' in locals() and conn:
                conn.close()

    except Exception as e: # pylint: disable=broad-exception-caught
        logger.exception("Unexpected error in report_view for slug %s", slug)
        return _render_error(f"An unexpected error occurred: {e}", 500)


@app.route("/download_csv/<slug>")
def download_csv(slug: str) -> Union[Any, str]:
    """Generate CSV download for a report.
    
    Args:
        slug: Unique identifier for the report
        
    Returns:
        CSV file response or error page
    """
    try:
        # Try cache first
        if slug in REPORT_CONTEXT_CACHE:
            context = REPORT_CONTEXT_CACHE[slug]
            df = pd.DataFrame(context['games_data'])
        else:
            # Fall back to database
            try:
                conn = psycopg2.connect(DATABASE_URL)
                report = data_io.get_report_by_slug(conn, slug)

                if not report:
                    return _render_error("Report not found", 404)

                games_data = data_io.get_games_by_report_id(conn, report["id"])
                df = pd.DataFrame(games_data)

            except psycopg2.Error as e:
                logger.error("Database error: %s", str(e))
                return _render_error("Database operation failed", 500)
            finally:
                if 'conn' in locals() and conn:
                    conn.close()

        # Prepare CSV response
        output = io.StringIO()
        df.drop(columns=['report_id'], inplace=True, errors='ignore')
        df.to_csv(output, index=False)
        output.seek(0)

        response = make_response(output.getvalue())
        response.headers["Content-Disposition"] = f"attachment; filename=chess_report_{slug}.csv"
        response.headers["Content-type"] = "text/csv"
        return response

    except Exception as e: # pylint: disable=broad-exception-caught
        logger.exception("CSV generation failed for slug %s: %s", slug, str(e))
        return _render_error(f"Could not generate CSV: {str(e)}", 500)


def _show_form() -> str:
    """Render the empty input form.
    
    Returns:
        Rendered form template
    """
    return render_template("form.html")


def _handle_form_submission(form_data: Dict) -> Union[str, Any]:
    """Process submitted form data and return results or errors.
    
    Args:
        form_data: Dictionary of form submission data
        
    Returns:
        Redirect to report or error page
    """
    try:
        params = _validate_inputs(form_data)
        slug = create_and_store_report(params)
        return _redirect_to_report(slug)

    except ValueError as e:
        logger.warning("Validation failed: %s", str(e))
        return _render_error(f"Invalid input: {str(e)}", 400)
    except Exception as e: # pylint: disable=broad-exception-caught
        logger.exception("Unexpected error during form submission")
        return _render_error(f"Processing error: {str(e)}", 500)


def _validate_inputs(form_data: Dict) -> Dict:
    """Validate and sanitize form inputs.
    
    Args:
        form_data: Raw form submission data
        
    Returns:
        Validated and sanitized parameters
        
    Raises:
        ValueError: If any validation fails
    """
    username = form_data.get("username", "").strip()
    max_games = int(form_data.get("max_games", 0))
    platform = form_data.get("platform", "lichess").lower()

    if not re.match(r"^[\w-]{3,20}$", username):
        raise ValueError("Username: 3-20 chars (letters, numbers, _-)")

    if platform not in ["lichess.org", "chess.com"]:
        raise ValueError("Platform must be 'lichess.org' or 'chess.com'")

    if max_games > MAX_GAMES_LIMIT:
        raise ValueError(f"Maximum {MAX_GAMES_LIMIT} games allowed")

    return {
        "username": username,
        "max_games": min(max_games, MAX_GAMES_LIMIT),
        "perf_type": form_data.get("perf_type", "blitz"),
        "platform": platform
    }


@log_execution_time
def create_and_store_report(params: Dict) -> str:
    """Create and store a new analysis report.
    
    Args:
        params: Validated report parameters
        
    Returns:
        Unique report slug
        
    Raises:
        RuntimeError: If any step fails
    """
    step_timings = {}
    total_start = time.perf_counter()

    try:
        # Database connection
        with psycopg2.connect(DATABASE_URL) as conn:
            step_start = time.perf_counter()
            slug = uuid.uuid4().hex[:8]

            # Data processing
            game_processor, user_processor = _fetch_and_prepare_data(params)
            step_timings["data_processing"] = time.perf_counter() - step_start

            # Save report metadata
            report_id = data_io.save_report_data(
                conn,
                username=params["username"],
                number_of_games=params["max_games"],
                time_control=params["perf_type"],
                platform=params["platform"],
                slug=slug
            )

            # Prepare and save data
            game_df = game_processor.get_dataframe()
            game_df["report_id"] = report_id
            data_io.save_processed_game_data(conn, game_df)

            user_df = user_processor.get_dataframe()
            user_df["report_id"] = report_id
            data_io.save_processed_user_data(conn, user_df)

            # Create and cache context
            context = _generate_template_context(
                params,
                game_df,
                user_df.iloc[0].to_dict()
            )
            REPORT_CONTEXT_CACHE[slug] = context

            # Log performance
            total_time = time.perf_counter() - total_start
            data_io.save_report_execution_time(conn, report_id, round(total_time, 3))

            logger.info(
                "Report created for %s in %.3fs (games: %d)",
                params["username"],
                total_time,
                len(game_df)
            )

            return slug

    except Exception as e:
        logger.error("Failed to create report: %s", str(e))
        raise RuntimeError(f"Report creation failed: {str(e)}") from e


def _fetch_and_prepare_data(params: Dict) -> Tuple[GameProcessor, UserProcessor]:
    """Fetch and process game and user data.
    
    Args:
        params: Report parameters
        
    Returns:
        Tuple of (GameProcessor, UserProcessor) instances
    """
    timings = {}

    # Process games
    game_start = time.perf_counter()
    game_processor = GameProcessor(
        username=params["username"],
        max_games=params["max_games"],
        perf_type=params["perf_type"],
        platform=params["platform"]
    )
    game_processor.run_all()
    timings["game_processing"] = time.perf_counter() - game_start

    # Process user
    user_start = time.perf_counter()
    user_processor = UserProcessor(
        username=params["username"],
        platform=params["platform"]
    )
    user_processor.fetch_user_data()
    user_processor.process_user_data()
    timings["user_processing"] = time.perf_counter() - user_start

    logger.info(
        "Data processing completed in %.2fs (games: %.2fs, user: %.2fs)",
        sum(timings.values()),
        timings["game_processing"],
        timings["user_processing"]
    )

    return game_processor, user_processor


def _redirect_to_report(slug: str) -> Any:
    """Redirect to report view page.
    
    Args:
        slug: Report identifier
        
    Returns:
        Flask redirect response
    """
    return redirect(url_for("report_view", slug=slug))


def _generate_template_context(
    params: Dict,
    df: pd.DataFrame,
    user_data: Dict
) -> Dict:
    """Prepare template context with all required data.
    
    Args:
        params: Report parameters
        df: Processed games DataFrame
        user_data: Processed user data
        
    Returns:
        Complete template context dictionary
    """
    player_data = calculate_advantage_stats(df)

    with open("data/lichess_analysis_snapshot.json", "r", encoding='utf-8') as f:
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


def _get_visualizations(
    df: pd.DataFrame,
    player_data: Dict,
    lichess_data: Dict
) -> Dict:
    """Generate visualization data for templates.
    
    Args:
        df: Processed games DataFrame
        player_data: Calculated player statistics
        lichess_data: Reference statistics
        
    Returns:
        Dictionary of visualization data
    """
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


def _get_insights(
    df: pd.DataFrame,
    player_data: Dict,
    lichess_data: Dict
) -> Dict:
    """Generate insight data for templates.
    
    Args:
        df: Processed games DataFrame
        player_data: Calculated player statistics
        lichess_data: Reference statistics
        
    Returns:
        Dictionary of insight data
    """
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
            "when_ahead": insights.insight_conversion_stat(
                player_data, lichess_data, "pct_won_when_ahead"),
            "when_behind": insights.insight_conversion_stat(
                player_data, lichess_data, "pct_won_or_drawn_when_behind"),
        }
    }


def _render_error(error_message: str, status_code: int = 400) -> Tuple[str, int]:
    """Render error page with consistent styling.
    
    Args:
        error_message: Description of the error
        status_code: HTTP status code
        
    Returns:
        Tuple of (rendered template, status code)
    """
    logger.error("Rendering error page: %s (code %d)", error_message, status_code)
    return render_template('error.html', error_message=error_message), status_code


@app.errorhandler(404)
def page_not_found() -> Tuple[str, int]:
    """Handle 404 errors.
    
    Args:
        error: The error object
        
    Returns:
        Error page response
    """
    return _render_error("Page not found", 404)


@app.errorhandler(500)
def internal_server_error() -> Tuple[str, int]:
    """Handle 500 errors.
    
    Args:
        error: The error object
        
    Returns:
        Error page response
    """
    return _render_error("Internal server error", 500)


@app.route('/error')
def show_error() -> Tuple[str, int]:
    """Display custom error message from query parameter.
    
    Returns:
        Error page response
    """
    message = request.args.get('message', 'An unknown error occurred')
    return _render_error(message)
