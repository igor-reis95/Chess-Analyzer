"""Entry point for the Chess Analyzer web application.

This script starts the Flask development server when run directly.
It imports the Flask `app` instance from the web application module
and enables debug mode and auto-reloading for development purposes.
"""

from src.webapp import app, setup_logging

if __name__ == "__main__":
    setup_logging()
    app.run(debug=True, use_reloader = True)
