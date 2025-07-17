"""Main code to execute flask, dotenv and the logging system"""
import logging
from flask import Flask
from dotenv import load_dotenv # Runs dotenv for all the files
load_dotenv()

app = Flask(
    __name__,
    template_folder="web/templates",
    static_folder="web/static"
)

def setup_logging():
    """
    Configure logging for the application.

    Sets up a console handler for INFO-level logs and a file handler for WARNING and above.
    Prevents duplicate handlers if logging is already configured.
    Adjusts the Werkzeug logger to match the desired log level without adding extra handlers.

    Logs:
        - Console: INFO and above
        - File (app.log): WARNING and above
    """
    if len(logging.root.handlers) > 0:
        return

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # File handler (now includes INFO level for timing logs)
    file_handler = logging.FileHandler('app.log')
    file_handler.setLevel(logging.INFO)  # Changed from WARNING to INFO
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(file_handler)
    logging.root.addHandler(console_handler)

    # Configure werkzeug logger
    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(logging.INFO)

# Import routes after creating app to avoid circular imports
# pylint: disable=wrong-import-position
# pylint: disable=unused-import
from src.web import routes
