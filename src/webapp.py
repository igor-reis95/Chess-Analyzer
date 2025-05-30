"""Main code to execute flask and the logging system"""
import logging
from flask import Flask

app = Flask(
    __name__,
    template_folder="web/templates",  # Custom path to templates
    static_folder="web/static"        # Custom path to static files
)

def setup_logging():
    """Logging setup for the project"""
    # Clear any existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Create a formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # File handler (for your log file)
    file_handler = logging.FileHandler('app.log')
    file_handler.setLevel(logging.WARNING)  # your desired level
    file_handler.setFormatter(formatter)

    # Console handler (for terminal output)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Show INFO and above in console
    console_handler.setFormatter(formatter)

    # Add both handlers
    logging.root.addHandler(file_handler)
    logging.root.addHandler(console_handler)

    # Set werkzeug logger level to INFO and ensure it uses our console handler
    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(logging.INFO)
    werkzeug_logger.addHandler(console_handler)

# Import routes after creating app to avoid circular imports
from src.web import routes
