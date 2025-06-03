"""Main code to execute flask and the logging system"""
import logging
from flask import Flask

app = Flask(
    __name__,
    template_folder="web/templates",
    static_folder="web/static"
)

def setup_logging():
    if len(logging.root.handlers) > 0:
        return

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler('app.log')
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(file_handler)
    logging.root.addHandler(console_handler)

    # Configure werkzeug logger level only (DO NOT add handlers)
    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(logging.INFO)

# Import routes after creating app to avoid circular imports
from src.web import routes
