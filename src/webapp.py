from flask import Flask

app = Flask(
    __name__,
    template_folder="web/templates",  # Custom path to templates
    static_folder="web/static"        # Custom path to static files
)

# Import routes after creating app to avoid circular imports
from src.web import routes