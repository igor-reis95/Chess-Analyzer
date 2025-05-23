# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
from src.webapp import app

if __name__ == "__main__":
    app.run(debug=True, use_reloader=True,)
