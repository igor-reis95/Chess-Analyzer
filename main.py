# main.py
from src.api import get_games
from src.data_processing import save_games_to_json
from src import app

if __name__ == "__main__":
    app.run(debug=True)
