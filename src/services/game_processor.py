# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

from src.api.api import get_games
from src.services.data_io import save_games_to_json, save_df_to_csv
from src.services.flatten import flatten_game_data
from src.services.post_process import post_process

class GameProcessor:
    def __init__(self, username, max_games, perf_type, color):
        self.username = username
        self.max_games = max_games
        self.perf_type = perf_type
        self.color = color
        self.games = None
        self.df_flat = None
        self.df_processed = None

    def fetch_games(self):
        self.games = get_games(self.username, self.max_games, self.perf_type, self.color)

    def save_raw_data(self):
        save_games_to_json(self.games, self.username)

    def flatten_games(self):
        self.df_flat = flatten_game_data(self.games)

    def post_process_games(self):
        self.df_processed = post_process(self.df_flat, self.username)
        save_df_to_csv(self.df_processed, self.username)

    def get_dataframe(self):
        return self.df_processed

    def run_all(self):
        self.fetch_games()
        self.save_raw_data()
        self.flatten_games()
        self.post_process_games()
