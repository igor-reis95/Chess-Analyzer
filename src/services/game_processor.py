import pandas as pd
from src.api.api import get_games
from src.services.data_io import save_games_to_json, save_df_to_csv
from src.services.flatten import flatten_game_data

class GameProcessor:
    def __init__(self, username, max_games=50, perf_type="blitz", color=None):
        self.username = username
        self.max_games = max_games
        self.perf_type = perf_type
        self.color = color
        self.games = None
        self.df_raw = None
        self.df_flat = None

    def fetch_games(self):
        self.games = get_games(self.username, self.max_games, self.perf_type, self.color)

    def save_raw_data(self):
        save_games_to_json(self.games, self.username)

    def flatten_games(self):
        self.df_flat = flatten_game_data(self.games)
        save_df_to_csv(self.df_flat, self.username)

    def build_winner_column(self):
        """Adiciona coluna com o nome do vencedor da partida"""
        self.df_flat["winner_username"] = self.df_flat.apply(self._extract_winner_username, axis=1)

    def _extract_winner_username(self, row):
        if row["winner"] == "white":
            return row["white_name"]
        elif row["winner"] == "black":
            return row["black_name"]
        return None

    def get_user_winrate(self):
        """Calcula a taxa de vitórias do usuário"""
        user_games = self.df_flat[
            (self.df_flat["white_name"] == self.username) | (self.df_flat["black_name"] == self.username)
        ]
        wins = (user_games["winner_username"] == self.username).sum()
        total = len(user_games)
        return {"wins": wins, "total": total, "winrate": round(wins / total, 2) if total else 0}

    def get_summary_stats(self):
        # Retorna outros dados para exibição na interface
        return {
            "game_count": len(self.df_flat),
            "winrate": self.get_user_winrate(),
            "perf_type": self.perf_type,
            "color_filter": self.color
        }

    def run_all(self):
        """Executa todo o pipeline"""
        self.fetch_games()
        self.save_raw_data()
        self.flatten_games()
        self.build_winner_column()