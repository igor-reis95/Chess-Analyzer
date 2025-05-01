# main.py
from src.api import get_games
from src.data_processing import flatten_game_data, save_df_to_csv
from src import app

def run_pipeline(username: str, max_games: int, perf_type: str):
    games_list = get_games(username, max_games, perf_type)
    df = flatten_game_data(games_list)
    save_df_to_csv(df, username)
    return df

if __name__ == "__main__":
    app.run(debug=True)
