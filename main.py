# main.py
from src.api import get_games
from src.data_processing import save_games_to_json

def main():
    username = "IgorSReis"  # Exemplo de nome de usuário
    max_games = 50
    perf_type = "Bullet"
    
    # Coletar os jogos da API
    games_list = get_games(username, max_games, perf_type)
    
    # Salvar os jogos em um arquivo JSON
    save_games_to_json(games_list, username)

if __name__ == "__main__":
    main()
