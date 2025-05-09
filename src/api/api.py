import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("lichess_token")
headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/x-ndjson"
}

def get_games(username, max, perfType, color=None):
    """Função para buscar jogos da API do Lichess"""
    params = {
        "max": max,
        "perfType": perfType,
        "color": color,
        "rated": True,
        "accuracy": True,
        "division": True,
        "opening": True
    }
    url = f"https://lichess.org/api/games/user/{username}"
    response = requests.get(url, headers=headers, params=params, stream=True)

    games_list = []
    for line in response.iter_lines():
        if line:
            game = json.loads(line.decode("utf-8"))
            games_list.append(game)
    
    return games_list
