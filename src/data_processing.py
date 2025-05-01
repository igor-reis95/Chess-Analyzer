# src/data_processing.py
import json
import os

def save_games_to_json(games_list, username):
    """Função para salvar os jogos em um arquivo .json"""
    filename = os.path.join("data", f"{username}_games.json")
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(games_list, f, indent=2, ensure_ascii=False)
    print(f"Jogos salvos em: {filename}")