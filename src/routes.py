from src import app
from flask import request, render_template
from src.api import get_games
from src.data_processing import save_games_to_json

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/buscar", methods=["POST"])
def buscar():
    username = request.form["username"]
    games_list = get_games(username, max=50, perfType="Bullet")
    save_games_to_json(games_list, username)
    return f"Jogos de {username} salvos com sucesso"