from src import app
from flask import request, render_template
from src.api import get_games
from src.data_processing import save_games_to_json

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        username = request.form["username"]
        max_games = int(request.form.get("max_games", 50))
        perf_type = request.form.get("perf_type", "blitz")
        color = request.form.get("color", None)

        games = get_games(username, max_games, perf_type, color)

        return render_template("result.html", username=username, count=len(games))

    return render_template("form.html")

@app.route("/buscar", methods=["POST"])
def buscar():
    username = request.form["username"]
    games_list = get_games(username, max=50, perfType="Bullet")
    save_games_to_json(games_list, username)
    return f"Jogos de {username} salvos com sucesso"