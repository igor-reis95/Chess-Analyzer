import io
from datetime import datetime
import pandas as pd
import chess
import chess.pgn
import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# ----------------------
# Collect chess.com user data
# ----------------------
def fetch_user_profile(username):
    url = f"https://api.chess.com/pub/player/{username}"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.json()

def fetch_user_stats(username):
    url = f"https://api.chess.com/pub/player/{username}/stats"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.json()

def extract_rating_data(stats, mode):
    mode_data = stats.get(f"chess_{mode}", {})
    last = mode_data.get("last", {})
    record = mode_data.get("record", {})
    rating = last.get("rating", 0)
    games = sum(record.get(key, 0) for key in ["win", "draw", "loss"])
    return {"rating": rating, "games": games, "prog": 0}

def convert_to_milliseconds(df):
    # Convert Chess.com timestamps (s → ms)
    df['createdAt'] = df['createdAt'] * 1000
    df['lastMoveAt'] = df['lastMoveAt'] * 1000
    return df

def build_user_data(profile, stats):
    return {
        "id": profile.get("player_id"),
        "username": profile.get("username"),
        "perfs": {
            mode: extract_rating_data(stats, mode)
            for mode in ["bullet", "blitz", "rapid"]
        } | {
            "classical": {"rating": 0, "games": 0, "prog": 0},
            "puzzle": {"rating": 0, "games": 0, "prog": 0},
        },
        "createdAt": profile.get("joined") * 1000,
        "seenAt": profile.get("last_online") * 1000,
        "playTime": {"total": None, "tv": None},
        "url": profile.get("url")
    }

def collect_user_data(username):
    profile = fetch_user_profile(username)
    stats = fetch_user_stats(username)
    return build_user_data(profile, stats)

# ----------------------
# Collect chess.com games data
# ----------------------

def fetch_games_chesscom(username, target_count, time_class):
    ARCHIVES_URL = f"https://api.chess.com/pub/player/{username}/games/archives"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    resp = requests.get(ARCHIVES_URL, headers=headers, timeout=60)
    resp.raise_for_status()
    archives = resp.json().get("archives", [])

    selected = []
    for url in reversed(archives):
        if len(selected) >= target_count:
            break
        r = requests.get(url, headers=headers, timeout=60)
        r.raise_for_status()
        for game in r.json().get("games", []):
            if time_class and game.get("time_class") != time_class:
                continue
            if not game.get("rated", False):
                continue
            if game.get("rules") != "chess":
                continue
            selected.append(game)
            if len(selected) >= target_count:
                break
    return selected

def translate_result(game_status):
    if not game_status:
        return None
    status = game_status.lower()
    if 'resignation' in status or 'abandoned' in status:
        return 'resignation'
    elif 'on time' in status:
        return 'outoftime'
    elif 'checkmate' in status:
        return 'mate'
    elif 'drawn' in status:
        return 'draw'
    else:
        return None

def pgn_str_to_json(pgn_str):
    pgn = io.StringIO(pgn_str)
    game = chess.pgn.read_game(pgn)
    metadata = dict(game.headers)
    moves = []
    node = game
    while not node.is_end():
        next_node = node.variation(0)
        moves.append(node.board().san(next_node.move))
        node = next_node
    return {
        "metadata": metadata,
        "moves": moves,
        "result": metadata.get("Result", "")
    }

def datetime_str_to_unix(date_str, time_str):
    try:
        return int(datetime.strptime(f"{date_str} {time_str}", "%Y.%m.%d %H:%M:%S").timestamp()) * 1000
    except Exception:
        return None
    
def convert_moves(move_string):
    """Convert chess.com UCI moves to standard algebraic notation"""
    if not move_string:
        return ""
    
    board = chess.Board()
    san_moves = []
    
    for uci_move in move_string.split():
        try:
            move = chess.Move.from_uci(uci_move)
            san_moves.append(board.san(move))
            board.push(move)
        except ValueError:
            # Fallback for invalid moves
            san_moves.append(uci_move)
    
    return " ".join(san_moves)

def eco_to_opening(eco_code: str) -> str:
    """Returns the first opening name for a given ECO code."""
    eco_df = pd.read_csv("data/opening_ecos.csv")
    eco_mapping = eco_df.drop_duplicates("eco").set_index("eco")["name"].to_dict()
    return eco_mapping.get(eco_code, "Unknown Opening")

def transform_game(game):
    try:
        pgn_data = pgn_str_to_json(game['pgn'])

        game_id = game['url'].split("/")[-1]
        created_at = datetime_str_to_unix(pgn_data['metadata'].get('UTCDate', ''), pgn_data['metadata'].get('UTCTime', ''))
        last_move_at = datetime_str_to_unix(pgn_data['metadata'].get('EndDate', ''), pgn_data['metadata'].get('EndTime', ''))
        game_status = translate_result(pgn_data['metadata'].get('Termination', ''))
        raw_moves = " ".join(pgn_data['moves']) # moves in different nottation from lichess
        moves = convert_moves(raw_moves)
        result = pgn_data['metadata'].get('Result', '')
        game_winner = 'white' if result == "1-0" else 'black' if result == "0-1" else None
        opening_eco = pgn_data['metadata'].get('ECO', '')
        opening_name = eco_to_opening(opening_eco)
        clock_initial = pgn_data['metadata'].get('TimeControl', '').split("+")[0]
        time_control = pgn_data['metadata'].get('TimeControl', '')
        clock_increment = time_control.split("+")[1] if "+" in time_control else None

        return {
            'id': game_id,
            'rated': game.get('rated'),
            'variant': "standard",
            'speed': game.get('time_class'),
            'time_control_with_increment': game.get('time_control'),
            'perf': None,
            'createdAt': created_at,
            'lastMoveAt': last_move_at,
            'status': game_status,
            'source': 'chess.com',
            'players': {
                'white': {
                    'user': {
                        'name': pgn_data['metadata'].get('White'),
                        'id': game['white']['@id'].split('player/')[-1]
                    },
                    'rating': pgn_data['metadata'].get('WhiteElo'),
                    'ratingDiff': None
                },
                'black': {
                    'user': {
                        'name': pgn_data['metadata'].get('Black'),
                        'id': game['black']['@id'].split('player/')[-1]
                    },
                    'rating': pgn_data['metadata'].get('BlackElo'),
                    'ratingDiff': None
                }
            },
            'fullId': None,
            'winner': game_winner,
            'opening': {
                'eco': opening_eco,
                'name': opening_name,
                'ply': None
            },
            'moves': moves,
            'clocks': [],
            'clock': {
                'initial': clock_initial,
                'increment': clock_increment,
                'TotalTime': None
            },
            'division': {
                'middle': None,
                'end': None
            }
        }
    except Exception as e:
        print(f"Error transforming game: {e}")
        return None

def get_games(username, target_count, time_class):
    raw_games = fetch_games_chesscom(username, target_count, time_class)
    transformed_games = []
    for game in raw_games:
        transformed = transform_game(game)
        if transformed is not None:
            transformed_games.append(transformed)
    return transformed_games