import pandas as pd
import chess
import chess.engine
import logging

logging.basicConfig(level=logging.INFO)

# ---------- Utility Functions ----------

def convert_moves_to_fen(moves_str):
    if moves_str is None or not isinstance(moves_str, str):
        return None
    board = chess.Board()
    moves = moves_str.split()
    for move in moves:
        try:
            board.push_san(move)
        except ValueError as e:
            logging.warning(f"Invalid move: {move} — {e}")
            return None
    return board.fen()

def get_stockfish_eval_batch(fens, depth=20, threads=4, hash_mb=16, move_time=0.2):
    with chess.engine.SimpleEngine.popen_uci("/usr/games/stockfish") as engine:
        engine.configure({
            "Threads": threads,
            "Hash": hash_mb,
        })
        def evaluate(fen):
            if fen is None:
                return None
            board = chess.Board(fen)
            try:
                result = engine.analyse(board, chess.engine.Limit(depth=depth, time=move_time))
                return result["score"]
            except Exception as e:
                logging.warning(f"Error analyzing FEN: {fen} — {e}")
                return None
        return [evaluate(fen) for fen in fens]

def format_evaluation(score):
    if score is None:
        return None
    if score.is_mate():
        mate_in = score.relative.mate()
        return float('inf') if mate_in > 0 else float('-inf')
    return score.white().score()

def get_readable_eval(score):
    if score is None:
        return "No evaluation"
    if score == float('inf'):
        return "White mates"
    elif score == float('-inf'):
        return "Black mates"
    return f"{score / 100:.2f}"

# ---------- Core Evaluation Pipeline ----------

def evaluate_position_from_stage(df, stage_name):
    if stage_name == 'opening':
        division_column = 'division_middle'
    else:
        division_column = 'division_end'
    fen_col = f"{stage_name}_fen"
    eval_raw_col = f"{stage_name}_eval_raw"
    eval_col = f"{stage_name}_eval"

    # Generate FENs
    def extract_fen(row):
        if pd.isna(row.get(division_column)) or not isinstance(row.get('moves_split'), list):
            return None
        try:
            division = int(row[division_column])
            moves = row['moves_split'][:division]
            return convert_moves_to_fen(' '.join(moves))
        except Exception as e:
            logging.warning(f"Error extracting FEN for {stage_name}: {e}")
            return None

    df[fen_col] = df.apply(extract_fen, axis=1)

    # Evaluate with Stockfish
    df[eval_raw_col] = get_stockfish_eval_batch(df[fen_col].tolist())

    # Format evaluation
    df[eval_col] = df[eval_raw_col].apply(format_evaluation).apply(get_readable_eval)

    return df

# ---------- Apply to DataFrame ----------

def run_evaluation_pipeline(df):
    # Pre-split moves once for efficiency
    df['moves_split'] = df['moves'].apply(lambda x: x.split() if isinstance(x, str) else [])

    # Evaluate opening and middlegame stages
    df = evaluate_position_from_stage(df, stage_name="opening")
    df = evaluate_position_from_stage(df, stage_name="middlegame")

    return df
