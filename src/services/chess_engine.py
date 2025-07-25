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

def get_stockfish_eval_batch(fens, depth=15, threads=1, hash_mb=16, move_time=0.1):
    with chess.engine.SimpleEngine.popen_uci("stockfish/stockfish-ubuntu-x86-64-avx2") as engine:
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
        return None
    if score == float('inf'):
        return "White mates"
    elif score == float('-inf'):
        return "Black mates"
    return f"{score / 100:.2f}"

# ---------- Core Evaluation Pipeline ----------

def evaluate_opening_position(df, fallback_cutoff=15):
    """
    Evaluate opening positions using either:
    1. division_middle when available (Lichess)
    2. Fixed move cutoff when division_middle is empty (Chess.com)
    
    Args:
        df: DataFrame containing game data
        fallback_cutoff: Move number to use when division_middle is empty
        
    Returns:
        DataFrame with added evaluation columns
    """
    # Column names
    fen_col = 'opening_fen'
    eval_raw_col = 'opening_eval_raw'
    eval_col = 'opening_eval'
    
    def get_opening_fen(row):
        """Extract FEN using best available method"""
        try:
            if not isinstance(row.get('moves_split'), list):
                return None
                
            # Try Lichess division_middle first
            if pd.notna(row.get('division_middle')):
                division = int(row['division_middle'])
                moves = row['moves_split'][:division]
                source = 'division_middle'
            # Fallback to fixed cutoff
            else:
                cutoff = min(fallback_cutoff, len(row['moves_split']))
                if cutoff < fallback_cutoff:  # Game too short
                    return None
                moves = row['moves_split'][:cutoff]
                source = f'move_{fallback_cutoff}'
                
            fen = convert_moves_to_fen(' '.join(moves))
            return (fen, source) if fen else None
            
        except Exception as e:
            logging.warning(f"Error extracting opening FEN: {e}")
            return None

    # Extract FENs and their sources
    fen_results = df.apply(get_opening_fen, axis=1)
    df[fen_col] = fen_results.apply(lambda x: x[0] if x else None)
    df['opening_eval_source'] = fen_results.apply(lambda x: x[1] if x else None)
    
    # Batch evaluate with Stockfish
    df[eval_raw_col] = get_stockfish_eval_batch(df[fen_col].tolist())
    
    # Format evaluations
    df[eval_col] = (
        df[eval_raw_col]
        .dropna()
        .apply(format_evaluation)
        .apply(get_readable_eval)
    )
    
    return df

# ---------- Apply to DataFrame ----------

def run_evaluation_pipeline(df):
    # Pre-split moves once for efficiency
    df['moves_split'] = df['moves'].apply(lambda x: x.split() if isinstance(x, str) else [])

    # Evaluate opening and middlegame stages
    df = evaluate_opening_position(df)
    df = evaluate_opening_position(df)

    return df
