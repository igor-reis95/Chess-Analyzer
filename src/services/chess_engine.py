import chess
import chess.engine

def fen_stockfish_eval(fen, depth=15, threads=4, hash_mb=64, move_time=0.2):
    board = chess.Board(fen)
    with chess.engine.SimpleEngine.popen_uci("/usr/games/stockfish") as engine:
        # Configure for low-resource usage
        engine.configure({
            "Threads": threads,
            "Hash": hash_mb,
        })
        # Get evaluation with limited time/depth
        result = engine.analyse(
            board,
            chess.engine.Limit(depth=depth, time=move_time),
        )
        return result["score"]
    