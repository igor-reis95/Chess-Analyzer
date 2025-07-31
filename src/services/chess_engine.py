"""
Module for chess position evaluation using Stockfish engine.

Provides functions to convert move lists to FEN, batch evaluate positions with Stockfish,
and run an evaluation pipeline over a pandas DataFrame containing chess games data.
"""

import logging
from typing import Optional, List, Union, Tuple

import chess
import chess.engine
import pandas as pd

logger = logging.getLogger(__name__)


def convert_moves_to_fen(moves_str: Optional[str]) -> Optional[str]:
    """
    Convert a string of SAN moves into a FEN string representing the final position.

    Args:
        moves_str: String containing moves in Standard Algebraic Notation (SAN),
            separated by spaces.

    Returns:
        FEN string of the final position, or None if input is invalid or moves are invalid.
    """
    if moves_str is None or not isinstance(moves_str, str):
        logger.debug("convert_moves_to_fen called with invalid moves_str: %s", moves_str)
        return None

    board = chess.Board()
    moves = moves_str.split()
    for move in moves:
        try:
            board.push_san(move)
        except ValueError as exc:
            logger.warning("Invalid move '%s': %s", move, exc)
            return None
    fen = board.fen()
    logger.debug("Converted moves to FEN: %s", fen)
    return fen


def get_stockfish_eval_batch(
    fens: List[Optional[str]],
    depth: int = 15,
    threads: int = 1,
    hash_mb: int = 16,
    move_time: float = 0.1,
) -> List[Optional[chess.engine.PovScore]]:
    """
    Evaluate a batch of FEN positions using Stockfish engine.

    Args:
        fens: List of FEN strings or None values to evaluate.
        depth: Search depth for Stockfish evaluation.
        threads: Number of engine threads to use.
        hash_mb: Hash size in megabytes.
        move_time: Maximum time (in seconds) to spend per move evaluation.

    Returns:
        List of Stockfish evaluation scores or None for invalid inputs/errors.
    """
    with chess.engine.SimpleEngine.popen_uci("stockfish/stockfish-ubuntu-x86-64-avx2") as engine:
        engine.configure({"Threads": threads, "Hash": hash_mb})

        def evaluate(fen: Optional[str]) -> Optional[chess.engine.PovScore]:
            if fen is None:
                return None
            board = chess.Board(fen)
            try:
                result = engine.analyse(board, chess.engine.Limit(depth=depth, time=move_time))
                return result["score"]
            except Exception as exc: # pylint: disable=broad-exception-caught
                logger.warning("Error analyzing FEN '%s': %s", fen, exc)
                return None

        results = [evaluate(fen) for fen in fens]
        logger.info("Completed batch Stockfish evaluation for %d positions", len(fens))
        return results


def format_evaluation(score: Optional[chess.engine.PovScore]) -> Optional[Union[int, float]]:
    """
    Convert Stockfish score to numeric evaluation.

    Mate scores are converted to +/- infinity, others to centipawns.

    Args:
        score: Stockfish evaluation score or None.

    Returns:
        Numeric evaluation as float or int, or None if input is None.
    """
    if score is None:
        return None
    if score.is_mate():
        mate_in = score.relative.mate()
        return float("inf") if mate_in > 0 else float("-inf")
    value = score.white().score()
    logger.debug("Formatted evaluation score: %s", value)
    return value


def get_readable_eval(score: Optional[Union[int, float]]) -> Optional[str]:
    """
    Return human-readable evaluation string from numeric score.

    Args:
        score: Numeric evaluation or None.

    Returns:
        String description or None if input is None.
    """
    if score is None:
        return None
    if score == float("inf"):
        return "White mates"
    if score == float("-inf"):
        return "Black mates"
    readable = f"{score / 100:.2f}"
    logger.debug("Readable evaluation: %s", readable)
    return readable


def evaluate_opening_position(df: pd.DataFrame, fallback_cutoff: int = 15) -> pd.DataFrame:
    """
    Evaluate opening positions in a DataFrame of chess games.

    Uses either 'division_middle' move count (Lichess) or a fixed cutoff (Chess.com)
    to determine the opening position to evaluate.

    Args:
        df: DataFrame containing at least columns 'moves_split' and optionally
            'division_middle'.
        fallback_cutoff: Number of moves to use if 'division_middle' is not present.

    Returns:
        DataFrame with new columns:
        - 'opening_fen': FEN string of opening position.
        - 'opening_eval_raw': Raw Stockfish evaluation.
        - 'opening_eval': Readable evaluation string.
        - 'opening_eval_source': Source method ('division_middle' or fixed move).
    """
    fen_col = "opening_fen"
    eval_raw_col = "opening_eval_raw"
    eval_col = "opening_eval"

    def get_opening_fen(row) -> Optional[Tuple[str, str]]:
        """Extract FEN string and source from row data."""
        try:
            moves_split = row.get("moves_split")
            if not isinstance(moves_split, list):
                logger.debug("Row missing valid 'moves_split': %s", moves_split)
                return None

            if pd.notna(row.get("division_middle")):
                division = int(row["division_middle"])
                moves = moves_split[:division]
                source = "division_middle"
            else:
                cutoff = min(fallback_cutoff, len(moves_split))
                if cutoff < fallback_cutoff:
                    logger.debug("Game too short for fallback cutoff: %d", cutoff)
                    return None
                moves = moves_split[:cutoff]
                source = f"move_{fallback_cutoff}"

            fen = convert_moves_to_fen(" ".join(moves))
            return (fen, source) if fen else None
        except Exception as exc: # pylint: disable=broad-exception-caught
            logger.warning("Error extracting opening FEN: %s", exc)
            return None

    fen_results = df.apply(get_opening_fen, axis=1)
    df[fen_col] = fen_results.apply(lambda x: x[0] if x else None)
    df["opening_eval_source"] = fen_results.apply(lambda x: x[1] if x else None)

    df[eval_raw_col] = get_stockfish_eval_batch(df[fen_col].tolist())

    df[eval_col] = (
        df[eval_raw_col]
        .dropna()
        .apply(format_evaluation)
        .apply(get_readable_eval)
    )
    df[eval_col] = pd.to_numeric(df[eval_col], errors="coerce")

    logger.info(
        "Evaluated opening positions for %d games with fallback cutoff %d",
        len(df),
        fallback_cutoff,
    )
    return df


def run_evaluation_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run the full evaluation pipeline on a DataFrame of chess games.

    Splits moves once and evaluates opening positions twice (can be extended for
    middlegame or other stages).

    Args:
        df: DataFrame containing at least a 'moves' column with SAN moves string.

    Returns:
        DataFrame with evaluation columns added.
    """
    df["moves_split"] = df["moves"].apply(lambda x: x.split() if isinstance(x, str) else [])
    logger.info("Split moves for %d games", len(df))

    df = evaluate_opening_position(df)

    logger.info("Completed evaluation pipeline for %d games", len(df))
    return df
