"""
Utility functions for extracting and matching chess opening names
based on ECO codes and SAN/PGN moves from Chess.com or PGN files.
"""

import logging
from typing import List, Tuple, Dict

import chess.pgn
import chess
import pandas as pd

logger = logging.getLogger(__name__)


def load_eco_pgn(file_path: str) -> List[Tuple[str, str, List[str]]]:
    """
    Load a PGN file and extract ECO codes, opening names and UCI moves.

    :param file_path: Path to the PGN file.
    :return: List of tuples with (eco, opening name, list of UCI moves).
    """
    openings = []
    try:
        with open(file_path, encoding="utf-8") as pgn_file:
            while True:
                game = chess.pgn.read_game(pgn_file)
                if game is None:
                    break
                eco = game.headers.get("ECO", "")
                name = game.headers.get("Opening", "")
                board = game.board()
                moves = [move.uci() for move in game.mainline_moves()]
                for move in game.mainline_moves():
                    board.push(move)
                openings.append((eco, name, moves))
        logger.info("Loaded %d openings from PGN file: %s", len(openings), file_path)
    except Exception: # pylint: disable=broad-except
        logger.exception("Error while loading PGN file: %s", file_path)
    return openings


def build_opening_dict(opening_list: List[Tuple[str, str, List[str]]]) -> Dict[str, str]:
    """
    Build a dictionary mapping UCI move strings to opening names.

    :param opening_list: List of (eco, name, moves) tuples.
    :return: Dictionary with key as 'uci1 uci2 ...' and value as opening name.
    """
    opening_dict = {
        ' '.join(moves): name
        for _, name, moves in opening_list
    }
    logger.info("Built opening dictionary with %d entries", len(opening_dict))
    return opening_dict


def san_to_uci_list(san_moves: str) -> List[str]:
    """
    Convert a string of SAN moves into UCI format.

    :param san_moves: String containing SAN moves separated by space.
    :return: List of UCI move strings.
    """
    board = chess.Board()
    uci_moves = []
    try:
        for san in san_moves.strip().split():
            move = board.parse_san(san)
            uci_moves.append(move.uci())
            board.push(move)
        logger.info("Converted SAN moves to UCI: %s", uci_moves)
    except Exception:
        logger.exception("Failed to convert SAN to UCI: %s", san_moves)
        raise
    return uci_moves


def find_opening_from_moves(moves: List[str], opening_dict: Dict[str, str]) -> str:
    """
    Find the opening name by matching a prefix of the UCI moves.

    :param moves: List of UCI move strings.
    :param opening_dict: Dictionary mapping UCI sequences to opening names.
    :return: Matched opening name or "Unknown Opening".
    """
    for i in reversed(range(1, len(moves) + 1)):
        prefix = ' '.join(moves[:i])
        if prefix in opening_dict:
            logger.info("Found opening from moves: %s", opening_dict[prefix])
            return opening_dict[prefix]
    logger.info("No opening found from moves.")
    return "Unknown Opening"


def load_eco_mapping(csv_path: str) -> Dict[str, str]:
    """
    Load a CSV file mapping ECO codes to opening names.

    :param csv_path: Path to the CSV file.
    :return: Dictionary mapping ECO codes to opening names.
    """
    try:
        df = pd.read_csv(csv_path)
        mapping = df.drop_duplicates("eco").set_index("eco")["name"].to_dict()
        logger.info("Loaded ECO mapping with %d entries from %s", len(mapping), csv_path)
        return mapping
    except Exception: # pylint: disable=broad-except
        logger.exception("Failed to load ECO mapping from: %s", csv_path)
        return {}


def get_opening_from_eco(eco_code: str, eco_mapping: Dict[str, str]) -> str:
    """
    Retrieve opening name from ECO code.

    :param eco_code: The ECO code string.
    :param eco_mapping: Mapping of ECO code to opening names.
    :return: Opening name or "Unknown Opening".
    """
    opening = eco_mapping.get(eco_code, "Unknown Opening")
    logger.info("Opening from ECO code '%s': %s", eco_code, opening)
    return opening


def get_opening_name(eco_code: str, moves: str) -> str:
    """
    Get the most accurate opening name from either moves or ECO code.

    :param eco_code: ECO code from the chess game.
    :param moves: String with SAN-formatted moves.
    :return: Opening name.
    """
    if not hasattr(get_opening_name, "opening_dict"):
        logger.info("Initializing opening dictionary...")
        opening_list = load_eco_pgn("data/eco.pgn")
        get_opening_name.opening_dict = build_opening_dict(opening_list)

    if not hasattr(get_opening_name, "eco_mapping"):
        logger.info("Initializing ECO mapping...")
        get_opening_name.eco_mapping = load_eco_mapping("data/opening_ecos.csv")

    try:
        uci_moves = san_to_uci_list(moves)
        opening = find_opening_from_moves(uci_moves, get_opening_name.opening_dict)
        if opening != "Unknown Opening":
            return opening
    except Exception: # pylint: disable=broad-except
        logger.warning("Failed to match opening from moves, falling back to ECO.")

    return get_opening_from_eco(eco_code, get_opening_name.eco_mapping)
