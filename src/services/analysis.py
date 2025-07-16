"""
This module provides utility functions for analyzing chess game data stored in pandas DataFrames.

It includes functions for:
- Validating data and inputs
- Filtering by player color
- Summarizing game results and accuracy
- Identifying common opponents and top openings
- Calculating rating statistics and winrate percentages

The module is designed to support downstream analytics or dashboard features for
performance insights.
"""

import logging
from typing import Optional, Tuple, Dict, Union, List
from enum import Enum
import pandas as pd

logger = logging.getLogger(__name__)

# Module-level constants/enums
class Result(str, Enum):
    """
    Enumeration for possible game results from the player's perspective.
    """
    WIN = 'win'
    LOSS = 'loss'
    DRAW = 'draw'

class Color(str, Enum):
    """
    Enumeration for the two possible player colors in chess.
    """
    WHITE = 'white'
    BLACK = 'black'

def validate_color(color: Optional[str]) -> Optional[Color]:
    """
    Validate and convert a string to a Color enum if valid.
    
    Args:
        color (Optional[str]): Player color as a string, or None.
        
    Returns:
        Optional[Color]: Corresponding Color enum or None if input is None.
        
    Raises:
        ValueError: If color is not 'white' or 'black'.
    """
    if color is None:
        logger.debug("No color provided; returning None.")
        return None
    try:
        validated = Color(color)
        logger.debug("Validated color: %s", validated)
        return validated
    except ValueError as exc:
        logger.error("Invalid color provided: %s", color)
        raise ValueError(f"Invalid color: {color}. Expected 'white' or 'black'.") from exc

def validate_columns(df: pd.DataFrame, required_cols: List[str]) -> None:
    """
    Check if DataFrame contains required columns.
    
    Args:
        df (pd.DataFrame): DataFrame to check.
        required_cols (List[str]): List of columns required.
        
    Raises:
        ValueError: If any required columns are missing.
    """
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        logger.error("Missing required columns: %s", missing)
        raise ValueError(f"Missing required columns: {missing}")
    logger.debug("All required columns are present: %s", required_cols)

def filter_by_color(df: pd.DataFrame, color: Optional[str] = None) -> pd.DataFrame:
    """
    Filter DataFrame rows by player color.
    
    Args:
        df (pd.DataFrame): Games DataFrame.
        color (Optional[str]): Player color to filter by ('white' or 'black').
        
    Returns:
        pd.DataFrame: Filtered DataFrame by color or original if color is None.
    """
    validated_color = validate_color(color)
    if validated_color is None:
        logger.debug("No color filter applied.")
        return df
    filtered_df = df[df['player_color'] == validated_color]
    logger.debug(
        "Filtered DataFrame by color %s, resulting rows: %d",
        validated_color,
        len(filtered_df),
    )

    return filtered_df

def get_rating_diff(df: pd.DataFrame) -> int:
    """
    Sum player rating differences.
    
    Args:
        df (pd.DataFrame): Games DataFrame.
        
    Returns:
        int: Sum of 'player_rating_diff' column.
    """
    validate_columns(df, ['player_rating_diff'])
    total_diff = df['player_rating_diff'].sum()
    logger.debug("Calculated total player_rating_diff: %d", total_diff)
    return total_diff

def get_top_openings(df: pd.DataFrame, n: int = 5) -> pd.Series:
    """
    Get most frequent openings played.
    
    Args:
        df (pd.DataFrame): Games DataFrame.
        n (int): Number of top openings to return.
        
    Returns:
        pd.Series: Top n openings by frequency.
    """
    validate_columns(df, ['normalized_opening_name'])
    top_openings = df['normalized_opening_name'].value_counts(dropna=False).head(n)
    logger.debug("Top %d openings:\n%s", n, top_openings)
    return top_openings

def get_top_openings_by_result(
    df: pd.DataFrame,
    color: Optional[str] = None,
    n: int = 5
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Get top openings grouped by result for a player color.
    
    Args:
        df (pd.DataFrame): Games DataFrame.
        color (Optional[str]): Filter by player color.
        n (int): Number of top openings per result.
        
    Returns:
        Tuple[pd.Series, pd.Series, pd.Series]: Top openings for wins, losses, draws.
    """
    validate_columns(df, ['result', 'player_color', 'normalized_opening_name'])
    validated_color = validate_color(color)
    if validated_color is not None:
        df = df[df['player_color'] == validated_color]
        logger.debug("Filtered by color %s in get_top_openings_by_result.", validated_color)

    wins = df[df['result'] == Result.WIN]
    losses = df[df['result'] == Result.LOSS]
    draws = df[df['result'] == Result.DRAW]

    openings_for_win = wins['normalized_opening_name'].value_counts().head(n)
    openings_for_losses = losses['normalized_opening_name'].value_counts().head(n)
    openings_for_draws = draws['normalized_opening_name'].value_counts().head(n)

    logger.debug("Top %d openings for wins:\n%s", n, openings_for_win)
    logger.debug("Top %d openings for losses:\n%s", n, openings_for_losses)
    logger.debug("Top %d openings for draws:\n%s", n, openings_for_draws)

    return openings_for_win, openings_for_losses, openings_for_draws

def get_rating_range(df: pd.DataFrame) -> Tuple[int, int]:
    """
    Get minimum and maximum player ratings.
    
    Args:
        df (pd.DataFrame): Games DataFrame.
        
    Returns:
        Tuple[int, int]: Minimum and maximum ratings.
    """
    validate_columns(df, ['player_rating'])
    min_rating, max_rating = df['player_rating'].min(), df['player_rating'].max()
    logger.debug("Rating range: min=%d, max=%d", min_rating, max_rating)
    return min_rating, max_rating

def count_results(df: pd.DataFrame) -> Tuple[int, int, int]:
    """
    Count number of wins, losses, and draws.
    
    Args:
        df (pd.DataFrame): Games DataFrame.
        
    Returns:
        Tuple[int, int, int]: Counts of wins, losses, and draws.
    """
    validate_columns(df, ['result'])
    counts = df['result'].value_counts()
    wins = counts.get(Result.WIN, 0)
    losses = counts.get(Result.LOSS, 0)
    draws = counts.get(Result.DRAW, 0)
    logger.debug("Result counts - wins: %d, losses: %d, draws: %d", wins, losses, draws)
    return wins, losses, draws

def get_common_opponents(df: pd.DataFrame, n: int = 5) -> pd.Series:
    """
    Get most common opponents.
    
    Args:
        df (pd.DataFrame): Games DataFrame.
        n (int): Number of top opponents to return.
        
    Returns:
        pd.Series: Top n opponents by frequency.
    """
    validate_columns(df, ['opponent_name'])
    common_opponents = df['opponent_name'].value_counts().head(n)
    logger.debug("Top %d common opponents:\n%s", n, common_opponents)
    return common_opponents

def get_accuracy_stats(df: pd.DataFrame) -> Dict[str, float]:
    """
    Calculate average player accuracy overall and by result.
    
    Args:
        df (pd.DataFrame): Games DataFrame.
        
    Returns:
        Dict[str, float]: Accuracy stats with keys 'overall', 'wins', 'losses', 'draws'.
    """
    validate_columns(df, ['player_accuracy', 'result'])
    overall = round(df['player_accuracy'].mean(), 2)
    wins = round(df[df['result'] == Result.WIN]['player_accuracy'].mean(), 2)
    losses = round(df[df['result'] == Result.LOSS]['player_accuracy'].mean(), 2)
    draws = round(df[df['result'] == Result.DRAW]['player_accuracy'].mean(), 2)
    logger.debug("Accuracy stats - overall: %.2f, wins: %.2f, losses: %.2f, draws: %.2f",
                 overall, wins, losses, draws)
    return {
        'overall': overall,
        'wins': wins,
        'losses': losses,
        'draws': draws
    }

def result_streak(df):
    if len(df['result']) == 0:
        return 0
    
    first_result = df['result'].iloc[0]
    streak = 0
    
    for result in df['result']:
        if result != first_result:
            break
        streak += 1
    
    return streak

def adjust_evaluations(df):
    """Adjust evaluations based on player color."""
    df["opening_eval"] = pd.to_numeric(df["opening_eval"], errors='coerce')
    return df.apply(
        lambda row: -row["opening_eval"] if row["player_color"] == "black" else row["opening_eval"],
        axis=1
    )

def calculate_conversion_rate(condition, success_condition, total_games):
    """Calculate percentage of games meeting success_condition given initial condition."""
    if total_games == 0:
        return 0.0
    return (condition & success_condition).sum() / total_games * 100

def calculate_advantage_stats(df):
    """Calculate all advantage-related statistics."""
    df['adjusted_eval'] = adjust_evaluations(df)

    advantage = df['adjusted_eval'] > 1
    disadvantage = df['adjusted_eval'] < -1
    won = df['result'] == 'win'
    drawn = df['result'] == 'draw'

    stats = {
        'pct_won_when_ahead': calculate_conversion_rate(advantage, won, advantage.sum()),
        'pct_won_or_drawn_when_behind': calculate_conversion_rate(
            disadvantage, (won | drawn), disadvantage.sum()
        ),
        'games_with_advantage': advantage.sum(),
        'games_with_disadvantage': disadvantage.sum()
    }

    return stats

def prepare_winrate_data(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """
    Prepare win/draw/loss percentages for white, black, and overall.
    
    Args:
        df (pd.DataFrame): Games DataFrame.
        
    Returns:
        Dict[str, Dict[str, float]]: Percentages keyed by 'White', 'Black', and 'Both'.
    """
    validate_columns(df, ['result', 'player_color'])
    results = [Result.WIN, Result.DRAW, Result.LOSS]

    def get_percentages(subset: pd.DataFrame) -> Dict[str, float]:
        counts = subset['result'].value_counts(normalize=True) * 100
        return {r.value: round(counts.get(r.value, 0), 2) for r in results}

    total = get_percentages(df)
    white = get_percentages(df[df['player_color'] == Color.WHITE])
    black = get_percentages(df[df['player_color'] == Color.BLACK])

    logger.debug("Winrate data prepared for White, Black, and Both.")
    return {
        'white': white,
        'black': black,
        'overall': total
    }
