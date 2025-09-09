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
from pandas import Interval
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

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
    logger.debug(
        "Accuracy stats - overall: %.2f, wins: %.2f, losses: %.2f, draws: %.2f",
        overall,
        wins,
        losses,
        draws
    )
    return {
        'overall': overall,
        'wins': wins,
        'losses': losses,
        'draws': draws
    }

def result_streak(df: pd.DataFrame) -> int:
    """
    Calculate length of the current result streak at the start of the DataFrame.
    
    Args:
        df (pd.DataFrame): Games DataFrame.
        
    Returns:
        int: Length of streak of identical results from the first game.
    """
    if len(df['result']) == 0:
        return 0

    first_result = df['result'].iloc[0]
    streak = 0

    for result in df['result']:
        if result != first_result:
            break
        streak += 1

    return streak

def adjust_evaluations(df: pd.DataFrame) -> pd.Series:
    """
    Adjust evaluation scores based on player color (invert for black).
    
    Args:
        df (pd.DataFrame): Games DataFrame.
        
    Returns:
        pd.Series: Adjusted evaluation scores.
    """
    df["opening_eval"] = pd.to_numeric(df["opening_eval"], errors='coerce')
    return df.apply(
        lambda row: -row["opening_eval"] if row["player_color"] == "black" else row["opening_eval"],
        axis=1
    )

def calculate_conversion_rate(
    condition: pd.Series,
    success_condition: pd.Series,
    total_games: int
) -> float:
    """
    Calculate percentage of games meeting success_condition given initial condition.
    
    Args:
        condition (pd.Series): Boolean series where condition is met.
        success_condition (pd.Series): Boolean series where success condition is met.
        total_games (int): Number of games to consider.
        
    Returns:
        float: Percentage of successful outcomes.
    """
    if total_games == 0:
        return 0.0
    return (condition & success_condition).sum() / total_games * 100

def calculate_conversion_stats(df: pd.DataFrame) -> Dict[str, Union[int, float]]:
    """
    Calculate advantage-related statistics.
    
    Args:
        df (pd.DataFrame): Games DataFrame.
        
    Returns:
        Dict[str, Union[int, float]]: Statistics about advantage and outcomes.
    """
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
        'White': white,
        'Black': black,
        'Overall': total
    }

def get_player_rating_bracket_evaluation(
    perf_type: str,
    lichess_stats: Dict,
    user_data: Dict
) -> Tuple[Optional[float], Optional[Interval]]:
    """
    Retrieve the average opening evaluation for the player's rating bracket.
    
    Args:
        perf_type: The performance type (e.g., 'blitz', 'rapid', 'classical').
        lichess_stats: Dictionary containing Lichess statistics data.
        user_data: Dictionary containing user information including ratings.
        
    Returns:
        Tuple containing:
            - Average opening evaluation for the rating bracket (float or None if not found)
            - Rating bracket interval object (Interval or None if not found)
            
    Raises:
        KeyError: If required keys are missing from input dictionaries.
        ValueError: If rating bracket data is malformed or invalid.
    """
    logger.debug("Retrieving rating bracket opening eval for perf_type: %s", perf_type)

    # Validate inputs
    if not perf_type:
        raise ValueError("perf_type cannot be empty")

    if not user_data or f'{perf_type}_rating' not in user_data:
        raise KeyError(f"Missing {perf_type}_rating in user_data")

    if not lichess_stats or "eval_per_rating_bracket" not in lichess_stats:
        raise KeyError("Missing eval_per_rating_bracket in lichess_stats")

    # Extract player rating
    player_rating = user_data[f'{perf_type}_rating']

    # Extract and validate rating brackets data
    rating_brackets_data = lichess_stats["eval_per_rating_bracket"]
    if not rating_brackets_data:
        logger.warning("Empty rating brackets data")
        return None, None

    # Convert rating brackets to pandas Series with Interval index
    rating_brackets = {}
    for key, value in rating_brackets_data.items():
        try:
            # Parse interval string format "[low, high)"
            clean_key = key[1:].split(', ')
            low_str = clean_key[0]
            high_str = clean_key[1][:-1]  # Remove trailing ')'

            low = int(low_str)
            high = int(high_str)

            interval = Interval(low, high, closed='left')
            rating_brackets[interval] = float(value)

        except (ValueError, IndexError, TypeError) as e:
            logger.warning("Skipping malformed rating bracket: %s. Error: %s", key, e)
            continue

    rating_brackets_series = pd.Series(rating_brackets)

    # Find the player's rating bracket
    player_bracket = None
    rating_avg_eval = None

    for bracket, eval_value in rating_brackets_series.items():
        if bracket.left <= player_rating < bracket.right:
            player_bracket = bracket
            rating_avg_eval = eval_value
            logger.debug("Found matching bracket: %s with eval: %.3f", bracket, eval_value)
            break

    if player_bracket is None:
        logger.warning("No rating bracket found for player rating: %s", player_rating)

    return rating_avg_eval, player_bracket

def prepare_logistic_regression(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Prepare features and target for logistic regression model.
    
    Args:
        df: Input DataFrame containing chess game data.
        
    Returns:
        Tuple containing feature matrix (X) and target vector (y).
        
    Raises:
        ValueError: If required columns are missing from input DataFrame.
    """
    try:
        logger.debug("Preparing data for logistic regression")
        df_model = df.copy()

        # Filter out rows with missing or invalid results
        valid_results = ['win', 'draw', 'loss']
        df_model = df_model[df_model['result'].isin(valid_results)]
        logger.debug("Filtered dataset to %s valid results", len(df_model))

        # Create binary target (win = 1, else = 0)
        df_model['target'] = df_model['result'].apply(
            lambda x: 1 if x == 'win' else 0
        )

        # Feature engineering
        df_model['is_white'] = df_model['player_color'].apply(
            lambda x: 1 if x == 'white' else 0
        )

        # Group openings to avoid high cardinality
        top_openings = df_model['normalized_opening_name'].value_counts().nlargest(5).index
        df_model['opening_group'] = df_model['normalized_opening_name'].apply(
            lambda x: x if x in top_openings else 'Other'
        )

        # One-hot encode categorical features
        df_encoded = pd.get_dummies(
            df_model[['speed', 'opening_group']],
            drop_first=True
        )

        # Select numerical features
        features_numeric = df_model[[
            'rating_difference',
            'full_moves',
            'is_white'
        ]]

        # Combine all features
        X = pd.concat([features_numeric, df_encoded], axis=1) # pylint: disable=invalid-name
        y = df_model['target']

        logger.debug("Prepared dataset with %s features and %s samples", X.shape[1], X.shape[0])
        return X, y

    except KeyError as e:
        logger.error("Missing required column in DataFrame: %s", e)
        raise ValueError(f"Missing required column: {e}") from e
    except Exception as e:
        logger.error("Unexpected error in data preparation: %s", e)
        raise


def train_logistic_regression_model(df: pd.DataFrame) -> pd.DataFrame:
    """
    Train logistic regression model and return feature importance.
    
    Args:
        df: Input DataFrame containing chess game data.
        
    Returns:
        DataFrame with feature coefficients sorted by absolute value.
        
    Raises:
        ValueError: If insufficient data for model training.
    """
    try:
        logger.debug("Training logistic regression model")
        X, y = prepare_logistic_regression(df) # pylint: disable=invalid-name

        # Check if we have enough data
        if len(X) < 10:
            logger.error("Insufficient data for model training")
            raise ValueError("Insufficient data for model training")

        # Train/test split
        X_train, X_test, y_train, y_test = train_test_split( # pylint: disable=invalid-name
            X, y, test_size=0.2, random_state=42
        )

        # Train model
        model = LogisticRegression(max_iter=1000)
        model.fit(X_train, y_train)

        # Evaluate model
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        logger.debug("Model accuracy: %.3f", accuracy)

        # Extract feature importance
        feature_names = X.columns
        coefficients = model.coef_[0]

        importance_df = pd.DataFrame({
            'feature': feature_names,
            'coefficient': coefficients,
            'abs_coeff': np.abs(coefficients)
        }).sort_values(by='abs_coeff', ascending=False)

        logger.debug("Generated feature importance DataFrame")
        return importance_df

    except ValueError as e:
        logger.error("Data-related error in model training: %s", e)
        raise
    except Exception as e:
        logger.error("Unexpected error in model training: %s", e)
        raise
