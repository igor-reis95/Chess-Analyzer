"""
Insights module to generate textual feedback based on chess game statistics.

Functions provide interpretations of win rates, opening evaluations, 
and opening-specific performance. Includes general insights and comparisons
against lichess data.

Designed to support user-facing analytic features.
"""

import logging
from typing import List, Optional, Dict

import pandas as pd
from src.services.data_viz import get_opening_stats

logger = logging.getLogger(__name__)


def winrate_graph_insights(data: Dict[str, Dict[str, float]], color: str) -> str:
    """
    Generate feedback message based on win rate percentage for a color.

    Args:
        data: Nested dict with keys for colors containing 'win' percentages.
        color: Player color key to extract win rate ('white', 'black', 'overall').

    Returns:
        A user-friendly feedback string on win rate performance.
    """
    win_percent = data[color]["win"]
    logger.debug("Winrate for %s: %.2f%%", color, win_percent)

    def feedback_for_winrate(win_percent: float) -> str:
        if win_percent > 60:
            return (
                "You're dominating your games — well done! Make sure you're still "
                "challenging yourself."
            )
        elif win_percent > 52:
            return (
                "You are constantly improving and might be underrated. Keep up the good "
                "work and don't lose focus!"
            )
        elif win_percent >= 48:
            return (
                "You're doing fine, but you might want to check your other analytics on "
                "ways to improve."
            )
        elif win_percent >= 40:
            return (
                "Your win rate could improve. Review your most common losses and focus "
                "on converting drawn positions."
            )
        else:
            return (
                "You're losing more often than expected. Use the other graphs to spot "
                "patterns and work on fundamentals."
            )

    return feedback_for_winrate(win_percent)


def opening_stats_insights(df: pd.DataFrame, color: str) -> Optional[str]:
    """
    Provide textual insights on opening evaluations for a given player color.

    Args:
        df: DataFrame containing at least 'opening_eval' and 'player_color' columns.
        color: 'white', 'black', or 'overall' to select subset or full data.

    Returns:
        Insight string or None if insufficient data.
    """
    df = df.copy()
    df.loc[:, 'opening_eval'] = pd.to_numeric(df['opening_eval'], errors='coerce')
    df['adjusted_eval'] = df.apply(
        lambda row: -row['opening_eval'] if row['player_color'] == 'black' else row['opening_eval'],
        axis=1
    )

    def eval_feedback(avg: float, color_label: str) -> str:
        if color_label == "white":
            if avg > 0.4:
                return (
                    "As White, you're getting very strong positions out of the opening — "
                    "great work!"
                )
            elif avg > 0.1:
                return (
                    "As White, you're often coming out slightly ahead — as expected. Solid "
                    "openings!"
                )
            elif avg > -0.1:
                return (
                    "As White, you're not taking much advantage of the first move. You may "
                    "want to sharpen your opening prep."
                )
            else:
                return (
                    "As White, you're often starting with a disadvantage — review your "
                    "opening choices and look out for early mistakes."
                )

        if color_label == "black":
            if avg > 0.1:
                return (
                    "As Black, you're outperforming expectations in the opening — impressive!"
                )
            elif avg > -0.2:
                return (
                    "As Black, you're holding your ground well in the opening. That's a good "
                    "sign."
                )
            elif avg > -0.4:
                return (
                    "As Black, you're often slightly worse after the opening — consider "
                    "studying lines where you're more comfortable."
                )
            else:
                return (
                    "As Black, you're struggling in the opening. It may help to build a more "
                    "solid repertoire or study key defenses."
                )

        if color_label == "overall":
            if avg > 0.2:
                return (
                    "Overall, you're getting strong positions after the opening — great "
                    "consistency!"
                )
            elif avg > -0.1:
                return (
                    "Your opening play is stable overall. Keep working on both White and Black "
                    "repertoires."
                )
            else:
                return (
                    "You're often behind after the opening phase — this might be an area to "
                    "prioritize."
                )

        return "No insights available."

    if color == "overall":
        opening_avg = df["adjusted_eval"].mean()
        logger.debug("Overall opening average eval: %.3f", opening_avg)
        return eval_feedback(opening_avg, "overall")

    if color in ("white", "black"):
        subset = df[df["player_color"] == color]
        opening_avg = subset["adjusted_eval"].mean()
        logger.debug("%s opening average eval: %.3f", color.capitalize(), opening_avg)
        return eval_feedback(opening_avg, color)

    logger.warning("Invalid color argument in opening_stats_insights: %s", color)
    return None


def eval_per_opening_insights(df: pd.DataFrame, color: str) -> List[str]:
    """
    Generate insights for each opening based on average evaluation.

    Args:
        df: DataFrame with opening stats including 'normalized_opening_name' and 'avg_eval'.
        color: Filter color ('white', 'black', 'overall').

    Returns:
        List of user-friendly strings describing performance per opening.
    """
    if color == "overall":
        df = get_opening_stats(df)
    else:
        df = get_opening_stats(df[df["player_color"] == color])

    df = df.sort_values("count", ascending=False)
    logger.debug("Sorted openings by frequency for color %s", color)

    opening_insights: List[str] = []

    for _, row in df.iterrows():
        opening_name = row["normalized_opening_name"]
        avg_eval = round(row["avg_eval"], 2)

        if avg_eval > 0.4:
            msg = (
                f"With the opening '{opening_name}', you often come out of the opening phase "
                f"clearly ahead (avg eval: +{avg_eval}). That's excellent — it could be a "
                "strong weapon in your repertoire."
            )
        elif avg_eval > 0.2:
            msg = (
                f"The opening '{opening_name}' gives you consistent small advantages (avg "
                f"eval: +{avg_eval}). You're playing it well — keep refining it."
            )
        elif avg_eval > 0.05:
            msg = (
                f"'{opening_name}' tends to lead to slight advantages for you (avg eval: "
                f"+{avg_eval}). It might be worth studying deeper lines to increase your "
                "edge."
            )
        elif avg_eval > -0.05:
            msg = (
                f"You're reaching equal positions with '{opening_name}' (avg eval: {avg_eval:+}). "
                "Try exploring variations to create more dynamic opportunities."
            )
        elif avg_eval > -0.2:
            msg = (
                f"The opening '{opening_name}' often leaves you slightly worse (avg eval: "
                f"{avg_eval:+}). You might want to revisit key lines or common traps in it."
            )
        else:
            msg = (
                f"'{opening_name}' seems to be giving you trouble (avg eval: {avg_eval:+}). "
                "Consider replacing it or deeply reviewing your approach to it."
            )

        opening_insights.append(msg)

    return opening_insights


def lichess_popular_openings_insights() -> str:
    """
    Provide a textual insight about popular openings on lichess.

    Returns:
        Insight string describing the nature of popular openings.
    """
    insight = (
        "Among the most popular openings, we see several unorthodox lines, especially A00, "
        "which includes many offbeat first moves commonly played by beginners. A40, B00, "
        "D00 also appear — they represent generic starts with 1.d4 and 1.e4 before "
        "transposing into known openings. Interestingly, the Scandinavian Defense (B01) "
        "stands out among these as a defined and respectable response to 1.e4, suggesting "
        "it's a frequent choice even at early levels."
    )
    logger.debug("Provided lichess popular openings insight.")
    return insight


def lichess_successful_openings_insights(color: str) -> str:
    """
    Provide insight on successful openings for a given color on lichess.

    Args:
        color: Player color ('white' or 'black').

    Returns:
        Insight string about successful openings.
    """
    insight = (
        f"The most successful openings for {color} include either highly specific lines "
        "or rare choices. Some of these may reflect the preferences of individual strong "
        "players using them consistently, while others represent deep, theoretical "
        "variations that tend to lead to early advantages. This suggests that in these "
        "games, preparation or unfamiliarity played a key role."
    )
    logger.debug("Provided lichess successful openings insight for color: %s", color)
    return insight


def insight_conversion_stat(
    player_stats: Dict[str, float], lichess_stats: Dict[str, Dict[str, float]], stat_key: str
) -> str:
    """
    Compare player's conversion statistics to lichess averages and provide feedback.

    Args:
        player_stats: Player's statistics dict with stat_key included.
        lichess_stats: Lichess average statistics dict containing conversion_stats.
        stat_key: Key for the statistic to compare, e.g., 'pct_won_when_ahead'.

    Returns:
        Feedback string interpreting player's performance relative to average.
    """
    player_value = player_stats.get(stat_key)
    lichess_value = lichess_stats.get("conversion_stats", {}).get(stat_key)

    if player_value is None or lichess_value is None:
        logger.warning(
            "Missing stats for key '%s': player_value=%s, lichess_value=%s",
            stat_key,
            player_value,
            lichess_value,
        )
        return "Insufficient data to provide insight."

    if stat_key == "pct_won_when_ahead":
        if player_value < lichess_value - 5:
            return (
                "Compared to the average, you often fail to convert winning positions. "
                "This could be due to rushed attacks or blunders. Practice converting "
                "advantages into wins."
            )
        if player_value > lichess_value + 5:
            return (
                "You convert winning positions more reliably than most players. This shows "
                "strong technique and discipline — great job!"
            )
        return (
            "Your ability to convert winning positions is close to average. Keep working "
            "on your technique to consistently finish strong positions."
        )

    if stat_key == "pct_won_or_drawn_when_behind":
        if player_value < lichess_value - 5:
            return (
                "Compared to average players, you struggle to recover when behind. "
                "Consider practicing defensive and counter-attacking tactics to improve "
                "your resilience."
            )
        if player_value > lichess_value + 5:
            return (
                "You outperform most players when behind. This shows strong defensive "
                "skills and mental resilience."
            )
        return (
            "Your recovery rate from losing positions is around average. Keep working "
            "on your defense and focus during tough games."
        )

    logger.warning("Stat key '%s' is not recognized for insight generation.", stat_key)
    return "No insight available for this statistic."
