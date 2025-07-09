# insights.py

import pandas as pd
from src.services.data_viz import get_opening_stats

def winrate_graph_insights(data, color):
    win_percent = data[f'{color}']['win']

    if win_percent > 52:
        return "You are constantly improving and might be underrated. Keep up the good work and don't lose focus!"
    elif win_percent < 48:
        return "Your win rate could improve. Check the other graphs below to find ways to improve!"
    else:
        return "You're doing fine, but you might want to check your other analytics on ways to improve"
    
def eval_per_opening_insights(df, color):
    df['opening_eval'] = pd.to_numeric(df['opening_eval'], errors='coerce')
    df["adjusted_eval"] = df.apply(
        lambda row: -row["opening_eval"] if row["player_color"] == "black" else row["opening_eval"],
        axis=1
    )

    if color == 'both':
        opening_avg = df["adjusted_eval"].mean()
    else:
        opening_avg = df[df["player_color"] == f"{color}"]["adjusted_eval"].mean()

    if opening_avg < 0.2:
        return "You might want to reavaluate your choice of openings or practice the ones you are using (https://lichess.org/training/openings)"
    elif opening_avg > 0.2:
        return "You're doing well on the opening phase and could keep improving your game by focusing on your middlegame and endgame abilities (https://lichess.org/training/middlegame / https://lichess.org/training/endgame)"
    else:
        return "It is not bad, but the games are coming down to who plays middlegame/endgame best. You might want to practice your opening to be able to reach the middlegame with advantage or practice your middlegame/endgame to be able to convert games into wins (https://lichess.org/training/middlegame / https://lichess.org/training/endgame)"
    
def opening_stats_insights(df, color):
    if color == "Overall":
        df = get_opening_stats(df)
    else:
        df = get_opening_stats(df[df['player_color'] == color])

    # Sort by frequency (assuming 'count' exists in your stats)
    df = df.sort_values('count', ascending=True)

    opening_insights = []
    for i, opening_name in enumerate(df['normalized_opening_name']):
        avg_eval = df['avg_eval'].iloc[i].round(2)
        if avg_eval > 0.2:
            opening_insights.append(f"The opening {opening_name} is your most played with an average {avg_eval} eval after opening. You're doing well with it. Keep up the good work!")
        elif avg_eval < 0.2:
            opening_insights.append(f"The opening {opening_name} is your most played with an average {avg_eval} eval after opening. It could use some practice. How about doing some studies or lichess puzzles?")
        else:
            opening_insights.append(f"The opening {opening_name} is your most played with an average {avg_eval} eval after opening. You're reaching middlegame in an OK position. I hope your middlegame is more polished")
    return opening_insights
    
def lichess_popular_openings_insights():
    insight = """When looking at the most popular openings played, we don't see much, aside from A00,
    being unusual openings, mostly done by beginner players still discovering the beautiful world of chess.
    Aside from that we see an ECO that represents just the move e4 (B00), another for only the move d4 (A40),
    and a common queen's pawn game. What is actually interesting is seeing the Scandinavian Defense on the top (B01)
    """
    return insight
    
def lichess_successful_openings_insights(color):
    insight = f"""Most of the ECOs presented here are either extremely uncommon, which might be high
    because of a specific player using it and doing well or very deep variations that mostly results
    in advantage for {color}.
    """
    return insight

def insight_conversion_stat(player_stats, lichess_stats, stat_key):
    player_value = player_stats[stat_key]
    lichess_value = lichess_stats["conversion_stats"][stat_key]

    if player_value < lichess_value - 5:
        return "Compared to average players, you struggle to recover when behind. It is recommended to practice defensive and counter-attacking tactics.",

    elif player_value > lichess_value + 5:
        return "You outperform most players when behind. This shows good resilience under pressure!"
    else:
        return "Your recovery rate is around average. Keep working on improving your defense and resilience."