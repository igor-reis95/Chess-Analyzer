# insights.py

import pandas as pd
from src.services.data_viz import get_opening_stats

def winrate_graph_insights(data, color):
    win_percent = data[color]['win']

    def feedback_for_winrate(win_percent):
        if win_percent > 60:
            return "You're dominating your games — well done! Make sure you're still challenging yourself."
        elif win_percent > 52:
            return "You are constantly improving and might be underrated. Keep up the good work and don't lose focus!"
        elif win_percent >= 48:
            return "You're doing fine, but you might want to check your other analytics on ways to improve."
        elif win_percent >= 40:
            return "Your win rate could improve. Review your most common losses and focus on converting drawn positions."
        else:
            return "You're losing more often than expected. Use the other graphs to spot patterns and work on fundamentals."

    return feedback_for_winrate(win_percent)
    
def opening_stats_insights(df, color):
    df.loc[:, 'opening_eval'] = pd.to_numeric(df['opening_eval'], errors='coerce')
    df['adjusted_eval'] = df.apply(
        lambda row: -row['opening_eval'] if row['player_color'] == 'black' else row['opening_eval'],
        axis=1
    )

    def eval_feedback(avg, color_label):
        if color_label == 'white':
            if avg > 0.4:
                return "As White, you're getting very strong positions out of the opening — great work!"
            elif avg > 0.1:
                return "As White, you're often coming out slightly ahead — as expected. Solid openings!"
            elif avg > -0.1:
                return "As White, you're not taking much advantage of the first move. You may want to sharpen your opening prep."
            else:
                return "As White, you're often starting with a disadvantage — review your opening choices and look out for early mistakes."

        elif color_label == 'black':
            if avg > 0.1:
                return "As Black, you're outperforming expectations in the opening — impressive!"
            elif avg > -0.2:
                return "As Black, you're holding your ground well in the opening. That's a good sign."
            elif avg > -0.4:
                return "As Black, you're often slightly worse after the opening — consider studying lines where you're more comfortable."
            else:
                return "As Black, you're struggling in the opening. It may help to build a more solid repertoire or study key defenses."

        elif color_label == 'overall':
            if avg > 0.2:
                return "Overall, you're getting strong positions after the opening — great consistency!"
            elif avg > -0.1:
                return "Your opening play is stable overall. Keep working on both White and Black repertoires."
            else:
                return "You're often behind after the opening phase — this might be an area to prioritize."

    if color == 'overall':
        opening_avg = df["adjusted_eval"].mean()
        return eval_feedback(opening_avg, 'overall')
    elif color in ['white', 'black']:
        subset = df[df['player_color'] == color]
        opening_avg = subset["adjusted_eval"].mean()
        return eval_feedback(opening_avg, color)
    
def eval_per_opening_insights(df, color):
    if color == "overall":
        df = get_opening_stats(df)
    else:
        df = get_opening_stats(df[df['player_color'] == color])

    df = df.sort_values('count', ascending=False)

    opening_insights = []

    for i, row in df.iterrows():
        opening_name = row['normalized_opening_name']
        avg_eval = round(row['avg_eval'], 2)

        if avg_eval > 0.4:
            msg = (
                f"With the opening '{opening_name}', you often come out of the opening phase clearly ahead "
                f"(avg eval: +{avg_eval}). That's excellent — it could be a strong weapon in your repertoire."
            )
        elif avg_eval > 0.2:
            msg = (
                f"The opening '{opening_name}' gives you consistent small advantages (avg eval: +{avg_eval}). "
                "You're playing it well — keep refining it."
            )
        elif avg_eval > 0.05:
            msg = (
                f"'{opening_name}' tends to lead to slight advantages for you (avg eval: +{avg_eval}). "
                "It might be worth studying deeper lines to increase your edge."
            )
        elif avg_eval > -0.05:
            msg = (
                f"You're reaching equal positions with '{opening_name}' (avg eval: {avg_eval:+}). "
                "Try exploring variations to create more dynamic opportunities."
            )
        elif avg_eval > -0.2:
            msg = (
                f"The opening '{opening_name}' often leaves you slightly worse (avg eval: {avg_eval:+}). "
                "You might want to revisit key lines or common traps in it."
            )
        else:
            msg = (
                f"'{opening_name}' seems to be giving you trouble (avg eval: {avg_eval:+}). "
                "Consider replacing it or deeply reviewing your approach to it."
            )

        opening_insights.append(msg)

    return opening_insights

    
def lichess_popular_openings_insights():
    insight = (
        "Among the most popular openings, we see several unorthodox lines, especially A00, "
        "which includes many offbeat first moves commonly played by beginners. B00 and A40 also appear — "
        "they represent generic starts with 1.e4 and 1.d4 before transposing into known openings. "
        "Interestingly, the Scandinavian Defense (B01) stands out among these as a defined and respectable response to 1.e4, "
        "suggesting it's a frequent choice even at early levels."
    )
    return insight
    
def lichess_successful_openings_insights(color):
    insight = (
        f"The most successful openings for {color} include either highly specific lines or rare choices. "
        "Some of these may reflect the preferences of individual strong players using them consistently, "
        "while others represent deep, theoretical variations that tend to lead to early advantages. "
        "This suggests that in these games, preparation or unfamiliarity played a key role."
    )
    return insight

def insight_conversion_stat(player_stats, lichess_stats, stat_key):
    player_value = player_stats[stat_key]
    lichess_value = lichess_stats["conversion_stats"][stat_key]

    if stat_key == "pct_won_when_ahead":
        if player_value < lichess_value - 5:
            return (
                "Compared to the average, you often fail to convert winning positions. "
                "This could be due to rushed attacks or blunders. Practice converting advantages into wins."
            )
        elif player_value > lichess_value + 5:
            return (
                "You convert winning positions more reliably than most players. "
                "This shows strong technique and discipline — great job!"
            )
        else:
            return (
                "Your ability to convert winning positions is close to average. "
                "Keep working on your technique to consistently finish strong positions."
            )
    
    elif stat_key == "pct_won_or_drawn_when_behind":
        if player_value < lichess_value - 5:
            return (
                "Compared to average players, you struggle to recover when behind. "
                "Consider practicing defensive and counter-attacking tactics to improve your resilience."
            )
        elif player_value > lichess_value + 5:
            return (
                "You outperform most players when behind. This shows strong defensive skills and mental resilience."
            )
        else:
            return (
                "Your recovery rate from losing positions is around average. "
                "Keep working on your defense and focus during tough games."
            )
