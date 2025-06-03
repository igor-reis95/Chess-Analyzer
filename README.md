# Chess Data Coach

## Overview
Chess Data Coach is a data science-driven web application designed to help chess players improve by analyzing their game data. Inspired by the use of analytics in sports like football, this project applies similar techniques to chess, providing players with actionable insights based on their past matches.

### How It Works
- User Input (Frontend)
- Users enter their Lichess / chess.com username.
- They can filter by number of games, game type (e.g., blitz, rapid), and color played (white, black, or both).

### Data Collection (Backend)
- The app fetches game data directly from the Lichess API / chess.com API.
- It respects API rate limits (30 games per second).
- Games are normalized to consistently show the player’s perspective.

### Data Processing
- The API response is converted into a pandas DataFrame.
- Post-processing calculates metrics such as rating differences, time spent, number of moves, openings used, results, and performance.
- Data is formatted for readability (dates, percentages, etc.).

### Data Visualization
- Generates tables and charts showing insights like:
- Most used openings
- Win/loss rates by time control
- Accuracy and mistake patterns

### Optional Reports (Planned)
- Users will be able to generate downloadable reports in HTML or CSV format.
- Future versions will include personalized reports with shareable links.

### Technology Stack
- Backend: Python, Flask
- Data Manipulation: pandas
- Visualization: matplotlib, seaborn
- Frontend: HTML, Jinja2 templating, JavaScript
- Data Source: Lichess API / chess.com API

### Design Considerations
- In-memory data handling to avoid unnecessary file storage.
- Static HTML or CSV reports generated only on demand.

### Future Features
- Retrieve data from chess.com API
- User-friendly report generation interface
- CSV export button
- Enhanced frontend interactivity
- Smart recommendation system highlighting key mistakes
- Shareable game analysis links