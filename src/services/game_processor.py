"""
Module for fetching, saving, flattening, and processing chess game data.

Defines the GameProcessor class that orchestrates the entire data pipeline:
1. Fetch raw games via API
2. Flatten nested JSON into tabular format
3. Evaluate games using a chess engine
4. Post-process data for analysis
5. Save processed data as CSV and to database
"""

import logging
from typing import Optional

from pandas import DataFrame

from src.api.api import get_games
from src.services.flatten import flatten_game_data
from src.services.post_process import post_process
from src.services.chess_engine import run_evaluation_pipeline

logger = logging.getLogger(__name__)


class GameProcessor:
    """
    Orchestrates the fetching, processing, and saving of chess game data for a player.

    Attributes:
        username (str): Player username to fetch games for.
        max_games (int): Maximum number of games to fetch.
        perf_type (str): Performance type filter (e.g., 'rapid', 'blitz').
        platform (str): Platform to fetch games from ('lichess', 'chess.com').
        games (Optional[list]): Raw game data fetched from API.
        df_flat (Optional[DataFrame]): Flattened game data.
        df_processed (Optional[DataFrame]): Final post-processed game data.
    """

    def __init__(self, username: str, max_games: int,
                 perf_type: str, platform: str) -> None:
        """
        Initialize the GameProcessor with user parameters.

        Args:
            username (str): Player username.
            max_games (int): Max number of games to fetch.
            perf_type (str): Performance type filter.
            platform (str): Chess platform name.
        """
        self.username = username
        self.max_games = max_games
        self.perf_type = perf_type
        self.platform = platform
        self.games: Optional[list] = None
        self.df_flat: Optional[DataFrame] = None
        self.df_processed: Optional[DataFrame] = None

    def fetch_games(self) -> None:
        """
        Fetch games from the API based on player, game type, and platform.
        """
        logger.info(
            "Fetching up to %d games for user '%s' [type=%s, platform=%s]",
            self.max_games, self.username, self.perf_type, self.platform
        )
        self.games = get_games(
            self.username, self.max_games, self.perf_type, self.platform
        )
        if not self.games:
            logger.warning("No games returned from API for user '%s'", self.username)
            return
        logger.info("Fetched %d games for user '%s'", len(self.games), self.username)

    def flatten_games(self) -> None:
        """
        Flatten raw game JSON into a structured DataFrame.
        """
        if not self.games:
            logger.warning("No games to flatten for user '%s'", self.username)
            return
        self.df_flat = flatten_game_data(self.games)
        logger.info(
            "Flattened games into DataFrame with %d rows for user '%s'",
            len(self.df_flat), self.username
        )

    def post_process_games(self) -> None:
        """
        Post-process flattened data, save CSV and store to the database.
        """
        if self.df_flat is None:
            logger.warning("No flattened data to process for user '%s'", self.username)
            return
        self.df_processed = post_process(self.df_flat, self.username)
        logger.info("Post-processed and saved games for user '%s'", self.username)

    def get_dataframe(self) -> Optional[DataFrame]:
        """
        Return the final processed DataFrame.

        Returns:
            Optional[DataFrame]: Processed DataFrame, or None if not available.
        """
        return self.df_processed

    def run_all(self) -> None:
        """
        Execute full pipeline: fetch, flatten, evaluate, post-process.
        """
        logger.info("Starting full processing pipeline for user '%s'", self.username)
        self.fetch_games()
        if not self.games:
            logger.error("Aborting pipeline: no games fetched for user '%s'", self.username)
            return

        self.flatten_games()
        if self.df_flat is None or self.df_flat.empty:
            logger.error("Aborting pipeline: flattening returned no data for user '%s'", self.username)
            return

        self.df_flat = run_evaluation_pipeline(self.df_flat)
        logger.info("Evaluation pipeline completed for user '%s'", self.username)

        self.post_process_games()
        logger.info("Full pipeline completed for user '%s'", self.username)
