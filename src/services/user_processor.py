"""Module for fetching, processing, and saving chess user data from online sources.

This module provides the UserProcessor class which orchestrates the complete data workflow:
1. Fetch user data via platform API (Lichess/Chess.com)
2. Process the user stats and profile information
3. Provide access to processed data for storage or analysis

The class handles all aspects of user data collection and transformation in a structured way.
"""

import logging
from typing import Optional

import pandas as pd

from src.api.api import collect_user_data
import src.services.post_process as post_process

logger = logging.getLogger(__name__)


class UserProcessor:
    """Orchestrates the collection and processing of chess user data.

    Handles the complete workflow from API collection to processed DataFrame output,
    with logging and error handling at each stage.

    Attributes:
        username: Chess player username to process.
        platform: Source platform ('lichess' or 'chess.com').
        raw_data: Raw JSON data collected from API.
        df_processed: Processed DataFrame of user statistics.
    """

    def __init__(self, username: str, platform: str) -> None:
        """Initialize processor for specific user and platform.

        Args:
            username: Player username to fetch data for.
            platform: Source platform ('lichess' or 'chess.com').

        Raises:
            ValueError: If platform is not one of supported options.
        """
        if platform not in ('lichess.org', 'chess.com'):
            logger.error('Unsupported platform: %s', platform)
            raise ValueError("Platform must be 'lichess' or 'chess.com'")

        self.username = username
        self.platform = platform
        self.raw_data: Optional[dict] = None
        self.df_processed: Optional[pd.DataFrame] = None
        logger.debug('Initialized UserProcessor for %s on %s', username, platform)

    def fetch_user_data(self) -> None:
        """Fetch raw user data from platform API.

        Makes authenticated API call to collect user profile and statistics.
        Updates raw_data attribute with results.

        Raises:
            ConnectionError: If API request fails.
            ValueError: If response data is malformed.
        """
        logger.info("Fetching %s user data for '%s'", self.platform, self.username)
        try:
            self.raw_data = collect_user_data(self.username, self.platform)
            if not self.raw_data:
                logger.warning("Empty response for user '%s'", self.username)
                return

            logger.debug(
                "Successfully fetched %s bytes of data for %s",
                len(str(self.raw_data)),
                self.username
            )
        except Exception as e:
            logger.error("Failed to fetch data for %s: %s", self.username, str(e))
            raise ConnectionError(f"API request failed: {str(e)}") from e

    def process_user_data(self) -> None:
        """Transform raw API data into structured DataFrame.

        Processes raw JSON data through post-processing pipeline to create
        analysis-ready DataFrame. Updates df_processed attribute.

        Raises:
            RuntimeError: If called before successful data fetch.
            ValueError: If data processing fails.
        """
        if self.raw_data is None:
            msg = f"No raw data available for {self.username}"
            logger.error(msg)
            raise RuntimeError(msg)

        logger.info("Processing user data for '%s'", self.username)
        try:
            self.df_processed = post_process.process_user_data(
                self.raw_data,
                self.platform
            )
            logger.debug(
                "Processed data shape: %s rows, %s columns",
                *self.df_processed.shape
            )
        except Exception as e:
            logger.error("Processing failed for %s: %s", self.username, str(e))
            raise ValueError(f"Data processing error: {str(e)}") from e

    def get_dataframe(self) -> Optional[pd.DataFrame]:
        """Retrieve the processed user data DataFrame.

        Returns:
            Processed DataFrame if available, None otherwise.

        Note:
            Will return None if either fetch or process steps haven't been
            successfully completed.
        """
        if self.df_processed is not None:
            logger.debug(
                "Returning processed DataFrame for %s (%s rows)",
                self.username,
                len(self.df_processed)
            )
        else:
            logger.debug("No processed data available for %s", self.username)
        return self.df_processed
