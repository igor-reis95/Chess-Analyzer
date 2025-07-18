"""
Module for fetching, processing, and saving chess user data from an online source.

This module defines a UserProcessor class that orchestrates the entire data workflow:
1. Fetch user data via API
2. Process the user stats
3. Save processed data to database
"""

import logging
from typing import Optional
import pandas as pd
from src.api.api import collect_user_data
from src.services.data_io import get_user_data
import src.services.post_process as post_process

logger = logging.getLogger(__name__)

import time


class UserProcessor:
    """
    Orchestrates the fetching, processing, and saving of chess user data.

    Attributes:
        username (str): The username of the chess player.
        raw_data (Optional[dict]): Raw user data from the API.
        df_processed (Optional[DataFrame]): Post-processed user data.
    """

    def __init__(self, username: str) -> None:
        """
        Initialize the UserProcessor with the target username.

        Args:
            username (str): Player username to fetch user data for.
        """
        self.username = username
        self.raw_data: Optional[dict] = None
        self.df_processed: Optional[pd.DataFrame] = None

    def fetch_user_data(self) -> None:
        """
        Fetch raw user data from the Lichess API.
        """
        logger.info("Fetching user data for '%s'", self.username)
        self.raw_data = collect_user_data(self.username)
        if self.raw_data:
            logger.info("Successfully fetched user data for '%s'", self.username)
        else:
            logger.warning("No user data found for '%s'", self.username)

    def process_user_data(self) -> None:
        """
        Process the raw user data into a structured DataFrame.
        """
        if self.raw_data is None:
            logger.warning("No raw data to process for '%s'", self.username)
            return
        self.df_processed = post_process.process_user_data(self.raw_data)
        logger.info("Processed user data for '%s'", self.username)

    def get_dataframe(self) -> Optional[pd.DataFrame]:
        """
        Get the final processed user DataFrame.

        Returns:
            Optional[DataFrame]: Processed DataFrame or None if not available.
        """
        return self.df_processed