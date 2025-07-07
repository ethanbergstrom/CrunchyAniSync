# coding=utf-8
from configparser import SectionProxy
import logging
from typing import List, Optional

import crunpyroll.types

from plexanisync.logger_adapter import PrefixLoggerAdapter
from plexanisync.plexmodule import PlexWatchedSeries, PlexSeason

import crunpyroll

from collections import defaultdict

import uuid
import hashlib

logger = PrefixLoggerAdapter(logging.getLogger("PlexAniSync"), {"prefix": "CRUNCHYROLL"})


class Crunchyroll:
    def __init__(self, settings: SectionProxy):
        self.settings = settings

    async def __authenticate(self) -> crunpyroll.Client:
        client = crunpyroll.Client(
            email=self.settings['email'],
            password=self.settings['password'],
            # Encode the username to a consistent user-specific device ID
            device_id=uuid.UUID(hex=hashlib.md5(self.settings['email'].encode("UTF-8")).hexdigest()),
            device_name='PlexAniSync',
            device_type='Python'
        )

        await client.start()

        return client

    async def get_watched_shows(self) -> Optional[List[PlexWatchedSeries]]:
        logger.debug("Retrieving watch count for series")
        watched_series: List[PlexWatchedSeries] = []

        client = await self.__authenticate()

        history = await client.get_history()

        show_ids = defaultdict(list)
        # Maps every episode to a set of unique season IDs, each paired with a series ID
        for season_id, series_id in list({viewing.episode.season_id: viewing.episode.series_id for viewing in history if viewing.episode.series_id is not None}.items()):
            # Groups the set into a dict of series IDs mapped to a list of season IDs
            show_ids[series_id].append(season_id)

        for show_id, season_ids in show_ids.items():
            show = await client.get_series(show_id)
            show_seasons = await client.get_seasons(show_id)

            seasons = []
            for season in [season for season in show_seasons.items if season.id in season_ids]:
                all_episodes_of_season = await client.get_episodes(season.id)

                # Crunchyroll sometimes inconsistently numbers their episodes within a season, so we cant use max episode number like we do with Plex
                season_watchcount = len([viewing.episode.episode_number for viewing in history if viewing.episode.season_id == season.id and viewing.episode.episode_number is not None] or [0])

                logger.debug(f'{season_watchcount} episodes watched for {show.title} season {season.season_number}')

                seasons.append(
                    PlexSeason(
                        int(season.season_sequence_number or season.season_number),
                        0,  # self.__get_plex_rating(season.userRating),
                        season_watchcount,
                        all_episodes_of_season.items[0].episode_number,
                        all_episodes_of_season.items[-1].episode_number,
                        season.title
                    )
                )

            # We clearly watched SOMETHING, so for there to be no season data is a goof on Crunchyroll's end. Stub out a generic Season 1.
            if not seasons:
                logger.debug(f'Crunchyroll has not linked the show to the episode season. Inferring a Season 1...')
                seasons.append(
                    PlexSeason(
                        int(1),
                        0,  # self.__get_plex_rating(season.userRating),
                        len([viewing.episode.episode_number for viewing in history if viewing.episode.series_id == show_id and viewing.episode.episode_number is not None] or [0]),
                        all_episodes_of_season.items[0].episode_number,
                        show.episode_count,
                        show.title
                    )
                )

            watched_show = PlexWatchedSeries(
                show.title.strip(),
                show.title.strip(),
                show.title.strip(),
                show.id,
                min(
                    [viewing.episode.air_date.year for viewing in history if viewing.episode.series_id == show.id and viewing.episode.air_date is not None]
                    or [viewing.episode.premium_available_date.year for viewing in history if viewing.episode.series_id == show.id and viewing.episode.premium_available_date is not None]
                    or [show.launch_year]
                ),
                seasons,
                None,
                0  # self.__get_plex_rating(show.userRating)
            )
            watched_series.append(watched_show)

        logger.info(f"Found {len(watched_series)} watched series")

        if watched_series is not None and len(watched_series) == 0:
            return None
        else:
            return watched_series
