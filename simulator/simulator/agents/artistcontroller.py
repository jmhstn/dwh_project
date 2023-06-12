import asyncio
import queue
import random
from datetime import date, datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from flask import current_app as app
from numpy.random import normal
from sqlalchemy.sql.expression import func

log = app.logger

from .. import db
from ..client import APIClient
from ..models import ArtistSim
from ..namegen import fake
from ..utils import clamp
from . import Clock


class CollectionType(Enum):
    LP = 0
    EP = 1
    SINGLE = 2


class ArtistControllerAgent:
    def __init__(self, api_client: APIClient, config, clock: Clock) -> None:
        self.clock = clock
        # config values
        self.sim_params = config["STAT_ARTIST_CONTROLLER"]

        self.last_artist_id = 0
        self.api_client = api_client
        self.config = config

        self.countries = self.api_client.get_countries()
        self.artist_query: queue.Queue[ArtistSim] = queue.Queue(maxsize=100)

    def warmup_db(self) -> None:
        self.countries = self.api_client.get_countries()
        num_of_artists = self.config["WARMUP_NUM_OF_ARTISTS"]
        total_artists = 0
        total_collections = 0
        for _ in range(num_of_artists):
            artist_sim = self._generate_artist(warmup=True)
            if artist_sim:
                total_artists += 1
                num_collection = random.randint(1, 3)
                for _ in range(num_collection):
                    self._generate_collection(artist_sim)
                    total_collections += 1
        log.info(
            f"{total_artists} artists and {total_collections} collections generated."
        )

    def _get_next_artist_id(self) -> int:
        self.last_artist_id += 1
        return self.last_artist_id

    def _generate_average_duration(self) -> int:
        min_v = 30
        mu, sigma = 210, 200  # let 210 seconds be global average
        while True:
            v = normal(210, 200)
            if v > min_v:
                return int(v)

    def _generate_new_genre(self, country_code: str) -> UUID:
        genre_data = {
            "name": fake.genre(),
            "happiness_index": random.randint(-100, 100),
            "mean_duration_sec": self._generate_average_duration(),
            "country_code": country_code,
        }
        log.info(f"Creating new genre with data {genre_data}.")
        id = self.api_client.create_genre(genre_data)
        return id

    def _derive_new_genre(self, genre: dict, country: str) -> UUID:
        new_name = fake.genre(base=genre["name"])
        happiness_i = genre["happiness_index"] + random.randint(-10, 10)
        mean_duration = genre["mean_duration_sec"] + random.randint(-30, 30)
        new_genre_data = {
            "name": new_name,
            "happiness_index": clamp(happiness_i, -100, 100),
            "mean_duration_sec": mean_duration,
            "country_code": country,
        }
        log.info(f"Deriving new genre with data {new_genre_data}, old genre: {genre}.")
        id = self.api_client.create_genre(new_genre_data)
        return id

    def _pick_existing_genre(self, country_code: str) -> Optional[UUID]:
        same_country = random.random() < self.sim_params["prob_same_country_genre"]
        genres = self.api_client.get_genres(country_code if same_country else None)
        if genres:
            genre = random.choice(genres)
            genre_id = UUID(genre["id"])
            if same_country:
                derive_new_genre_p = self.sim_params["prob_derived_genre_same_country"]
            else:
                derive_new_genre_p = self.sim_params["prob_derived_genre_diff_country"]
            derive_new_genre = random.random() < derive_new_genre_p
            if derive_new_genre:
                genre_id = self._derive_new_genre(genre, country_code)
            return genre_id
        else:
            log.info(f"No genres for country {country_code}, skipping.")
            return None

    def _generate_country(self) -> str:
        return random.choice(list(self.countries.keys()))

    def _generate_artist(self, warmup: bool) -> Optional[ArtistSim]:
        artist = fake.artist()
        current_year = date.today().year
        founded_year = (
            current_year if not warmup else random.randint(1950, date.today().year)
        )
        # generate country and genre
        country = self._generate_country()
        is_new_genre = random.random() < self.sim_params["prob_new_genre"]
        if warmup or is_new_genre:
            genre_id = self._generate_new_genre(country)
        else:
            genre_id = self._pick_existing_genre(country)

        if genre_id is None:
            return None
        else:
            artist_data = {
                "name": artist,
                "founded_year": founded_year,
                "country_code": country,
                "genre_id": str(genre_id),
            }
            log.info(f"Creating a new artist: {artist_data}.")
            artist_id = self.api_client.create_artist(artist_data)
            # Create artist sim row in the simulator's db
            artist_sim = ArtistSim(artist_id=artist_id)
            db.session.add(artist_sim)
            db.session.commit()
            log.info(f"Artist '{artist}' successfully created with id={artist_id}.")
            return artist_sim

    def _generate_songs(self, col_type: CollectionType, genre: dict) -> list[dict]:
        match col_type:
            case CollectionType.LP:
                num_songs = random.randint(7, 15)
            case CollectionType.EP:
                num_songs = random.randint(4, 7)
            case CollectionType.SINGLE:
                num_songs = random.randint(1, 3)
        songs = []
        for _ in range(num_songs):
            duration = normal(loc=genre["mean_duration_sec"], scale=30)
            song = {"name": fake.song(), "duration_sec": max(duration, 30)}
            songs.append(song)
        return songs

    def _generate_collection(self, artist_sim: ArtistSim) -> UUID:
        # general collection info
        artist = self.api_client.get_artist(artist_sim.artist_id)
        type: CollectionType = random.choice(list(CollectionType))
        released_dt = date.today()
        name = fake.collection()

        # use the artist's genre or create a new one
        genre = self.api_client.get_genre(artist["genre_id"])
        genre_id = genre["id"]
        if random.random() < self.sim_params["prob_col_new_genre"]:
            # create some new genre for this collection
            if random.random() < self.sim_params["prob_col_invent_genre"]:
                # completely new genre
                genre_id = self._generate_new_genre(country_code=artist["country_code"])
            else:
                # derive from existing
                genre_id = self._derive_new_genre(genre, country=artist["country_code"])

        data = {
            "artist_id": str(artist["id"]),
            "name": name,
            "type": type.name,
            "genre_id": str(genre_id),
            "released_dt": released_dt.isoformat(),
            "songs": self._generate_songs(type, genre),
        }
        log.info(f"Creating a new collection: {data}.")

        collection_id = self.api_client.create_collection(data)
        log.info(f"Collection '{name}' successfully created with id={collection_id}.")

        # Update sim model information
        artist_sim = ArtistSim.query.get(artist["id"])
        artist_sim.last_release_dtm = datetime.utcnow()
        if random.random() < self.sim_params["prob_retired"]:
            artist_sim.retired = True

        return collection_id

    async def _create_artist_task(self) -> None:
        """
        Periodically generates a new artist.
        """
        while True:
            await self.running.wait()
            try:
                await self.clock.sim_seconds(
                    (self.sim_params["delay_create_artist_sec"])
                )
                self._generate_artist(warmup=False)
            except:
                log.exception("Error at _create_artist_task")

    async def _select_artists_task(self) -> None:
        while True:
            await self.running.wait()
            try:
                await self.clock.sim_seconds(
                    (self.sim_params["delay_select_artist_sec"])
                )
                artist = (
                    ArtistSim.query.filter_by(retired=False)
                    .order_by(func.random())
                    .first()
                )
                if artist is not None:
                    self.artist_query.put(artist)
                    log.debug(
                        f"Selected for running artist {artist}. Current artist query: {self.artist_query.qsize()}."
                    )
                else:
                    log.debug(
                        f"No active artists selected. Current artist query: {self.artist_query.qsize()}"
                    )
            except:
                log.exception("Error at _select_artists_task")

    async def _run_artist_task(self) -> None:
        """
        Takes an artist and creates a new collection by him.

        More likely to be chosen are recently founded or recently active artists.
        """
        while True:
            await self.running.wait()
            try:
                await self.clock.sim_seconds((self.sim_params["delay_run_artist_sec"]))
                if not self.artist_query.empty():
                    id = self.artist_query.get()
                    self._generate_collection(id)
                else:
                    log.info(f"Run artists: artist query is empty.")
            except:
                log.exception("Error at _run_artist_task")

    async def run(self, running) -> None:
        log.info(f"Initializing artist controller agent.")
        self.running = running
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._create_artist_task())
            tg.create_task(self._select_artists_task())
            tg.create_task(self._run_artist_task())
        log.info(f"Artist controller agent tasks stopped.")
