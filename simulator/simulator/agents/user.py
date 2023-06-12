from enum import Enum
import random
from collections import deque
from uuid import UUID
from flask import current_app as app
from scipy.stats import lognorm

from simulator.simulator.utils import clamp

from .clock import Clock
from .. import db
from ..utils import commit_db
from ..client import APIClient
from ..models import SongSim, UserSim

log = app.logger


class UserAgentState(Enum):
    CREATED = 0
    IDLE = 1
    LISTENING = 2
    LEFT = 3
    INVALID = 4


class UserAgent:
    # todo better logging
    def __init__(
        self,
        agent_id: int,
        usersim: UserSim,
        api_client: APIClient,
        sim_params,
        clock: Clock,
    ) -> None:
        self.clock = clock
        self.sim_params = sim_params
        self.agent_id = agent_id
        self.usersim = usersim
        self.api_client = api_client
        self.state = UserAgentState.CREATED
        self.song_queue = deque()

    def __repr__(self) -> str:
        return f"UserAgent<agent_id={self.agent_id}, usersim={self.usersim}>"

    def _sign_in(self) -> None:
        try:
            self.token = self.api_client.sign_in(
                self.usersim.email, self.usersim.password
            )
            self.start_time = self.clock.get_current_sim_time()
            log.info(
                f"[USER-{self.agent_id}] Signed in successfully at {self.start_time} sim time."
            )
            self.state = UserAgentState.IDLE
        except:
            log.info(f"[USER-{self.agent_id}] Sign in failed.")
            self.state = UserAgentState.INVALID

    def _choose_liked_music(self) -> None:
        if len(self.liked_songs) == 0:
            pick_collection = True
        elif len(self.followed_artists) == 0:
            pick_collection = False
        else:
            pick_collection = random.random() < self.usersim.collection_p

        if pick_collection:
            log.debug(
                f"[USER-{self.agent_id}] Wants to listen to a collection of a followed artist."
            )
            # listen to a random collection from a followed artist
            next_artist = random.choice(self.followed_artists)
            collections = self.api_client.get_collections_by_artist(
                artist_id=next_artist
            )
            collection = random.choice(collections)
            self.song_queue.extend(collection["songs"])
            log.debug(
                f"[USER-{self.agent_id}] Added to queue collection id={collection['id']}. Queue: {self.song_queue}"
            )
        else:
            # listen to a random liked song
            log.debug(f"[USER-{self.agent_id}] Wants to listen to a liked song.")
            next_id = random.choice(self.liked_songs)
            song = self.api_client.get_song(next_id)
            self.song_queue.append(song)
            log.debug(f"[USER-{self.agent_id}] Listening to song={next_id}.")
        self.state = UserAgentState.LISTENING

    def _choose_new_music(self) -> None:
        log.debug(f"[USER-{self.agent_id}] Searching for new music.")
        from_same_country = random.random() < self.usersim.patriot_p
        country_param = self.usersim.country_code if from_same_country else None
        if random.random() < self.usersim.popular_p:
            log.debug(f"[USER-{self.agent_id}] Searching for most popular songs.")
            len_playlist = random.randint(1, 5)
            songs = self._get_most_popular_songs(len_playlist)
            if songs:
                log.debug(
                    f"[USER-{self.agent_id}] Found {len_playlist} most popular songs: {songs}."
                )
                for songsim in songs:
                    song = self.api_client.get_song(songsim.song_id)
                    self.song_queue.append(song)
                self.state = UserAgentState.LISTENING
            else:
                log.debug(
                    f"[USER-{self.agent_id}] No popular songs found, looking for a random song."
                )
                song = self.api_client.get_random_song(country_param)
                log.debug(f"[USER-{self.agent_id}] Found random song {song}.")
                if song:
                    self.song_queue.append(song)
                    self.state = UserAgentState.LISTENING
        else:
            log.debug(
                f"[USER-{self.agent_id}] Searching for new artists by favorite genres."
            )
            if self.liked_songs:
                random_liked_song = random.choice(self.liked_songs)
                genre_id = self.api_client.get_song(random_liked_song)["genre_id"]
                artist = self.api_client.get_random_artist(
                    country=country_param, genre_id=genre_id
                )
            else:
                artist = None

            if artist is None:
                log.debug(
                    f"[USER-{self.agent_id}] No artist found, chosing random artist."
                )
                artist = self.api_client.get_random_artist(country=country_param)

            if artist:
                log.debug(f"[USER-{self.agent_id}] Found artist {artist}.")
                collections = self.api_client.get_collections_by_artist(
                    artist_id=artist["id"]
                )
                if collections:
                    chosen = random.choice(collections)
                    log.debug(f"[USER-{self.agent_id}] Chose collection {chosen}.")
                    for song in chosen["songs"]:
                        song["artist_id"] = artist["id"]
                        song["collection_id"] = chosen["id"]
                        song["genre_id"] = artist["genre_id"]
                        self.song_queue.append(song)
                    self.state = UserAgentState.LISTENING
                else:
                    log.debug(
                        f"[USER-{self.agent_id}] Artist {artist} has not released anything yet."
                    )
            else:
                log.debug(f"[USER-{self.agent_id}] No new artists found.")

    def _consider_leaving(self) -> bool:
        p = self.sim_params["prob_leave_session"]
        if random.random() < p:
            log.info(f"[USER-{self.agent_id}] User left.")
            self.state = UserAgentState.LEFT
            return True
        return False

    def _consider_subscription(self) -> None:
        p = self.sim_params["prob_subscription"]
        if not self.usersim.is_premium and random.random() < p:
            self.api_client.post_subscribe(self.token)
            self.usersim.is_premium = True
            db.session.add(self.usersim)
            commit_db(self.usersim, "UserSim")
            log.info(f"[USER-{self.agent_id}] User made a premium subscription.")

    def _choose_next_action(self) -> None:
        if self._consider_leaving():
            return
        self._consider_subscription()
        self.liked_songs = self.api_client.get_all_likes(token=self.token)
        self.followed_artists = self.api_client.get_all_follows(self.token)
        if len(self.liked_songs) == 0 and len(self.followed_artists) == 0:
            search_new = True
        else:
            search_new = random.random() < self.usersim.explorer_p
        if search_new:
            self._choose_new_music()
        else:
            self._choose_liked_music()

    def _get_skip_time(self, duration: int) -> int:
        return clamp(int(lognorm.rvs(s=0.999, loc=-1, scale=10)), 1, duration)

    def _like_song(self, song) -> None:
        song_id = song["id"]
        log.debug(f"[USER-{self.agent_id}] Wants to like song {song_id}.")
        self.api_client.like(song_id, self.token)
        self.liked_songs.append(song_id)
        artist_id = song["artist_id"]
        by_artist = self.api_client.get_all_likes(self.token, artist_id=artist_id)

        # maybe also follow the artist (if liked already 3 or more their songs)
        follow_artist = (
            artist_id not in self.followed_artists
            and random.random() < 0.8
            and len(by_artist) >= 3
        )
        if follow_artist:
            log.debug(
                f"[USER-{self.agent_id}] Followed new artist {song['artist_id']}."
            )
            self.api_client.follow(artist_id, token=self.token)
            self.followed_artists.append(artist_id)

        # maybe put on repeat after the like
        on_repeat = random.random() < 0.8
        if on_repeat:
            num_listen = random.randint(1, 4)
            self.song_queue.extendleft([song] * num_listen)
            log.debug(
                f"[USER-{self.agent_id}] Put the song on repeat {num_listen} times. Current queue: {self.song_queue}"
            )

    def _get_most_popular_songs(self, count: int) -> list[SongSim]:
        songsims = (
            SongSim.query.order_by(SongSim.listen_count.desc()).limit(count).all()
        )
        return songsims

    def _inc_song_listen_count(self, song_id: UUID) -> None:
        log.debug(f"[USER-{self.agent_id}] Looking for SongSim by id {song_id}")
        songsim = SongSim.query.get(song_id)
        if songsim is None:
            update = False
            songsim = SongSim(song_id=song_id)
            songsim.listen_count = 0
        else:
            update = True
            songsim.listen_count += 1
        db.session.add(songsim)
        db.session.commit()
        db.session.flush()

    async def _listen_to_next(self) -> None:
        if self._consider_leaving():
            log.debug(
                f"[USER-{self.agent_id}] Clearing song queue ({self.song_queue})."
            )
            self.song_queue.clear()
            return
        if len(self.song_queue) == 0:
            log.debug(f"[USER-{self.agent_id}] Song queue is empty, going idle.")
            self.state = UserAgentState.IDLE
        else:
            next_song = self.song_queue.popleft()
            next_song_id = next_song["id"]
            # some probability that the song will not be finished, more likely stopped near the start
            is_full_listen = random.random() > self.usersim.skip_p
            if is_full_listen:
                listen_for = next_song["duration_sec"]
                log.debug(f"[USER-{self.agent_id}] Full listen for {listen_for}.")
            else:
                listen_for = self._get_skip_time(next_song["duration_sec"])
                log.debug(f"[USER-{self.agent_id}] Will skip at {listen_for}s.")
            # listen to the song
            self.api_client.play_song(next_song_id, start_time=0, token=self.token)
            await self.clock.sim_seconds(listen_for)
            self.api_client.stop_song(
                next_song_id, stop_time=listen_for, token=self.token
            )
            if listen_for > 0.5 * next_song["duration_sec"]:
                # increase song listen count (for popularity stats)
                self._inc_song_listen_count(next_song_id)
                # maybe like the song
                like_the_song = (
                    random.random() > self.usersim.picky_p
                    and not next_song_id in self.liked_songs
                )
                if like_the_song:
                    self._like_song(next_song)
                else:
                    log.debug(f"[USER-{self.agent_id}] Won't like the song.")

                log.debug(
                    f"[USER-{self.agent_id}] Finished listening at {listen_for}: {next_song}."
                )
            else:
                # skipped the song
                log.debug(
                    f"[USER-{self.agent_id}] Skipped at {listen_for}s: {next_song}."
                )

    async def _next_state(self) -> None:
        log.debug(f"[USER-{self.agent_id}] Going to the next state from {self.state}.")
        match self.state:
            case UserAgentState.CREATED:
                self._sign_in()
            case UserAgentState.IDLE:
                self._choose_next_action()
            case UserAgentState.LISTENING:
                await self._listen_to_next()

    async def run(self, running) -> None:
        self.running = running
        log.info(f"[USER-{self.agent_id}] Starting user agent.")
        while self.state is not UserAgentState.LEFT:
            await self.running.wait()
            try:
                await self.clock.sim_seconds(
                    self.sim_params["delay_between_states_sec"]
                )
                await self._next_state()
            except:
                log.exception(f"[USER-{self.agent_id}] Could not go to the next state.")
                self.state = UserAgentState.INVALID
                return
