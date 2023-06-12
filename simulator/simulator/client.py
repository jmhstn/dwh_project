import json
from datetime import datetime
from typing import Optional

import urllib3
from flask import current_app as app
from tenacity import retry, stop_after_attempt, wait_random_exponential

from uuid import UUID

from .agents.clock import Clock

from .models import UserSim

log = app.logger


class APIClient:
    http_schema = "http"

    def __init__(self, host: str, port: int, clock: Clock) -> None:
        self.url = f"http://{host}:{port}"
        log.info(f"Initializing HTTP client for url {self.url}")
        self.http = urllib3.PoolManager()
        self.clock = clock

    _override_time_header = "Override-Current-Time"

    @retry(
        stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=5)
    )
    def send_post(self, path: str, data: Optional[dict] = None, jwt=None):
        url = f"{self.url}{path}"
        payload = json.dumps(data) if data else None
        headers = {
            "Content-Type": "application/json",
            "Override-Current-Time": self.clock.get_current_sim_time().isoformat(),
        }
        if jwt:
            headers["Authorization"] = f"Bearer {jwt}"
        auth = f"Bearer {jwt}" if jwt else None
        log.info(f"REQ POST {url} {payload}")
        resp = self.http.request("POST", url, headers=headers, body=payload)
        log.info(f"RESP POST {url}: HTTP {resp.status}, data: {resp.data}")
        return resp

    @retry(
        stop=stop_after_attempt(3), wait=wait_random_exponential(multiplier=1, max=5)
    )
    def send_get(self, path: str, params: Optional[dict[str, str]] = None, jwt=None):
        url = f"{self.url}{path}"
        headers = {
            "Override-Current-Time": self.clock.get_current_sim_time().isoformat()
        }
        if jwt:
            headers["Authorization"] = f"Bearer {jwt}"
        auth = f"Bearer {jwt}" if jwt else None
        log.info(f"REQ GET {url} with params {params}")
        resp = self.http.request("GET", url, headers=headers, fields=params)
        log.info(f"RESP GET {url}: HTTP {resp.status}, data: {resp.data}")
        return resp

    def sign_up(self, usersim: UserSim, profile: dict) -> UUID:
        resp = self.send_post(
            "/auth/sign_up",
            {
                "email": usersim.email,
                "password": usersim.password,
                "country_code": usersim.country_code,
                "profile": profile,
            },
        )
        return resp.json()["user_id"]

    def sign_in(self, email: str, password: str) -> str:
        resp = self.send_post(
            "/auth/sign_in",
            {"email": email, "password": password},
        )
        return resp.json()["access_token"]

    def play_song(self, song_id: UUID, start_time: int, token: str) -> None:
        resp = self.send_post(
            "/api/play", {"song_id": str(song_id), "start_time": start_time}, jwt=token
        )

    def stop_song(self, song_id: UUID, stop_time: int, token: str) -> None:
        resp = self.send_post(
            "/api/stop", {"song_id": str(song_id), "stop_time": stop_time}, jwt=token
        )

    def like(self, song_id: UUID, token: str) -> None:
        self.send_post(f"/api/like/song", {"song_id": str(song_id)}, jwt=token)

    def get_all_likes(self, token: str, artist_id: Optional[UUID] = None) -> list[UUID]:
        if artist_id is not None:
            params = {"artist_id": str(artist_id)}
        else:
            params = None
        resp = self.send_get(f"/api/like/songs", params, jwt=token)
        return resp.json()

    def follow(self, artist_id: UUID, token: str) -> None:
        self.send_post(f"/api/follow/artist", {"artist_id": str(artist_id)}, jwt=token)

    def get_all_follows(self, token: str) -> list[UUID]:
        resp = self.send_get(f"/api/follow/artists", jwt=token)
        return resp.json()

    def create_artist(self, artist_data: dict) -> UUID:
        resp = self.send_post("/music/artist", artist_data)
        return UUID(resp.json()["id"])

    def create_genre(self, genre_data: dict) -> UUID:
        resp = self.send_post("/music/genre", genre_data)
        return UUID(resp.json()["id"])

    def create_collection(self, collection_data: dict) -> UUID:
        resp = self.send_post("/music/collection", collection_data)
        return UUID(resp.json()["id"])

    def add_country(self, code: str, name: str) -> None:
        self.send_post("/common/country", {"code": code, "name": name})

    def enable_country(self, country_code: str) -> None:
        self.send_post(f"/common/country/{country_code}/enable")

    def get_countries(self, only_enabled: bool = False) -> dict:
        resp = self.send_get(
            f"/common/countries", params={"enabled": str(only_enabled)}
        )
        return {
            country["code"]: datetime.fromisoformat(country["enabled_at"])
            if country["enabled"]
            else None
            for country in resp.json()
        }

    def get_genre(self, genre_id: UUID) -> dict:
        resp = self.send_get(f"/music/genre/{str(genre_id)}")
        return resp.json()

    def get_genres(self, country: Optional[str] = None) -> list[dict]:
        params = {} if country is None else {"country": country}
        resp = self.send_get(f"/music/genres", params=params)
        return resp.json()

    def get_artist(self, artist_id: UUID) -> dict:
        resp = self.send_get(f"/music/artist/{str(artist_id)}")
        return resp.json()

    def get_song(self, song_id: UUID) -> dict:
        resp = self.send_get(f"/music/song/{str(song_id)}")
        return resp.json()

    def get_random_song(self, country: Optional[str] = None) -> Optional[dict]:
        params = {} if country is None else {"country": country}
        resp = self.send_get(f"/music/song/random", params)
        if resp.status == 404:
            return None
        else:
            return resp.json()

    def get_random_artist(
        self, country: Optional[str] = None, genre_id: Optional[UUID] = None
    ) -> Optional[dict]:
        params = {}
        if country:
            params["country"] = country
        if genre_id:
            params["genre_id"] = genre_id
        resp = self.send_get(f"/music/artist/random", params)
        if resp.status == 404:
            return None
        else:
            return resp.json()

    def get_collection(self, collection_id: UUID) -> dict:
        resp = self.send_get(f"/music/collection/{str(collection_id)}")
        return resp.json()

    def get_collections_by_artist(self, artist_id: UUID) -> dict:
        resp = self.send_get(f"/music/artist/{str(artist_id)}/collections")
        return resp.json()

    def post_subscribe(self, token: str) -> None:
        resp = self.send_post("/api/subscribe", jwt=token)
