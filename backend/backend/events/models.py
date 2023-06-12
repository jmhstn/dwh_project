from dataclasses import dataclass
from datetime import date, datetime
from uuid import UUID


### Abstract events


@dataclass(kw_only=True)
class DwhEvent:
    event_time: datetime


@dataclass(kw_only=True)
class UserEvent(DwhEvent):
    user_id: UUID


@dataclass(kw_only=True)
class SongEvent(UserEvent):
    session_id: UUID
    song_id: UUID


### User events


@dataclass(kw_only=True)
class SignUpEvent(UserEvent):
    email: str
    country_code: str
    first_name: str
    last_name: str
    birth_date: date


@dataclass(kw_only=True)
class SignInSuccessEvent(UserEvent):
    session_id: UUID


@dataclass(kw_only=True)
class SignInFailureEvent(DwhEvent):
    reason: str


@dataclass(kw_only=True)
class UserSubscriptionEvent(UserEvent):
    session_id: UUID


### Playback events


@dataclass(kw_only=True)
class SongPlayEvent(SongEvent):
    at_time_sec: int


@dataclass(kw_only=True)
class SongStopEvent(SongEvent):
    at_time_sec: int
    finished: bool


### User library events


@dataclass(kw_only=True)
class SongLikedEvent(SongEvent):
    pass


@dataclass(kw_only=True)
class ArtistFollowedEvent(UserEvent):
    session_id: UUID
    artist_id: UUID


### Music library events


@dataclass(kw_only=True)
class ArtistCreated(DwhEvent):
    id: UUID
    name: str
    founded_year: int
    country_code: str
    genre_id: UUID


@dataclass(kw_only=True)
class CollectionCreated(DwhEvent):
    id: UUID
    name: str
    collection_type: str
    artist_id: UUID
    genre_id: UUID
    released_dt: date


@dataclass(kw_only=True)
class SongCreated(DwhEvent):
    collection_id: UUID
    artist_id: UUID
    genre_id: UUID
    id: UUID
    name: str
    duration_sec: int


@dataclass(kw_only=True)
class GenreCreated(DwhEvent):
    id: UUID
    name: str
    happiness_index: int
    mean_duration_sec: int
    country_code: str


@dataclass(kw_only=True)
class CountryEnabled(DwhEvent):
    country_code: str
