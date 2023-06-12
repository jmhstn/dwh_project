from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional
from uuid import UUID

import bcrypt
from flask import abort


from .. import db, log
from ..common.storage import find_country
from ..events import send_event
from ..events.models import (
    ArtistFollowedEvent,
    SignUpEvent,
    SongLikedEvent,
    SongPlayEvent,
    SongStopEvent,
    UserSubscriptionEvent,
)
from ..music.models import Artist, Collection, Song
from ..music.storage import (
    ArtistNotFound,
    PlaybackError,
    SongNotFound,
    find_artist,
    find_song,
)
from ..utils import commit_db
from .models import ArtistFollow, SongLike, User


@dataclass
class NewUserData:
    email: str
    password: str
    country_code: str
    profile: dict


@dataclass
class UserAlreadyExists(Exception):
    email: str


@dataclass
class UserAlreadySubscribed(Exception):
    email: str


def find_user(email: str) -> Optional[User]:
    user = User.query.filter_by(email=email).first()
    if user:
        log.debug(f"Found user {user} by email {email}.")
    else:
        log.debug(f"No user found by email {email}.")
    return user


def create_user(data: NewUserData, now: datetime) -> User:
    """
    Raises:
        UserAlreadyExists: if user with the given email already exists.
        CountryNotFound: if no country found by the given code.
        CountryNotEnabled: if the country is disabled and we can't register users from this country yet.
    """
    log.debug(f"Creating new user from data: {vars(data)}")
    if find_user(data.email):
        raise UserAlreadyExists(data.email)
    country = find_country(data.country_code)
    if not country:
        abort(400, f"Country {data.country_code} not found.")
    if not country.is_enabled():
        abort(400, f"Country {data.country_code} not enabled.")
    first_name = data.profile["first_name"]
    last_name = data.profile["last_name"]
    birth_date_opt = data.profile["birth_date"]
    birth_date = date.fromisoformat(birth_date_opt) if birth_date_opt else None
    user = User(
        email=data.email,
        country_code=data.country_code,
        first_name=first_name,
        last_name=last_name,
        birth_date=birth_date,
    )
    user.set_password(data.password)
    log.info(f"Creating new user: {user}.")
    db.session.add(user)
    commit_db(user, "User", is_update=False)
    event = SignUpEvent(
        user_id=user.id,
        email=user.email,
        country_code=user.country_code,
        first_name=user.first_name,
        last_name=user.last_name,
        birth_date=user.birth_date,
        event_time=now,
    )
    send_event(event)
    return user


@dataclass
class UserNotFound(Exception):
    email: str


@dataclass
class InvalidPassword(Exception):
    user_id: UUID


def validate_password(email: str, password: str) -> User:
    """
    Raises:
        UserNotFound: if no user found by this email.
        InvalidPassword: if the given password and the user's password don't match.
    """
    user = find_user(email)
    if not user:
        raise UserNotFound(email=email)
    valid = bcrypt.checkpw(
        password.encode(encoding="utf-8"), user.password_hash.encode(encoding="utf-8")
    )
    if valid:
        log.info(f"Successful sign in for user {user}")
        return user
    else:
        raise InvalidPassword(user_id=user.id)


def like_song(user_id: UUID, session_id: UUID, song_id: UUID, now: datetime) -> None:
    song = find_song(song_id)
    if not song:
        raise SongNotFound(song_id=song_id)
    like = SongLike(user_id=user_id, song_id=song.id)
    db.session.add(like)
    commit_db(like, "SongLike", is_update=False)
    log.info(f"User {user_id} liked song id={song.id}.")
    send_event(
        SongLikedEvent(
            user_id=user_id, session_id=session_id, song_id=song.id, event_time=now
        )
    )


def all_liked_songs(user_id: UUID, artist_id: Optional[UUID] = None) -> list[str]:
    if artist_id:
        artist = find_artist(artist_id)
        if not artist:
            raise ArtistNotFound(artist_id=artist_id)
        q = (
            db.session.query(SongLike)
            .join(Song, Song.id == SongLike.song_id)
            .filter(Song.artist_id == artist_id and SongLike.user_id == user_id)
        )
    else:
        q = SongLike.query.filter_by(user_id=user_id)
    likes = q.all()
    songs = [like.song_id for like in likes]
    return songs


def follow_artist(
    user_id: UUID, session_id: UUID, artist_id: UUID, now: datetime
) -> None:
    artist = find_artist(artist_id)
    if not artist:
        raise ArtistNotFound(artist_id=artist_id)
    follow = ArtistFollow(user_id=user_id, artist_id=artist.id)
    db.session.add(follow)
    commit_db(follow, "ArtistFollow", is_update=False)
    log.info(f"User {user_id} followed artist id={artist.id}.")
    send_event(
        ArtistFollowedEvent(
            user_id=user_id, session_id=session_id, artist_id=artist.id, event_time=now
        )
    )


def all_followed_artists(user_id: UUID) -> list[str]:
    follows = ArtistFollow.query.filter_by(user_id=user_id).all()
    artists = [follow.artist_id for follow in follows]
    return artists


def play_song(
    user_id: UUID, session_id: UUID, song_id: UUID, start_time: int, now: datetime
) -> None:
    """
    Sends a DWH event that user has started playing the song at `start_time`.

    Raises:
        SongNotFound: if no song found by the given id.
        PlaybackError: if duration is bigger than the length of the song.
    """
    song = find_song(song_id)
    if not song:
        raise SongNotFound(song_id=song_id)
    if start_time < 0 or start_time >= song.duration_sec:
        raise PlaybackError(
            f"Couldn't play song at {start_time}s: duration is {song.duration_sec}s"
        )
    send_event(
        SongPlayEvent(
            user_id=user_id,
            session_id=session_id,
            song_id=song_id,
            at_time_sec=start_time,
            event_time=now,
        )
    )


def stop_song(
    user_id: UUID, session_id: UUID, song_id: UUID, stop_time: int, now: datetime
) -> None:
    """
    Sends a DWH event that user has stopped playing the song at `stop_time`.

    Song is considered finished if `stop_time` is equal to the song's duration.

    Raises:
        SongNotFound: if no song found by the given id.
        PlaybackError: if duration is bigger than the length of the song.
    """
    song = find_song(song_id)
    if not song:
        raise SongNotFound(song_id=song_id)
    if stop_time < 0 or stop_time > song.duration_sec:
        raise PlaybackError(
            f"Couldn't stop song at {stop_time}s: duration is {song.duration_sec}s"
        )
    finished = stop_time == song.duration_sec
    send_event(
        SongStopEvent(
            user_id=user_id,
            song_id=song_id,
            session_id=session_id,
            at_time_sec=stop_time,
            finished=finished,
            event_time=now,
        )
    )


def subscribe_user(user: User, session_id: UUID, now: datetime) -> None:
    if user.is_premium:
        log.error(f"Cannot subscribe user: {user} is already premium.")
        raise UserAlreadySubscribed(user.email)
    user.is_premium = True
    db.session.add(user)
    commit_db(user, "User")
    send_event(
        UserSubscriptionEvent(user_id=user.id, session_id=session_id, event_time=now)
    )
    log.info(f"User {user} bought a premium subscription.")
