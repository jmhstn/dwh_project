from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional
from uuid import UUID
from sqlalchemy.sql.expression import func

from .. import db, log
from ..common.storage import CountryNotFound, find_country
from ..events import send_event
from ..events.models import ArtistCreated, CollectionCreated, GenreCreated, SongCreated
from ..utils import commit_db
from .models import Artist, Collection, Genre, Song


@dataclass
class ArtistNotFound(Exception):
    artist_id: UUID


@dataclass
class GenreNotFound(Exception):
    genre_id: UUID


@dataclass
class SongNotFound(Exception):
    song_id: UUID


class PlaybackError(Exception):
    pass


def find_artist(id: UUID) -> Optional[Artist]:
    artist = Artist.query.get(id)
    if artist:
        log.debug(f"Found artist {artist} by id={id}.")
    else:
        log.debug(f"No artist found by id={id}.")
    return artist


def find_collection(id: UUID) -> Optional[Collection]:
    collection = Collection.query.get(id)
    if collection:
        log.debug(f"Found collection {collection} by id={id}.")
    else:
        log.debug(f"No collection found by id={id}.")
    return collection


def find_songs_from_collection(id: UUID) -> list[Song]:
    songs = Song.query.filter_by(collection_id=id).all()
    if songs:
        log.debug(f"Found {len(songs)} songs by collection id={id}.")
    else:
        log.debug(f"No song found from collection id={id}.")
    return songs


def find_collections_by_artist(id: UUID) -> list[Collection]:
    if not find_artist(id):
        raise ArtistNotFound(artist_id=id)
    collections = Collection.query.filter_by(artist_id=id).all()
    if collections:
        log.debug(f"Found {len(collections)} collection by artist id={id}.")
    else:
        log.debug(f"No collections found by artist id={id}.")
    return collections


def find_song(id: UUID) -> Optional[Song]:
    song = Song.query.get(id)
    if song:
        log.debug(f"Found song {song} by id={id}.")
    else:
        log.debug(f"No song found by id={id}.")
    return song


def get_random_song(country_code: Optional[str] = None) -> Optional[Song]:
    q = Song.query
    if country_code:
        country = find_country(country_code)
        if country:
            q = q.join(Artist, Artist.id == Song.artist_id).filter(
                Artist.country_code == country.code
            )
        else:
            raise CountryNotFound(country_code=country_code)
    song = q.order_by(func.random()).first()
    if song:
        log.debug(f"Randomly found song {song} (country {country_code}).")
    else:
        log.debug(f"There is no songs (country {country_code}).")
    return song


def get_random_artist(
    country_code: Optional[str] = None, genre_id: Optional[UUID] = None
) -> Optional[UUID]:
    q = Artist.query
    if country_code:
        country = find_country(country_code)
        if country:
            q = q.filter(Artist.country_code == country.code)
        else:
            raise CountryNotFound(country_code=country_code)
    if genre_id:
        genre = find_genre(genre_id)
        if genre:
            q = q.filter(Artist.genre_id == genre_id)
        else:
            raise GenreNotFound(genre_id=genre_id)
    artist = q.order_by(func.random()).first()
    country_msg = f" from country {country_code}" if country_code else ""
    genre_msg = f" of genre {genre_id}" if genre_id else ""
    if artist:
        log.debug(f"Randomly found artist {artist}{country_msg}{genre_msg}.")
        return artist.id
    else:
        log.debug(f"No random artist{country_msg}{genre_msg} in the library.")
        return None


def find_genre(id: UUID) -> Optional[Genre]:
    genre = Genre.query.get(id)
    if genre:
        log.debug(f"Found genre {genre} by id={id}.")
    else:
        log.debug(f"No genre found by id={id}.")
    return genre


def create_genre(genre_data, now: datetime) -> Genre:
    country_code = genre_data["country_code"]
    if not find_country(country_code):
        raise CountryNotFound(country_code=country_code)
    genre = Genre(**genre_data)
    log.info(f"Creating a genre: {genre_data}")
    db.session.add(genre)
    commit_db(genre, "Genre", is_update=False)
    # send a dwh event
    event = GenreCreated(
        id=genre.id,
        name=genre.name,
        happiness_index=genre.happiness_index,
        mean_duration_sec=genre.mean_duration_sec,
        country_code=genre.country_code,
        event_time=now,
    )
    send_event(event)
    return genre


def create_artist(artist_data, now: datetime) -> Artist:
    country_code = artist_data["country_code"]
    if not find_country(country_code):
        raise CountryNotFound(country_code=country_code)
    genre_id = artist_data["genre_id"]
    if not find_genre(genre_id):
        raise GenreNotFound(genre_id=genre_id)
    artist = Artist(**artist_data)
    log.info(f"Creating an artist: {artist_data}")
    db.session.add(artist)
    commit_db(artist, "Artist", is_update=False)
    # send a dwh event
    event = ArtistCreated(
        id=artist.id,
        name=artist.name,
        founded_year=artist.founded_year,
        country_code=artist.country_code,
        genre_id=artist.genre_id,
        event_time=now,
    )
    send_event(event)
    return artist


def _create_song(now: datetime, **data) -> Song:
    song = Song(**data)
    db.session.add(song)
    commit_db(song, "Song", is_update=False)
    song_event = SongCreated(
        id=song.id,
        collection_id=song.collection_id,
        artist_id=song.artist_id,
        genre_id=song.genre_id,
        name=song.name,
        duration_sec=song.duration_sec,
        event_time=now,
    )
    send_event(song_event)
    return song


def create_collection(data: dict, now: datetime) -> Collection:
    # TODO do it transactionally
    # check artist_id
    artist = find_artist(data["artist_id"])
    if not artist:
        raise ArtistNotFound(artist_id=data["artist_id"])
    # check genre_id
    genre = find_genre(data["genre_id"])
    if not genre:
        raise GenreNotFound(genre_id=data["genre_id"])
    # create collection
    released_dt = date.fromisoformat(
        data["released_dt"]
    )  # ISO 8601 date example: '2022-31-12'
    collection = Collection(
        artist_id=artist.id,
        name=data["name"],
        collection_type=data["type"],
        genre_id=genre.id,
        released_dt=released_dt,
    )
    db.session.add(collection)
    # flush so that we can get the id of the collection
    commit_db(collection, "Collection", is_update=False)
    # send dwh event
    collection_event = CollectionCreated(
        id=collection.id,
        name=collection.name,
        collection_type=collection.collection_type,
        artist_id=collection.artist_id,
        genre_id=collection.genre_id,
        released_dt=collection.released_dt,
        event_time=now,
    )
    send_event(collection_event)
    # create each song and send a dwh event for each
    songs = data["songs"]
    for song_data in songs:
        song_data["collection_id"] = collection.id
        song_data["artist_id"] = artist.id
        song_data["genre_id"] = genre.id
        song = _create_song(now=now, **song_data)
    return collection


def get_genres(country: Optional[str] = None) -> list[Genre]:
    """
    Returns the list of all music genres.

    Args:
        country: return only genres from a selected country.
    """
    q = Genre.query
    if country is not None:
        q = q.filter(Genre.country_code == country.upper())
    genres = q.all()
    return genres
