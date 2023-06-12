from uuid import UUID
from flask import Blueprint, abort, jsonify, request, g

from ..common.storage import CountryNotFound
from ..utils import validate_request
from .storage import (
    ArtistNotFound,
    GenreNotFound,
    create_artist,
    create_collection,
    create_genre,
    find_artist,
    find_collection,
    find_collections_by_artist,
    find_genre,
    find_song,
    find_songs_from_collection,
    get_genres,
    get_random_artist,
    get_random_song,
)

music = Blueprint("music", __name__, url_prefix="/music")


@music.route("/artist", methods=["POST"])
def post_artist():
    now = g.request_time
    data = request.get_json()
    validate_request(data, ["name", "founded_year", "country_code", "genre_id"])
    try:
        artist = create_artist(data, now=now)
        response = jsonify({"id": artist.id.hex})
        return response, 201
    except CountryNotFound as e:
        abort(400, f"Country code={e.country_code} not found")
    except GenreNotFound as e:
        abort(400, f"Genre id={e.genre_id} not found")


@music.route("/artist/random", methods=["GET"])
def get_artist_random():
    country = request.args.get("country", default=None)
    genre_id = request.args.get("genre_id", default=None, type=UUID)
    artist_id = get_random_artist(country, genre_id)
    if artist_id:
        return get_artist(str(artist_id))
    else:
        country_msg = f" from country {country}" if country else ""
        genre_msg = f" of genre {genre_id}" if genre_id else ""
        abort(404, f"No random artists{country_msg}{genre_msg} were found.")


@music.route("/artist/<id>", methods=["GET"])
def get_artist(id):
    artist = find_artist(UUID(id))
    if artist:
        resp = jsonify(artist.to_dict())
        return resp, 200
    else:
        abort(404, f"Artist not found by id {id}.")


@music.route("/artist/<id>/collections", methods=["GET"])
def get_collections_by_artist(id):
    try:
        collections = find_collections_by_artist(id)
        resp = []
        for col in collections:
            songs = find_songs_from_collection(col.id)
            elem = col.to_dict(songs=songs)
            resp.append(elem)
        return resp, 200
    except ArtistNotFound as e:
        abort(404, f"No artist by id {e.artist_id}.")


@music.route("/collection", methods=["POST"])
def post_collection():
    now = g.request_time
    data = request.get_json()
    validate_request(
        data, ["artist_id", "name", "type", "genre_id", "released_dt", "songs"]
    )
    for song in data["songs"]:
        validate_request(song, ["name", "duration_sec"])
    try:
        collection = create_collection(data, now=now)
        response = jsonify({"id": collection.id.hex})
        return response, 201
    except ArtistNotFound as e:
        abort(400, f"Artist id={e.artist_id} not found")
    except GenreNotFound as e:
        abort(400, f"Genre id={e.genre_id} not found")


@music.route("/collection/<collection_id>", methods=["GET"])
def get_collection(collection_id: str):
    collection = find_collection(UUID(collection_id))
    if collection:
        songs = find_songs_from_collection(UUID(collection_id))
        resp = collection.to_dict(songs=songs)
        return jsonify(resp), 200
    else:
        abort(404, f"Collection not found by id {collection_id}.")


@music.route("/genre", methods=["POST"])
def post_genre():
    now = g.request_time
    data = request.get_json()
    validate_request(
        data, ["name", "happiness_index", "mean_duration_sec", "country_code"]
    )
    genre = create_genre(data, now=now)
    response = jsonify({"id": genre.id.hex})
    return response, 201


@music.route("/genre/<genre_id>", methods=["GET"])
def get_genre(genre_id: str):
    genre = find_genre(UUID(genre_id))
    if genre:
        resp = jsonify(genre.to_dict())
        return resp, 200
    else:
        abort(404, f"Genre not found by id {genre_id}.")


@music.route("/genres", methods=["GET"])
def get_genres_all():
    country = request.args.get("country", default=None)
    genres = get_genres(country)
    resp = jsonify([g.to_dict() for g in genres])
    return resp, 200


@music.route("/song/random", methods=["GET"])
def get_song_random():
    country = request.args.get("country", default=None)
    song = get_random_song(country_code=country)
    if song:
        resp = jsonify(song.to_dict(as_collection=False))
        return resp, 200
    else:
        country_msg = f" from country {country}" if country else ""
        abort(404, f"No songs{country_msg} were found by random.")


@music.route("/song/<song_id>", methods=["GET"])
def get_song(song_id: str):
    song = find_song(UUID(song_id))
    if song:
        resp = jsonify(song.to_dict(as_collection=False))
        return resp, 200
    else:
        abort(404, f"Song not found by id {song_id}.")
