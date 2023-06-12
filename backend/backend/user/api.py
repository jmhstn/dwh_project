from uuid import UUID
from flask import Blueprint, abort, jsonify, request, g
from flask_jwt_extended import current_user, get_jwt, jwt_required

from backend.backend.utils import validate_request

from .. import log
from ..music.storage import ArtistNotFound, PlaybackError, SongNotFound
from ..user.storage import (
    all_followed_artists,
    all_liked_songs,
    follow_artist,
    play_song,
    stop_song,
    like_song,
    subscribe_user,
)

api = Blueprint("api", __name__, url_prefix="/api")


@api.route("/play", methods=["POST"])
@jwt_required()
def post_play():
    now = g.request_time
    session_id = UUID(get_jwt()["session_id"])
    data = request.get_json()
    try:
        validate_request(data, ["song_id", "start_time"])
        song_id = UUID(data["song_id"])
        start_time = data["start_time"]
        play_song(
            user_id=current_user.id,
            session_id=session_id,
            song_id=song_id,
            start_time=start_time,
            now=now,
        )
        return "OK", 200
    except SongNotFound as e:
        log.error(
            f"Could not play song: song not found. Req: {data}, user_id={current_user.id}."
        )
        abort(404, "Song not found.")
    except PlaybackError as e:
        log.error(
            f"Could not play song: invalid start time. Req: {data}, user_id={current_user.id}."
        )
        abort(400, "Invalid start time.")


@api.route("/stop", methods=["POST"])
@jwt_required()
def post_stop():
    now = g.request_time
    session_id = UUID(get_jwt()["session_id"])
    data = request.get_json()
    try:
        validate_request(data, ["song_id", "stop_time"])
        song_id = UUID(data["song_id"])
        stop_time = data["stop_time"]
        stop_song(
            user_id=current_user.id,
            session_id=session_id,
            song_id=song_id,
            stop_time=stop_time,
            now=now,
        )
        return "OK", 200
    except SongNotFound as e:
        log.error(
            f"Could not stop song: song not found. Req: {data}, user_id={current_user.id}."
        )
        abort(404, "Song not found.")
    except PlaybackError as e:
        log.error(
            f"Could not stop song: invalid stop time. Req: {data}, user_id={current_user.id}."
        )
        abort(400, "Invalid stop time.")


@api.route("/like/song", methods=["POST"])
@jwt_required()
def post_like_song():
    now = g.request_time
    session_id = UUID(get_jwt()["session_id"])
    data = request.get_json()
    try:
        validate_request(data, ["song_id"])
        like_song(
            user_id=current_user.id,
            session_id=session_id,
            song_id=UUID(data["song_id"]),
            now=now,
        )
        return "OK", 200
    except SongNotFound as e:
        log.error(
            f"Could not like song: song not found. Req: {data}, user_id={current_user.id}."
        )
        abort(404, "Song not found.")


@api.route("/like/songs", methods=["GET"])
@jwt_required()
def get_liked_songs():
    by_artist = request.args.get("artist_id", default=None, type=UUID)
    songs = all_liked_songs(user_id=current_user.id, artist_id=by_artist)
    return jsonify(songs), 200


@api.route("/follow/artist", methods=["POST"])
@jwt_required()
def post_follow_artist():
    now = g.request_time
    session_id = UUID(get_jwt()["session_id"])
    data = request.get_json()
    try:
        validate_request(data, ["artist_id"])
        follow_artist(
            user_id=current_user.id,
            session_id=session_id,
            artist_id=UUID(data["artist_id"]),
            now=now,
        )
        return "OK", 200
    except ArtistNotFound as e:
        log.error(
            f"Could not follow artist: artist not found. Req: {data}, user_id={current_user.id}."
        )
        abort(404, "Artist not found.")


@api.route("/follow/artists", methods=["GET"])
@jwt_required()
def get_followed_artists():
    artists = all_followed_artists(user_id=current_user.id)
    return jsonify(artists), 200


@api.route("/subscribe", methods=["POST"])
@jwt_required()
def post_subscribe_user():
    now = g.request_time
    session_id = UUID(get_jwt()["session_id"])
    # we mock a subscribtion for now -- i.e., no credit card info or anything like that
    subscribe_user(user=current_user, session_id=session_id, now=now)
    return "OK", 200
