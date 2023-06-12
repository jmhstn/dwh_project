from uuid import UUID, uuid4
from flask import Blueprint, abort, jsonify, request, g
from flask_jwt_extended import create_access_token, current_user, get_jwt, jwt_required

from .. import jwt, log
from ..events import send_event
from ..events.models import SignInFailureEvent, SignInSuccessEvent
from ..utils import validate_request
from .models import User
from .storage import (
    InvalidPassword,
    NewUserData,
    UserAlreadyExists,
    UserNotFound,
    create_user,
    validate_password,
)

auth = Blueprint("auth", __name__, url_prefix="/auth")


@jwt.user_identity_loader
def user_identity_lookup(user: User):
    return user.id


@jwt.user_lookup_loader
def user_lookup_callback(_jwt_header, jwt_data):
    identity = jwt_data["sub"]
    return User.query.filter_by(id=identity).one_or_none()


@auth.route("/sign_up", methods=["POST"])
def sign_up():
    now = g.request_time
    json = request.get_json()
    try:
        validate_request(json, ["email", "password", "country_code", "profile"])
        data = NewUserData(**json)
        user = create_user(data, now)
        response = jsonify(user_id=user.id, email=user.email)
        return response, 201
    except UserAlreadyExists as e:
        abort(400, description=f"User with email {e.email} already exists")


@auth.route("/sign_in", methods=["POST"])
def sign_in():
    now = g.request_time
    json = request.get_json()
    try:
        validate_request(json, ["email", "password"])
        email = json["email"]
        password = json["password"]
        user = validate_password(email, password)
        session_id = uuid4()
        access_token = create_access_token(
            identity=user, additional_claims={"session_id": session_id}
        )
        response = jsonify(id=user.id, access_token=access_token)
        send_event(
            SignInSuccessEvent(user_id=user.id, session_id=session_id, event_time=now)
        )
        return response, 200
    except Exception as e:
        log.exception(f"Failed to sign in with request {json}")
        send_event(SignInFailureEvent(reason=e.__class__.__name__, event_time=now))
        if isinstance(e, (InvalidPassword, UserNotFound)):
            abort(400, description="Failed to sign in: check email or password.")
        raise


@auth.route("/refresh", methods=["POST"])
@jwt_required()
def refresh_token():
    session_id = UUID(get_jwt()["session_id"])
    access_token = create_access_token(
        identity=current_user, additional_claims={"session_id": session_id}
    )
    response = jsonify(id=current_user.id, access_token=access_token)
    return response, 200
