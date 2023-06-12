from datetime import datetime
import logging

from flask import Flask, jsonify, request, g
from flask_jwt_extended import JWTManager
from flask_sqlalchemy import SQLAlchemy
from werkzeug.exceptions import HTTPException

from .errors import FieldsNotFound


db = SQLAlchemy()

app = Flask(__name__, instance_relative_config=False)
app.config.from_object("backend.config.Config")

log = app.logger
log.setLevel(logging.DEBUG)

override_current_time_header = "Override-Current-Time"


@app.before_request
def add_current_time():
    override = request.headers.get(override_current_time_header)
    if override:
        t = datetime.fromisoformat(override)
    else:
        t = datetime.utcnow()
    g.request_time = t


@app.errorhandler(Exception)
def internal_error(e):
    log.exception(e)
    if isinstance(e, FieldsNotFound):
        resp = jsonify(error=str(e)), 400
    elif isinstance(e, HTTPException):
        code = e.code if e.code else 500
        resp = jsonify(error=e.description), code
    else:
        resp = jsonify(error="Internal server error."), 500
    return resp


db.init_app(app)
jwt = JWTManager(app)

with app.app_context():
    from .user.auth import auth as auth_api
    from .user.api import api as main_api
    from .music.handlers import music as music_api
    from .common.handlers import common as common_api

    app.register_blueprint(auth_api)
    app.register_blueprint(main_api)
    app.register_blueprint(music_api)
    app.register_blueprint(common_api)

    if app.config["DROP_DB_ON_START"]:
        app.logger.info("Dropping and re-creating the DB schema!")
        db.drop_all()
        db.create_all()
