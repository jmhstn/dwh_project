from datetime import datetime
from flask import Blueprint, abort, jsonify, request, g


from .storage import CountryNotFound, add_country, change_country, get_countries
from .. import log
from ..utils import validate_request


common = Blueprint("common", __name__, url_prefix="/common")


@common.route("/country", methods=["POST"])
def post_country():
    data = request.get_json()
    validate_request(data, ["code", "name"])
    add_country(data)
    return "", 201


@common.route("/countries", methods=["GET"])
def get_country_all():
    only_enabled = request.args.get(
        "enabled", default=False, type=lambda v: v.lower() == "true"
    )
    countries = get_countries(only_enabled)
    resp = jsonify([c.to_dict() for c in countries])
    return resp, 200


@common.route("/country/<country_code>/enable", methods=["POST"])
def enable_country(country_code: str):
    now = g.request_time
    try:
        enabled_at = datetime.utcnow().isoformat()
        change_country(country_code, {"enabled_at": enabled_at}, now=now)
        return "OK", 200
    except CountryNotFound as e:
        log.exception(f"Country not found by code {country_code}.")
        abort(404, f"Country {country_code} not found.")
