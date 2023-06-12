from flask import Blueprint, request

from ..simulator.engine import engine

sim = Blueprint("api", __name__, url_prefix="/sim")


@sim.route("/start", methods=["POST"])  # type: ignore
def start():
    engine.start()
    return "OK", 200


@sim.route("/stop", methods=["POST"])  # type: ignore
def stop():
    engine.stop()
    return "OK", 200


@sim.route("/clock", methods=["PUT"])  # type: ignore
def set_clock_multiplier():
    data = request.get_json()
    engine.set_clock_multiplier(data["multiplier"])
    return "OK", 200
