import logging

from flask import Flask, jsonify

from .models import db


def init_app():
    app = Flask(__name__, instance_relative_config=False)
    app.config.from_object("simulator.config.Config")
    app.logger.setLevel(logging.DEBUG)

    db.init_app(app)

    @app.errorhandler(400)
    def internal_error(e):
        return jsonify(error=e.description, code=400)

    with app.app_context():
        from .handlers import sim

        app.register_blueprint(sim)

        if app.config["DROP_DB_ON_LAUNCH"]:
            app.logger.info("Dropping and re-creating the database!")
            db.drop_all()
            db.create_all()

    return app
