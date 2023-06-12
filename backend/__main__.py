from .backend import app


if __name__ == "__main__":
    host = app.config["SERVER_HOST"]
    port = app.config["SERVER_PORT"]
    app.run(host=host, port=port)
