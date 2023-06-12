import asyncio
import threading

from .simulator import init_app

app = init_app()

from .simulator.engine import engine


async def main_engine():
    with app.app_context():
        await engine.run()


def main():
    t = threading.Thread(target=asyncio.run, args=(main_engine(),))
    t.start()
    app.run(host="127.0.0.1", port=8081)


if __name__ == "__main__":
    main()
