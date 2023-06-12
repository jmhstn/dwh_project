import asyncio

from flask import current_app as app

from .agents import clock
from .agents import ArtistControllerAgent, CountryControllerAgent, UserControllerAgent
from .client import APIClient

log = app.logger


class Engine:
    def __init__(self, config) -> None:
        self.config = config
        self.sim_clock = clock.Clock(config)
        api_client = APIClient(config["MSS_HOST"], config["MSS_PORT"], self.sim_clock)
        self.user_controller = UserControllerAgent(api_client, config, self.sim_clock)
        self.artist_controller = ArtistControllerAgent(
            api_client, config, self.sim_clock
        )
        self.country_controller = CountryControllerAgent(
            api_client, config, self.sim_clock
        )

    def warmup_db(self) -> None:
        log.info("Pre-generating country test data.")
        self.country_controller.warmup_db()
        log.info("Pre-generating music library test data.")
        self.artist_controller.warmup_db()
        log.info("Pre-generating user test data.")
        self.user_controller.warmup_db()
        log.info("Test data generated.")

    async def run(self) -> None:
        log.info("Initializing the simulator engine.")
        self._loop = asyncio.get_running_loop()
        self.running = asyncio.Event()

        if self.config["WARMUP_ENABLED"]:
            self.warmup_db()

        async with asyncio.TaskGroup() as tg:
            self.sim_clock_task = tg.create_task(self.sim_clock.run(self.running))
            self.user_controller_task = tg.create_task(
                self.user_controller.run(self.running)
            )
            self.artist_controller_task = tg.create_task(
                self.artist_controller.run(self.running)
            )
            self.country_controller_task = tg.create_task(
                self.country_controller.run(self.running)
            )
        log.info("Simulator engine tasks stopped.")

    def start(self) -> None:
        log.info("Starting the simulator engine.")
        self._loop.call_soon_threadsafe(self.running.set)

    def set_clock_multiplier(self, multiplier: int) -> None:
        log.info("Starting the simulator engine.")
        self.sim_clock.set_clock_multiplier(multiplier)

    def stop(self) -> None:
        if self.running.is_set():
            log.info("Stopping the simulator engine.")
            self._loop.call_soon_threadsafe(self.running.clear)
        else:
            raise Exception("Could not stop the engine: engine is not running.")


engine = Engine(app.config)
