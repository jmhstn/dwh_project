import asyncio
import random
from typing import Optional

import pycountry
from flask import current_app as app

from . import Clock
from ..client import APIClient

log = app.logger


class CountryControllerAgent:
    def __init__(self, api_client: APIClient, config, clock: Clock) -> None:
        self.clock = clock
        self.api_client = api_client
        self.countries = api_client.get_countries()

        # config values
        self.warmup_num_countries = config["WARMUP_NUM_OF_COUNTRIES"]
        self.warmup_num_countries_enabled = config["WARMUP_NUM_OF_COUNTRIES_ENABLED"]
        self.enable_country_delay = config["ENABLE_COUNTRY_DELAY_DAYS"]

    def warmup_db(self) -> None:
        all_countries = list(pycountry.countries)
        selected = random.sample(all_countries, self.warmup_num_countries)
        log.debug(f"Countries selected for addition: {selected}.")
        for c in selected:
            code, name = c.alpha_2, c.name
            self.api_client.add_country(code, name)
        self.countries = self.api_client.get_countries()
        to_enable = random.sample(
            sorted(self.countries.keys()), self.warmup_num_countries_enabled
        )
        log.debug(f"Countries selected to enable: {to_enable}.")
        for c in to_enable:
            self.api_client.enable_country(c)
        self.countries = self.api_client.get_countries()
        log.info(
            f"{self.warmup_num_countries} countries added, {self.warmup_num_countries_enabled} enabled."
        )

    def _pick_next_country(self) -> Optional[str]:
        # TODO: take into account number of new users being created by UserControllerAgent
        disabled = [
            code for code, enabled_at in self.countries.items() if enabled_at is None
        ]
        if disabled:
            return random.choice(disabled)
        else:
            return None

    def _enable_country(self, code: str):
        self.api_client.enable_country(code)
        self.countries = self.api_client.get_countries()
        log.info(f"Enabled new country {code}.")

    async def _enable_country_task(self) -> None:
        while True:
            await self.running.wait()
            try:
                await self.clock.sim_days(self.enable_country_delay)
                next_code = self._pick_next_country()
                if next_code:
                    self._enable_country(next_code)
                else:
                    log.info("No country was selected for this iteration.")

            except:
                log.exception("Error at _enable_country_task")

    async def run(self, running) -> None:
        log.info(f"Initializing country controller agent.")
        self.running = running
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._enable_country_task())
        log.info(f"Country controller agent tasks stopped.")
