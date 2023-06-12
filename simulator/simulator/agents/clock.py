import asyncio
from datetime import datetime, timedelta
import time
import random
from flask import current_app as app

log = app.logger


class Clock:
    def __init__(self, config) -> None:
        self.clock_multiplier = config["CLOCK_MULTIPLIER"]
        self.elapsed_real_time_ns = time.monotonic_ns()
        self.current_sim_time_sec = time.time()

    def get_current_sim_time(self) -> datetime:
        return datetime.fromtimestamp(self.current_sim_time_sec)

    def to_sim_time(self, delta: timedelta) -> timedelta:
        return delta * self.clock_multiplier

    def set_clock_multiplier(self, multiplier: int) -> None:
        log.info(
            f"Changing the clock multiplier from {self.clock_multiplier} to {multiplier}."
        )
        self.clock_multiplier = multiplier

    async def sim_days(self, num_days: float, with_noise: bool = True) -> None:
        noise_hours = random.randint(-2, 2) if with_noise else 0
        await self.sim_hours(num_days * 24 + noise_hours)

    async def sim_hours(self, num_hours: float, with_noise: bool = True) -> None:
        noise_minutes = random.randint(-5, 5) if with_noise else 0
        await self.sim_minutes(num_hours * 60 + noise_minutes)

    async def sim_minutes(self, num_minutes: float, with_noise: bool = True) -> None:
        noise_seconds = random.randint(-5, 5) if with_noise else 0
        await self.sim_seconds(num_minutes * 60 + noise_seconds)

    async def sim_seconds(self, num_seconds: float, with_noise: bool = True) -> None:
        noise_seconds = random.randint(-1, 1) if with_noise else 0
        await asyncio.sleep((num_seconds + noise_seconds) / self.clock_multiplier)

    async def _sync_time(self) -> None:
        while True:
            await self.running.wait()
            await self.sim_minutes(1, with_noise=False)
            elapsed_ns = time.monotonic_ns() - self.elapsed_real_time_ns
            self.current_sim_time_sec += (elapsed_ns / 1e9) * self.clock_multiplier
            self.elapsed_real_time_ns = time.monotonic_ns()

    async def _log_current_time(self) -> None:
        while True:
            await self.running.wait()
            await self.sim_hours(1)
            log.info(
                f"[CLOCK {self.get_current_sim_time()}] Elapsed {self.elapsed_real_time_ns / 10e9}s"
            )

    async def run(self, running):
        log.info("Initializing the simulator clock.")
        self.running = running
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._sync_time())
            tg.create_task(self._log_current_time())
        log.info(f"Simulator clock tasks stopped.")
