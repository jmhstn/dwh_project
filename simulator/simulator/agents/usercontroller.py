import asyncio
import random
from datetime import date, datetime, timedelta
from asyncio import Queue
from typing import Optional
from uuid import UUID

import numpy as np
from flask import current_app as app
from scipy.stats import truncnorm
from sqlalchemy.sql.expression import func

log = app.logger

from .. import db
from ..agents.user import UserAgentState
from ..client import APIClient
from ..models import UserSim
from ..namegen import choose_weighted, fake
from ..utils import commit_db
from . import Clock, UserAgent


class UserControllerAgent:
    def __init__(self, api_client: APIClient, config, clock: Clock) -> None:
        self.clock = clock

        # config values
        self.warmup_num_users = config["WARMUP_NUM_OF_USERS"]
        self.sim_params = config["STAT_USER_CONTROLLER"]
        self.user_sim_params = config["STAT_USER"]
        self.sim_params["prob_country_weight_lambda"] = (
            np.log(2) / self.sim_params["prob_country_weight_half_life"]
        )

        self.api_client = api_client

        self.last_user_id = 0
        self.user_query: Queue[UserSim] = Queue(maxsize=100)
        self.active_users: dict[UUID, UserAgent] = dict()
        self.total_ws = 1

        self._reset_user_metrics()

        self.countries = api_client.get_countries(only_enabled=True)

    def _reset_user_metrics(self) -> None:
        if hasattr(self, "user_metrics"):
            for k in self.user_metrics.keys():
                self.user_metrics[k] = 0
        else:
            self.user_metrics = {}
            for name in UserAgentState._member_names_:
                self.user_metrics[name] = 0

    def warmup_db(self) -> None:
        self.countries = self.api_client.get_countries(only_enabled=True)
        for _ in range(self.warmup_num_users):
            country_code = self._choose_country()
            if country_code:
                self._generate_user(country_code)
        log.info(f"{self.warmup_num_users} users generated.")

    def _get_next_user_id(self) -> int:
        self.last_user_id += 1
        return self.last_user_id

    def _calculate_countries_weights(self) -> tuple[dict[str, float], float]:
        countries_ws = dict()
        total_w = 0
        now = datetime.utcnow()
        for code, enabled_at in self.countries.items():
            elapsed = now - enabled_at
            elapsed_sim = self.clock.to_sim_time(elapsed)
            days_sim = elapsed_sim / timedelta(days=1)
            w = np.exp(-self.sim_params["prob_country_weight_lambda"] * days_sim)
            total_w += w
            countries_ws[code] = w
        return countries_ws, total_w

    def _choose_country(self) -> Optional[str]:
        self.countries_ws, self.total_ws = self._calculate_countries_weights()
        log.debug(f"Recalculated countries weights: {self.countries_ws}.")
        code = choose_weighted(self.countries_ws, with_none=True)
        return code

    def _gen_birth_date(self) -> date:
        """
        A birth date is generated as a random value from a normal distribution truncated between the min and max ages.
        """
        mu = self.sim_params["prob_user_age_mean"]
        sigma = self.sim_params["prob_user_age_sigma"]
        min_age = self.sim_params["prob_user_age_min"]
        max_age = self.sim_params["prob_user_age_max"]
        a = (min_age - mu) / sigma  # how many sigmas to the left of the mean
        b = (max_age - mu) / sigma  # how many sigmas to the right of the mean
        age = int(truncnorm.rvs(a, b, loc=mu, scale=sigma))
        return fake.date_of_birth(minimum_age=age, maximum_age=age).isoformat()

    def _generate_user(self, country_code: str) -> Optional[UserSim]:
        email = fake.ascii_free_email()
        password = fake.password()
        profile = {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "birth_date": self._gen_birth_date(),
        }
        usersim = UserSim(email=email, password=password, country_code=country_code)
        log.info(f"Signing up a new user with email={email}, password={password}.")
        try:
            user_id = self.api_client.sign_up(usersim, profile)
            usersim.id = user_id
            db.session.add(usersim)
            commit_db(usersim, "UserSim", is_update=False)
            log.info(f"User successfully registered: {usersim.to_dict()}.")
            return usersim
        except:
            log.exception(f"Could not create a new user with email={email}.")
            raise

    async def _create_user_task(self) -> None:
        while True:
            await self.running.wait()
            try:
                await self.clock.sim_seconds(self.sim_params["delay_create_users_sec"])
                noise = random.randint(-1, 1)
                new_users_num = int(self.total_ws) + noise
                log.info(f"Creating {new_users_num} new users.")
                for _ in range(new_users_num):
                    country_code = self._choose_country()
                    if country_code:
                        usersim = self._generate_user(country_code)
                        await self.user_query.put(usersim)
                        log.debug(
                            f"Registered new user {usersim}. Current user query: {self.user_query}."
                        )
                    else:
                        log.debug(
                            f"Skipped creating a new user. Current user query: {self.user_query}."
                        )
            except:
                log.exception("Error at _create_user_task")

    async def _select_users_task(self) -> None:
        while True:
            await self.running.wait()
            try:
                await self.clock.sim_seconds(self.sim_params["delay_select_users_sec"])
                num_of_users = random.randint(0, 5)
                users = UserSim.query.order_by(func.random()).limit(num_of_users).all()
                for user in users:
                    if user.id not in self.active_users.keys():
                        await self.user_query.put(user)
                log.debug(
                    f"Selected for running users {users}. Current user query: {self.user_query.qsize()}."
                )
            except:
                log.exception("Error at _select_users_task")

    async def _run_users_task(self) -> None:
        while True:
            await self.running.wait()
            try:
                await self.clock.sim_seconds(self.sim_params["delay_run_users_sec"])
                if not self.user_query.empty():
                    usersim = await self.user_query.get()
                    if usersim.id not in self.active_users.keys():
                        agent = UserAgent(
                            self._get_next_user_id(),
                            usersim,
                            self.api_client,
                            self.user_sim_params,
                            self.clock,
                        )
                        asyncio.create_task(agent.run(self.running))
                        self.active_users[usersim.id] = agent
                        log.info(f"Run user agent id={agent.agent_id}.")
                    else:
                        log.info(
                            f"Could not run user {usersim}: already active as {self.active_users[usersim.id]}."
                        )
                else:
                    log.debug(f"Run users: user query is empty.")
            except:
                log.exception("Error at _run_users_task")

    async def _clean_up_users_task(self) -> None:
        while True:
            await self.running.wait()
            try:
                await self.clock.sim_seconds(
                    self.sim_params["delay_clean_up_users_sec"]
                )
                self._reset_user_metrics()
                invalid, left, remaining = [], [], []
                old_users_len = len(self.active_users)
                active_user_ids = list(self.active_users.keys())
                if active_user_ids:
                    for user_id in active_user_ids:
                        usersim = self.active_users[user_id]
                        self.user_metrics[usersim.state.name] += 1
                        match usersim.state:
                            case UserAgentState.INVALID:
                                invalid.append(user_id)
                                del self.active_users[user_id]
                            case UserAgentState.LEFT:
                                left.append(user_id)
                                del self.active_users[user_id]
                    new_users_len = len(self.active_users)
                    log.debug(f"Current metrics: {self.user_metrics}")
                    log.debug(
                        f"Cleaned up users: {old_users_len} -> {new_users_len}. Left {len(left)}, invalid: {len(invalid)}"
                    )
                else:
                    log.debug(f"Skipping cleaning up: no users.")
            except:
                log.exception("Error at _clean_up_users_task")

    async def run(self, running) -> None:
        log.info(f"Initializing user controller agent.")
        self.running = running
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._create_user_task())
            tg.create_task(self._select_users_task())
            tg.create_task(self._run_users_task())
            tg.create_task(self._clean_up_users_task())
        log.info(f"User controller agent tasks stopped.")
