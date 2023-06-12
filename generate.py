import random
from typing import Optional

import inflect
from faker import Faker
from faker.providers import address, lorem


def choose_weighted(choices: dict):
    return random.choices(*zip(*choices.items()), k=1)[0]


from datetime import datetime, timedelta

countries = {
    "UK": datetime.fromisoformat("2023-05-19T22:04:05.249488"),
    "RU": datetime.fromisoformat("2023-05-19T21:04:05.249488"),
    "NL": datetime.fromisoformat("2023-05-19T12:04:05.249488"),
    "US": datetime.fromisoformat("2023-05-16T22:04:05.249488"),
}

now = datetime.utcnow() + timedelta(days=14)

countries_ws = {}

import numpy as np

l = 0.009

for k, v in countries.items():
    diff = now - v
    hours = diff / timedelta(hours=1)
    res = np.exp(-l * hours)
    countries_ws[k] = res, hours

print(countries_ws)
