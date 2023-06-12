from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .. import db, log
from ..events import send_event
from ..events.models import CountryEnabled
from ..utils import commit_db
from .models import Country


@dataclass
class CountryNotEnabled(Exception):
    country_code: str


@dataclass
class CountryNotFound(Exception):
    country_code: str


def find_country(code: str) -> Optional[Country]:
    """
    Returns a country by the given code, or `None` if not found.
    """
    country = Country.query.get(code)
    if country:
        log.debug(f"Found country {country} by code={code}.")
    else:
        log.debug(f"No country found by code={code}.")
    return country


def add_country(country_data) -> None:
    """
    Adds a new country to the model database.
    """
    country = Country(**country_data)
    log.info(f"Adding a country: {country_data}")
    db.session.add(country)
    commit_db(country, "Country", is_update=False)


def change_country(code: str, data: dict, now: datetime) -> None:
    """
    Finds a country by the given `code` and sets its fields as in the `data` object.

    Raises:
        CountryNotFound: In case no country found by the given code.
    """
    country = find_country(code)
    if country:
        log.info(f"Changing country {code} with the following data: {data}.")
        for k, v in data.items():
            if hasattr(country, k):
                setattr(country, k, v)
                # send a DWH event in case of the `enabled` field being set to True
                if k == "enabled_at" and v:
                    send_event(CountryEnabled(country_code=code, event_time=now))
            else:
                log.warn(f"Could not update {k} arrtibute: does not exist.")
        commit_db(country, "Country", is_update=True)
    else:
        raise CountryNotFound(country_code=code)


def get_countries(only_enabled: bool = False) -> list[Country]:
    """
    Returns the list of all supported countries.

    Args:
        only_enabled: return only enabled countries.
    """
    q = Country.query.order_by(Country.code)
    if only_enabled:
        q = q.filter(Country.enabled_at.isnot(None))
    country = q.all()
    return country
