import dataclasses
from datetime import date, datetime
import json
from uuid import UUID

from kafka import KafkaProducer

from .. import app, log
from .models import DwhEvent

kafka_servers = app.config["KAFKA_BOOTSTRAP_SERVERS"]
dwh_topic = app.config["KAFKA_DWH_TOPIC"]

producer = KafkaProducer(bootstrap_servers=kafka_servers)


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        if isinstance(o, UUID):
            return o.hex
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        return super().default(o)


def serialize_event(event: DwhEvent) -> bytes:
    obj = json.dumps(
        {"type": event.__class__.__name__, "data": event}, cls=EnhancedJSONEncoder
    )
    return bytes(obj, encoding="utf-8")


def send_event(event: DwhEvent) -> None:
    log.info(f"Sending event to {dwh_topic}: {event}")
    try:
        future = producer.send(dwh_topic, value=serialize_event(event))
        future.get(timeout=5)
    except Exception:
        log.exception(f"Failed sending event to {dwh_topic}: {event}")
