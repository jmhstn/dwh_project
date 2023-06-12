import json
from kafka import KafkaConsumer
from sqlalchemy.orm import Session

from . import models


# event_name: (table_name, instance)
event_to_table = {
    "SignUpEvent": models.KafkaUserHistory,
    "SignInSuccessEvent": models.KafkaUserAuthorization,
    "SongPlayEvent": models.KafkaPlayback,
    "SongStopEvent": models.KafkaPlayback,
    "SongLikedEvent": models.KafkaSongLike,
    "ArtistFollowedEvent": models.KafkaArtistFollowed,
    "ArtistCreated": models.KafkaArtistCreated,
    "CollectionCreated": models.KafkaCollectionCreated,
    "SongCreated": models.KafkaSongCreated,
    "GenreCreated": models.KafkaGenreCreated,
    "CountryEnabled": models.KafkaCountryEnabled,
    "UserSubscriptionEvent": models.KafkaSubscriptionEvent
}


def parser(event_type: str, payload_data: dict):
    table_model = event_to_table[event_type]
    object = table_model()
    for key, value in payload_data.items():
        if hasattr(table_model, key):
            setattr(object, key, value)
    print(object)
    return object


def load_to_table(event: dict) -> None:
    data = event["data"]
    data["event_type"] = event["type"]
    print(data)
    row_instance = parser(event["type"], data)
    with Session(models.engine) as session:
        session.add(row_instance)
        session.commit()


def load_to_stage():
    topic = "dwh_events"
    consumer = KafkaConsumer(
        topic,
        group_id=None,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
    )

    consumer.partitions_for_topic(topic)
    consumer.seek_to_beginning()
    existing_tables = [
        "SignInSuccessEvent",
        "SongPlayEvent",
        "SongStopEvent",
        "SongLikedEvent",
        "ArtistFollowedEvent",
    ]

    while True:
        records = consumer.poll(timeout_ms=1000) 
        for _, consumer_records in records.items():
            for consumer_record in consumer_records:
                event = json.loads(consumer_record.value.decode("utf-8"))
                if event["type"] in event_to_table.keys():
                    load_to_table(event)
            continue
        
        
        

