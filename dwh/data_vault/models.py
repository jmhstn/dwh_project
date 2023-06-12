from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DateTime,
    DECIMAL,
    Boolean,
)
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql import func
from datetime import datetime

from .. import engine

meta = MetaData()

schema_name = "dv"

conn = engine.connect()
if not conn.dialect.has_schema(conn, schema_name):
    conn.execute(CreateSchema(schema_name))
    conn.commit()

meta = MetaData(schema=schema_name)

hub_user = Table(
    "hub_user",
    meta,
    Column("user_id", String(50), nullable=False),
    Column("original_id", String, nullable=False),
    Column("source", String, nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

sat_user = Table(
    "sat_user",
    meta,
    Column("user_id", String(50), nullable=False),
    Column("actual_dtm", DateTime, nullable=False),
    Column("email", String, nullable=False),
    Column("first_name", String, nullable=False),
    Column("last_name", String, nullable=False),
    Column("birth_dt", DateTime, nullable=False),
    Column("registration_dtm", DateTime, nullable=False),
    Column("country_code", String(2), nullable=False),
    Column("source", String, nullable=False),
    Column("row_hash", String(50), nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

sat_user_subscription = Table(
    "sat_user_subscription",
    meta,
    Column("user_id", String(50), nullable=False),
    Column("actual_dtm", DateTime, nullable=False),
    Column("is_premium", Boolean, nullable=False),
    Column("source", String, nullable=False),
    Column("row_hash", String(50), nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

link_session_song_user_like = Table(
    "link_session_song_user_like",
    meta,
    Column("session_song_user_like_id", String(50), nullable=False),
    Column("session_id", String(50), nullable=False),
    Column("song_id", String(50), nullable=False),
    Column("user_id", String(50), nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

lsat_session_song_user_like = Table(
    "lsat_session_song_user_like",
    meta,
    Column("session_song_user_like_id", String(50), nullable=False),
    Column("actual_dtm", DateTime, nullable=False),
    Column("source", String, nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

hub_song = Table(
    "hub_song",
    meta,
    Column("song_id", String(50), nullable=False),
    Column("original_id", String, nullable=False),
    Column("source", String, nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

sat_song = Table(
    "sat_song",
    meta,
    Column("song_id", String(50), nullable=False),
    Column("actual_dtm", DateTime, nullable=False),
    Column("name", String, nullable=False),
    Column("original_genre_id", String, nullable=False),
    Column("duration_sec", Integer, nullable=False),
    Column("source", String, nullable=False),
    Column("row_hash", String(50), nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

link_session_song_user_listen = Table(
    "link_session_song_user_listen",
    meta,
    Column("session_song_user_listen_id", String(50), nullable=False),
    Column("session_id", String(50), nullable=False),
    Column("song_id", String(50), nullable=False),
    Column("user_id", String(50), nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

lsat_session_song_user_listen = Table(
    "lsat_session_song_user_listen",
    meta,
    Column("session_song_user_listen_id", String(50), nullable=False),
    Column("actual_dtm", DateTime, nullable=False),
    Column("action_type", String, nullable=False),
    Column("at_time_sec", Integer, nullable=False),
    Column("source", String, nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

hub_collection = Table(
    "hub_collection",
    meta,
    Column("collection_id", String(50), nullable=False),
    Column("original_id", String, nullable=False),
    Column("source", String, nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

sat_collection = Table(
    "sat_collection",
    meta,
    Column("collection_id", String(50), nullable=False),
    Column("actual_dtm", DateTime, nullable=False),
    Column("name", String, nullable=False),
    Column("original_genre_id", String, nullable=False),
    Column("release_dt", String, nullable=False),
    Column("collection_type", String, nullable=False),
    Column("source", String, nullable=False),
    Column("row_hash", String(50), nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

link_collection_song = Table(
    "link_collection_song",
    meta,
    Column("collection_song_id", String(50), nullable=False),
    Column("collection_id", String(50), nullable=False),
    Column("song_id", String(50), nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

hub_artist = Table(
    "hub_artist",
    meta,
    Column("artist_id", String(50), nullable=False),
    Column("original_id", String, nullable=False),
    Column("source", String, nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

sat_artist = Table(
    "sat_artist",
    meta,
    Column("artist_id", String(50), nullable=False),
    Column("actual_dtm", DateTime, nullable=False),
    Column("name", String, nullable=False),
    Column("country_code", String(2), nullable=False),
    Column("founded_year", Integer, nullable=False),
    Column("source", String, nullable=False),
    Column("row_hash", String(50), nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

link_artist_collection = Table(
    "link_artist_collection",
    meta,
    Column("artist_collection_id", String(50), nullable=False),
    Column("artist_id", String(50), nullable=False),
    Column("collection_id", String(50), nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

hub_session = Table(
    "hub_session",
    meta,
    Column("session_id", String(50), nullable=False),
    Column("original_id", String, nullable=False),
    Column("source", String, nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

sat_session = Table(
    "sat_session",
    meta,
    Column("session_id", String(50), nullable=False),
    Column("actual_dtm", DateTime, nullable=False),
    Column("source", String, nullable=False),
    Column("row_hash", String(50), nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)

link_session_artist_user_follow = Table(
    "link_session_artist_user_follow",
    meta,
    Column("session_artist_user_follow_id", String(50), nullable=False),
    Column("session_id", String(50), nullable=False),
    Column("artist_id", String(50), nullable=False),
    Column("user_id", String(50), nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)


lsat_session_artist_user_follow = Table('lsat_session_artist_user_follow',
                            meta,
                            Column('session_artist_user_follow_id', String(50), nullable=False),
                            Column('actual_dtm', DateTime, nullable=False),
                            Column('source', String, nullable=False),
                            Column('insert_dtm', DateTime, nullable=False, server_default=func.now()))
   
ref_country_subscription_cost = Table('ref_country_subscription_cost',
                                     meta,
                                     Column('country_code', String(2), nullable=False),
                                     Column('enabled_dt', DateTime, nullable=False),
                                     Column('subscription_type', String, nullable=False),
                                     Column('subscription_cost', DECIMAL(5, 2), nullable=False),
                                     Column('royalty', DECIMAL(5, 2), nullable=False),
                                     Column('source', String, nullable=False),
                                     Column('insert_dtm', DateTime, nullable=False, server_default=func.now()))

ref_genre = Table("ref_genre",
    meta,
    Column("original_genre_id", String, nullable=False),
    Column("genre_name", String, nullable=False),
    Column("creation_dtm", DateTime, nullable=False),
    Column("country_code", String(2), nullable=False),
    Column("happiness_index", Integer, nullable=False),
    Column("average_duration_sec", Integer, nullable=False),
    Column("source", String, nullable=False),
    Column("insert_dtm", DateTime, nullable=False, server_default=func.now()),
)
