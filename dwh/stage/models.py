from sqlalchemy import Column, Integer, String, Boolean, DateTime, Date, DECIMAL
from sqlalchemy.schema import CreateSchema
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import UUID

from uuid import uuid4


from .. import engine

schema_name = "stage"
Base = declarative_base()

conn = engine.connect()
if not conn.dialect.has_schema(conn, schema_name):
    conn.execute(CreateSchema(schema_name))
    conn.commit()


# SignUpEvent
class KafkaUserHistory(Base):
    __tablename__ = "kafka_user_history"
    __table_args__ = {"schema": schema_name}
    event_id = Column(UUID(), nullable=False, primary_key=True, default=uuid4)
    user_id = Column(String, nullable=False)
    email = Column(String)
    country_code = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    birth_date = Column(Date, nullable=False)
    event_type = Column(String, nullable=False)
    event_time = Column(DateTime, nullable=False)


# CountryEnabled
class KafkaCountryEnabled(Base):
    __tablename__ = "kafka_country_enabled"
    __table_args__ = {"schema": schema_name}
    event_id = Column(UUID(), nullable=False, primary_key=True, default=uuid4)
    country_code = Column(String)
    event_type = Column(String, nullable=False)
    event_time = Column(DateTime, nullable=False)


# GenreCreated
class KafkaGenreCreated(Base):
    __tablename__ = "kafka_genre"
    __table_args__ = {"schema": schema_name}
    event_id = Column(UUID(), nullable=False, primary_key=True, default=uuid4)
    id = Column(String, nullable=False)
    name = Column(String)
    happiness_index = Column(Integer)
    mean_duration_sec = Column(DECIMAL)
    country_code = Column(String)
    event_type = Column(String, nullable=False)
    event_time = Column(DateTime, nullable=False)


# ArtistCreated
class KafkaArtistCreated(Base):
    __tablename__ = "kafka_artist"
    __table_args__ = {"schema": schema_name}
    event_id = Column(UUID(), nullable=False, primary_key=True, default=uuid4)
    id = Column(String, nullable=False)
    name = Column(String)
    founded_year = Column(Integer)
    country_code = Column(String)
    event_type = Column(String, nullable=False)
    event_time = Column(DateTime, nullable=False)


# CollectionCreated
class KafkaCollectionCreated(Base):
    __tablename__ = "kafka_collection"
    __table_args__ = {"schema": schema_name}
    event_id = Column(UUID(), nullable=False, primary_key=True, default=uuid4)
    id = Column(String, nullable=False)
    name = Column(String)
    collection_type = Column(String)
    artist_id = Column(String)
    genre_id = Column(String)
    released_dt = Column(String)
    event_type = Column(String, nullable=False)
    event_time = Column(DateTime, nullable=False)


# SongCreated
class KafkaSongCreated(Base):
    __tablename__ = "kafka_song"
    __table_args__ = {"schema": schema_name}
    event_id = Column(UUID(), nullable=False, primary_key=True, default=uuid4)
    id = Column(String, nullable=False)
    name = Column(String)
    duration_sec = Column(Integer)
    artist_id = Column(String)
    genre_id = Column(String)
    collection_id = Column(String)
    event_type = Column(String, nullable=False)
    event_time = Column(DateTime, nullable=False)


# SignInSuccessEvent
class KafkaUserAuthorization(Base):
    __tablename__ = "kafka_user_authorization"
    __table_args__ = {"schema": schema_name}
    event_id = Column(UUID(), nullable=False, primary_key=True, default=uuid4)
    user_id = Column(String, nullable=False)
    session_id = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    event_time = Column(DateTime, nullable=False)


# SongPlayEvent
# SongStopEvent
class KafkaPlayback(Base):
    __tablename__ = "kafka_playback"
    __table_args__ = {"schema": schema_name}
    event_id = Column(UUID(), nullable=False, primary_key=True, default=uuid4)
    user_id = Column(String, nullable=False)
    session_id = Column(String, nullable=False)
    song_id = Column(String, nullable=False)
    at_time_sec = Column(Integer, nullable=False)
    finished = Column(Boolean)
    event_type = Column(String, nullable=False)
    event_time = Column(DateTime, nullable=False)


# SongLikedEvent
class KafkaSongLike(Base):
    __tablename__ = "kafka_song_like"
    __table_args__ = {"schema": schema_name}
    event_id = Column(UUID(), nullable=False, primary_key=True, default=uuid4)
    user_id = Column(String, nullable=False)
    session_id = Column(String, nullable=False)
    song_id = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    event_time = Column(DateTime, nullable=False)


# ArtistFollowedEvent
class KafkaArtistFollowed(Base):
    __tablename__ = "kafka_artist_follow"
    __table_args__ = {"schema": schema_name}
    event_id = Column(UUID(), nullable=False, primary_key=True, default=uuid4)
    user_id = Column(String, nullable=False)
    session_id = Column(String, nullable=False)
    artist_id = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    event_time = Column(DateTime, nullable=False)
    
class KafkaSubscriptionEvent(Base):
    __tablename__ = "kafka_user_subscription"
    __table_args__ = {"schema": schema_name}
    event_id = Column(UUID(), nullable=False, primary_key=True, default=uuid4)
    user_id = Column(String, nullable=False)
    session_id = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    event_time = Column(DateTime, nullable=False)


Base.metadata.create_all(engine)
