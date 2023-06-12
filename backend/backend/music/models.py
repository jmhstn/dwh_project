from datetime import datetime
import random

from sqlalchemy.dialects.postgresql import UUID

from .. import db
from ..common.models import Country
from ..utils import BaseModel


class Genre(BaseModel):
    __tablename__ = "genres"

    name = db.Column(db.String, nullable=False)
    happiness_index = db.Column(
        db.Integer, nullable=False, default=random.randint(-100, 100)
    )
    mean_duration_sec = db.Column(db.Integer, nullable=False)
    country_code = db.Column(db.String, db.ForeignKey(Country.code), nullable=False)

    def __repr__(self):
        return f"<Genre {self.id}: {self.name}>"

    def to_dict(self) -> dict:
        obj = {
            "id": self.id,
            "name": self.name,
            "happiness_index": self.happiness_index,
            "mean_duration_sec": self.mean_duration_sec,
            "country_code": self.country_code,
        }
        return obj


def current_year() -> int:
    return datetime.utcnow().year


class Artist(BaseModel):
    __tablename__ = "artists"

    name = db.Column(db.String, nullable=False)
    founded_year = db.Column(db.Integer, nullable=True, default=current_year)
    country_code = db.Column(db.String, db.ForeignKey(Country.code), nullable=False)
    genre_id = db.Column(UUID(), db.ForeignKey(Genre.id), nullable=False)

    def __repr__(self):
        return f"<Artist {self.id}: {self.name}>"

    def to_dict(self) -> dict:
        obj = {
            "id": self.id,
            "name": self.name,
            "founded_year": self.founded_year,
            "country_code": self.country_code,
            "genre_id": str(self.genre_id),
        }
        return obj


class Collection(BaseModel):
    __tablename__ = "collections"

    name = db.Column(db.String, nullable=False)
    collection_type = db.Column(db.String, nullable=False)
    genre_id = db.Column(UUID(), db.ForeignKey(Genre.id), nullable=False)
    released_dt = db.Column(db.Date, nullable=False, default=datetime.today)
    artist_id = db.Column(UUID(), db.ForeignKey(Artist.id), nullable=False)

    def __repr__(self):
        return f"<Collection {self.id}: {self.name}, type: {self.collection_type}>"

    def to_dict(self, songs: list):
        obj = {
            "id": str(self.id),
            "name": self.name,
            "collection_type": self.collection_type,
            "genre_id": str(self.genre_id),
            "released_dt": self.released_dt.isoformat(),
            "artist_id": str(self.artist_id),
        }
        if songs is not None:
            obj["songs"] = [s.to_dict(as_collection=False) for s in songs]
        return obj


class Song(BaseModel):
    __tablename__ = "songs"

    name = db.Column(db.String, nullable=False)
    duration_sec = db.Column(db.Integer, nullable=False)
    collection_id = db.Column(UUID(), db.ForeignKey(Collection.id))
    artist_id = db.Column(UUID(), db.ForeignKey(Artist.id))
    genre_id = db.Column(UUID(), db.ForeignKey(Genre.id))

    def __repr__(self):
        return f"<Song {self.id}: {self.name}>"

    def to_dict(self, as_collection: bool):
        obj = {
            "id": str(self.id),
            "name": self.name,
            "duration_sec": self.duration_sec,
        }
        if not as_collection:
            obj["collection_id"] = str(self.collection_id)
            obj["artist_id"] = str(self.artist_id)
            obj["genre_id"] = str(self.genre_id)
        return obj
