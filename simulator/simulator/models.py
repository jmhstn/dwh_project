from datetime import datetime
from uuid import uuid4
from scipy.stats import truncnorm
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import UUID

db = SQLAlchemy()


class TimedModel(db.Model):
    __abstract__ = True

    created_dtm = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    modified_dtm = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )


def gen_normal_p(mu: float, sigma: float) -> float:
    # a value between 0 and 1
    a, b = (0 - mu) / sigma, (100 - mu) / sigma
    percent = max(min(truncnorm.rvs(a, b, loc=mu, scale=sigma), 100), 0)
    return percent * 0.01


def gen_patriot_p() -> float:
    return gen_normal_p(mu=30, sigma=20)


def gen_explorer_p() -> float:
    return gen_normal_p(mu=30, sigma=20)


def gen_skip_p() -> float:
    return gen_normal_p(mu=80, sigma=30)


def gen_picky_p() -> float:
    return gen_normal_p(mu=30, sigma=20)


def gen_collection_p() -> float:
    return gen_normal_p(mu=20, sigma=20)


def gen_popular_p() -> float:
    return gen_normal_p(mu=80, sigma=20)


class UserSim(TimedModel):
    __tablename__ = "user_sims"

    id = db.Column(UUID(), nullable=False, primary_key=True, default=uuid4)

    email = db.Column(db.String, nullable=False, unique=True)
    password = db.Column(db.String, nullable=False)
    country_code = db.Column(db.String, nullable=False)
    is_premium = db.Column(db.Boolean, nullable=False, default=False)

    # how likely the user is to prefer music from its country
    patriot_p = db.Column(db.Float, nullable=False, default=gen_patriot_p)
    # how likely the user is to start looking for new music
    explorer_p = db.Column(db.Float, nullable=False, default=gen_explorer_p)
    # how likely the user is to skip a song
    skip_p = db.Column(db.Float, nullable=False, default=gen_skip_p)
    # how unlikely the user is to like a song
    picky_p = db.Column(db.Float, nullable=False, default=gen_picky_p)
    # how likely the user is to choose a whole collection, and not a random song
    collection_p = db.Column(db.Float, nullable=False, default=gen_collection_p)
    # how likely the user is to choose the most popular music
    popular_p = db.Column(db.Float, nullable=False, default=gen_popular_p)

    def __repr__(self):
        return f"<UserSim {self.email} (id {self.id})>"

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "email": self.email,
            "country": self.country_code,
            "is_premium": self.is_premium,
            "probs": {
                "patriot_p": self.patriot_p,
                "explorer_p": self.explorer_p,
                "skip_p": self.skip_p,
                "picky_p": self.picky_p,
                "collection_p": self.collection_p,
                "popular_p": self.popular_p,
            },
        }


class ArtistSim(TimedModel):
    __tablename__ = "artist_sims"

    artist_id = db.Column(UUID(), nullable=False, primary_key=True)

    last_release_dtm = db.Column(db.DateTime, nullable=True)
    retired = db.Column(db.Boolean, nullable=True, default=False)

    def __repr__(self):
        return f"<ArtistSim {self.artist_id})>"


class SongSim(TimedModel):
    __tablename__ = "song_sims"

    song_id = db.Column(UUID(), nullable=False, primary_key=True)
    listen_count = db.Column(db.Integer, nullable=False, default=int)

    def __repr__(self):
        return f"<SongSim {self.song_id})>"
