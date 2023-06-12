import bcrypt
from sqlalchemy.dialects import postgresql as psql

from .. import db
from ..common.models import Country
from ..utils import BaseModel, TimedModel

# todo user permissions: admin, creator (with artist id), user


def hash_password(password: str) -> bytes:
    b_password = bytes(password, "utf-8")
    salt = bcrypt.gensalt()
    hash = bcrypt.hashpw(b_password, salt)
    return hash


class User(BaseModel):
    __tablename__ = "users"

    email = db.Column(db.String, nullable=False, unique=True)
    password_hash = db.Column(db.String, nullable=False)
    country_code = db.Column(db.String, db.ForeignKey(Country.code), nullable=False)
    is_premium = db.Column(db.Boolean, nullable=False, default=False)

    first_name = db.Column(db.String, nullable=True)
    last_name = db.Column(db.String, nullable=True)
    birth_date = db.Column(db.Date, nullable=True)

    def set_password(self, password: str) -> None:
        new_hash = hash_password(password)
        self.password_hash = new_hash.decode(encoding="utf-8")

    def __repr__(self):
        return f"<User {self.id}: {self.email}>"


class SongLike(TimedModel):
    __tablename__ = "song_likes"

    user_id = db.Column(psql.UUID(), db.ForeignKey("users.id"), primary_key=True)
    song_id = db.Column(psql.UUID(), db.ForeignKey("songs.id"), primary_key=True)

    def __repr__(self):
        return f"<SongLike user_id={self.user_id}, song_id={self.song_id}>"


class ArtistFollow(TimedModel):
    __tablename__ = "artist_follows"

    user_id = db.Column(psql.UUID(), db.ForeignKey("users.id"), primary_key=True)
    artist_id = db.Column(psql.UUID(), db.ForeignKey("artists.id"), primary_key=True)

    def __repr__(self):
        return f"<ArtistFollow user_id={self.user_id}, artist_id={self.artist_id}>"
