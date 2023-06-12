from datetime import datetime
from uuid import uuid4

from sqlalchemy.dialects.postgresql import UUID

from . import db, log
from .errors import FieldsNotFound


def validate_request(req: dict, fields: list[str]) -> None:
    """
    Checks if mandatory fields exist in the request object.

    Raises:
        FieldsNotFound: containing the list of mandatory fields not found.
    """
    keys = req.keys()
    not_found = []
    for field in fields:
        if field not in keys:
            not_found.append(field)
    if not_found:
        raise FieldsNotFound(not_found)


class TimedModel(db.Model):
    __abstract__ = True

    created_dtm = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    modified_dtm = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )


class BaseModel(TimedModel):
    __abstract__ = True

    id = db.Column(UUID(), nullable=False, primary_key=True, default=uuid4)


def commit_db(entity, entity_name: str, is_update: bool = True) -> None:
    db.session.commit()
    db.session.flush()
    entity_id = (
        "" if entity is None or not hasattr(entity, "id") else f" ID {entity.id}"
    )
    log.info(f"{entity_name}{entity_id}: {'updated' if is_update else 'created'}")
