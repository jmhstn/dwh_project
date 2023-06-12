from flask import current_app as app

from . import db

log = app.logger


def commit_db(entity, entity_name: str, is_update: bool = True) -> None:
    db.session.commit()
    db.session.flush()
    log.info(f"{entity_name} ID {entity.id}: {'updated' if is_update else 'created'}")


def clamp(num, min_value, max_value):
    return max(min(num, max_value), min_value)
