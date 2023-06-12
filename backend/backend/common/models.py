from .. import db
from ..utils import TimedModel


class Country(TimedModel):
    __tablename__ = "countries"

    code = db.Column(db.String, nullable=False, primary_key=True)
    name = db.Column(db.String, nullable=False, unique=True)
    enabled_at = db.Column(db.DateTime, nullable=True)

    def __repr__(self):
        return f"<Country {self.code}: {self.name}>"

    def to_dict(self) -> dict:
        obj = {"code": self.code, "name": self.name}
        if self.enabled_at is not None:
            obj["enabled_at"] = self.enabled_at.isoformat()
            obj["enabled"] = True
        else:
            obj["enabled"] = False
        return obj

    def is_enabled(self) -> bool:
        return self.enabled_at is not None
