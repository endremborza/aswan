import datetime as dt

import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class SourceUrl(Base):
    __tablename__ = "source_urls"

    cid = db.Column(db.Integer, primary_key=True)
    url = db.Column(db.String)
    handler = db.Column(db.String)
    current_status = db.Column(db.String)
    expiry_seconds = db.Column(db.Integer, default=-1)
    uix = db.UniqueConstraint(url, handler)

    def __repr__(self):
        return f"SURL: {self.handler}: {self.url} - {self.current_status}"

    def to_update_dict(self):
        return {
            "current_status": self.current_status,
            "expiry_seconds": self.expiry_seconds,
        }


class CollectionEvent(Base):
    __tablename__ = "collection_events"

    cid = db.Column(db.Integer, primary_key=True)
    url = db.Column(db.String)
    handler = db.Column(db.String)
    status = db.Column(db.String)
    timestamp = db.Column(db.Integer)
    output_file = db.Column(db.String, nullable=True)
    integrated_to_t2 = db.Column(db.Boolean, default=False)

    def __repr__(self):
        return (
            "CollEvent: "
            f"{self.url} - {self.status} "
            f"({dt.datetime.fromtimestamp(self.timestamp).isoformat()}): "
            f"{self.output_file}"
        )
