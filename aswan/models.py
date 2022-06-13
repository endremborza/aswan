import datetime as dt
from hashlib import md5

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
    suind = db.Index("su_index", handler, url)

    def __repr__(self):  # pragma: no cover
        return f"SURL: {self.handler}: {self.url} - {self.current_status}"


class CollectionEvent(Base):
    __tablename__ = "collection_events"

    cid = db.Column(db.Integer, primary_key=True)
    url = db.Column(db.String)
    handler = db.Column(db.String)
    status = db.Column(db.String)
    timestamp = db.Column(db.Integer)
    output_file = db.Column(db.String, nullable=True)

    def __repr__(self):  # pragma: no cover
        return (
            "CollEvent: "
            f"{self.url} - {self.status} "
            f"({dt.datetime.fromtimestamp(self.timestamp).isoformat()}): "
            f"{self.output_file}"
        )


class IntegrationEvent(Base):
    __tablename__ = "integration_events"

    md5hash = db.Column(db.String(32), primary_key=True)
    cev = db.Column(db.Integer, db.ForeignKey("collection_events.cid", name="cev_key"))
    integrator = db.Column(db.String)
    timestamp = db.Column(db.Integer)
    ind = db.Index("integ_index", integrator)

    def __repr__(self):  # pragma: no cover
        return (
            "IntEvent: "
            f"{self.integrator} of {self.cev} "
            f"({dt.datetime.fromtimestamp(self.timestamp).isoformat()})"
        )

    @classmethod
    def create(cls, cev, integrator, ts):
        msg = f"{integrator} {ts} {cev}"
        md5hash = md5(msg.encode("utf-8")).hexdigest()
        return cls(md5hash=md5hash, cev=cev, integrator=integrator, timestamp=ts)
