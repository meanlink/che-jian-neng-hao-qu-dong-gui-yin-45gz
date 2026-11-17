from sqlalchemy import String, JSON, Double, Integer, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base


class EvidenceEvent(Base):
    """Business-side intent/outbox for the independent evidence station.

    The row is written before the station call, so an interrupted request can
    be retried with the same deterministic ``event_id`` without appending twice.
    ``payload`` is the exact content that was sent and must never be rewritten
    once the entry is confirmed.
    """

    __tablename__ = 'evidence_events'
    __table_args__ = (
        UniqueConstraint('biz_type', 'biz_id', 'action', name='ux_evidence_biz_key'),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[str] = mapped_column(String(80), unique=True)
    biz_type: Mapped[str] = mapped_column(String(30))
    biz_id: Mapped[str] = mapped_column(String(80))
    action: Mapped[str] = mapped_column(String(40))
    actor: Mapped[str] = mapped_column(String(150))
    object_ref: Mapped[str] = mapped_column(String(300), default='')
    content_sha256: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    depends_on: Mapped[list] = mapped_column(JSON, default=list)
    status: Mapped[str] = mapped_column(String(20), default='pending')
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str] = mapped_column(String(1000), default='')
    station_seq: Mapped[int | None] = mapped_column(Integer, nullable=True)
    station_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    next_attempt_at: Mapped[float] = mapped_column(Double, default=0.0)
    delivery_id: Mapped[int | None] = mapped_column(ForeignKey('deliveries.id'), nullable=True)
    created_at: Mapped[float] = mapped_column(Double)
    confirmed_at: Mapped[float | None] = mapped_column(Double, nullable=True)


class EvidenceConflict(Base):
    """Incoming content that reused a business key/event id with different data.

    Row is only a record of the rejected attempt; the original confirmed
    evidence is never overwritten.
    """

    __tablename__ = 'evidence_conflicts'
    id: Mapped[int] = mapped_column(primary_key=True)
    biz_type: Mapped[str] = mapped_column(String(30))
    biz_id: Mapped[str] = mapped_column(String(80))
    action: Mapped[str] = mapped_column(String(40))
    event_id: Mapped[str] = mapped_column(String(80))
    existing_event_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    existing_content_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    incoming_content_sha256: Mapped[str] = mapped_column(String(64))
    detail: Mapped[str] = mapped_column(String(500))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    at: Mapped[float] = mapped_column(Double)
