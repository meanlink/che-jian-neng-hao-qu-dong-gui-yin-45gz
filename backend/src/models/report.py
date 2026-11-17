from sqlalchemy import String, JSON, Double, ForeignKey, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base


class Report(Base):
    __tablename__ = 'reports'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160))
    job_id: Mapped[int] = mapped_column(ForeignKey('jobs.id'))
    destination: Mapped[str] = mapped_column(String(120))
    interval_seconds: Mapped[int]
    next_run: Mapped[float] = mapped_column(Double)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)


class Delivery(Base):
    __tablename__ = 'deliveries'
    id: Mapped[int] = mapped_column(primary_key=True)
    report_id: Mapped[int] = mapped_column(ForeignKey('reports.id'))
    created_at: Mapped[float] = mapped_column(Double)
    checksum: Mapped[str] = mapped_column(String(64))
    filename: Mapped[str] = mapped_column(String(160))
    destination: Mapped[str] = mapped_column(String(120))
    # One row per scheduled run.  The run key is stable across retries, which is
    # what lets a lost evidence receipt be retried without a second entry.
    run_key: Mapped[str] = mapped_column(String(120), unique=True, default='')
    status: Mapped[str] = mapped_column(String(20), default='pending')
    evidence_event_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    content_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
