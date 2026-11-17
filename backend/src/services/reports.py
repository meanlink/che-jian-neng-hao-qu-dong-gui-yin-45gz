import csv
import hashlib
import io
import json
import os
import time

from fastapi import HTTPException
from sqlalchemy import select

from src.config.settings import ARTIFACT_DIR
from src.models import Delivery, Job, Report
from src.services import evidence

CONFIRMED = 'confirmed'
PENDING = 'pending'
BLOCKED = 'blocked'
CONFLICT = 'conflict'


def create(db, data):
    j = db.get(Job, data.job_id)
    if not j or j.status != 'completed' or not j.approved:
        raise HTTPException(409, '请先选择已完成且获证据确认的批准结果')
    try:
        evidence.preflight()
    except (evidence.EvidenceUnavailable, evidence.EvidenceConflict) as exc:
        from src.services.audit import record as audit
        audit(db, 'admin', 'report.schedule.blocked',
              {'job': j.id, 'reason': str(getattr(exc, 'detail', exc))[:400]})
        db.commit()
        raise evidence.reason_for(exc) from None
    r = Report(name=data.name, job_id=j.id, destination=data.destination,
               interval_seconds=data.interval_seconds,
               next_run=time.time() + data.delay_seconds, enabled=True)
    db.add(r)
    db.flush()
    return r


def _snapshot(j: Job) -> dict:
    return {'job': j.name, 'job_id': j.id, 'params': j.params, 'result': j.result,
            'approval_event_id': j.approval_event_id}


def _csv_bytes(j: Job) -> bytes:
    out = io.StringIO()
    out.write('﻿')
    table = j.result.get('table', {})
    cols = table.get('columns', [])
    writer = csv.writer(out)

    def safe(v):
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return v
        text = str(v)
        return "'" + text if text.startswith(('=', '+', '-', '@', '\t', '\r')) else text

    writer.writerow([safe(c['label']) for c in cols])
    for row in table.get('rows', []):
        writer.writerow([safe(row.get(c['key'], '')) for c in cols])
    return out.getvalue().encode('utf-8')


def deliver(db, report: Report):
    """Deliver once per scheduled run.

    The run key is stable per report+duration, so a retried delivery (lost
    receipt, worker restart) yields the same evidence event id and the station
    replays instead of appending a second entry.  The artifact is written only
    after the evidence entry is confirmed; until then the row is pending and
    the brief cannot be downloaded.
    """
    j = db.get(Job, report.job_id)
    if not j or not j.approved:
        return None
    run_key = f'report-{report.id}-at-{int(report.next_run)}'
    delivery = db.scalar(select(Delivery).where(Delivery.run_key == run_key))
    if not delivery:
        delivery = Delivery(report_id=report.id, created_at=time.time(), checksum='',
                            filename='', destination=report.destination,
                            run_key=run_key, status=PENDING, content_sha256=None)
        db.add(delivery)
        db.flush()
    elif delivery.status == CONFIRMED:
        return delivery
    elif delivery.status == PENDING and delivery.evidence_event_id:
        # Respect the pending entry's backoff so an outage does not hammer the
        # station on every worker tick; the retry loop owns it.
        from src.models import EvidenceEvent
        event = db.scalar(select(EvidenceEvent).where(
            EvidenceEvent.event_id == delivery.evidence_event_id))
        if event and event.next_attempt_at > time.time():
            return None

    content = _csv_bytes(j)
    checksum = hashlib.sha256(content).hexdigest()
    snapshot = _snapshot(j)
    payload = {
        'report_id': report.id, 'report': report.name, 'destination': report.destination,
        'job_id': j.id, 'job': j.name, 'approval_event_id': j.approval_event_id,
        'run_key': run_key, 'checksum': checksum, 'snapshot_sha256': evidence.fingerprint(snapshot),
    }
    try:
        event = evidence.record(
            db, 'delivery', run_key, 'distribution', 'worker',
            f'report/{report.id}/run/{int(report.next_run)}', payload,
            depends_on=[j.approval_event_id] if j.approval_event_id else [],
            delivery_id=delivery.id, raise_on_error=True,
        )
    except (evidence.EvidenceUnavailable, evidence.EvidenceConflict) as exc:
        delivery.status = BLOCKED if isinstance(exc, evidence.EvidenceConflict) else PENDING
        db.commit()
        from src.services.audit import record as audit
        audit(db, 'worker', 'report.delivery.blocked',
              {'report': report.id, 'run_key': run_key,
               'reason': str(getattr(exc, 'detail', exc))[:400]})
        db.commit()
        return None

    delivery.evidence_event_id = event.event_id
    delivery.content_sha256 = checksum
    if event.status != evidence.CONFIRMED:
        delivery.status = PENDING
        db.commit()
        from src.services.audit import record as audit
        audit(db, 'worker', 'report.delivery.pending_confirmation',
              {'report': report.id, 'run_key': run_key, 'event_id': event.event_id})
        db.commit()
        return None

    materialize(db, delivery.id, content=content, snapshot=snapshot)
    db.commit()
    return delivery

def materialize(db, delivery_id, content: bytes | None = None, snapshot: dict | None = None,
                event=None):
    """Write the downloadable CSV/JSON once the evidence entry is confirmed.

    Idempotent: re-materialising the same delivery reuses its file name and
    checksum, so replaying a retry never invents a second artifact.
    """
    delivery = db.get(Delivery, delivery_id)
    if not delivery:
        return None
    if event is None and delivery.evidence_event_id:
        from src.models import EvidenceEvent
        event = db.scalar(select(EvidenceEvent).where(
            EvidenceEvent.event_id == delivery.evidence_event_id))
    if event is None or event.status != CONFIRMED:
        delivery.status = PENDING
        db.commit()
        return None
    report = db.get(Report, delivery.report_id)
    j = db.get(Job, report.job_id) if report else None
    if j is None:
        return None
    delivery.evidence_event_id = event.event_id
    content = content if content is not None else _csv_bytes(j)
    checksum = hashlib.sha256(content).hexdigest()
    if delivery.content_sha256 and delivery.content_sha256 != checksum:
        delivery.status = CONFLICT
        db.commit()
        return None
    if not delivery.filename:
        delivery.filename = f'report-{delivery.report_id}-delivery-{delivery.id}.csv'
    delivery.checksum = checksum
    delivery.content_sha256 = checksum
    delivery.status = CONFIRMED
    os.makedirs(ARTIFACT_DIR, exist_ok=True)
    with open(os.path.join(ARTIFACT_DIR, delivery.filename), 'wb') as handle:
        handle.write(content)
    snapshot = snapshot or _snapshot(j)
    with open(os.path.join(ARTIFACT_DIR, delivery.filename + '.json'), 'w') as handle:
        json.dump({**snapshot, 'checksum': checksum,
                   'evidence': {'event_id': event.event_id, 'seq': event.station_seq,
                                'entry_hash': event.station_hash}},
                  handle, ensure_ascii=False, allow_nan=False)
    if report and event:
        report.next_run = time.time() + report.interval_seconds
    db.commit()
    return delivery
