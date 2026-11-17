import logging
import time
from pathlib import Path

from redis import Redis
from sqlalchemy import select

from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models import Dataset, Job, Report
from src.services import evidence
from src.services.audit import record
from src.services.business import analyze
from src.services.reports import deliver

logging.basicConfig(level=logging.INFO)
_last_evidence_retry = 0.0


def execute(job_id):
    with Session() as db:
        j = db.get(Job, job_id)
        if not j or j.status != 'queued':
            return
        j.status = 'running'
        db.commit()
        try:
            d = db.get(Dataset, j.dataset_id)
            result = analyze(d.records, j.params)
            import json
            json.dumps(result, allow_nan=False)
            j.result = result
            j.status = 'completed'
            j.finished_at = time.time()
            record(db, 'worker', 'job.completed', {'id': j.id})
        except Exception as e:
            logging.exception('Analysis failed')
            j.status = 'failed'
            j.error = str(e)[:990]
            j.finished_at = time.time()
            record(db, 'worker', 'job.failed', {'id': j.id, 'error': j.error})
        db.commit()


def retry_evidence():
    """Finish evidence intents whose receipt was lost or whose station was down.

    Runs before new approvals/deliveries each tick.  Confirmed approvals flip
    ``job.approved`` and confirmed distributions materialise their download.
    """
    with Session() as db:
        try:
            result = evidence.retry_pending(db, limit=50)
            if result['attempted'] or result['frozen'] or result['unavailable']:
                record(db, 'worker', 'evidence.retry', result)
                db.commit()
            return result
        except Exception:
            logging.exception('Evidence retry failed')
            db.rollback()
            return {'error': True}


def tick():
    # Evidence retries are paced: the pending row carries its own backoff, and
    # the station preflight is not repeated on every one-second loop.
    global _last_evidence_retry
    if time.time() - _last_evidence_retry >= 5:
        _last_evidence_retry = time.time()
        retry_evidence()
    with Session() as db:
        queued = list(db.scalars(select(Job.id).where(Job.status == 'queued').limit(3)))
    for job_id in queued:
        execute(job_id)
    with Session() as db:
        for r in db.scalars(select(Report).where(Report.enabled == True, Report.next_run <= time.time())):
            d = deliver(db, r)
            if d:
                record(db, 'worker', 'report.delivered',
                       {'id': r.id, 'delivery': d.id, 'event_id': d.evidence_event_id})
        db.commit()


def main():
    r = Redis.from_url(REDIS_URL, socket_timeout=3)
    # This Compose stack has a single worker. A crash must not strand its jobs.
    with Session() as db:
        for j in db.scalars(select(Job).where(Job.status == 'running')):
            j.status = 'queued'
            record(db, 'worker', 'job.recovered', {'id': j.id})
        db.commit()
    while True:
        Path('/tmp/heartbeat').touch()
        try:
            item = r.blpop('analysis:queue', timeout=1)
            if item:
                execute(int(item[1]))
            tick()
        except Exception:
            logging.exception('Worker retry')
            time.sleep(2)


if __name__ == '__main__':
    main()
