import time

from fastapi import HTTPException
from redis import Redis
from src.config.settings import REDIS_URL
from src.models import Dataset, Job
from src.services import evidence
from src.services.business import metadata


def _canonical_result(job: Job) -> dict:
    """The exact reviewed content that approval binds to the evidence entry."""
    return {
        'job_id': job.id,
        'name': job.name,
        'dataset_id': job.dataset_id,
        'params': job.params,
        'result': job.result,
        'finished_at': job.finished_at,
    }


def create(db, data, user):
    if not db.get(Dataset, data.dataset_id):
        raise HTTPException(404, '数据批次不存在')
    allowed = {p['key']: p for p in metadata().get('parameters', [])}
    if set(data.params) - set(allowed):
        raise HTTPException(422, '包含未知分析参数')
    params = {k: p['default'] for k, p in allowed.items()}
    params.update(data.params)
    for key, p in allowed.items():
        v = params[key]
        if p['type'] == 'number':
            try:
                v = float(v)
                if not (p.get('min', -1e9) <= v <= p.get('max', 1e9)):
                    raise ValueError()
                params[key] = v
            except (ValueError, TypeError):
                raise HTTPException(422, f'{p["label"]} 超出范围或非数值')
        elif str(v) not in [str(o['value']) for o in p.get('options', [])]:
            raise HTTPException(422, f'{p["label"]} 不是有效选项')
    j = Job(name=data.name, dataset_id=data.dataset_id, params=params, created_by=user.id,
            created_at=time.time(), status='queued')
    db.add(j)
    db.flush()
    return j


def enqueue(job_id):
    try:
        r = Redis.from_url(REDIS_URL)
        r.rpush('analysis:queue', job_id)
        r.close()
    except Exception:
        pass  # SQL queued status is the durable fallback


def approve(db, job: Job, user) -> Job:
    """Approval is only effective after the independent station confirms it.

    1. The approval intent is registered as ``pending`` with a stable event id,
       so an interrupted request is retried, not duplicated.
    2. The station preflight verifies its chain.  A detected deletion or
       modification freezes this and every later approval/distribution (423).
    3. Only a confirmed entry flips ``job.approved``; an unconfirmed one leaves
       the job pending confirmation, which is retried by the worker and is
       never counted as approved in the meantime.
    """
    if job.status != 'completed':
        raise HTTPException(409, '仅已完成结果可以批准')
    if job.approved:
        return job
    # The job's result is immutable after completion, so the content
    # fingerprint registered here is exactly what approval binds to.
    if job.result:
        try:
            evidence.fingerprint(job.result)
        except (TypeError, ValueError):
            raise HTTPException(422, '分析结果包含不可存证的数值，无法批准') from None
    try:
        event = evidence.record(
            db, 'job', str(job.id), 'approval', user.email,
            f'job/{job.id}', {'job': _canonical_result(job), 'decision': 'approved'},
            depends_on=[], raise_on_error=True,
        )
    except (evidence.EvidenceUnavailable, evidence.EvidenceConflict) as exc:
        from src.services.audit import record as audit
        audit(db, user.email, 'job.approval.blocked',
              {'id': job.id, 'reason': str(getattr(exc, 'detail', exc))[:400]})
        db.commit()
        raise evidence.reason_for(exc) from None
    if event.status != evidence.CONFIRMED:
        from src.services.audit import record as audit
        audit(db, user.email, 'job.approval.pending_confirmation',
              {'id': job.id, 'event_id': event.event_id, 'status': event.status})
        db.commit()
        raise HTTPException(503, '业务已登记，存证待确认：请在存证页查看并在确认后继续')
    evidence.apply_confirmation(db, event)
    from src.services.audit import record as audit
    audit(db, user.email, 'job.approve',
          {'id': job.id, 'event_id': event.event_id, 'seq': event.station_seq})
    db.commit()
    return job
