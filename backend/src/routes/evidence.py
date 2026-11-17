"""Evidence page API: pending, confirmed, anomalies and business linkage."""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select

from src.config.database import get_db
from src.middlewares.auth import admin_only, current_user
from src.models import Delivery, EvidenceConflict, EvidenceEvent, Job, Report
from src.services import evidence
from src.services.audit import record

router = APIRouter(prefix='/evidence')


def _job_ref(db, job_id):
    j = db.get(Job, int(job_id)) if str(job_id).isdigit() else None
    return j


def _drill_enabled() -> bool:
    import os
    return os.getenv('EVIDENCE_DRILLS', 'true').lower() == 'true'


@router.get('/status')
def status(db=Depends(get_db), u=Depends(current_user)):
    station = evidence.station_status()
    counts = {row[0]: row[1] for row in db.execute(
        select(EvidenceEvent.status, func.count()).group_by(EvidenceEvent.status))}
    return {
        'station': station,
        'business': {
            'pending': counts.get(evidence.PENDING, 0),
            'confirmed': counts.get(evidence.CONFIRMED, 0),
            'conflict': counts.get(evidence.CONFLICT, 0),
            'blocked': counts.get(evidence.BLOCKED, 0),
            'conflicts_logged': db.scalar(select(func.count()).select_from(EvidenceConflict)),
        },
        'drills_enabled': _drill_enabled(),
        'frozen': bool(station.get('frozen')),
    }


def _event_business(db, event):
    """Resolve the business object each entry points at."""
    if event.biz_type == 'job':
        job = _job_ref(db, event.biz_id)
        if not job:
            return {'exists': False}
        return {
            'type': 'job', 'id': job.id, 'name': job.name, 'status': job.status,
            'approved': job.approved, 'approval_event_id': job.approval_event_id,
            'link': f'#/results/{job.id}',
        }
    if event.biz_type == 'delivery':
        delivery = db.scalar(select(Delivery).where(Delivery.evidence_event_id == event.event_id))
        if delivery:
            report = db.get(Report, delivery.report_id)
            return {
                'type': 'delivery', 'id': delivery.id,
                'name': delivery.filename or delivery.run_key,
                'status': delivery.status, 'report': report.name if report else None,
                'job_id': report.job_id if report else None,
                'link': '#/reports',
            }
        return {'type': 'delivery', 'id': event.biz_id, 'exists': False}
    return {'type': event.biz_type, 'id': event.biz_id}


@router.get('')
def overview(db=Depends(get_db), u=Depends(current_user)):
    station = evidence.station_status()
    events = list(db.scalars(select(EvidenceEvent).order_by(EvidenceEvent.id.desc()).limit(200)))
    rows = []
    for event in events:
        rows.append({
            'event_id': event.event_id, 'biz_type': event.biz_type, 'biz_id': event.biz_id,
            'action': event.action, 'actor': event.actor, 'object_ref': event.object_ref,
            'content_sha256': event.content_sha256, 'status': event.status,
            'attempts': event.attempts, 'last_error': event.last_error,
            'station_seq': event.station_seq, 'station_hash': event.station_hash,
            'created_at': event.created_at, 'confirmed_at': event.confirmed_at,
            'next_attempt_at': event.next_attempt_at,
            'business': _event_business(db, event),
        })
    conflicts = [{
        'id': c.id, 'biz_type': c.biz_type, 'biz_id': c.biz_id, 'action': c.action,
        'event_id': c.event_id, 'detail': c.detail, 'at': c.at,
        'incoming_content_sha256': c.incoming_content_sha256,
        'existing_content_sha256': c.existing_content_sha256,
        'payload': c.payload,
    } for c in db.scalars(select(EvidenceConflict).order_by(EvidenceConflict.id.desc()).limit(50))]
    counts = {row[0]: row[1] for row in db.execute(
        select(EvidenceEvent.status, func.count()).group_by(EvidenceEvent.status))}
    return {
        'station': station,
        'business': {
            'pending': counts.get(evidence.PENDING, 0),
            'confirmed': counts.get(evidence.CONFIRMED, 0),
            'conflict': counts.get(evidence.CONFLICT, 0),
            'blocked': counts.get(evidence.BLOCKED, 0),
            'conflicts_logged': len(conflicts),
        },
        'events': rows,
        'conflicts': conflicts,
        'pending': [r for r in rows if r['status'] in (evidence.PENDING, evidence.BLOCKED)],
        'confirmed': [r for r in rows if r['status'] == evidence.CONFIRMED],
        'anomalies': station.get('last_verify', {}).get('anomalies', []) if station.get('reachable', True) else [],
        'drills_enabled': _drill_enabled(),
    }


@router.post('/retry')
def retry(db=Depends(get_db), u=Depends(admin_only)):
    result = evidence.retry_pending(db, limit=100)
    record(db, u.email, 'evidence.retry.manual', result)
    db.commit()
    return result


@router.post('/verify')
def verify(db=Depends(get_db), u=Depends(admin_only)):
    try:
        result = evidence.verify()
    except evidence.EvidenceUnavailable as exc:
        raise HTTPException(503, f'存证站不可达：{exc}') from None
    record(db, u.email, 'evidence.verify',
           {'ok': result.get('ok'), 'frozen': result.get('frozen'),
            'anomalies': len(result.get('anomalies') or [])})
    db.commit()
    return result


@router.post('/repair')
def repair(db=Depends(get_db), u=Depends(admin_only)):
    try:
        result = evidence.repair()
    except evidence.EvidenceUnavailable as exc:
        raise HTTPException(503, f'存证站不可达：{exc}') from None
    except evidence.EvidenceConflict as exc:
        raise HTTPException(exc.status_code, exc.detail) from None
    record(db, u.email, 'evidence.repair',
           {'restored': result.get('restored'), 'quarantined': result.get('quarantined'),
            'frozen': result.get('frozen')})
    db.commit()
    return result


@router.post('/reconcile-conflicts')
def reconcile_conflicts(db=Depends(get_db), u=Depends(admin_only)):
    """Re-run the business check for isolated (same-key-different-content) events.

    The station entry itself is never rewritten.  Only the local outbox row is
    re-queued when its frozen payload matches the registered fingerprint, which
    lets a retry complete without touching confirmed evidence.
    """
    requeued = 0
    for event in db.scalars(select(EvidenceEvent).where(EvidenceEvent.status == evidence.CONFLICT)):
        if evidence.fingerprint(event.payload) == event.content_sha256:
            event.status = evidence.PENDING
            requeued += 1
    db.commit()
    result = evidence.retry_pending(db, limit=100)
    record(db, u.email, 'evidence.conflicts.requeued', {'requeued': requeued, **result})
    db.commit()
    return {'requeued': requeued, **result}


@router.post('/drills/lost-receipt')
def drill_lost_receipt(db=Depends(get_db), u=Depends(admin_only)):
    if not _drill_enabled():
        raise HTTPException(403, '演练已关闭')
    try:
        result = evidence.drill_lost_receipt(db, u.email)
    except evidence.EvidenceUnavailable as exc:
        raise HTTPException(503, f'存证站不可达：{exc}') from None
    record(db, u.email, 'evidence.drill.lost_receipt',
           {'event_id': result.get('event_id'), 'replayed': result.get('replayed')})
    db.commit()
    return result


@router.post('/drills/tamper')
def drill_tamper(mode: str = 'delete_middle', db=Depends(get_db), u=Depends(admin_only)):
    if not _drill_enabled():
        raise HTTPException(403, '演练已关闭')
    try:
        result = evidence.drill_tamper(u.email, mode)
    except evidence.EvidenceUnavailable as exc:
        raise HTTPException(503, f'存证站不可达：{exc}') from None
    record(db, u.email, 'evidence.drill.tamper',
           {'mode': mode, 'frozen': (result.get('verify') or {}).get('frozen')})
    db.commit()
    return result
