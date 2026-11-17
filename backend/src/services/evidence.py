"""Client and outbox for the independent evidence station.

Design rules
------------
* The business row is registered as ``pending`` *before* the station call, so a
  crash between the two leaves a durable intent that a retry can finish.
* ``event_id`` is derived from the business key, never random.  Repeating a
  request (lost receipt, worker restart) therefore reuses the same id and the
  station replays instead of appending a second entry.
* Content is frozen at registration time.  Different content under the same
  business key is isolated in ``evidence_conflicts`` and is never appended as a
  second entry; the original entry is left untouched.
* Nothing becomes effective in the business database (approval, downloadable
  brief) until the station confirms the entry.
* Every confirmation is preceded by the station's own chain verification, so a
  detected deletion/modification freezes subsequent approvals and deliveries.
"""
from __future__ import annotations

import hashlib
import json
import time
import urllib.error
import urllib.request

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from src.config.settings import EVIDENCE_RETRY_CAP, EVIDENCE_TIMEOUT, EVIDENCE_URL
from src.models import EvidenceConflict as EvidenceConflictRow, EvidenceEvent

PENDING = 'pending'
CONFIRMED = 'confirmed'
CONFLICT = 'conflict'
BLOCKED = 'blocked'


class EvidenceUnavailable(Exception):
    pass


class EvidenceConflict(Exception):
    def __init__(self, detail: str, status_code: int = 409):
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def fingerprint(value) -> str:
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def event_id_for(biz_type: str, biz_id: str, action: str) -> str:
    return 'evt-' + hashlib.sha256(f'{biz_type}|{biz_id}|{action}'.encode()).hexdigest()[:40]


def _request(method: str, path: str, body=None, timeout: float | None = None):
    data = None if body is None else canonical(body).encode('utf-8')
    request = urllib.request.Request(
        EVIDENCE_URL.rstrip('/') + path, data=data, method=method,
        headers={'Content-Type': 'application/json'},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout or EVIDENCE_TIMEOUT) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            payload = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            payload = {'detail': raw.decode('utf-8', 'replace')[:500]}
        return exc.code, payload
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        raise EvidenceUnavailable(str(exc)) from None


def station_status() -> dict:
    try:
        status, payload = _request('GET', '/status')
        if status == 200:
            return {**payload, 'reachable': True}
        return {'reachable': False, 'error': (payload or {}).get('detail', '异常')}
    except EvidenceUnavailable as exc:
        return {'reachable': False, 'error': str(exc)}


def preflight() -> dict:
    """Ask the station to verify the chain before a business transition.

    Raises ``EvidenceUnavailable`` when the station cannot answer and
    ``EvidenceConflict`` (423) when the chain is frozen.
    """
    try:
        status, payload = _request('POST', '/preflight')
    except EvidenceUnavailable:
        raise EvidenceUnavailable('存证站不可达') from None
    if status != 200:
        raise EvidenceUnavailable((payload or {}).get('detail', '存证站返回异常'))
    if payload.get('frozen'):
        reason = (payload.get('status') or {}).get('freeze_reason') or '存证链核验异常'
        raise EvidenceConflict(f'存证链已冻结，后续批准与分发暂停：{reason}', 423)
    return payload


def _log_conflict(db, event: EvidenceEvent | None, biz_type, biz_id, action, event_id,
                  incoming: str, detail: str, payload: dict | None = None) -> None:
    db.add(EvidenceConflictRow(
        biz_type=biz_type, biz_id=str(biz_id), action=action, event_id=event_id,
        existing_event_id=event.event_id if event else None,
        existing_content_sha256=event.content_sha256 if event else None,
        incoming_content_sha256=incoming, detail=detail[:500],
        payload=payload or {}, at=time.time(),
    ))
    db.commit()


def _register(db, biz_type, biz_id, action, actor, object_ref, payload, depends_on, delivery_id=None) -> EvidenceEvent:
    event_id = event_id_for(biz_type, biz_id, action)
    existing = db.scalar(select(EvidenceEvent).where(
        EvidenceEvent.biz_type == biz_type,
        EvidenceEvent.biz_id == str(biz_id),
        EvidenceEvent.action == action,
    ))
    # occurred_at is part of the fingerprinted content, so it is fixed on the
    # first registration and reused verbatim on every retry.  Otherwise a
    # retried action would look like "same key, different content".
    if existing is not None:
        occurred_at = float((existing.payload or {}).get('occurred_at') or existing.created_at)
    else:
        occurred_at = float(payload.get('occurred_at') or time.time())
    payload = {**payload, 'occurred_at': occurred_at}
    content = fingerprint(payload)
    if existing:
        if existing.event_id == event_id and existing.content_sha256 == content:
            return existing
        detail = '同号异内容：该业务动作已登记过不同内容的存证，新内容隔离，原存证不改写'
        _log_conflict(db, existing, biz_type, biz_id, action, event_id, content, detail, payload)
        raise EvidenceConflict(detail)
    event = EvidenceEvent(
        event_id=event_id, biz_type=biz_type, biz_id=str(biz_id), action=action, actor=actor,
        object_ref=object_ref, content_sha256=content, payload=payload,
        depends_on=list(depends_on or []), status=PENDING, created_at=time.time(),
        next_attempt_at=0.0, delivery_id=delivery_id,
    )
    db.add(event)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = db.scalar(select(EvidenceEvent).where(
            EvidenceEvent.biz_type == biz_type, EvidenceEvent.biz_id == str(biz_id),
            EvidenceEvent.action == action))
        if existing and existing.content_sha256 == content:
            return existing
        _log_conflict(db, existing, biz_type, biz_id, action, event_id, content,
                      '同一业务动作并发登记了不同内容，已隔离', payload)
        raise EvidenceConflict('同一业务动作并发登记了不同内容，已隔离') from None
    return event


def _confirm(event: EvidenceEvent, entry: dict, note: str = '') -> None:
    event.status = CONFIRMED
    event.station_seq = int(entry.get('seq') or 0)
    event.station_hash = entry.get('entry_hash')
    event.confirmed_at = time.time()
    event.last_error = note
    event.attempts = int(event.attempts or 0) + 1


def _reconcile(db, event: EvidenceEvent) -> bool:
    try:
        status, entry = _request('GET', f'/entries/{event.event_id}')
    except EvidenceUnavailable:
        return False
    if status != 200 or not isinstance(entry, dict):
        return False
    if entry.get('content_sha256') != event.content_sha256:
        event.status = CONFLICT
        event.last_error = '存证站已有同号不同内容，已隔离'
        _log_conflict(db, event, event.biz_type, event.biz_id, event.action, event.event_id,
                      event.content_sha256, event.last_error, event.payload)
        db.commit()
        return False
    _confirm(event, entry, '回执丢失后按存证编号核对，未重复写入')
    db.commit()
    return True


def _attempt(db, event: EvidenceEvent, raise_on_error: bool, preflight_first: bool = False) -> EvidenceEvent:
    if event.status != PENDING:
        if event.status == CONFLICT and raise_on_error:
            raise EvidenceConflict(event.last_error or '同号异内容，已隔离')
        return event
    if event.content_sha256 != fingerprint(event.payload):
        event.status = CONFLICT
        event.last_error = '本地存证内容在登记后被改动，已隔离，不写入存证链'
        _log_conflict(db, event, event.biz_type, event.biz_id, event.action, event.event_id,
                      fingerprint(event.payload), event.last_error, event.payload)
        if raise_on_error:
            raise EvidenceConflict(event.last_error)
        return event
    if preflight_first:
        # The intent is already durable (pending).  Only now check that the
        # station is reachable and its chain is intact.
        try:
            preflight()
        except EvidenceConflict as exc:
            event.status = BLOCKED
            event.last_error = exc.detail
            event.next_attempt_at = time.time() + min(2 ** min(int(event.attempts), 6), EVIDENCE_RETRY_CAP)
            db.commit()
            if raise_on_error:
                raise
            return event
        except EvidenceUnavailable as exc:
            event.last_error = f'存证站不可达，待确认：{exc}'[:990]
            event.next_attempt_at = time.time() + min(2 ** min(int(event.attempts), 6), EVIDENCE_RETRY_CAP)
            db.commit()
            if raise_on_error:
                raise
            return event
    body = {
        'event_id': event.event_id, 'biz_type': event.biz_type, 'biz_id': event.biz_id,
        'action': event.action, 'actor': event.actor, 'object_ref': event.object_ref,
        'content_sha256': event.content_sha256,
        'occurred_at': float(event.payload.get('occurred_at') or event.created_at),
        'payload': event.payload, 'depends_on': event.depends_on or [],
    }
    event.attempts = int(event.attempts or 0) + 1
    try:
        status, payload = _request('POST', '/entries', body)
    except EvidenceUnavailable as exc:
        if _reconcile(db, event):
            return event
        event.last_error = f'回执未确认：{exc}'
        event.next_attempt_at = time.time() + min(2 ** min(int(event.attempts), 6), EVIDENCE_RETRY_CAP)
        db.commit()
        if raise_on_error:
            raise EvidenceUnavailable(str(exc)) from None
        return event
    if status in (200, 201) and payload:
        _confirm(event, payload, '重放确认，未重复写入' if payload.get('replayed') else '')
        db.commit()
        return event
    if status == 423:
        event.status = BLOCKED
        event.last_error = (payload or {}).get('detail', '存证链冻结')
        db.commit()
        if raise_on_error:
            raise EvidenceConflict(event.last_error, 423)
        return event
    if status == 409:
        event.status = CONFLICT
        event.last_error = (payload or {}).get('detail', '同号异内容，已隔离')
        _log_conflict(db, event, event.biz_type, event.biz_id, event.action, event.event_id,
                      event.content_sha256, event.last_error, event.payload)
        if raise_on_error:
            raise EvidenceConflict(event.last_error)
        return event
    event.last_error = f'存证站返回 {status}：{(payload or {}).get("detail", "")}'[:990]
    event.next_attempt_at = time.time() + min(2 ** min(int(event.attempts), 6), EVIDENCE_RETRY_CAP)
    db.commit()
    if raise_on_error:
        raise EvidenceUnavailable(event.last_error)
    return event


def record(db, biz_type, biz_id, action, actor, object_ref, payload, depends_on=None,
           delivery_id=None, raise_on_error=True) -> EvidenceEvent:
    """Register intent, then try once to confirm it at the station.

    The intent row is durably pending before any network call, so a station
    outage leaves a visible "待确认" record rather than losing the action.
    """
    payload = {**payload, 'occurred_at': payload.get('occurred_at') or time.time()}
    event = _register(db, biz_type, str(biz_id), action, actor, object_ref, payload,
                      depends_on or [], delivery_id)
    if event.status == BLOCKED:
        # An explicit business retry re-attempts a previously frozen intent;
        # preflight will refuse again if the chain is still inconsistent.
        event.status = PENDING
        db.commit()
    if event.status == PENDING:
        event = _attempt(db, event, raise_on_error=raise_on_error, preflight_first=True)
    elif event.status == CONFLICT and raise_on_error:
        raise EvidenceConflict(event.last_error or '同号异内容，已隔离')
    return event


def apply_confirmation(db, event: EvidenceEvent) -> None:
    """Business effects that only happen after evidence confirmation."""
    if event.status != CONFIRMED:
        return
    if event.biz_type == 'job' and event.action == 'approval':
        from src.models import Job
        from src.services.audit import record as audit
        job = db.get(Job, int(event.biz_id))
        if job and job.status == 'completed' and not job.approved:
            job.approved = True
            job.approved_by = event.actor
            job.approved_at = event.confirmed_at
            job.approval_event_id = event.event_id
            audit(db, event.actor, 'job.approval.evidenced',
                  {'id': job.id, 'event_id': event.event_id, 'seq': event.station_seq})
    elif event.biz_type == 'delivery' and event.action == 'distribution':
        from src.services import reports
        if event.delivery_id:
            reports.materialize(db, event.delivery_id)
            from src.services.audit import record as audit
            audit(db, event.actor, 'report.delivery.evidenced',
                  {'delivery': event.delivery_id, 'event_id': event.event_id,
                   'seq': event.station_seq})


def retry_pending(db, limit: int = 50) -> dict:
    """Retry pending/blocked intents.  Safe to call concurrently: appends replay."""
    result = {'attempted': 0, 'confirmed': 0, 'still_pending': 0, 'conflicts': 0,
              'blocked': 0, 'frozen': False, 'unavailable': False}
    try:
        preflight()
    except EvidenceConflict:
        from sqlalchemy import func
        result['frozen'] = True
        result['blocked'] = int(db.scalar(select(func.count()).select_from(EvidenceEvent).where(
            EvidenceEvent.status.in_((PENDING, BLOCKED)))) or 0)
        return result
    except EvidenceUnavailable:
        result['unavailable'] = True
        return result
    # The chain verified, so intents blocked by an earlier anomaly (or by an
    # outage) can be retried immediately; the backoff only paces automatic
    # retries while the station is still unhealthy.
    for blocked in db.scalars(select(EvidenceEvent).where(EvidenceEvent.status == BLOCKED)):
        blocked.status = PENDING
        blocked.next_attempt_at = 0.0
    db.commit()
    events = list(db.scalars(
        select(EvidenceEvent).where(
            EvidenceEvent.status == PENDING,
            EvidenceEvent.next_attempt_at <= time.time(),
        ).order_by(EvidenceEvent.id).limit(limit)))
    for event in events:
        before = event.status
        event = _attempt(db, event, raise_on_error=False)
        result['attempted'] += 1
        if event.status == CONFIRMED and before != CONFIRMED:
            result['confirmed'] += 1
            apply_confirmation(db, event)
            db.commit()
        elif event.status == CONFLICT:
            result['conflicts'] += 1
        elif event.status == BLOCKED:
            result['blocked'] += 1
        else:
            result['still_pending'] += 1
    return result


def reason_for(exc: Exception) -> HTTPException:
    if isinstance(exc, EvidenceConflict):
        return HTTPException(exc.status_code, exc.detail)
    return HTTPException(503, '存证站暂不可用，业务动作已登记为待确认，请稍后重试')


def latest_for(db, biz_type: str, biz_id, action: str) -> EvidenceEvent | None:
    return db.scalar(select(EvidenceEvent).where(
        EvidenceEvent.biz_type == biz_type, EvidenceEvent.biz_id == str(biz_id),
        EvidenceEvent.action == action).order_by(EvidenceEvent.id.desc()).limit(1))


def pending_count(db) -> int:
    from sqlalchemy import func
    return int(db.scalar(select(func.count()).select_from(EvidenceEvent).where(
        EvidenceEvent.status.in_((PENDING, BLOCKED, CONFLICT)))) or 0)


# --------------------------------------------------------------------------
# Local drills exercised from the evidence page.
# --------------------------------------------------------------------------

def drill_lost_receipt(db, actor: str) -> dict:
    """Append once, drop the receipt, then prove the retry does not duplicate."""
    nonce = hashlib.sha256(f'{time.time()}-{actor}'.encode()).hexdigest()[:12]
    biz_id = f'drill-lost-{nonce}'
    payload = {'drill': 'lost-receipt', 'nonce': nonce, 'note': '演练：回执丢失后重试'}
    event = _register(db, 'drill', biz_id, 'lost_receipt', actor, 'drill/lost-receipt', payload, [])
    body = {
        'event_id': event.event_id, 'biz_type': event.biz_type, 'biz_id': event.biz_id,
        'action': event.action, 'actor': event.actor, 'object_ref': event.object_ref,
        'content_sha256': event.content_sha256,
        'occurred_at': float(event.payload.get('occurred_at') or event.created_at),
        'payload': event.payload, 'depends_on': [],
    }
    before = station_status()
    try:
        status, dropped = _request('POST', '/drills/lost-receipt', body)
    except EvidenceUnavailable as exc:
        raise EvidenceUnavailable(str(exc)) from None
    if status != 504:
        raise EvidenceUnavailable(f'存证站未执行丢回执演练：{status} {(dropped or {}).get("detail", "")}')
    # The append succeeded but the receipt was dropped; reconcile by event id.
    recovered = _reconcile(db, event)
    retry_status = None
    replayed = None
    if recovered:
        retry_status, replayed_payload = _request('POST', '/entries', body)
        replayed = bool((replayed_payload or {}).get('replayed'))
    after = station_status()
    return {
        'event_id': event.event_id, 'biz_id': biz_id,
        'dropped_receipt_status': status,
        'recovered_by_event_id': recovered,
        'retry_status': retry_status, 'replayed': replayed,
        'entries_before': before.get('entries'), 'entries_after': after.get('entries'),
        'station_seq': event.station_seq, 'confirmed': event.status == CONFIRMED,
        'message': '回执丢失后按同一存证编号重试，存证站返回重放而非新增条目。'
                   if recovered and replayed else '回执丢失后的恢复未完成，请查看事件状态。',
    }


def drill_tamper(actor: str, mode: str) -> dict:
    try:
        status, payload = _request('POST', '/drills/tamper', {'mode': mode})
    except EvidenceUnavailable as exc:
        raise EvidenceUnavailable(str(exc)) from None
    if status != 200:
        raise HTTPException(status if status in (403, 409) else 503,
                            (payload or {}).get('detail', '存证站未执行篡改演练'))
    verify_status, verify_payload = _request('POST', '/verify')
    return {'tamper': payload, 'verify': verify_payload,
            'verify_http_status': verify_status, 'actor': actor,
            'message': '已模拟存储层篡改并核验；存证链已冻结，后续批准与分发将被拒绝。'
                       if (verify_payload or {}).get('frozen') else '核验未发现异常。'}


def verify() -> dict:
    status, payload = _request('POST', '/verify', timeout=max(EVIDENCE_TIMEOUT, 10))
    if status != 200:
        raise EvidenceUnavailable((payload or {}).get('detail', '存证站返回异常'))
    return payload


def repair() -> dict:
    status, payload = _request('POST', '/repair', timeout=max(EVIDENCE_TIMEOUT, 20))
    if status not in (200, 409):
        raise EvidenceUnavailable((payload or {}).get('detail', '存证站返回异常'))
    if status == 409:
        raise EvidenceConflict((payload or {}).get('detail', '恢复镜像损坏，无法自动补齐'), 409)
    return payload
