import hashlib
import json


def checksum(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def dataset_dict(d):
    return {'id': d.id, 'name': d.name, 'source': d.source, 'rows': len(d.records),
            'checksum': d.checksum, 'created_at': d.created_at}


def job_dict(j):
    data = {k: getattr(j, k) for k in ['id', 'name', 'dataset_id', 'params', 'status', 'result',
                                        'error', 'approved', 'approved_by', 'approved_at',
                                        'approval_event_id', 'created_at', 'finished_at']}
    data['approval_pending'] = False
    return data


def report_dict(r):
    return {k: getattr(r, k) for k in ['id', 'name', 'job_id', 'destination', 'interval_seconds',
                                        'next_run', 'enabled']}


def delivery_dict(d):
    return {k: getattr(d, k) for k in ['id', 'report_id', 'created_at', 'checksum', 'filename',
                                        'destination', 'run_key', 'status', 'evidence_event_id']}


def evidence_event_dict(e):
    return {k: getattr(e, k) for k in ['event_id', 'biz_type', 'biz_id', 'action', 'actor',
                                        'object_ref', 'content_sha256', 'status', 'attempts',
                                        'last_error', 'station_seq', 'station_hash', 'created_at',
                                        'confirmed_at', 'next_attempt_at']}
