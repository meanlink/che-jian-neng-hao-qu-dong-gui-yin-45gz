"""End-to-end evidence acceptance checks.

Run with a live station and backend, or let this file boot both in-process:

    python -m tests.test_evidence

The checks cover: ordered append with operator/object/fingerprint, pending
before confirmation, lost-receipt retry without duplication, same-id different
content isolation, approval/distribution gating, freeze on tamper or missing
entry, repair + re-verify recovery, and concurrent approval/distribution order.
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

STATION = os.getenv('EVIDENCE_TEST_URL', 'http://127.0.0.1:8010')
BACKEND = os.getenv('BUSINESS_TEST_URL', 'http://127.0.0.1:8000')


def station(method, path, body=None, timeout=10):
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode()
    request = urllib.request.Request(STATION + path, data=data, method=method,
                                     headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read() or b'null')
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b'null')


def business(method, path, body=None, token=None, timeout=30):
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode()
    headers = {'Content-Type': 'application/json'}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    request = urllib.request.Request(BACKEND + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            try:
                return response.status, json.loads(raw)
            except json.JSONDecodeError:
                return response.status, raw
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, raw


def login(email):
    status, body = business('POST', '/auth/login', {'email': email, 'password': 'Demo123!'})
    assert status == 200, body
    return body['access_token']


def make_completed_job(token, name):
    status, job = business('POST', '/jobs', {'name': name, 'dataset_id': 1,
                                             'params': {'max_features': 3, 'scenario_pct': 10}}, token)
    assert status == 201, job
    # The worker is asynchronous in compose; in tests it is executed inline.
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from src.worker import execute
    execute(job['id'])
    status, job = business('GET', f"/jobs/{job['id']}", token=token)
    assert job['status'] == 'completed', job
    return job


class EvidenceAcceptance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin = login('admin@demo.local')
        cls.analyst = login('analyst@demo.local')
        cls.viewer = login('viewer@demo.local')
        # Make sure no earlier drill left the station frozen.
        station('POST', '/repair')

    def test_01_chain_shape(self):
        status, body = station('GET', '/entries?limit=5')
        self.assertEqual(status, 200)
        self.assertTrue(body, '批准或分发后应存在存证条目')
        entry = body[0]
        for key in ('seq', 'actor', 'object_ref', 'content_sha256', 'prev_hash', 'entry_hash'):
            self.assertIn(key, entry)
        status, verify = station('POST', '/verify')
        self.assertTrue(verify['ok'], verify)

    def test_02_approval_requires_confirmation(self):
        job = make_completed_job(self.analyst, '验收 · 批准主路')
        self.assertEqual(business('POST', f"/jobs/{job['id']}/approve", token=self.analyst)[0], 403)
        self.assertEqual(business('POST', f"/jobs/{job['id']}/approve", token=self.viewer)[0], 403)
        status, approved = business('POST', f"/jobs/{job['id']}/approve", token=self.admin)
        self.assertEqual(status, 200, approved)
        self.assertTrue(approved['approved'])
        self.assertTrue(approved['approval_event_id'].startswith('evt-'))
        status, entries = station('GET', f"/entries/{approved['approval_event_id']}")
        self.assertEqual(status, 200)
        self.assertEqual(entries['action'], 'approval')
        self.assertEqual(entries['biz_id'], str(job['id']))
        self.assertEqual(entries['actor'], 'admin@demo.local')

    def test_03_distribution_gated_and_downloadable(self):
        status, jobs = business('GET', '/jobs', token=self.admin)
        approved = [j for j in jobs if j['approved'] and j['status'] == 'completed'][0]
        status, report = business('POST', '/reports', {
            'name': '验收分发', 'job_id': approved['id'], 'destination': '验收收件箱',
            'interval_seconds': 60, 'delay_seconds': 0}, token=self.admin)
        self.assertEqual(status, 201, report)
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from src.worker import tick
        tick()
        status, deliveries = business('GET', '/deliveries', token=self.viewer)
        mine = [d for d in deliveries if d['report_id'] == report['id']]
        self.assertTrue(mine, deliveries)
        delivery = mine[0]
        self.assertEqual(delivery['status'], 'confirmed')
        entry_status, entry = station('GET', f"/entries/{delivery['evidence_event_id']}")
        self.assertEqual(entry_status, 200)
        self.assertEqual(entry['biz_type'], 'delivery')
        self.assertEqual(entry['payload']['checksum'], delivery['checksum'])
        status, jobs = business('GET', '/jobs', token=self.admin)
        approved = [j for j in jobs if j['id'] == approved['id']][0]
        self.assertEqual(entry['depends_on'], [approved['approval_event_id']],
                         '分发存证必须挂接对应的批准存证，保持先后关系')
        self.assertEqual(business('GET', f"/deliveries/{delivery['id']}/download?format=csv",
                                  token=self.viewer)[0], 200)

    def test_04_unapproved_cannot_schedule(self):
        job = make_completed_job(self.analyst, '验收 · 未批准')
        status, _ = business('POST', '/reports', {
            'name': '违规分发', 'job_id': job['id'], 'destination': '验收组',
            'interval_seconds': 60, 'delay_seconds': 0}, token=self.admin)
        self.assertEqual(status, 409)

    def test_05_lost_receipt_retry_is_idempotent(self):
        before = station('GET', '/status')[1]['entries']
        status, drill = business('POST', '/evidence/drills/lost-receipt', token=self.admin)
        self.assertEqual(status, 200, drill)
        self.assertEqual(drill['dropped_receipt_status'], 504)
        self.assertTrue(drill['recovered_by_event_id'])
        self.assertTrue(drill['replayed'])
        self.assertEqual(drill['entries_after'], before + 1,
                         '回执丢失后重试应只新增一条存证')

    def test_06_same_id_different_content_isolated(self):
        status, isolated = station('POST', '/drills/conflict', {})
        self.assertEqual(status, 200, isolated)
        self.assertTrue(isolated['isolated'])
        conflicts = station('GET', '/conflicts')[1]
        self.assertTrue(any(c['reason'].startswith('同号异内容') for c in conflicts), conflicts)

    def test_07_tamper_freezes_then_repair_recovers(self):
        job = make_completed_job(self.analyst, '验收 · 冻结')
        status, drill = business('POST', '/evidence/drills/tamper?mode=delete_middle', token=self.admin)
        self.assertEqual(status, 200, drill)
        self.assertTrue(drill['verify']['frozen'])
        status, body = business('POST', f"/jobs/{job['id']}/approve", token=self.admin)
        self.assertEqual(status, 423, body)
        status, still = business('GET', f"/jobs/{job['id']}", token=self.admin)
        self.assertFalse(still['approved'], '存证未确认前结果不算获批')
        # A distribution against another approved job is refused too.
        status, jobs = business('GET', '/jobs', token=self.admin)
        approved = [j for j in jobs if j['approved']][0]
        self.assertEqual(business('POST', '/reports', {
            'name': '冻结期分发', 'job_id': approved['id'], 'destination': '验收组',
            'interval_seconds': 60, 'delay_seconds': 0}, token=self.admin)[0], 423)
        status, repair = business('POST', '/evidence/repair', token=self.admin)
        self.assertEqual(status, 200, repair)
        self.assertTrue(repair['verify']['ok'])
        status, approved_job = business('POST', f"/jobs/{job['id']}/approve", token=self.admin)
        self.assertEqual(status, 200, approved_job)
        self.assertTrue(approved_job['approved'])

    def test_08_concurrent_approvals_keep_total_order(self):
        one = make_completed_job(self.analyst, '验收 · 并发甲')
        two = make_completed_job(self.analyst, '验收 · 并发乙')
        results = {}

        def approve(key, job):
            results[key] = business('POST', f"/jobs/{job['id']}/approve", token=self.admin)

        threads = [threading.Thread(target=approve, args=('a', one)),
                   threading.Thread(target=approve, args=('b', two))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(results['a'][0], 200, results['a'])
        self.assertEqual(results['b'][0], 200, results['b'])
        seq_a = results['a'][1]['approval_event_id']
        seq_b = results['b'][1]['approval_event_id']
        entry_a = station('GET', f'/entries/{seq_a}')[1]
        entry_b = station('GET', f'/entries/{seq_b}')[1]
        self.assertNotEqual(entry_a['seq'], entry_b['seq'])
        status, verify = station('POST', '/verify')
        self.assertTrue(verify['ok'], verify)



class EvidenceOrdering(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin = login('admin@demo.local')
        cls.analyst = login('analyst@demo.local')
        station('POST', '/repair')

    def test_09_distribution_requires_prior_approval_entry(self):
        status, body = station('POST', '/entries', {
            'event_id': 'evt-order-test-0001', 'biz_type': 'delivery',
            'biz_id': 'order-run-1', 'action': 'distribution', 'actor': 'worker',
            'object_ref': 'report/999/run/1', 'content_sha256': 'a' * 64,
            'occurred_at': time.time(), 'payload': {'run_key': 'order-run-1'},
            'depends_on': ['evt-does-not-exist'],
        })
        self.assertEqual(status, 409, body)
        self.assertIn('前置存证不存在', body['detail'])

    def test_10_same_key_different_content_is_isolated_business_side(self):
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from src.config.database import Session
        from sqlalchemy import select
        from src.models import EvidenceConflict, EvidenceEvent
        from src.services import evidence

        job = make_completed_job(self.analyst, '验收 · 业务侧同号异内容')
        with Session() as db:
            first = evidence.record(
                db, 'job', str(job['id']), 'approval', 'admin@demo.local', f"job/{job['id']}",
                {'job': {'id': job['id'], 'result': 'v1'}, 'decision': 'approved'})
            self.assertEqual(first.status, evidence.CONFIRMED)
            original_seq = first.station_seq
            original_hash = first.station_hash
            with self.assertRaises(evidence.EvidenceConflict):
                evidence.record(
                    db, 'job', str(job['id']), 'approval', 'admin@demo.local', f"job/{job['id']}",
                    {'job': {'id': job['id'], 'result': 'v2-mutated'}, 'decision': 'approved'})
            row = db.scalar(select(EvidenceEvent).where(
                EvidenceEvent.biz_type == 'job', EvidenceEvent.biz_id == str(job['id']),
                EvidenceEvent.action == 'approval'))
            # The confirmed entry is never rewritten, so it stays confirmed;
            # the rejected different content is isolated and recorded.
            self.assertEqual(row.status, evidence.CONFIRMED)
            self.assertEqual(row.station_seq, original_seq)
            self.assertEqual(row.station_hash, original_hash)
            self.assertTrue(db.scalar(select(EvidenceConflict).where(
                EvidenceConflict.biz_id == str(job['id']))))
        status, entry = station('GET', f'/entries/{first.event_id}')
        self.assertEqual(status, 200)
        self.assertEqual(entry['payload']['job']['result'], 'v1')

if __name__ == '__main__':
    unittest.main(verbosity=2)
