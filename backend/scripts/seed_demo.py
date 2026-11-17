import time, json
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from src.config.database import Session as DbSession
from migrations.initial import migrate
from src.models import User, Dataset, Job, Report, Delivery, Audit, EvidenceEvent
from src.services.business import metadata, seed_records, analyze
from src.utils.security import hash_password
from src.utils.serialization import checksum


def _ensure_baseline(db: Session, d: Dataset, m: dict) -> None:
    if db.scalar(select(Job).where(Job.name == '基线复核 · ' + m['name'])):
        return
    params = {p['key']: p['default'] for p in m.get('parameters', [])}
    result = analyze(d.records, params)
    json.dumps(result, ensure_ascii=False, allow_nan=False)
    admin = db.scalar(select(User).where(User.role == 'admin'))
    j = Job(name='基线复核 · ' + m['name'], dataset_id=d.id, params=params, status='completed',
            result=result, created_by=admin.id, created_at=time.time() - 3600,
            finished_at=time.time() - 3595, approved=False)
    db.add(j)
    db.flush()
    r = Report(name='每日业务复核简报', job_id=j.id, destination='业务复核组 · 本地收件箱',
               interval_seconds=86400, next_run=time.time(), enabled=True)
    db.add(r)
    db.flush()
    db.commit()
    # Approval and its evidence only happen through the live path, so the seed
    # never fabricates a confirmed entry.  If the station is not reachable yet
    # the approval stays pending and the worker confirms it on a later tick;
    # only then does the report deliver.
    from src.services import jobs as jobs_service
    try:
        jobs_service.approve(db, j, admin)
    except Exception as exc:
        db.rollback()
        print(json.dumps({'seed_approval_pending': str(exc)[:160]}, ensure_ascii=False))


def seed():
    migrate()
    with DbSession() as db:
        for role, name in [('admin', '林知 · 业务负责人'), ('analyst', '陈舟 · 分析员'),
                           ('viewer', '周禾 · 观察员')]:
            email = role + '@demo.local'
            if not db.scalar(select(User).where(User.email == email)):
                db.add(User(email=email, name=name, role=role, password=hash_password('Demo123!')))
        db.flush()
        m = metadata()
        d = db.scalar(select(Dataset).where(Dataset.name == m['dataset_name']))
        if not d:
            rows = seed_records()
            d = Dataset(name=m['dataset_name'], source=m['short'] + '-202611.csv',
                        records=rows, checksum=checksum(rows), created_at=time.time())
            db.add(d)
            db.flush()
        _ensure_baseline(db, d, m)
        db.commit()
        counts = {model.__tablename__: db.scalar(select(func.count()).select_from(model))
                  for model in [User, Dataset, Job, Report, Delivery, Audit, EvidenceEvent]}
        print(json.dumps(counts, ensure_ascii=False))
        return counts


if __name__ == '__main__':
    seed()
