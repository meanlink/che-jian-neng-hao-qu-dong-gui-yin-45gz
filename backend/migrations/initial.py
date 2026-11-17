from src.config.database import Base, engine
from src import models


def _columns(connection, table):
    from sqlalchemy import inspect
    inspector = inspect(connection)
    if table not in inspector.get_table_names():
        return {}
    return {c['name']: c for c in inspector.get_columns(table)}


def _add_missing(connection):
    """Additive-only upgrade, so an existing demo volume keeps its data."""
    from sqlalchemy import text
    inspector = __import__('sqlalchemy').inspect(connection)
    tables = set(inspector.get_table_names())
    additions = {
        'jobs': [
            ('approved_by', 'VARCHAR(150) NULL'),
            ('approved_at', 'DOUBLE NULL'),
            ('approval_event_id', 'VARCHAR(80) NULL'),
        ],
        'deliveries': [
            ('run_key', "VARCHAR(120) NOT NULL DEFAULT ''"),
            ('status', "VARCHAR(20) NOT NULL DEFAULT 'legacy'"),
            ('evidence_event_id', 'VARCHAR(80) NULL'),
            ('content_sha256', 'VARCHAR(64) NULL'),
        ],
    }
    upgraded = set()
    for table, columns in additions.items():
        existing = _columns(connection, table)
        if not existing:
            continue
        for name, ddl in columns:
            if name not in existing:
                connection.execute(text(f'ALTER TABLE {table} ADD COLUMN {name} {ddl}'))
                upgraded.add((table, name))
    if ('jobs', 'approval_event_id') in upgraded:
        # Approvals made before the station existed have no confirmed entry.
        # They are not counted as approved until re-approved through the
        # evidenced path; the historical rows themselves are left intact.
        connection.execute(text(
            'UPDATE jobs SET approved = 0 WHERE approval_event_id IS NULL'))
    if ('deliveries', 'run_key') in upgraded:
        # Legacy rows predate the run key.  Give each a unique value before the
        # unique index is enforced, so old archives keep working.
        rows = connection.execute(text('SELECT id FROM deliveries WHERE run_key = \'\'')).fetchall()
        for (row_id,) in rows:
            connection.execute(text('UPDATE deliveries SET run_key = :key WHERE id = :id'),
                               {'key': f'legacy-{row_id}', 'id': row_id})
    if 'deliveries' in tables:
        existing = _columns(connection, 'deliveries')
        indexes = {index['name'] for index in inspector.get_indexes('deliveries')}
        if 'ux_deliveries_run_key' not in indexes:
            try:
                connection.execute(text(
                    'CREATE UNIQUE INDEX ux_deliveries_run_key ON deliveries (run_key)'))
            except Exception:
                pass  # Pre-existing duplicates; leave data untouched.


def migrate():
    Base.metadata.create_all(engine)
    with engine.begin() as connection:
        _add_missing(connection)


if __name__ == '__main__':
    migrate()
