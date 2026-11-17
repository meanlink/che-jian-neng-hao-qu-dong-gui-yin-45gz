import os

DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///./local.db')
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')
JWT_SECRET = os.environ.get('JWT_SECRET', 'local-development-change-before-deploy')
ARTIFACT_DIR = os.getenv('ARTIFACT_DIR', '/artifacts')

# Independent evidence station.  It has its own database and volume; the
# business database is never used to reconstruct evidence.
EVIDENCE_URL = os.getenv('EVIDENCE_URL', 'http://evidence:8010')
EVIDENCE_TIMEOUT = float(os.getenv('EVIDENCE_TIMEOUT', '3'))
EVIDENCE_RETRY_CAP = float(os.getenv('EVIDENCE_RETRY_CAP', '60'))
