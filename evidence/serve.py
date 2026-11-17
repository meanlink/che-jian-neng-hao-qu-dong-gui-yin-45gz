import hashlib
import os
import sys

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

sys.path.insert(0, os.getenv('EVIDENCE_SRC', '/evidence-src'))
from app import main as station  # noqa: E402

# The station is served by uvicorn; this module re-exports it and adds the
# integrity-enforcing middleware used in the compose deployment.
app = station.app


@app.middleware('http')
async def integrity_gate(request: Request, call_next):
    """Keep the append-only surface honest.

    Writes only go through /entries (and the drill endpoints).  Direct table
    edits are outside the API and are what the tamper drill simulates.
    """
    if request.method in ('PUT', 'PATCH', 'DELETE') and request.url.path.startswith('/entries'):
        return JSONResponse({'detail': '存证条目不可改写，只能追加'}, status_code=405)
    response = await call_next(request)
    response.headers['X-Evidence-Append-Only'] = 'true'
    return response
