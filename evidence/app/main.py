"""Independent evidence station.

A deliberately small, self-contained service that owns an append-only,
hash-chained ledger.  It is deployed separately from the business database so
that approvals and brief distributions can be proven to have happened, in
order, without trusting the business rows that they refer to.

Storage model
-------------
* SQLite database (its own volume) is the queryable copy of the ledger.
* An append-only JSONL mirror is written on every append.  The mirror is the
  recovery source: repair restores *missing* rows and moves tampered rows to a
  quarantine table; it never edits a valid ledger entry.
* Every entry stores the operator, business object, content fingerprint and the
  hash of the previous entry.  ``entry_hash`` covers all of those fields.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

DATA_DIR = Path(os.getenv("EVIDENCE_DATA_DIR", "/evidence-data"))
DB_PATH = DATA_DIR / "evidence.db"
MIRROR_PATH = DATA_DIR / "ledger.jsonl"
DRILLS_ENABLED = os.getenv("EVIDENCE_DRILLS", "true").lower() == "true"
GENESIS = "0" * 64

DATA_DIR.mkdir(parents=True, exist_ok=True)

_lock = threading.RLock()
_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
_conn.row_factory = sqlite3.Row
_conn.execute("PRAGMA journal_mode=WAL")
_conn.execute("PRAGMA synchronous=FULL")


def _canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _hash_fields(fields: dict) -> str:
    return _sha(_canonical(fields))


def _init() -> None:
    with _lock:
        _conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS entries (
                seq INTEGER PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                biz_type TEXT NOT NULL,
                biz_id TEXT NOT NULL,
                action TEXT NOT NULL,
                actor TEXT NOT NULL,
                object_ref TEXT NOT NULL,
                content_sha256 TEXT NOT NULL,
                occurred_at REAL NOT NULL,
                payload TEXT NOT NULL,
                depends_on TEXT NOT NULL DEFAULT '[]',
                prev_hash TEXT NOT NULL,
                entry_hash TEXT NOT NULL,
                received_at REAL NOT NULL,
                restored_at REAL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS ux_entries_biz
                ON entries (biz_type, biz_id, action);
            CREATE TABLE IF NOT EXISTS conflicts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT, biz_key TEXT, reason TEXT,
                existing_seq INTEGER, incoming_payload TEXT,
                incoming_hash TEXT, at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS quarantine (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seq INTEGER, row_json TEXT, reason TEXT, at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                frozen INTEGER NOT NULL DEFAULT 0,
                frozen_at REAL, freeze_reason TEXT,
                last_verify_at REAL, last_verify TEXT,
                revision INTEGER NOT NULL DEFAULT 0
            );
            INSERT OR IGNORE INTO state (id) VALUES (1);
            """
        )
        _conn.commit()
        # A pre-existing database without a mirror can be bootstrapped once.
        if not MIRROR_PATH.exists():
            rows = [dict(row) for row in _conn.execute("SELECT * FROM entries ORDER BY seq")]
            with MIRROR_PATH.open("w", encoding="utf-8") as handle:
                for row in rows:
                    handle.write(_mirror_row(row) + "\n")
                handle.flush()
                os.fsync(handle.fileno())


def _mirror_row(row: dict) -> str:
    return _canonical({key: row.get(key) for key in (
        "seq", "event_id", "biz_type", "biz_id", "action", "actor", "object_ref",
        "content_sha256", "occurred_at", "payload", "depends_on", "prev_hash",
        "entry_hash", "received_at")})


_init()


def _append_mirror(row: dict) -> int:
    size = MIRROR_PATH.stat().st_size if MIRROR_PATH.exists() else 0
    with MIRROR_PATH.open("a", encoding="utf-8") as handle:
        handle.write(_mirror_row(row) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return size


def _truncate_mirror(size: int) -> None:
    """Undo only the line this failed append wrote; never touches earlier ones."""
    with MIRROR_PATH.open("r+b") as handle:
        handle.truncate(size)
        handle.flush()
        os.fsync(handle.fileno())


def _mirror_entries() -> list[dict]:
    if not MIRROR_PATH.exists():
        return []
    items = []
    with MIRROR_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError:
                items.append({"__corrupt__": line})
    return items


def _entry_fields(row: dict, seq: int, prev_hash: str) -> dict:
    return {
        "seq": seq,
        "event_id": row["event_id"],
        "biz_type": row["biz_type"],
        "biz_id": row["biz_id"],
        "action": row["action"],
        "actor": row["actor"],
        "object_ref": row["object_ref"],
        "content_sha256": row["content_sha256"],
        "occurred_at": row["occurred_at"],
        "payload": row["payload"],
        "depends_on": row["depends_on"],
        "prev_hash": prev_hash,
    }


def _state() -> dict:
    row = dict(_conn.execute("SELECT * FROM state WHERE id = 1").fetchone())
    if row.get("last_verify"):
        try:
            row["last_verify"] = json.loads(row["last_verify"])
        except json.JSONDecodeError:
            row["last_verify"] = {"ok": False, "anomalies": [{"type": "CORRUPT_STATE"}]}
    return row


def _set_state(**values) -> None:
    keys = ", ".join(f"{key} = ?" for key in values)
    _conn.execute(f"UPDATE state SET {keys} WHERE id = 1", tuple(values.values()))


def _conflict(event_id: str, biz_key: str, reason: str, existing_seq, payload, content: str) -> None:
    _conn.execute(
        "INSERT INTO conflicts (event_id, biz_key, reason, existing_seq, incoming_payload, incoming_hash, at)"
        " VALUES (?,?,?,?,?,?,?)",
        (event_id, biz_key, reason, existing_seq, _canonical(payload), content, time.time()),
    )
    _conn.commit()


def _row_public(row: dict) -> dict:
    result = dict(row)
    try:
        result["depends_on"] = json.loads(result.get("depends_on") or "[]")
    except json.JSONDecodeError:
        result["depends_on"] = []
    result["payload"] = json.loads(result.get("payload") or "{}") if isinstance(result.get("payload"), str) else result.get("payload")
    return result


class EntryIn(BaseModel):
    event_id: str = Field(min_length=8, max_length=120)
    biz_type: str = Field(min_length=1, max_length=60)
    biz_id: str = Field(min_length=1, max_length=80)
    action: str = Field(min_length=1, max_length=80)
    actor: str = Field(min_length=1, max_length=150)
    object_ref: str = Field(default="", max_length=300)
    content_sha256: str = Field(min_length=64, max_length=64)
    occurred_at: float = Field(default_factory=time.time)
    payload: dict = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)


class TamperIn(BaseModel):
    mode: str = Field(default="delete_middle", pattern="^(delete_middle|modify_middle|delete_tail)$")


class ConflictIn(BaseModel):
    event_id: str | None = None


def _append(entry: EntryIn) -> tuple[dict, bool]:
    """Append an entry.  Returns (public row, replayed).  Caller holds the lock."""
    payload_text = _canonical(entry.payload)
    biz_key = f"{entry.biz_type}:{entry.biz_id}:{entry.action}"
    existing = _conn.execute("SELECT * FROM entries WHERE event_id = ?", (entry.event_id,)).fetchone()
    if existing:
        existing = dict(existing)
        if (existing["payload"] == payload_text
                and existing["content_sha256"] == entry.content_sha256
                and existing["biz_type"] == entry.biz_type
                and existing["biz_id"] == entry.biz_id
                and existing["action"] == entry.action
                and existing["actor"] == entry.actor
                and existing["object_ref"] == entry.object_ref):
            return _row_public(existing), True
        _conflict(entry.event_id, biz_key, "同号异内容：event_id 已存在但内容不同", existing["seq"], entry.payload, entry.content_sha256)
        raise HTTPException(409, "同号异内容，已隔离：该存证编号已存在且内容不同")

    by_biz = _conn.execute(
        "SELECT * FROM entries WHERE biz_type = ? AND biz_id = ? AND action = ?",
        (entry.biz_type, entry.biz_id, entry.action),
    ).fetchone()
    if by_biz:
        by_biz = dict(by_biz)
        if by_biz["payload"] == payload_text and by_biz["content_sha256"] == entry.content_sha256:
            return _row_public(by_biz), True
        _conflict(entry.event_id, biz_key, "业务键已存在但内容不同", by_biz["seq"], entry.payload, entry.content_sha256)
        raise HTTPException(409, "业务编号已存在不同内容的存证，已隔离")

    state = _state()
    if state["frozen"]:
        raise HTTPException(423, "存证核验异常已冻结，补齐并复验通过后才可新增存证")

    dependencies = sorted(set(entry.depends_on))
    for dependency in dependencies:
        dep = _conn.execute("SELECT seq FROM entries WHERE event_id = ?", (dependency,)).fetchone()
        if not dep:
            raise HTTPException(409, f"前置存证不存在：{dependency}")

    last = _conn.execute("SELECT seq, entry_hash FROM entries ORDER BY seq DESC LIMIT 1").fetchone()
    seq = (last["seq"] if last else 0) + 1
    prev_hash = last["entry_hash"] if last else GENESIS
    row = {
        "event_id": entry.event_id, "biz_type": entry.biz_type, "biz_id": entry.biz_id,
        "action": entry.action, "actor": entry.actor, "object_ref": entry.object_ref,
        "content_sha256": entry.content_sha256, "occurred_at": float(entry.occurred_at),
        "payload": payload_text, "depends_on": _canonical(dependencies),
    }
    entry_hash = _hash_fields(_entry_fields(row, seq, prev_hash))
    received_at = time.time()
    row.update(seq=seq, prev_hash=prev_hash, entry_hash=entry_hash, received_at=received_at)
    # Mirror first, then the queryable copy.  A crash between the two leaves a
    # mirror line the DB lacks (recoverable by repair); the reverse order would
    # leave a DB row the mirror never saw, which is indistinguishable from
    # tampering.  The append-only mirror is the recovery source of record.
    offset = _append_mirror(row)
    try:
        _conn.execute(
            "INSERT INTO entries (seq, event_id, biz_type, biz_id, action, actor, object_ref,"
            " content_sha256, occurred_at, payload, depends_on, prev_hash, entry_hash, received_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (seq, row["event_id"], row["biz_type"], row["biz_id"], row["action"], row["actor"],
             row["object_ref"], row["content_sha256"], row["occurred_at"], row["payload"],
             row["depends_on"], prev_hash, entry_hash, received_at),
        )
        _set_state(revision=state["revision"] + 1)
        _conn.commit()
    except sqlite3.IntegrityError:
        _conn.rollback()
        _truncate_mirror(offset)
        raise HTTPException(409, "存证编号并发冲突，请以原回执重试") from None
    except Exception:
        _conn.rollback()
        _truncate_mirror(offset)
        raise
    return _row_public(row), False


def _verify_locked() -> dict:
    db_rows = [dict(row) for row in _conn.execute("SELECT * FROM entries ORDER BY seq")]
    mirror_rows = _mirror_entries()
    anomalies: list[dict] = []
    seen: set[tuple] = set()

    def add(kind: str, seq, detail: str) -> None:
        key = (kind, seq)
        if key not in seen:
            seen.add(key)
            anomalies.append({"type": kind, "seq": seq, "detail": detail})

    db_map = {row["seq"]: row for row in db_rows}
    mirror_map: dict = {}
    for item in mirror_rows:
        if "__corrupt__" in item:
            add("MIRROR_CORRUPT", None, "恢复镜像存在无法解析的行")
            continue
        mirror_map[item.get("seq")] = item

    for seq, item in mirror_map.items():
        if seq not in db_map:
            add("MISSING", seq, "恢复镜像中存在但存证库缺失")
        elif _mirror_row(db_map[seq]) != _mirror_row(item):
            add("MODIFIED", seq, "存证内容与恢复镜像不一致")
    for seq in db_map:
        if seq not in mirror_map:
            add("EXTRA", seq, "存证库中存在但恢复镜像没有的条目")

    expected_seq = 1
    expected_hash = GENESIS
    for row in db_rows:
        if row["seq"] != expected_seq:
            add("CHAIN_GAP", row["seq"], f"序号不连续，期望 {expected_seq}")
        if row["prev_hash"] != expected_hash:
            add("CHAIN_BREAK", row["seq"], "前序哈希不衔接")
        try:
            fields = _entry_fields(
                {
                    "event_id": row["event_id"], "biz_type": row["biz_type"], "biz_id": row["biz_id"],
                    "action": row["action"], "actor": row["actor"], "object_ref": row["object_ref"],
                    "content_sha256": row["content_sha256"], "occurred_at": row["occurred_at"],
                    "payload": row["payload"], "depends_on": row["depends_on"],
                },
                row["seq"], row["prev_hash"],
            )
            if _hash_fields(fields) != row["entry_hash"]:
                add("MODIFIED", row["seq"], "条目哈希与内容不匹配")
        except Exception:
            add("MODIFIED", row["seq"], "条目内容无法重算哈希")
        expected_seq = row["seq"] + 1
        expected_hash = row["entry_hash"]

    mirror_expected_seq = 1
    mirror_expected_hash = GENESIS
    for seq in sorted(mirror_map):
        item = mirror_map[seq]
        if item.get("prev_hash") != mirror_expected_hash:
            add("MIRROR_CORRUPT", seq, "恢复镜像哈希链断裂")
        mirror_expected_seq = seq + 1
        mirror_expected_hash = item.get("entry_hash")

    state = _state()
    now = time.time()
    ok = not anomalies
    if ok:
        _set_state(frozen=0, frozen_at=None, freeze_reason=None,
                   last_verify_at=now, last_verify=_canonical({"ok": True, "anomalies": [], "checked": len(db_rows)}))
    else:
        reason = "；".join(f"{item['type']}#{item['seq']}" for item in anomalies[:6])
        _set_state(frozen=1, frozen_at=state.get("frozen_at") or now,
                   freeze_reason="存证核验异常：" + reason,
                   last_verify_at=now,
                   last_verify=_canonical({"ok": False, "anomalies": anomalies, "checked": len(db_rows)}))
    _conn.commit()
    head = db_rows[-1] if db_rows else None
    return {
        "ok": ok, "frozen": not ok, "checked": len(db_rows),
        "head_seq": head["seq"] if head else 0,
        "head_hash": head["entry_hash"] if head else GENESIS,
        "anomalies": anomalies,
    }


app = FastAPI(title="独立存证站", version="1.0")


@app.get("/health")
def health() -> dict:
    with _lock:
        _conn.execute("SELECT 1").fetchone()
    return {"status": "healthy"}


@app.get("/status")
def status() -> dict:
    with _lock:
        state = _state()
        count = _conn.execute("SELECT COUNT(*) AS n FROM entries").fetchone()["n"]
        head = _conn.execute("SELECT seq, entry_hash FROM entries ORDER BY seq DESC LIMIT 1").fetchone()
        conflicts = _conn.execute("SELECT COUNT(*) AS n FROM conflicts").fetchone()["n"]
        return {
            "service": "evidence-station",
            "frozen": bool(state["frozen"]),
            "frozen_at": state["frozen_at"],
            "freeze_reason": state["freeze_reason"],
            "entries": count,
            "head_seq": head["seq"] if head else 0,
            "head_hash": head["entry_hash"] if head else GENESIS,
            "revision": state["revision"],
            "last_verify_at": state["last_verify_at"],
            "last_verify": state["last_verify"],
            "conflicts": conflicts,
            "mirror": str(MIRROR_PATH),
        }


@app.post("/entries", status_code=201)
def append_entry(entry: EntryIn):
    with _lock:
        row, replayed = _append(entry)
        return JSONResponse(status_code=200 if replayed else 201,
                            content={**row, "replayed": replayed})


@app.get("/entries")
def list_entries(limit: int = Query(default=200, ge=1, le=2000)) -> list[dict]:
    with _lock:
        rows = _conn.execute("SELECT * FROM entries ORDER BY seq DESC LIMIT ?", (limit,)).fetchall()
        return [_row_public(dict(row)) for row in rows]


@app.get("/entries/{event_id}")
def get_entry(event_id: str) -> dict:
    with _lock:
        row = _conn.execute("SELECT * FROM entries WHERE event_id = ?", (event_id,)).fetchone()
        if not row:
            raise HTTPException(404, "存证不存在")
        return _row_public(dict(row))


@app.get("/conflicts")
def list_conflicts(limit: int = Query(default=50, ge=1, le=500)) -> list[dict]:
    with _lock:
        rows = _conn.execute("SELECT * FROM conflicts ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            try:
                item["incoming_payload"] = json.loads(item["incoming_payload"])
            except (TypeError, json.JSONDecodeError):
                pass
            result.append(item)
        return result


@app.post("/verify")
def verify() -> dict:
    with _lock:
        return _verify_locked()


@app.get("/verify")
def verify_get() -> dict:
    with _lock:
        return _verify_locked()


@app.post("/preflight")
def preflight() -> dict:
    with _lock:
        result = _verify_locked()
        state = _state()
        return {**result, "status": {
            "frozen": bool(state["frozen"]),
            "freeze_reason": state["freeze_reason"],
            "last_verify_at": state["last_verify_at"],
        }}


@app.post("/repair")
def repair() -> dict:
    """Restore from the append-only mirror without editing valid content."""
    with _lock:
        mirror_rows = _mirror_entries()
        if any("__corrupt__" in item for item in mirror_rows):
            raise HTTPException(409, "恢复镜像损坏，无法自动补齐；请从独立备份恢复镜像")
        mirror_map = {item["seq"]: item for item in mirror_rows}
        db_rows = {row["seq"]: dict(row) for row in _conn.execute("SELECT * FROM entries ORDER BY seq")}
        quarantined = 0
        restored = 0
        for seq, row in db_rows.items():
            reference = mirror_map.get(seq)
            if reference is None:
                _conn.execute("INSERT INTO quarantine (seq, row_json, reason, at) VALUES (?,?,?,?)",
                              (seq, _canonical(row), "恢复镜像中不存在，移入隔离区", time.time()))
                _conn.execute("DELETE FROM entries WHERE seq = ?", (seq,))
                quarantined += 1
            elif _mirror_row(row) != _mirror_row(reference):
                _conn.execute("INSERT INTO quarantine (seq, row_json, reason, at) VALUES (?,?,?,?)",
                              (seq, _canonical(row), "内容与恢复镜像不一致，原样隔离", time.time()))
                _conn.execute("DELETE FROM entries WHERE seq = ?", (seq,))
                _insert_mirror_row(reference)
                quarantined += 1
                restored += 1
        for seq, reference in mirror_map.items():
            if seq not in db_rows:
                _insert_mirror_row(reference)
                restored += 1
        _set_state(revision=_state()["revision"] + 1)
        _conn.commit()
        result = _verify_locked()
        return {"restored": restored, "quarantined": quarantined, "verify": result,
                "frozen": bool(_state()["frozen"]), "mirror_is_source": True}


def _insert_mirror_row(reference: dict) -> None:
    _conn.execute(
        "INSERT OR REPLACE INTO entries (seq, event_id, biz_type, biz_id, action, actor, object_ref,"
        " content_sha256, occurred_at, payload, depends_on, prev_hash, entry_hash, received_at, restored_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (reference["seq"], reference["event_id"], reference["biz_type"], reference["biz_id"],
         reference["action"], reference["actor"], reference.get("object_ref", ""),
         reference["content_sha256"], reference["occurred_at"], reference["payload"],
         reference.get("depends_on", "[]"), reference["prev_hash"], reference["entry_hash"],
         reference.get("received_at", time.time()), time.time()),
    )


@app.get("/quarantine")
def quarantine() -> list[dict]:
    with _lock:
        rows = _conn.execute("SELECT * FROM quarantine ORDER BY id DESC LIMIT 100").fetchall()
        result = []
        for row in rows:
            item = dict(row)
            try:
                item["row_json"] = json.loads(item["row_json"])
            except (TypeError, json.JSONDecodeError):
                pass
            result.append(item)
        return result


def _require_drills() -> None:
    if not DRILLS_ENABLED:
        raise HTTPException(403, "演练接口已关闭")


@app.post("/drills/tamper")
def drill_tamper(data: TamperIn) -> dict:
    """Simulate out-of-band tampering.  The append-only mirror is untouched."""
    _require_drills()
    with _lock:
        rows = [dict(row) for row in _conn.execute("SELECT * FROM entries ORDER BY seq")]
        if not rows:
            raise HTTPException(409, "存证链为空，先完成一次批准或分发")
        if data.mode == "delete_tail":
            target = rows[-1]
            _conn.execute("DELETE FROM entries WHERE seq = ?", (target["seq"],))
        else:
            target = rows[len(rows) // 2]
            if data.mode == "delete_middle":
                _conn.execute("DELETE FROM entries WHERE seq = ?", (target["seq"],))
            else:
                _conn.execute(
                    "UPDATE entries SET actor = ?, object_ref = ? WHERE seq = ?",
                    ("外部篡改者", (target.get("object_ref") or "") + "（被改写）", target["seq"]),
                )
        _conn.commit()
        return {"mode": data.mode, "target_seq": target["seq"],
                "message": "已模拟存储层篡改；请执行核验以触发冻结"}


@app.post("/drills/lost-receipt")
def drill_lost_receipt(entry: EntryIn):
    """Append successfully, then drop the response as a lost-receipt drill."""
    _require_drills()
    with _lock:
        row, replayed = _append(entry)
    return JSONResponse(status_code=504, content={
        "detail": "演练：存证已写入但回执在网络中丢失，请使用同一存证编号重试",
        "simulated": "lost-receipt", "seq": row["seq"], "replayed": replayed,
        "event_id": row["event_id"],
    })


@app.post("/drills/conflict")
def drill_conflict(data: ConflictIn):
    """Attempt to append different content under an existing event id."""
    _require_drills()
    with _lock:
        row = _conn.execute("SELECT * FROM entries ORDER BY seq LIMIT 1").fetchone()
        if not row:
            raise HTTPException(409, "存证链为空，无法演练同号异内容")
        row = dict(row)
        try:
            _append(EntryIn(
                event_id=data.event_id or row["event_id"],
                biz_type=row["biz_type"], biz_id=row["biz_id"], action=row["action"],
                actor="同号异内容演练", object_ref=row["object_ref"],
                content_sha256="f" * 64,
                occurred_at=time.time(),
                payload={"drill": "same-id-different-content"},
                depends_on=[],
            ))
        except HTTPException as exc:
            return {"isolated": True, "status": exc.status_code, "detail": exc.detail,
                    "existing_seq": row["seq"]}
        raise HTTPException(409, "演练未产生冲突，请使用已存在的存证编号")
