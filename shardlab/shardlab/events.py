"""事件流：让 fencing、切换、中断、迟到副本等过程在页面上可复核。"""
from __future__ import annotations

import json
import sqlite3
import time


def log_event(
    conn: sqlite3.Connection,
    kind: str,
    message: str,
    payload: dict | None = None,
) -> None:
    conn.execute(
        "INSERT INTO events(ts,kind,message,payload) VALUES(?,?,?,?)",
        (int(time.time() * 1000), kind, message,
         json.dumps(payload, ensure_ascii=False) if payload else None),
    )


def recent_events(conn: sqlite3.Connection, limit: int = 60) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]
