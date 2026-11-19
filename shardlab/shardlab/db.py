"""SQLite 存储层：DDL、连接、全局串行锁、演示设置。

设计说明
--------
* 每个请求操作开一个短连接，所有写事务在 ``store_lock`` 内串行执行
  （BEGIN IMMEDIATE），模拟真实分片存储的单写者语义，让 fencing 与
  一次性切换的推理是确定性的。
* 全部状态都落盘：进程重启后路由、订单、迁移游标、扫描游标、事件流
  都能恢复，不存在只活在内存里的关键状态。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = BASE_DIR / "data" / "shardlab.db"
DB_PATH = Path(os.environ.get("SHARDLAB_DB", str(DEFAULT_DB_PATH)))

# 路由区间使用的整数宇宙边界（订单号为整数）。
MIN_KEY = -10**18
MAX_KEY = 10**18

# 模拟“单分片存储串行提交”的锁。故意在锁外保留网络延迟窗口，
# 以便真实复现切换瞬间的 fencing 场景。
store_lock = threading.RLock()

SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS orders (
        shard_id   TEXT NOT NULL,
        order_id   INTEGER NOT NULL,
        status     TEXT NOT NULL,            -- active | tombstone
        version    INTEGER NOT NULL,
        price      INTEGER,
        epoch      INTEGER NOT NULL,        -- 最近一次写入时路由条目的任期
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (shard_id, order_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS changelog (
        seq      INTEGER PRIMARY KEY AUTOINCREMENT, -- 全局单调的逻辑日志序号
        shard_id TEXT NOT NULL,
        order_id INTEGER NOT NULL,
        op       TEXT NOT NULL,                     -- create | price | delete
        version  INTEGER NOT NULL,
        price    INTEGER,
        epoch    INTEGER NOT NULL,
        ts       INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS router (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        range_lo  INTEGER NOT NULL,
        range_hi  INTEGER NOT NULL,                -- 左闭右开 [lo, hi)
        shard_id  TEXT NOT NULL,
        epoch     INTEGER NOT NULL,
        state     TEXT NOT NULL,                   -- active | migrating | retired
        is_owner  INTEGER NOT NULL,                -- 1=当前接写方
        updated_at INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS migrations (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        range_lo       INTEGER NOT NULL,
        range_hi       INTEGER NOT NULL,
        src_shard      TEXT NOT NULL,
        dst_shard      TEXT NOT NULL,
        state          TEXT NOT NULL,              -- backfilling | catching_up | done
        last_copied_id INTEGER NOT NULL DEFAULT 0, -- 回填游标（源分片订单号）
        applied_seq    INTEGER NOT NULL DEFAULT 0, -- 已消费的 changelog 序号
        backfill_total INTEGER NOT NULL DEFAULT 0,
        backfill_done  INTEGER NOT NULL DEFAULT 0,
        src_epoch      INTEGER NOT NULL,
        dst_epoch      INTEGER NOT NULL,
        switch_count   INTEGER NOT NULL DEFAULT 0, -- 实际切换次数，幂等门禁
        stuck_reason   TEXT,
        created_at     INTEGER NOT NULL,
        updated_at     INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS scan_runs (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        state          TEXT NOT NULL,              -- running | done
        total_seen     INTEGER NOT NULL DEFAULT 0, -- 物理行数（含重复副本）
        distinct_count INTEGER NOT NULL DEFAULT 0, -- 去重后逻辑订单数
        stuck_reason   TEXT,
        created_at     INTEGER NOT NULL,
        finished_at    INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS scan_cursors (
        run_id        INTEGER NOT NULL,
        shard_id      TEXT NOT NULL,
        last_order_id INTEGER NOT NULL DEFAULT 0,
        done          INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (run_id, shard_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS scan_results (
        run_id   INTEGER NOT NULL,
        order_id INTEGER NOT NULL,
        shard_id TEXT NOT NULL,
        version  INTEGER NOT NULL,
        status   TEXT NOT NULL,
        price    INTEGER,
        PRIMARY KEY (run_id, order_id)             -- 去重门禁：同单号只留一条
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        ts      INTEGER NOT NULL,
        kind    TEXT NOT NULL,                      -- write|fenced|migration|scan|sim|warn
        message TEXT NOT NULL,
        payload TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
]


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db() -> None:
    with connect() as conn:
        for ddl in SCHEMA:
            conn.execute(ddl)


# ---- 演示设置（故障注入、离线分片、写入延迟） -----------------------------

DEFAULT_SETTINGS = {
    "offline_shards": "",       # 逗号分隔的离线分片
    "fail_next_migration_batch": "0",
    "fail_next_scan_batch": "0",
    "write_delay_ms": "0",      # 写单在“路由解析后、提交前”的停留时长
}


def get_setting(conn: sqlite3.Connection, key: str, default: str | None = None) -> str:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    if row is None:
        return DEFAULT_SETTINGS.get(key, default or "")
    return row["value"]


def set_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO settings(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def offline_shards(conn: sqlite3.Connection) -> set[str]:
    raw = get_setting(conn, "offline_shards")
    return {s.strip() for s in raw.split(",") if s.strip()}


def reset_database() -> None:
    """清空全部沙盘数据（删文件），下次连接自动重建。"""
    with store_lock:
        for suffix in ("", "-wal", "-shm"):
            p = Path(str(DB_PATH) + suffix)
            if p.exists():
                p.unlink()
        init_db()
