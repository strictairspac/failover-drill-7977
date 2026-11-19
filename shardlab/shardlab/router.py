"""路由表：区间 -> 属主分片 + 任期(epoch)。

不变量
------
1. 任一订单号在任一时刻只命中至多一条 ``is_owner=1`` 的区间条目，
   因此同一单号只会被一边接写。
2. 切换归属 = 在一个串行写事务里把旧条目 ``is_owner`` 置 0、插入新
   属主条目并抬高 epoch，原子完成；外部观察不到“两边都是属主”的状态。
3. 写路径同时校验「条目 id + epoch + is_owner」，切换后持有旧条目的
   在途写入必然被 fence，永远无法在旧分片落盘。
"""
from __future__ import annotations

import sqlite3
import time

from .db import MIN_KEY


def list_entries(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM router ORDER BY range_lo, id"
    ).fetchall()
    return [dict(r) for r in rows]


def owner_entry(conn: sqlite3.Connection, order_id: int) -> sqlite3.Row | None:
    """返回当前接写该单号的路由条目；没有区间接收时返回 None。"""
    return conn.execute(
        "SELECT * FROM router WHERE is_owner=1 AND range_lo <= ? AND ? < range_hi "
        "ORDER BY epoch DESC, id DESC LIMIT 1",
        (order_id, order_id),
    ).fetchone()


def get_entry(conn: sqlite3.Connection, entry_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM router WHERE id=?", (entry_id,)).fetchone()


def find_migration_entry(
    conn: sqlite3.Connection, range_lo: int, range_hi: int
) -> sqlite3.Row | None:
    """找到覆盖待迁移区间的那条 active/migrating 属主条目。"""
    return conn.execute(
        "SELECT * FROM router WHERE is_owner=1 AND range_lo <= ? AND range_hi >= ? "
        "ORDER BY id LIMIT 1",
        (range_lo, range_hi),
    ).fetchone()


def mark_migrating(conn: sqlite3.Connection, entry_id: int) -> None:
    conn.execute(
        "UPDATE router SET state='migrating', updated_at=? WHERE id=?",
        (int(time.time() * 1000), entry_id),
    )


def all_shard_ids(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT DISTINCT shard_id FROM router ORDER BY 1").fetchall()
    return [r["shard_id"] for r in rows]


def reset_router(conn: sqlite3.Connection) -> None:
    """沙盘重置：只保留 shard-a 拥有全宇宙的初始路由。"""
    conn.execute("DELETE FROM router")
    conn.execute(
        "INSERT INTO router(range_lo,range_hi,shard_id,epoch,state,is_owner,updated_at)"
        " VALUES(?,?,?,1,'active',1,?)",
        (MIN_KEY, 10**18, "shard-a", int(time.time() * 1000)),
    )
