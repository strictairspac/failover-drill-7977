"""订单写路径：create / price / delete，带 epoch fencing 与自动重定向重试。

两阶段提交窗口
--------------
1. 持锁解析属主条目（entry_id、shard、epoch），随后释放锁；
2. 在锁外等待一个可配置的“网络延迟窗口”；
3. 重新持锁并复查同一条目仍是属主且 epoch 未变 —— 若期间发生了切换，
   本次写入被 fence（绝不落到旧分片），客户端按最新路由自动重试，
   重试在新属主上成功。因此同一单号永远只在一边接写，且已确认的
   改动不会因为切换丢失或分裂成两份。
"""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field

from .db import connect, get_setting, offline_shards, store_lock
from .events import log_event
from .router import get_entry, owner_entry

MAX_ATTEMPTS = 3


class SimOfflineError(Exception):
    """目标分片被演练开关置为离线（可恢复，不是数据错误）。"""


class OrderConflictError(Exception):
    """业务语义冲突，例如对已删订单改价、重复建单。"""


@dataclass
class WriteResult:
    ok: bool
    order_id: int
    op: str
    shard: str | None = None
    version: int | None = None
    price: int | None = None
    fenced: bool = False
    retried: bool = False
    attempts: list[dict] = field(default_factory=list)
    error: str | None = None


def _do_write(
    conn: sqlite3.Connection,
    entry: sqlite3.Row,
    order_id: int,
    op: str,
    price: int | None,
) -> tuple[str, int, int | None]:
    shard = entry["shard_id"]
    epoch = entry["epoch"]
    now = int(time.time() * 1000)
    row = conn.execute(
        "SELECT * FROM orders WHERE shard_id=? AND order_id=?",
        (shard, order_id),
    ).fetchone()

    if op == "create":
        if row is not None:
            if row["status"] == "active":
                raise OrderConflictError(f"订单 {order_id} 已存在，不能重复建单")
            # 对墓碑单重新建单：版本必须在旧版本上递增，否则跨分片合并时
            # 会被目标端更高版本的墓碑当成旧副本丢弃（版本必须全局单调）。
            new_version = row["version"] + 1
        else:
            new_version = 1
        conn.execute(
            "INSERT INTO orders(shard_id,order_id,status,version,price,epoch,updated_at)"
            " VALUES(?,?, 'active', ?, ?, ?, ?)"
            " ON CONFLICT(shard_id,order_id) DO UPDATE SET"
            " status='active', version=excluded.version, price=excluded.price,"
            " epoch=excluded.epoch, updated_at=excluded.updated_at",
            (shard, order_id, new_version, price, epoch, now),
        )
    else:
        if row is None:
            raise OrderConflictError(f"订单 {order_id} 不存在（当前属主 {shard}）")
        if row["status"] == "tombstone":
            raise OrderConflictError(f"订单 {order_id} 已删除，不能再{'改价' if op == 'price' else '删单'}")
        new_version = row["version"] + 1
        if op == "price":
            if price is None:
                raise OrderConflictError("改价必须提供新价格")
            conn.execute(
                "UPDATE orders SET version=?, price=?, epoch=?, updated_at=?,"
                " status='active' WHERE shard_id=? AND order_id=?",
                (new_version, price, epoch, now, shard, order_id),
            )
        elif op == "delete":
            conn.execute(
                "UPDATE orders SET status='tombstone', version=?, epoch=?,"
                " updated_at=? WHERE shard_id=? AND order_id=?",
                (new_version, epoch, now, shard, order_id),
            )
        else:
            raise OrderConflictError(f"未知操作 {op}")

    conn.execute(
        "INSERT INTO changelog(shard_id,order_id,op,version,price,epoch,ts)"
        " VALUES(?,?,?,?,?,?,?)",
        (shard, order_id, op, new_version, price, epoch, now),
    )
    return shard, new_version, price


def write_order(
    order_id: int,
    op: str,
    price: int | None = None,
    *,
    auto_retry: bool = True,
    delay_ms: int | None = None,
) -> WriteResult:
    result = WriteResult(ok=False, order_id=order_id, op=op)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        # ---- 阶段 1：解析属主 ------------------------------------------------
        with store_lock:
            conn = connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                entry = owner_entry(conn, order_id)
                if entry is None:
                    conn.execute("ROLLBACK")
                    result.error = f"订单号 {order_id} 不落在任何可写区间"
                    return result
                if entry["shard_id"] in offline_shards(conn):
                    log_event(
                        conn, "sim",
                        f"写入被拒：分片 {entry['shard_id']} 处于离线演练状态",
                        {"order_id": order_id, "op": op},
                    )
                    conn.execute("ROLLBACK")
                    raise SimOfflineError(entry["shard_id"])
                snapshot = (entry["id"], entry["shard_id"], entry["epoch"])
                conn.execute("ROLLBACK")
            finally:
                conn.close()

        # ---- 锁外窗口：切换可能就发生在这里 ----------------------------------
        if delay_ms is not None:
            wait_ms = delay_ms
        else:
            cfg = connect()
            try:
                wait_ms = int(get_setting(cfg, "write_delay_ms", "0"))
            finally:
                cfg.close()
        if wait_ms > 0:
            time.sleep(wait_ms / 1000.0)

        # ---- 阶段 2：复查 fencing token 并提交 -------------------------------
        with store_lock:
            conn = connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                current = get_entry(conn, snapshot[0])
                if current is None or not current["is_owner"] or current["epoch"] != snapshot[2]:
                    log_event(
                        conn, "fenced",
                        f"fencing 拦截 {op}#{order_id}：旧条目 epoch={snapshot[2]}"
                        f"@{snapshot[1]} 已失效，当前属主="
                        f"{current['shard_id'] if current else '无'}",
                        {"order_id": order_id, "op": op,
                         "stale_shard": snapshot[1], "stale_epoch": snapshot[2]},
                    )
                    conn.execute("ROLLBACK")
                    result.attempts.append({
                        "attempt": attempt, "fenced": True,
                        "shard": snapshot[1], "epoch": snapshot[2],
                    })
                    result.fenced = True
                    if auto_retry and attempt < MAX_ATTEMPTS:
                        result.retried = True
                        continue
                    result.error = "写入与切换并发，已被 fencing 拦截且未配置自动重试"
                    return result

                shard, new_version, new_price = _do_write(conn, current, order_id, op, price)
                log_event(
                    conn, "write",
                    f"{op}#{order_id} 落盘 {shard} v{new_version}"
                    + ("（fencing 后重定向重试成功）" if result.fenced else ""),
                    {"order_id": order_id, "op": op, "shard": shard,
                     "version": new_version, "epoch": current["epoch"],
                     "attempt": attempt},
                )
                conn.execute("COMMIT")
                result.attempts.append({
                    "attempt": attempt, "fenced": False,
                    "shard": shard, "epoch": current["epoch"],
                })
                result.ok = True
                result.shard = shard
                result.version = new_version
                result.price = new_price
                return result
            except OrderConflictError:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
    result.error = "重试次数耗尽"
    return result


def list_orders(conn: sqlite3.Connection, shard: str | None = None) -> list[dict]:
    if shard:
        rows = conn.execute(
            "SELECT * FROM orders WHERE shard_id=? ORDER BY order_id", (shard,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM orders ORDER BY order_id, shard_id").fetchall()
    return [dict(r) for r in rows]


def list_changelog(conn: sqlite3.Connection, limit: int = 100) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM changelog ORDER BY seq DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]
