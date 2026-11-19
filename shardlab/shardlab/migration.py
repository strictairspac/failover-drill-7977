"""在线拆分迁移状态机。

阶段
----
backfilling（回填存量）→ catching_up（追增量日志）→ done（已切换）。

* 搬运期间源分片照常接单/改价/删单：所有写入仍在源分片产生 changelog；
  回填按订单号游标推进，增量按 changelog 序号推进，两个游标都落盘，
  任何一步中断后从游标继续，不重搬整段。
* 目标追齐后才允许 cutover：切换在单个串行事务内完成
  「旧条目降为 retired → 插入两条新属主条目（目标 epoch+1）」，
  外部永远观察不到两边同属主；switch_count 保证同次重试不重复切换。
* 消息合并按 (order_id, version) 幂等：晚到的旧副本 version 更低时
  直接丢弃；删除墓碑与存活记录同表同版本号比较，旧副本不能让删单复活。
"""
from __future__ import annotations

import sqlite3
import time

from .db import (
    MIN_KEY,
    MAX_KEY,
    connect,
    get_setting,
    offline_shards,
    set_setting,
    store_lock,
)
from .events import log_event
from .router import find_migration_entry, get_entry, mark_migrating

BATCH_SIZE = 20


class MigrationError(Exception):
    pass


class SimRecoverableError(Exception):
    """故障注入：搬运在批次中途中断，状态已保留，可直接继续。"""


# --------------------------------------------------------------------------
# 消息应用：回填行 / 增量日志 / 迟到旧副本 走同一个幂等合并函数
# --------------------------------------------------------------------------

def apply_dest_message(
    conn: sqlite3.Connection,
    dst: str,
    order_id: int,
    version: int,
    status: str,
    price: int | None,
    epoch: int,
    *,
    origin: str,
    expect_epoch: int | None = None,
    fence_epoch: bool = False,
) -> dict:
    """把一条副本消息合并进目标分片。

    返回 ``{"action": ...}``：applied / skipped_stale / skipped_same /
    fenced_epoch。
    """
    now = int(time.time() * 1000)

    # 场景5：切换后从旧分片飘来的迟到副本，携带旧 epoch，直接 fence。
    if fence_epoch and expect_epoch is not None and epoch < expect_epoch:
        log_event(
            conn, "fenced",
            f"迟到旧副本被 epoch 拦截：订单 {order_id} v{version} 来自"
            f"旧任期 {epoch}，新任期门槛 {expect_epoch}",
            {"order_id": order_id, "version": version,
             "stale_epoch": epoch, "expect_epoch": expect_epoch},
        )
        return {"action": "fenced_epoch"}

    row = conn.execute(
        "SELECT * FROM orders WHERE shard_id=? AND order_id=?",
        (dst, order_id),
    ).fetchone()

    if row is not None:
        if version < row["version"]:
            log_event(
                conn, "fenced",
                f"旧副本丢弃：订单 {order_id} 目标已有 v{row['version']}，"
                f"迟到消息仅 v{version}（{origin}）",
                {"order_id": order_id, "incoming_version": version,
                 "current_version": row["version"]},
            )
            return {"action": "skipped_stale"}
        if version == row["version"]:
            # 同版本重放：幂等无操作。特别地，墓碑不会被同版本旧消息覆盖。
            return {"action": "skipped_same"}

    conn.execute(
        "INSERT INTO orders(shard_id,order_id,status,version,price,epoch,updated_at)"
        " VALUES(?,?,?,?,?,?,?)"
        " ON CONFLICT(shard_id,order_id) DO UPDATE SET"
        " status=excluded.status, version=excluded.version,"
        " price=excluded.price, epoch=excluded.epoch, updated_at=excluded.updated_at",
        (dst, order_id, status, version, price, epoch, now),
    )
    return {"action": "applied"}


def current_migration(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM migrations ORDER BY id DESC LIMIT 1"
    ).fetchone()


# --------------------------------------------------------------------------
# 1) 发起拆分
# --------------------------------------------------------------------------

def start_split(range_lo: int, range_hi: int, dst_shard: str = "shard-b") -> dict:
    if range_lo >= range_hi:
        raise MigrationError("区间必须满足 lo < hi（左闭右开）")
    with store_lock:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = current_migration(conn)
            if existing is not None and existing["state"] != "done":
                raise MigrationError("已有进行中的拆分，请先完成或重置沙盘")
            entry = find_migration_entry(conn, range_lo, range_hi)
            if entry is None:
                raise MigrationError("没有属主条目完整覆盖该区间")
            if entry["range_lo"] > range_lo or entry["range_hi"] < range_hi:
                raise MigrationError(
                    f"区间必须包含在单一属主条目内（当前可迁移区间 "
                    f"[{entry['range_lo']},{entry['range_hi']}) @ {entry['shard_id']}）"
                )
            total = conn.execute(
                "SELECT COUNT(*) c FROM orders WHERE shard_id=? AND ? <= order_id AND order_id < ?",
                (entry["shard_id"], range_lo, range_hi),
            ).fetchone()["c"]
            now = int(time.time() * 1000)
            src_epoch = entry["epoch"]
            dst_epoch = src_epoch + 1
            cur = conn.execute(
                "INSERT INTO migrations(range_lo,range_hi,src_shard,dst_shard,state,"
                "last_copied_id,applied_seq,backfill_total,backfill_done,src_epoch,"
                "dst_epoch,switch_count,created_at,updated_at)"
                " VALUES(?,?,?,?,'backfilling',0,0,?,0,?,?,0,?,?)",
                (range_lo, range_hi, entry["shard_id"], dst_shard, total,
                 src_epoch, dst_epoch, now, now),
            )
            mark_migrating(conn, entry["id"])
            log_event(
                conn, "migration",
                f"拆分启动：[{range_lo},{range_hi}) 从 {entry['shard_id']}"
                f"(epoch {src_epoch}) 搬到 {dst_shard}(epoch {dst_epoch})，"
                f"存量 {total} 单",
                {"migration_id": cur.lastrowid, "range": [range_lo, range_hi]},
            )
            conn.execute("COMMIT")
            return migration_status(conn)
        finally:
            conn.close()


def _maybe_fail(conn: sqlite3.Connection, setting_key: str, phase: str, mig_id: int) -> None:
    if get_setting(conn, setting_key, "0") == "1":
        set_setting(conn, setting_key, "0")
        conn.execute("COMMIT")
        log_event(
            conn, "sim",
            f"故障注入：{phase} 批次中断（migration {mig_id}），游标已落盘，可继续恢复",
            {"migration_id": mig_id, "phase": phase},
        )
        raise SimRecoverableError(phase)


# --------------------------------------------------------------------------
# 2) 回填存量（按订单号游标，可中断续跑）
# --------------------------------------------------------------------------

def backfill_batch() -> dict:
    with store_lock:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            mig = current_migration(conn)
            if mig is None:
                raise MigrationError("还没有发起拆分")
            if mig["state"] == "done":
                return migration_status(conn)
            if mig["state"] != "backfilling":
                raise MigrationError(f"当前阶段为 {mig['state']}，回填已结束")
            src, dst = mig["src_shard"], mig["dst_shard"]
            if src in offline_shards(conn):
                raise MigrationError(f"源分片 {src} 离线，回填无法读取，恢复后即可继续")

            rows = conn.execute(
                "SELECT * FROM orders WHERE shard_id=? AND ? <= order_id AND order_id < ?"
                " AND order_id > ? ORDER BY order_id LIMIT ?",
                (src, mig["range_lo"], mig["range_hi"],
                 mig["last_copied_id"], BATCH_SIZE),
            ).fetchall()
            applied = 0
            last_id = mig["last_copied_id"]
            for row in rows:
                apply_dest_message(
                    conn, dst, row["order_id"], row["version"], row["status"],
                    row["price"], row["epoch"], origin="backfill",
                )
                last_id = row["order_id"]
                applied += 1
            done_count = mig["backfill_done"] + applied
            remaining = conn.execute(
                "SELECT COUNT(*) c FROM orders WHERE shard_id=? AND ? <= order_id"
                " AND order_id < ? AND order_id > ?",
                (src, mig["range_lo"], mig["range_hi"], last_id),
            ).fetchone()["c"]
            conn.execute(
                "UPDATE migrations SET last_copied_id=?, backfill_done=?, updated_at=? WHERE id=?",
                (last_id, done_count, int(time.time() * 1000), mig["id"]),
            )
            _maybe_fail(conn, "fail_next_migration_batch", "backfill", mig["id"])
            if remaining == 0:
                conn.execute(
                    "UPDATE migrations SET state='catching_up', updated_at=? WHERE id=?",
                    (int(time.time() * 1000), mig["id"]),
                )
                log_event(
                    conn, "migration",
                    f"回填完成：{done_count} 单已搬到 {dst}，转入增量追赶阶段",
                    {"migration_id": mig["id"]},
                )
            conn.execute("COMMIT")
            return migration_status(connect())
        finally:
            conn.close()


# --------------------------------------------------------------------------
# 3) 增量追赶（按 changelog 序号，幂等合并）
# --------------------------------------------------------------------------

def catchup_batch() -> dict:
    with store_lock:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            mig = current_migration(conn)
            if mig is None:
                raise MigrationError("还没有发起拆分")
            if mig["state"] == "backfilling":
                raise MigrationError("存量回填尚未完成")
            if mig["state"] == "done":
                return migration_status(conn)
            src, dst = mig["src_shard"], mig["dst_shard"]
            if src in offline_shards(conn):
                raise MigrationError(f"源分片 {src} 离线，无法读取增量日志")

            logs = conn.execute(
                "SELECT * FROM changelog WHERE shard_id=? AND ? <= order_id AND order_id < ?"
                " AND seq > ? ORDER BY seq LIMIT ?",
                (src, mig["range_lo"], mig["range_hi"],
                 mig["applied_seq"], BATCH_SIZE),
            ).fetchall()
            applied = 0
            last_seq = mig["applied_seq"]
            for log in logs:
                status = "tombstone" if log["op"] == "delete" else "active"
                apply_dest_message(
                    conn, dst, log["order_id"], log["version"], status,
                    log["price"], log["epoch"], origin=f"changelog#{log['seq']}",
                )
                last_seq = log["seq"]
                applied += 1
            conn.execute(
                "UPDATE migrations SET applied_seq=?, updated_at=? WHERE id=?",
                (last_seq, int(time.time() * 1000), mig["id"]),
            )
            _maybe_fail(conn, "fail_next_migration_batch", "catchup", mig["id"])
            if applied > 0:
                log_event(
                    conn, "migration",
                    f"增量批次：消费 {applied} 条日志，applied_seq={last_seq}",
                    {"migration_id": mig["id"], "applied": applied,
                     "applied_seq": last_seq},
                )
            conn.execute("COMMIT")
            return migration_status(connect())
        finally:
            conn.close()


# --------------------------------------------------------------------------
# 4) 一次性切换归属（幂等：switch_count 门禁 + 最终排空）
# --------------------------------------------------------------------------

def cutover() -> dict:
    with store_lock:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            mig = current_migration(conn)
            if mig is None:
                raise MigrationError("还没有发起拆分")
            if mig["state"] == "done":
                # 同次/重复重试：不重复切换，直接回显已完成状态。
                log_event(conn, "migration",
                          "重复切换请求被忽略：归属已切换，操作幂等",
                          {"migration_id": mig["id"]})
                conn.execute("ROLLBACK")
                return migration_status(conn)
            if mig["state"] != "catching_up":
                raise MigrationError("必须先完成回填并进入追赶阶段才能切换")
            src, dst = mig["src_shard"], mig["dst_shard"]
            if src in offline_shards(conn):
                raise MigrationError(f"源分片 {src} 离线，无法做最终排空与切换")

            entry = find_migration_entry(conn, mig["range_lo"], mig["range_hi"])
            if entry is None or entry["shard_id"] != src:
                raise MigrationError("迁移区间的源属主条目已变化，拒绝切换")

            # 最终排空：把追赶游标之后到达的全部日志一次性消费完。
            tail = conn.execute(
                "SELECT * FROM changelog WHERE shard_id=? AND ? <= order_id AND order_id < ?"
                " AND seq > ? ORDER BY seq",
                (src, mig["range_lo"], mig["range_hi"], mig["applied_seq"]),
            ).fetchall()
            last_seq = mig["applied_seq"]
            for log in tail:
                status = "tombstone" if log["op"] == "delete" else "active"
                apply_dest_message(
                    conn, dst, log["order_id"], log["version"], status,
                    log["price"], log["epoch"], origin=f"final-drain#{log['seq']}",
                )
                last_seq = log["seq"]

            now = int(time.time() * 1000)
            new_epoch = mig["dst_epoch"]
            # 原子改路由：旧条目退休，左右两个区间各插一条新属主条目。
            conn.execute(
                "UPDATE router SET is_owner=0, state='retired', updated_at=? WHERE id=?",
                (now, entry["id"]),
            )
            if entry["range_lo"] < mig["range_lo"]:
                conn.execute(
                    "INSERT INTO router(range_lo,range_hi,shard_id,epoch,state,is_owner,updated_at)"
                    " VALUES(?,?,?,?, 'active', 1, ?)",
                    (entry["range_lo"], mig["range_lo"], src, new_epoch, now),
                )
            conn.execute(
                "INSERT INTO router(range_lo,range_hi,shard_id,epoch,state,is_owner,updated_at)"
                " VALUES(?,?,?,?, 'active', 1, ?)",
                (mig["range_lo"], mig["range_hi"], dst, new_epoch, now),
            )
            if mig["range_hi"] < entry["range_hi"]:
                conn.execute(
                    "INSERT INTO router(range_lo,range_hi,shard_id,epoch,state,is_owner,updated_at)"
                    " VALUES(?,?,?,?, 'active', 1, ?)",
                    (mig["range_hi"], entry["range_hi"], src, new_epoch, now),
                )
            conn.execute(
                "UPDATE migrations SET state='done', applied_seq=?, switch_count=1,"
                " stuck_reason=NULL, updated_at=? WHERE id=?",
                (last_seq, now, mig["id"]),
            )
            log_event(
                conn, "migration",
                f"归属一次性切换完成：[{mig['range_lo']},{mig['range_hi']}) "
                f"{src} → {dst}，epoch {mig['src_epoch']}→{new_epoch}，"
                f"最终排空 {len(tail)} 条日志，switch_count=1",
                {"migration_id": mig["id"], "tail": len(tail),
                 "new_epoch": new_epoch},
            )
            conn.execute("COMMIT")
            return migration_status(connect())
        finally:
            conn.close()


# --------------------------------------------------------------------------
# 5) 迟到旧副本注入（验证删单不复活 + epoch fence）
# --------------------------------------------------------------------------

def inject_late_replica(order_id: int, version: int, status: str,
                        price: int | None, stale_epoch: int) -> dict:
    with store_lock:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            mig = current_migration(conn)
            if mig is None:
                raise MigrationError("没有拆分记录，无法模拟迟到副本")
            dst = mig["dst_shard"]
            expect_epoch = mig["dst_epoch"] if mig["state"] == "done" else None
            outcome = apply_dest_message(
                conn, dst, order_id, version, status, price, stale_epoch,
                origin="late-replica-injection",
                expect_epoch=expect_epoch,
                fence_epoch=mig["state"] == "done",
            )
            log_event(
                conn, "sim",
                f"注入来自旧分片的迟到副本：订单 {order_id} v{version} "
                f"status={status} epoch={stale_epoch} → {outcome['action']}",
                {"order_id": order_id, "version": version, "status": status,
                 "stale_epoch": stale_epoch, **outcome},
            )
            conn.execute("COMMIT")
            return outcome
        finally:
            conn.close()


# --------------------------------------------------------------------------
# 进度与卡住原因
# --------------------------------------------------------------------------

def _stuck_reason(conn: sqlite3.Connection, mig: sqlite3.Row) -> str | None:
    if mig["state"] == "done":
        return None
    src = mig["src_shard"]
    if src in offline_shards(conn):
        return f"源分片 {src} 处于离线演练状态，{mig['state']} 读不到数据；恢复离线开关后即可续跑"
    if get_setting(conn, "fail_next_migration_batch", "0") == "1":
        return "下一批次被故障注入开关挂起，执行一次推进即触发可恢复中断"
    if mig["state"] == "backfilling":
        return "回填进行中：点击「推一批」按订单号游标继续，中断后从此处恢复"
    # catching_up
    lag = conn.execute(
        "SELECT COUNT(*) c FROM changelog WHERE shard_id=? AND ? <= order_id"
        " AND order_id < ? AND seq > ?",
        (src, mig["range_lo"], mig["range_hi"], mig["applied_seq"]),
    ).fetchone()["c"]
    if lag > 0:
        return f"增量追赶中：源分片仍有 {lag} 条新日志未消费，追齐前切换按钮保持禁用"
    return "已追齐：源端暂无新日志，可执行一次性切换；若源端继续写单则按钮重新禁用"


def migration_status(conn: sqlite3.Connection | None = None) -> dict:
    own = conn is None
    if own:
        conn = connect()
    try:
        mig = current_migration(conn)
        if mig is None:
            return {"active": False}
        lag = 0
        if mig["state"] != "done":
            lag = conn.execute(
                "SELECT COUNT(*) c FROM changelog WHERE shard_id=? AND ? <= order_id"
                " AND order_id < ? AND seq > ?",
                (mig["src_shard"], mig["range_lo"], mig["range_hi"],
                 mig["applied_seq"]),
            ).fetchone()["c"]
        data = dict(mig)
        data["active"] = True
        data["lag"] = lag
        data["stuck_reason"] = _stuck_reason(conn, mig)
        data["can_cutover"] = mig["state"] == "catching_up" and lag == 0
        data["can_backfill"] = mig["state"] == "backfilling"
        data["can_catchup"] = mig["state"] == "catching_up"
        return data
    finally:
        if own:
            conn.close()
