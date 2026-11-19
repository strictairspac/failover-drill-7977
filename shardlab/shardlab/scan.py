"""跨分片扫单：不漏（每分片持久游标、断点续扫）不重（主键去重 + 版本取高）。

* 每个分片一条 (run_id, shard_id) 游标，按 order_id 键集分页推进，
  批次落盘后再推进，中断重启从最后一个已确认订单号继续。
* 结果表主键 (run_id, order_id)：同一逻辑订单在源/目标各有一份副本时，
  只保留 version 更高的一条（同版本保留先到的），因此 distinct 计数
  与逻辑订单一一对应；墓碑也计入结果并显式标注，避免“删单被扫丢”。
"""
from __future__ import annotations

import sqlite3
import time

from .db import connect, get_setting, offline_shards, set_setting, store_lock
from .events import log_event
from .router import all_shard_ids

BATCH_SIZE = 30


class ScanError(Exception):
    pass


class SimRecoverableError(Exception):
    pass


def _get_run(conn, run_id: int):
    return conn.execute("SELECT * FROM scan_runs WHERE id=?", (run_id,)).fetchone()


def start_scan() -> dict:
    with store_lock:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            running = conn.execute(
                "SELECT id FROM scan_runs WHERE state='running'"
            ).fetchall()
            if running:
                raise ScanError(f"扫描 run {running[0]['id']} 仍在进行，请先推进或完成它")
            now = int(time.time() * 1000)
            cur = conn.execute(
                "INSERT INTO scan_runs(state,total_seen,distinct_count,created_at)"
                " VALUES('running',0,0,?)",
                (now,),
            )
            run_id = cur.lastrowid
            for shard in all_shard_ids(conn):
                conn.execute(
                    "INSERT INTO scan_cursors(run_id,shard_id,last_order_id,done)"
                    " VALUES(?,?,0,0)",
                    (run_id, shard),
                )
            log_event(conn, "scan", f"跨分片扫描启动 run {run_id}",
                      {"run_id": run_id, "shards": all_shard_ids(conn)})
            conn.execute("COMMIT")
            return scan_status(run_id, connect())
        finally:
            conn.close()


def scan_batch(run_id: int | None = None) -> dict:
    with store_lock:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if run_id is None:
                row = conn.execute(
                    "SELECT id FROM scan_runs WHERE state='running' ORDER BY id DESC LIMIT 1"
                ).fetchone()
                if row is None:
                    raise ScanError("没有进行中的扫描，请先启动")
                run_id = row["id"]
            run = _get_run(conn, run_id)
            if run is None:
                raise ScanError(f"扫描 run {run_id} 不存在")
            if run["state"] == "done":
                conn.execute("ROLLBACK")
                return scan_status(run_id, conn)

            offline = offline_shards(conn)
            cursors = conn.execute(
                "SELECT * FROM scan_cursors WHERE run_id=? ORDER BY shard_id",
                (run_id,),
            ).fetchall()
            seen_delta = 0
            distinct_delta = 0
            progressed = False
            blocked: list[str] = []
            for cur in cursors:
                if cur["done"]:
                    continue
                shard = cur["shard_id"]
                if shard in offline:
                    blocked.append(shard)
                    continue
                rows = conn.execute(
                    "SELECT * FROM orders WHERE shard_id=? AND order_id > ?"
                    " ORDER BY order_id LIMIT ?",
                    (shard, cur["last_order_id"], BATCH_SIZE),
                ).fetchall()
                last_id = cur["last_order_id"]
                for r in rows:
                    seen_delta += 1
                    last_id = r["order_id"]
                    existing = conn.execute(
                        "SELECT version FROM scan_results WHERE run_id=? AND order_id=?",
                        (run_id, r["order_id"]),
                    ).fetchone()
                    if existing is None:
                        conn.execute(
                            "INSERT INTO scan_results(run_id,order_id,shard_id,version,status,price)"
                            " VALUES(?,?,?,?,?,?)",
                            (run_id, r["order_id"], shard, r["version"],
                             r["status"], r["price"]),
                        )
                        distinct_delta += 1
                    elif r["version"] > existing["version"]:
                        # 跨分片重复副本：保留高版本（删除墓碑版本更高，不会复活）
                        conn.execute(
                            "UPDATE scan_results SET shard_id=?, version=?, status=?, price=?"
                            " WHERE run_id=? AND order_id=?",
                            (shard, r["version"], r["status"], r["price"],
                             run_id, r["order_id"]),
                        )
                if len(rows) < BATCH_SIZE:
                    conn.execute(
                        "UPDATE scan_cursors SET last_order_id=?, done=1"
                        " WHERE run_id=? AND shard_id=?",
                        (last_id, run_id, shard),
                    )
                else:
                    conn.execute(
                        "UPDATE scan_cursors SET last_order_id=?"
                        " WHERE run_id=? AND shard_id=?",
                        (last_id, run_id, shard),
                    )
                progressed = True

            conn.execute(
                "UPDATE scan_runs SET total_seen=total_seen+?, "
                "distinct_count=distinct_count+? WHERE id=?",
                (seen_delta, distinct_delta, run_id),
            )

            if get_setting(conn, "fail_next_scan_batch", "0") == "1" and progressed:
                set_setting(conn, "fail_next_scan_batch", "0")
                conn.execute("COMMIT")
                log_event(conn, "sim",
                          f"故障注入：扫描 run {run_id} 批次中断，游标已落盘，可续扫",
                          {"run_id": run_id})
                raise SimRecoverableError("scan")

            cursors = conn.execute(
                "SELECT * FROM scan_cursors WHERE run_id=?", (run_id,)
            ).fetchall()
            all_done = all(c["done"] for c in cursors)
            if all_done and not blocked:
                conn.execute(
                    "UPDATE scan_runs SET state='done', finished_at=? WHERE id=?",
                    (int(time.time() * 1000), run_id),
                )
                log_event(
                    conn, "scan",
                    f"扫描完成 run {run_id}：物理行 {run['total_seen'] + seen_delta}，"
                    f"去重后逻辑订单 {run['distinct_count'] + distinct_delta}",
                    {"run_id": run_id},
                )
            elif progressed:
                log_event(
                    conn, "scan",
                    f"扫描批次推进 run {run_id}：本批 {seen_delta} 行，"
                    f"新增逻辑订单 {distinct_delta}，卡住分片={blocked or '无'}",
                    {"run_id": run_id, "seen": seen_delta,
                     "distinct": distinct_delta, "blocked": blocked},
                )
            conn.execute("COMMIT")
            return scan_status(run_id, connect())
        finally:
            conn.close()


def scan_status(run_id: int | None = None,
                conn: sqlite3.Connection | None = None) -> dict:
    own = conn is None
    if own:
        conn = connect()
    try:
        if run_id is None:
            run = conn.execute(
                "SELECT * FROM scan_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
        else:
            run = _get_run(conn, run_id)
        if run is None:
            return {"active": False, "has_run": False}
        data = dict(run)
        data["has_run"] = True
        data["active"] = run["state"] == "running"
        cursors = conn.execute(
            "SELECT * FROM scan_cursors WHERE run_id=? ORDER BY shard_id",
            (run["id"],),
        ).fetchall()
        data["cursors"] = [dict(c) for c in cursors]
        offline = offline_shards(conn)
        blocked = [c["shard_id"] for c in cursors
                   if not c["done"] and c["shard_id"] in offline]
        if run["state"] == "done":
            data["stuck_reason"] = None
        elif blocked:
            data["stuck_reason"] = (
                f"分片 {', '.join(blocked)} 离线，游标停在 "
                f"{[c['last_order_id'] for c in cursors if c['shard_id'] in blocked]}；"
                "恢复离线开关后续扫，已确认的行不会重扫"
            )
        elif get_setting(conn, "fail_next_scan_batch", "0") == "1":
            data["stuck_reason"] = "下一批次被故障注入挂起，执行一次推进即触发可恢复中断"
        else:
            pending = [c["shard_id"] for c in cursors if not c["done"]]
            data["stuck_reason"] = f"扫描进行中：待完成分片 {pending}，继续推批即可"
        return data
    finally:
        if own:
            conn.close()


def scan_results(run_id: int, limit: int = 200) -> list[dict]:
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT * FROM scan_results WHERE run_id=? ORDER BY order_id LIMIT ?",
            (run_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
