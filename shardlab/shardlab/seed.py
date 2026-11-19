"""演示种子：初始状态所有订单挤在 shard-a，包含墓碑与改价历史。"""
from __future__ import annotations

import time

from .db import connect, store_lock
from .events import log_event
from .router import reset_router


def seed(reset: bool = False, n_orders: int = 120) -> None:
    with store_lock:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute("SELECT COUNT(*) c FROM router").fetchone()["c"]
            if existing and not reset:
                return
            if reset:
                for table in ("scan_results", "scan_cursors", "scan_runs",
                              "migrations", "changelog", "orders", "router",
                              "events", "settings"):
                    conn.execute(f"DELETE FROM {table}")
            reset_router(conn)
            now = int(time.time() * 1000)
            deleted_ids = {7, 23, 58}
            for oid in range(1, n_orders + 1):
                if oid in deleted_ids:
                    # 建单后删单，产生墓碑（version=2）
                    conn.execute(
                        "INSERT INTO orders VALUES('shard-a',?,'tombstone',2,100,1,?)",
                        (oid, now),
                    )
                    conn.execute(
                        "INSERT INTO changelog(shard_id,order_id,op,version,price,epoch,ts)"
                        " VALUES('shard-a',?,'create',1,100,1,?)",
                        (oid, now),
                    )
                    conn.execute(
                        "INSERT INTO changelog(shard_id,order_id,op,version,price,epoch,ts)"
                        " VALUES('shard-a',?,'delete',2,100,1,?)",
                        (oid, now),
                    )
                else:
                    price = 100 + oid
                    version = 2 if oid % 5 == 0 else 1
                    conn.execute(
                        "INSERT INTO orders VALUES('shard-a',?,'active',?,?,1,?)",
                        (oid, version, price, now),
                    )
                    conn.execute(
                        "INSERT INTO changelog(shard_id,order_id,op,version,price,epoch,ts)"
                        " VALUES('shard-a',?,'create',1,?,1,?)",
                        (oid, price, now),
                    )
                    if version == 2:
                        conn.execute(
                            "INSERT INTO changelog(shard_id,order_id,op,version,price,epoch,ts)"
                            " VALUES('shard-a',?,'price',2,?,1,?)",
                            (oid, price, now),
                        )
            log_event(conn, "sim",
                      f"种子数据就绪：{n_orders} 单全部位于 shard-a，"
                      f"其中 {len(deleted_ids)} 单已删（墓碑）",
                      {"n_orders": n_orders, "deleted": sorted(deleted_ids)})
            conn.execute("COMMIT")
        finally:
            conn.close()
