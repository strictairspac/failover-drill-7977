"""shardlab 端到端验收测试（标准库 unittest，零外部依赖）。

运行：
    cd shardlab && python -m unittest discover -s tests -v

覆盖需求对照：
  1. 在线拆分，源分片搬运中照常接单/改价/删单
  2. 追齐后一次性切换，其他区间一直可写
  3. 切换与写入并发：fencing 保证同一单号只归一边接写，已确认改动不丢
  4. 跨分片扫单不漏不重
  5. 迟到旧副本不能让删单复活（版本 + epoch 双重拦截）
  6. 搬运中断可恢复；同次重试不重复切换
  7. 进程重启后全部进度可恢复
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# 每个测试进程使用独立临时库
_TMP_DB = Path(tempfile.mkdtemp(prefix="shardlab-test-")) / "test.db"
os.environ["SHARDLAB_DB"] = str(_TMP_DB)

from shardlab import db  # noqa: E402
from shardlab.migration import (  # noqa: E402
    backfill_batch, catchup_batch, cutover, current_migration,
    inject_late_replica, start_split,
)
from shardlab.orders import write_order  # noqa: E402
from shardlab.router import list_entries, owner_entry  # noqa: E402
from shardlab.scan import scan_batch, scan_status, start_scan  # noqa: E402
from shardlab.seed import seed  # noqa: E402


class ShardlabTestCase(unittest.TestCase):
    def setUp(self):
        db.reset_database()
        seed()

    def _conn(self):
        return db.connect()

    def _order(self, conn, shard, oid):
        return conn.execute(
            "SELECT * FROM orders WHERE shard_id=? AND order_id=?", (shard, oid)
        ).fetchone()

    def _run_to_cutover_ready(self, lo=1, hi=61):
        """回填+追日志直到 lag=0、可切换。"""
        start_split(lo, hi)
        for _ in range(100):
            st = backfill_batch()
            if not st["can_backfill"]:
                break
        for _ in range(100):
            st = catchup_batch()
            if st["can_cutover"]:
                return st
        self.fail("追赶始终无法追齐")

    # ------------------------------------------------------------------
    # 1. 完整在线拆分流程 + 其他区间可写 + 一次性切换
    # ------------------------------------------------------------------
    def test_01_full_split_and_router_invariants(self):
        # 迁移前所有订单在 shard-a
        with self._conn() as conn:
            shards = [r[0] for r in conn.execute(
                "SELECT DISTINCT shard_id FROM orders").fetchall()]
            self.assertEqual(shards, ["shard-a"])
            # 路由不变量：每个单号恰好一个属主
            for oid in (-500, 1, 30, 60, 61, 9999):
                self.assertIsNotNone(owner_entry(conn, oid))

        # 其他区间（61..）在整个迁移期间一直可写
        r = write_order(500, "create", 1)
        self.assertTrue(r.ok)
        self.assertEqual(r.shard, "shard-a")

        self._run_to_cutover_ready(1, 61)
        # 切换前区间仍归 shard-a
        with self._conn() as conn:
            self.assertEqual(owner_entry(conn, 30)["shard_id"], "shard-a")
        cutover()

        with self._conn() as conn:
            # 切换后路由：迁移区间归 b，其余仍归 a，且不存在双属主
            self.assertEqual(owner_entry(conn, 1)["shard_id"], "shard-b")
            self.assertEqual(owner_entry(conn, 60)["shard_id"], "shard-b")
            self.assertEqual(owner_entry(conn, 61)["shard_id"], "shard-a")
            self.assertEqual(owner_entry(conn, 500)["shard_id"], "shard-a")
            owners = [e for e in list_entries(conn) if e["is_owner"] == 1]
            for oid in (1, 30, 60, 61, 500):
                hits = [e for e in owners
                        if e["range_lo"] <= oid < e["range_hi"]]
                self.assertEqual(len(hits), 1, f"单号 {oid} 属主不唯一")

        # 数据一致：区间内全部订单（含墓碑）都在 b，且版本与源一致
        with self._conn() as conn:
            src_rows = conn.execute(
                "SELECT order_id,version,status,price FROM orders"
                " WHERE shard_id='shard-a' AND 1<=order_id AND order_id<61"
            ).fetchall()
            for s in src_rows:
                d = self._order(conn, "shard-b", s["order_id"])
                self.assertIsNotNone(d, f"订单 {s['order_id']} 未搬到目标")
                self.assertEqual(d["version"], s["version"])
                self.assertEqual(d["status"], s["status"])
                self.assertEqual(d["price"], s["price"])
            # 墓碑仍是墓碑
            self.assertEqual(self._order(conn, "shard-b", 23)["status"], "tombstone")

        # 切换后新写入落到新属主
        r = write_order(30, "price", 999)
        self.assertTrue(r.ok)
        self.assertEqual(r.shard, "shard-b")
        with self._conn() as conn:
            self.assertEqual(self._order(conn, "shard-b", 30)["price"], 999)
            # 旧分片不会再出现这个单号的新写入
            self.assertNotEqual(
                conn.execute(
                    "SELECT version FROM orders WHERE shard_id='shard-a' AND order_id=30"
                ).fetchone()["version"],
                self._order(conn, "shard-b", 30)["version"],
            )

    # ------------------------------------------------------------------
    # 2. 搬运期间源分片照常接单/改价/删单，且改动全部追到目标，不丢
    # ------------------------------------------------------------------
    def test_02_writes_during_migration_are_not_lost(self):
        start_split(1, 61)
        backfill_batch()  # 搬走前 20 单（1..20）
        # 对“已搬走”和“未搬走”的单号分别改价/删单/建单
        write_order(10, "price", 1111)        # 已回填的订单改价
        write_order(15, "delete")             # 已回填的订单删单
        write_order(40, "price", 2222)        # 未回填的订单改价
        write_order(55, "delete")             # 未回填的订单删单
        write_order(58, "create", 333)        # 对已删墓碑单重新建单（原 v2 → 新 v1）
        write_order(70, "price", 4444)        # 区间外，永远留 a
        for _ in range(100):
            backfill_batch()
            st = current_migration(self._conn())
            if st["state"] != "backfilling":
                break
        for _ in range(100):
            st = catchup_batch()
            if st["can_cutover"]:
                break
        self.assertTrue(st["can_cutover"], "搬运期间的写入必须能被追齐")
        cutover()
        with self._conn() as conn:
            self.assertEqual(self._order(conn, "shard-b", 10)["price"], 1111)
            self.assertEqual(self._order(conn, "shard-b", 15)["status"], "tombstone")
            self.assertEqual(self._order(conn, "shard-b", 40)["price"], 2222)
            self.assertEqual(self._order(conn, "shard-b", 55)["status"], "tombstone")
            row58 = self._order(conn, "shard-b", 58)
            self.assertEqual(row58["status"], "active")
            self.assertEqual(row58["price"], 333)
            # 墓碑原为 v2，重建版本单调递增到 v3，才能覆盖目标端的墓碑
            self.assertEqual(row58["version"], 3)
            self.assertEqual(self._order(conn, "shard-a", 70)["price"], 4444)
            self.assertIsNone(self._order(conn, "shard-b", 70))

    # ------------------------------------------------------------------
    # 3. 切换与写入并发：fencing + 重定向，单属主写、改动不丢
    # ------------------------------------------------------------------
    def test_03_fencing_on_concurrent_cutover(self):
        self._run_to_cutover_ready(1, 61)
        fence_seen = threading.Event()
        write_done = threading.Event()

        def delayed_write():
            # 500ms 延迟窗口：解析路由后挂起，期间主线程完成切换
            res = write_order(30, "price", 7777, auto_retry=True, delay_ms=500)
            write_done.result = res  # type: ignore[attr-defined]
            write_done.set()

        t = threading.Thread(target=delayed_write)
        t.start()
        time.sleep(0.15)  # 确保写入线程已解析完旧路由并进入等待
        cutover()
        t.join(timeout=5)
        self.assertTrue(write_done.is_set())
        res = write_done.result  # type: ignore[attr-defined]
        self.assertTrue(res.ok, "fencing 后自动重试必须成功")
        self.assertTrue(res.fenced, "应当真实发生一次 fencing 拦截")
        self.assertEqual(res.shard, "shard-b")
        with self._conn() as conn:
            # 改动只存在于新属主，旧分片没有这次写入
            self.assertEqual(self._order(conn, "shard-b", 30)["price"], 7777)
            old = self._order(conn, "shard-a", 30)
            self.assertNotEqual(old["price"], 7777)
        with self._conn() as conn:
            kinds = [r["kind"] for r in conn.execute(
                "SELECT kind FROM events WHERE kind='fenced'").fetchall()]
            self.assertIn("fenced", kinds)

    def test_04_fenced_write_without_retry_does_not_split_brain(self):
        self._run_to_cutover_ready(1, 61)
        holder = {}

        def delayed_write():
            holder["res"] = write_order(
                30, "price", 8888, auto_retry=False, delay_ms=400)

        t = threading.Thread(target=delayed_write)
        t.start()
        time.sleep(0.12)
        cutover()
        t.join(timeout=5)
        res = holder["res"]
        self.assertFalse(res.ok)
        self.assertTrue(res.fenced)
        with self._conn() as conn:
            # 两个分片都没有 8888：旧写被拦截，新写从未发起
            self.assertNotEqual(self._order(conn, "shard-a", 30)["price"], 8888)
            self.assertNotEqual(self._order(conn, "shard-b", 30)["price"], 8888)

    # ------------------------------------------------------------------
    # 4. 迟到旧副本：版本拦截 + epoch 拦截，删单不复活
    # ------------------------------------------------------------------
    def test_05_late_replica_cannot_resurrect_deleted_order(self):
        # 订单 23 在种子里已是墓碑 v2
        self._run_to_cutover_ready(1, 61)
        # 切换前注入同订单的旧版本 active 副本
        out = inject_late_replica(23, version=1, status="active",
                                  price=100, stale_epoch=1)
        self.assertEqual(out["action"], "skipped_stale")
        cutover()
        with self._conn() as conn:
            self.assertEqual(self._order(conn, "shard-b", 23)["status"], "tombstone")
        # 切换后再注入携带旧 epoch 的副本（哪怕伪造更高版本）
        out = inject_late_replica(23, version=99, status="active",
                                  price=100, stale_epoch=1)
        self.assertEqual(out["action"], "fenced_epoch")
        with self._conn() as conn:
            row = self._order(conn, "shard-b", 23)
            self.assertEqual(row["status"], "tombstone")
            self.assertEqual(row["version"], 2)
        # 同版本重放幂等
        out = inject_late_replica(10, version=1, status="active",
                                  price=200, stale_epoch=2)
        self.assertIn(out["action"], ("skipped_same", "skipped_stale"))

    # ------------------------------------------------------------------
    # 5. 跨分片扫描：不漏不重，墓碑计入、高版本覆盖
    # ------------------------------------------------------------------
    def test_06_scan_exactly_once_across_shards(self):
        self._run_to_cutover_ready(1, 61)
        # 切换前后都写一些，让同一逻辑订单在两个分片各有一份副本
        write_order(30, "price", 5050)      # 切换前写源 a
        cutover()
        write_order(30, "price", 6060)      # 切换后写目标 b
        start_scan()
        for _ in range(100):
            st = scan_batch()
            if st["state"] == "done":
                break
        self.assertEqual(st["state"], "done")
        # 物理行数 > 逻辑订单数（存在双分片副本）
        self.assertGreater(st["total_seen"], st["distinct_count"])
        with self._conn() as conn:
            # 逻辑订单数 = 全宇宙订单号去重数
            expected = conn.execute(
                "SELECT COUNT(DISTINCT order_id) c FROM orders").fetchone()["c"]
            self.assertEqual(st["distinct_count"], expected)
            kept = conn.execute(
                "SELECT * FROM scan_results WHERE order_id=30").fetchone()
            self.assertEqual(kept["shard_id"], "shard-b")
            # 种子中 30 为 v2（每 5 单一改价），切换前后各改价一次 → v4
            self.assertEqual(kept["version"], 4)
            self.assertEqual(kept["price"], 6060)
            tomb = conn.execute(
                "SELECT * FROM scan_results WHERE order_id=23").fetchone()
            self.assertEqual(tomb["status"], "tombstone")

    def test_07_scan_resumes_after_interruption_without_dup(self):
        start_scan()
        with self._conn() as conn:
            db.set_setting(conn, "fail_next_scan_batch", "1")
        try:
            scan_batch()
        except Exception:
            pass
        st1 = scan_status()
        self.assertTrue(st1["active"])
        # 继续到完成
        for _ in range(100):
            st = scan_batch()
            if st["state"] == "done":
                break
        with self._conn() as conn:
            expected = conn.execute(
                "SELECT COUNT(DISTINCT order_id) c FROM orders").fetchone()["c"]
        self.assertEqual(st["distinct_count"], expected)

    # ------------------------------------------------------------------
    # 6. 搬运中断可恢复；同次重试不重复切换
    # ------------------------------------------------------------------
    def test_08_migration_interrupt_resumes_and_cutover_idempotent(self):
        start_split(1, 61)
        with self._conn() as conn:
            db.set_setting(conn, "fail_next_migration_batch", "1")
        try:
            backfill_batch()
        except Exception:
            pass
        st = backfill_batch()  # 立即恢复，游标继续
        self.assertTrue(st["active"] and st["can_backfill"])
        for _ in range(100):
            st = backfill_batch()
            if not st["can_backfill"]:
                break
        for _ in range(100):
            st = catchup_batch()
            if st["can_cutover"]:
                break
        cutover()
        with self._conn() as conn:
            mid = current_migration(conn)["id"]
            before = conn.execute(
                "SELECT switch_count FROM migrations WHERE id=?", (mid,)
            ).fetchone()["switch_count"]
            n_router = conn.execute("SELECT COUNT(*) c FROM router").fetchone()["c"]
        cutover()  # 重复调用：必须幂等
        cutover()
        with self._conn() as conn:
            after = conn.execute(
                "SELECT switch_count FROM migrations WHERE id=?", (mid,)
            ).fetchone()["switch_count"]
            n_router2 = conn.execute("SELECT COUNT(*) c FROM router").fetchone()["c"]
        self.assertEqual(before, 1)
        self.assertEqual(after, 1)
        self.assertEqual(n_router, n_router2, "重复切换不得新增路由条目")

    # ------------------------------------------------------------------
    # 7. 离线分片：写入被拒、迁移与扫描报告卡住原因，恢复后续跑
    # ------------------------------------------------------------------
    def test_09_offline_shard_blocks_and_recovers(self):
        start_split(1, 61)
        with self._conn() as conn:
            db.set_setting(conn, "offline_shards", "shard-a")
        from shardlab.orders import SimOfflineError
        with self.assertRaises(SimOfflineError):
            write_order(30, "price", 1)
        from shardlab.migration import MigrationError
        with self.assertRaises(MigrationError):
            backfill_batch()
        start_scan()
        scan_st = scan_batch()
        self.assertIn("离线", scan_st["stuck_reason"])
        # 恢复
        with self._conn() as conn:
            db.set_setting(conn, "offline_shards", "")
        for _ in range(100):
            st = scan_batch()
            if st["state"] == "done":
                break
        self.assertEqual(st["state"], "done")

    # ------------------------------------------------------------------
    # 8. 进程重启恢复（模拟新进程：重开连接，全部状态从盘上读）
    # ------------------------------------------------------------------
    def test_10_state_survives_process_restart(self):
        start_split(1, 61)
        backfill_batch()
        backfill_batch()
        with self._conn() as conn:
            saved = dict(current_migration(conn))
        # 重置模块级连接缓存（本实现无缓存）并重新初始化 DDL，模拟重启
        db.init_db()
        with self._conn() as conn:
            again = dict(current_migration(conn))
        self.assertEqual(again["last_copied_id"], saved["last_copied_id"])
        self.assertEqual(again["state"], "backfilling")
        # 从中断点继续直到完成
        for _ in range(100):
            st = backfill_batch()
            if not st["can_backfill"]:
                break
        for _ in range(100):
            st = catchup_batch()
            if st["can_cutover"]:
                break
        cutover()
        with self._conn() as conn:
            self.assertEqual(owner_entry(conn, 30)["shard_id"], "shard-b")


class HttpSmokeTest(unittest.TestCase):
    """通过真实 HTTP 栈验证页面与 API 可用。"""
    @classmethod
    def setUpClass(cls):
        db.reset_database()
        seed()
        from shardlab.server import ThreadingHTTPServer, Handler
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()

    def _get(self, path):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=5) as r:
            return r.status, r.read().decode("utf-8")

    def _post(self, path, payload):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read().decode())

    def test_http_smoke(self):
        status, html = self._get("/")
        self.assertEqual(status, 200)
        self.assertIn("在线分片拆分沙盘", html)
        status, _ = self._get("/app.js")
        self.assertEqual(status, 200)
        st, body = self._get("/api/state")
        self.assertEqual(st, 200)
        self.assertIn("router", json.loads(body))
        d = self._post("/api/orders/write",
                       {"order_id": 4321, "op": "create", "price": 5})
        self.assertTrue(d["ok"])
        d = self._post("/api/migration/start",
                       {"range_lo": 1, "range_hi": 31, "dst_shard": "shard-b"})
        self.assertTrue(d["ok"])
        d = self._post("/api/sim/reset", {})
        self.assertTrue(d["ok"])
        self.assertEqual(d["state"]["router"][0]["shard_id"], "shard-a")


if __name__ == "__main__":
    unittest.main(verbosity=2)
