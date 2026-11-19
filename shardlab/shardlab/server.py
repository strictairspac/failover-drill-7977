"""零依赖 HTTP 服务：JSON API + 静态前端，ThreadingHTTPServer。

所有关键状态都在 SQLite 里，服务重启只是重建连接，迁移与扫描进度不丢。
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from . import __version__
from .db import connect, get_setting, init_db, offline_shards, reset_database, set_setting
from .events import recent_events
from .migration import (
    MigrationError,
    SimRecoverableError as MigRecoverable,
    backfill_batch,
    catchup_batch,
    current_migration,
    cutover,
    inject_late_replica,
    migration_status,
    start_split,
)
from .orders import OrderConflictError, SimOfflineError, list_changelog, list_orders, write_order
from .router import list_entries
from .scan import (
    ScanError,
    SimRecoverableError as ScanRecoverable,
    scan_batch,
    scan_results,
    scan_status,
    start_scan,
)
from .seed import seed

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "web"

MIME = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".svg": "image/svg+xml",
}


def _shard_summary(conn) -> dict:
    rows = conn.execute(
        "SELECT shard_id, status, COUNT(*) c FROM orders GROUP BY shard_id, status"
    ).fetchall()
    out: dict[str, dict] = {}
    for r in rows:
        out.setdefault(r["shard_id"], {"active": 0, "tombstone": 0, "total": 0})
        out[r["shard_id"]][r["status"]] = r["c"]
        out[r["shard_id"]]["total"] += r["c"]
    return out


def build_state() -> dict:
    conn = connect()
    try:
        return {
            "version": __version__,
            "router": list_entries(conn),
            "shards": _shard_summary(conn),
            "offline_shards": sorted(offline_shards(conn)),
            "settings": {
                "write_delay_ms": int(get_setting(conn, "write_delay_ms", "0")),
                "fail_next_migration_batch": get_setting(
                    conn, "fail_next_migration_batch", "0") == "1",
                "fail_next_scan_batch": get_setting(
                    conn, "fail_next_scan_batch", "0") == "1",
            },
            "migration": migration_status(conn),
            "scan": scan_status(conn=conn),
            "events": recent_events(conn),
        }
    finally:
        conn.close()


class Handler(BaseHTTPRequestHandler):
    server_version = f"shardlab/{__version__}"

    def log_message(self, fmt, *args):  # 安静一点，事件流本身就是审计面
        return

    # ---------- 基础工具 ----------
    def _send_json(self, obj, status: int = 200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, message: str, status: int = 400, code: str = "error"):
        self._send_json({"ok": False, "error": message, "code": code}, status)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _guard(self, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except (MigrationError, ScanError, OrderConflictError) as e:
            self._send_error(str(e), 400, "business")
        except SimOfflineError as e:
            self._send_error(f"分片 {e} 离线，写入被拒（恢复离线开关后可重试）",
                             503, "offline")
        except (MigRecoverable, ScanRecoverable) as e:
            self._send_json({
                "ok": True,
                "interrupted": True,
                "message": f"批次按演练计划中断（{e}），状态已持久化，再次推进即恢复",
                "state": build_state(),
            })
        except KeyError as e:
            self._send_error(f"缺少参数 {e}", 400, "bad_request")
        except Exception as e:  # noqa: BLE001
            self._send_error(f"服务端错误：{type(e).__name__}: {e}", 500, "internal")

    # ---------- GET ----------
    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/health":
            self._send_json({"ok": True, "service": "shardlab", "version": __version__})
            return
        if path == "/api/state":
            self._send_json(build_state())
            return
        if path == "/api/orders":
            conn = connect()
            try:
                self._send_json({"orders": list_orders(conn)})
            finally:
                conn.close()
            return
        if path == "/api/changelog":
            conn = connect()
            try:
                self._send_json({"changelog": list_changelog(conn, 200)})
            finally:
                conn.close()
            return
        if path.startswith("/api/scan/results"):
            conn = connect()
            try:
                run = conn.execute(
                    "SELECT id FROM scan_runs ORDER BY id DESC LIMIT 1"
                ).fetchone()
                if run is None:
                    self._send_json({"results": []})
                else:
                    self._send_json({"results": scan_results(run["id"], 500)})
            finally:
                conn.close()
            return
        self._serve_static(path)

    def _serve_static(self, path: str):
        if path in ("", "/"):
            path = "/index.html"
        target = (FRONTEND_DIR / path.lstrip("/")).resolve()
        try:
            target.relative_to(FRONTEND_DIR.resolve())
        except ValueError:
            self.send_error(403)
            return
        if not target.is_file():
            self.send_error(404)
            return
        body = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", MIME.get(target.suffix, "application/octet-stream"))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ---------- POST ----------
    def do_POST(self):
        path = urlparse(self.path).path.rstrip("/")
        if path == "/api/orders/write":
            def work():
                data = self._read_json()
                result = write_order(
                    int(data["order_id"]),
                    str(data["op"]),
                    data.get("price"),
                    auto_retry=bool(data.get("auto_retry", True)),
                )
                self._send_json({"ok": result.ok, "write": result.__dict__})
            self._guard(work)
            return
        if path == "/api/migration/start":
            def work():
                d = self._read_json()
                start_split(int(d["range_lo"]), int(d["range_hi"]),
                            d.get("dst_shard", "shard-b"))
                self._send_json({"ok": True, "state": build_state()})
            self._guard(work)
            return
        if path == "/api/migration/backfill":
            self._guard(lambda: (backfill_batch(),
                                 self._send_json({"ok": True, "state": build_state()})))
            return
        if path == "/api/migration/catchup":
            self._guard(lambda: (catchup_batch(),
                                 self._send_json({"ok": True, "state": build_state()})))
            return
        if path == "/api/migration/cutover":
            def work():
                cutover()
                self._send_json({"ok": True, "state": build_state()})
            self._guard(work)
            return
        if path == "/api/migration/late-replica":
            def work():
                d = self._read_json()
                outcome = inject_late_replica(
                    int(d["order_id"]), int(d["version"]),
                    str(d.get("status", "active")), d.get("price"),
                    int(d["stale_epoch"]),
                )
                self._send_json({"ok": True, "outcome": outcome,
                                 "state": build_state()})
            self._guard(work)
            return
        if path == "/api/scan/start":
            self._guard(lambda: (start_scan(),
                                 self._send_json({"ok": True, "state": build_state()})))
            return
        if path == "/api/scan/batch":
            self._guard(lambda: (scan_batch(),
                                 self._send_json({"ok": True, "state": build_state()})))
            return
        if path == "/api/sim/offline":
            def work():
                d = self._read_json()
                conn = connect()
                try:
                    set_setting(conn, "offline_shards",
                                ",".join(sorted(set(d.get("shards", [])))))
                finally:
                    conn.close()
                self._send_json({"ok": True, "state": build_state()})
            self._guard(work)
            return
        if path == "/api/sim/fail-next":
            def work():
                d = self._read_json()
                conn = connect()
                try:
                    target = str(d["target"])  # migration | scan
                    key = ("fail_next_migration_batch" if target == "migration"
                           else "fail_next_scan_batch")
                    set_setting(conn, key, "1" if d.get("enabled", True) else "0")
                finally:
                    conn.close()
                self._send_json({"ok": True, "state": build_state()})
            self._guard(work)
            return
        if path == "/api/sim/delay":
            def work():
                d = self._read_json()
                conn = connect()
                try:
                    set_setting(conn, "write_delay_ms",
                                str(max(0, int(d.get("write_delay_ms", 0)))))
                finally:
                    conn.close()
                self._send_json({"ok": True, "state": build_state()})
            self._guard(work)
            return
        if path == "/api/sim/reset":
            def work():
                reset_database()
                init_db()
                seed()
                self._send_json({"ok": True, "state": build_state()})
            self._guard(work)
            return
        if path == "/api/sim/reseed":
            def work():
                seed(reset=True)
                self._send_json({"ok": True, "state": build_state()})
            self._guard(work)
            return
        self._send_error(f"未知接口 {path}", 404, "not_found")


def serve(host: str = "0.0.0.0", port: int = 18080) -> None:
    init_db()
    seed()
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.daemon_threads = True
    print(f"shardlab 沙盘已启动: http://{host}:{port}  (Ctrl+C 退出)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n收到退出信号，停止服务（数据已全部落盘）")
        httpd.shutdown()
