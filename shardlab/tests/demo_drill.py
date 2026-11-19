"""通过真实 HTTP 接口跑一遍完整拆分演练（人工演示用，可重复执行）。

用法：python -m tests.demo_drill
"""
from __future__ import annotations

import json
import threading
import time
import urllib.request

BASE = "http://127.0.0.1:18080"


def call(path: str, payload: dict | None = None) -> dict:
    req = urllib.request.Request(
        BASE + path, data=json.dumps(payload or {}).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())


def get(path: str) -> dict:
    with urllib.request.urlopen(BASE + path, timeout=10) as r:
        return json.loads(r.read())


def banner(t: str) -> None:
    print(f"\n{'=' * 70}\n {t}\n{'=' * 70}")


def main() -> None:
    banner("0. 重置沙盘并确认初始状态：120 单全在 shard-a")
    call("/api/sim/reset")
    st = get("/api/state")
    print("分片分布:", st["shards"])
    assert st["shards"]["shard-a"]["total"] == 120

    banner("1. 发起拆分 [1,61) shard-a -> shard-b，源端持续可写")
    call("/api/migration/start", {"range_lo": 1, "range_hi": 61})
    r = call("/api/orders/write", {"order_id": 30, "op": "price", "price": 7001})
    print("搬运中改价 30 号:", r["write"]["shard"], "v" + str(r["write"]["version"]))
    r = call("/api/orders/write", {"order_id": 12, "op": "delete"})
    print("搬运中删单 12 号:", r["write"]["shard"])
    r = call("/api/orders/write", {"order_id": 900, "op": "create", "price": 9})
    print("区间外建单 900 号（其他区间一直可写）:", r["write"]["shard"])

    banner("2. 回填 + 追增量，直到可切换（演示一次中断恢复）")
    call("/api/sim/fail-next", {"target": "migration", "enabled": True})
    for i in range(20):
        d = call("/api/migration/backfill")
        if d.get("interrupted"):
            print(">> 回填批次中断（游标已落盘），下一次调用即恢复")
        st = d["state"]["migration"]
        if not st["can_backfill"]:
            print(f">> 回填完成 {st['backfill_done']}/{st['backfill_total']}")
            break
    for i in range(20):
        d = call("/api/migration/catchup")
        st = d["state"]["migration"]
        if st["can_cutover"]:
            print(">> 增量追齐 lag =", st["lag"], "，切换按钮可用")
            break

    banner("3. 并发场景：写入延迟窗口内切换，验证 fencing + 重定向")
    call("/api/sim/delay", {"write_delay_ms": 600})
    holder = {}

    def slow_write():
        holder["r"] = call("/api/orders/write",
                           {"order_id": 30, "op": "price", "price": 7777})

    t = threading.Thread(target=slow_write)
    t.start()
    time.sleep(0.2)
    print(">> 写入已解析旧路由并挂起，此刻执行一次性切换 …")
    d = call("/api/migration/cutover")
    t.join()
    w = holder["r"]["write"]
    print(">> 写入结果: fenced =", w["fenced"], " retried =", w["retried"],
          " 最终落盘 =", w["shard"], "v" + str(w["version"]), "price =", w["price"])
    assert w["fenced"] and w["retried"] and w["shard"] == "shard-b"
    call("/api/sim/delay", {"write_delay_ms": 0})

    banner("4. 重复切换必须幂等（switch_count 不增加）")
    d = call("/api/migration/cutover")
    print("再次调用切换后 switch_count =", d["state"]["migration"]["switch_count"])
    assert d["state"]["migration"]["switch_count"] == 1
    st = get("/api/state")
    owners = [(e["range_lo"], e["range_hi"], e["shard_id"])
              for e in st["router"] if e["is_owner"]]
    print("当前属主区间:", owners)

    banner("5. 迟到旧副本：试图让已删的 23 号复活")
    r = call("/api/migration/late-replica",
             {"order_id": 23, "version": 1, "status": "active",
              "price": 100, "stale_epoch": 1})
    print("旧 epoch + 旧版本 active 副本 →", r["outcome"]["action"])
    assert r["outcome"]["action"] == "fenced_epoch"
    orders = {o["order_id"]: o for o in get("/api/orders")["orders"]
              if o["shard_id"] == "shard-b" and o["order_id"] == 23}
    print("23 号在 shard-b 的状态:", orders[23]["status"], "v" + str(orders[23]["version"]))
    assert orders[23]["status"] == "tombstone"

    banner("6. 跨分片扫描：不漏不重（含中断恢复）")
    call("/api/scan/start")
    call("/api/sim/fail-next", {"target": "scan", "enabled": True})
    for i in range(20):
        d = call("/api/scan/batch")
        if d.get("interrupted"):
            print(">> 扫描批次中断，游标落盘，继续推批恢复")
        st = d["state"]["scan"]
        if st["state"] == "done":
            break
    print(f">> 物理行 {st['total_seen']}（含双分片副本），"
          f"去重后逻辑订单 {st['distinct_count']}，去重副本 "
          f"{st['total_seen'] - st['distinct_count']} 条")
    print(">> 各分片游标:", [(c["shard_id"], c["last_order_id"], c["done"])
                            for c in st["cursors"]])

    banner("7. 全部事件留痕（最近 8 条）")
    for e in get("/api/state")["events"][:8]:
        print(f"[{e['kind']:9s}] {e['message']}")

    print("\n✅ 演练全部断言通过")


if __name__ == "__main__":
    main()
