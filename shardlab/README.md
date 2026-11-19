# shardlab · 在线分片拆分沙盘

一段订单挤在同一分片，在线搬到新分片的可演练沙盘。**纯 Python 标准库实现**
（SQLite + `http.server` + 原生前端），无需 Docker、无需安装任何依赖。

## 启动

```bash
cd shardlab
python3 -m shardlab.app --port 18080        # 数据落在 data/shardlab.db
python3 -m shardlab.app --fresh             # 启动前清空并重新播种
# 浏览器打开 http://localhost:18080
```

- 验收测试：`python3 -m unittest discover -s tests -v`
- HTTP 全流程演练（需先启动服务）：`python3 -m tests.demo_drill`

## 需求 → 实现对照

| 需求 | 实现方式 | 关键代码 |
|---|---|---|
| 搬运时源分片照常接单/改价/删单 | 迁移期间路由属主不变，写入全部落源分片并追加 `changelog`；回填按订单号游标、增量按日志序号两路追赶 | `migration.py`、`orders.py` |
| 目标追齐后一次性切换，其他区间一直可写 | 单个串行事务内：旧路由条目退休 + 插入新属主条目（epoch+1）；区间外条目不动，始终 `is_owner=1` | `migration.cutover`、`router.py` |
| 同一单号只归一边接写 | 路由表任一单号至多命中一条 `is_owner=1` 条目；切换原子翻转，无双属主窗口 | `router.owner_entry` |
| 切换与写入并发，已确认改动不丢 | 两阶段写 + epoch fencing：解析路由后保留 `(entry_id, epoch)`，提交前复查；失效即拦截并按新路由重试，旧分片永不接收切换后的写 | `orders.write_order` |
| 跨分片扫单不漏不重 | 每分片持久游标按键集续扫（不漏）；结果表主键 `(run_id, order_id)` 去重，重复副本取高版本（不重），墓碑同样入结果 | `scan.py` |
| 晚到旧副本不能让删单复活 | 双重门禁：版本号单调比较，低版本直接丢弃；切换后携带旧 epoch 的副本被任期 fence；墓碑不会被旧 active 副本覆盖 | `migration.apply_dest_message` |
| 搬运中断能恢复 | 回填游标、增量 `applied_seq`、扫描游标全部落盘；故障注入只中断批次不丢状态，再次推进从游标继续 | `db.py` 各表 + `fail_next_*` 开关 |
| 同次重试不重复切换 | `switch_count` 门禁 + 状态机 `done` 短路，重复调用幂等，不新增路由条目 | `migration.cutover` |
| 页面可写单、推进拆分、扫单，显示进度与卡住原因 | 单页控制台：进度条、双游标、lag、`stuck_reason`（离线/待追平/待推批）、事件流 | `web/` |

## 状态机

```
backfilling ──存量搬完──▶ catching_up ──lag=0──▶ [cutover] ──▶ done
   │ 游标 last_copied_id      │ 游标 applied_seq        │ 原子改路由
   └─ 任意中断后从游标续跑     └─ 任意中断后从序号续跑    └─ switch_count=1，重试幂等
```

## 建议的页面演练路径

1. ②区写单（建单/改价/删单），观察全部落在 `shard-a`。
2. ⑥区把「写入延迟窗口」设为 600ms；③区发起拆分 `[1,61)`，反复点「推一批回填 / 推一批增量」。
3. 勾选「下一批搬运中断」再推批，看到中断提示后继续点，验证游标恢复、不重搬。
4. 追齐后在②区提交一次改价，趁延迟窗口立即点「一次性切换」：事件流出现
   `fenced` 拦截，写入自动重定向到 `shard-b` 成功。
5. ④区给已删的 23 号注入旧 epoch 的 active 副本，看到 `fenced_epoch`，删单不复活。
6. ⑤区启动扫描并推批（可再注入一次扫描中断），核对物理行数 > 去重后逻辑订单数。
7. ⑥区把 `shard-a` 设为离线：写入返回 503、迁移/扫描显示卡住原因；解除后继续。
8. 直接杀掉服务进程再启动（不带 `--fresh`），迁移与扫描进度原样恢复。

## 目录

```
shardlab/
├── shardlab/          # 后端：db / router / orders / migration / scan / server
├── web/               # 前端：index.html + app.js + styles.css（无构建）
├── tests/             # unittest 验收（11 项）+ HTTP 演练脚本
└── data/              # SQLite 数据（运行后生成，已 gitignore）
```

## 边界说明

这是教学沙盘：用进程内串行锁 + SQLite 单库模拟分片存储与单写者语义，
用来确定性地复现 fencing、一次性切换、断点续跑等分布式协议问题；
不包含真实网络复制、多节点一致性协议。
