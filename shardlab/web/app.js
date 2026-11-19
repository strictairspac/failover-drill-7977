/* shardlab 前端：原生 JS，无构建步骤。 */
'use strict';

const $ = (id) => document.getElementById(id);
let state = null;
let showResults = false;

async function api(path, body) {
  const opt = body
    ? { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }
    : {};
  const res = await fetch(path, opt);
  const data = await res.json();
  if (!res.ok || data.ok === false) {
    const err = new Error(data.error || `HTTP ${res.status}`);
    err.data = data;
    throw err;
  }
  return data;
}

function toast(msg, kind = '') {
  const el = document.createElement('div');
  el.className = `toast ${kind}`;
  el.textContent = msg;
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 5200);
}

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function pct(done, total) {
  if (!total) return 100;
  return Math.min(100, Math.round((done / total) * 100));
}

function fmtTime(ts) {
  const d = new Date(ts);
  return d.toLocaleTimeString('zh-CN', { hour12: false }) +
    '.' + String(d.getMilliseconds()).padStart(3, '0');
}

// ---------------- 渲染 ----------------

function renderRouter() {
  const rows = state.router.map((r) => `
    <tr>
      <td class="mono">[${r.range_lo}, ${r.range_hi})</td>
      <td><b>${esc(r.shard_id)}</b>${state.offline_shards.includes(r.shard_id)
        ? ' <span class="badge offline">离线</span>' : ''}</td>
      <td>epoch ${r.epoch}</td>
      <td><span class="badge ${r.state}">${r.state}</span></td>
      <td>${r.is_owner ? '✅ 当前接写' : '⛔ 不接写'}</td>
    </tr>`).join('');
  $('routerBox').innerHTML = `<table>
    <tr><th>键区间（左闭右开）</th><th>分片</th><th>任期</th><th>状态</th><th>写归属</th></tr>
    ${rows}</table>`;

  const shards = state.shards || {};
  $('shardBox').innerHTML = Object.entries(shards).map(([sid, s]) => `
    <div class="shard-pill ${state.offline_shards.includes(sid) ? 'offline' : ''}">
      <b>${esc(sid)}</b>
      <div class="meta">存活 ${s.active} 单 · 墓碑 ${s.tombstone} 单 · 共 ${s.total} 行</div>
    </div>`).join('') || '<p class="hint">暂无数据</p>';
}

function renderMigration() {
  const m = state.migration;
  if (!m || !m.active) {
    $('migrationBox').innerHTML = '<p class="hint">当前没有进行中的拆分。填写区间后发起。</p>';
    $('backfillBtn').disabled = true;
    $('catchupBtn').disabled = true;
    $('cutoverBtn').disabled = true;
    return;
  }
  const phase = { backfilling: '阶段1/3 存量回填', catching_up: '阶段2/3 增量追赶', done: '阶段3/3 已切换' }[m.state];
  const bp = pct(m.backfill_done, m.backfill_total);
  const stuckClass = m.state === 'done' ? 'ok' : '';
  $('migrationBox').innerHTML = `
    <div class="kv">
      <div class="cell"><div class="k">迁移区间</div><div class="v mono">[${m.range_lo}, ${m.range_hi})</div></div>
      <div class="cell"><div class="k">源 → 目标</div><div class="v">${esc(m.src_shard)} → ${esc(m.dst_shard)}</div></div>
      <div class="cell"><div class="k">阶段</div><div class="v">${phase}</div></div>
      <div class="cell"><div class="k">任期</div><div class="v">${m.src_epoch} → ${m.dst_epoch}</div></div>
      <div class="cell"><div class="k">回填游标 order_id</div><div class="v">${m.last_copied_id}</div></div>
      <div class="cell"><div class="k">增量游标 changelog seq</div><div class="v">${m.applied_seq}</div></div>
      <div class="cell"><div class="k">未消费日志（lag）</div><div class="v">${m.lag}</div></div>
      <div class="cell"><div class="k">实际切换次数</div><div class="v">${m.switch_count}</div></div>
    </div>
    <div class="progress"><div style="width:${bp}%"></div></div>
    <div class="hint">回填 ${m.backfill_done}/${m.backfill_total}（${bp}%）·
      状态机状态：<b>${m.state}</b>${m.state === 'done' ? ' · 同次重试不会重复切换' : ''}</div>
    <div class="stuck ${stuckClass}">${m.state === 'done'
      ? '✅ 拆分完成：区间已由新分片接写，其他区间始终可写。'
      : '⏸ 卡住原因 / 下一步：' + esc(m.stuck_reason || '')}</div>`;

  $('backfillBtn').disabled = !m.can_backfill;
  $('catchupBtn').disabled = !m.can_catchup;
  $('cutoverBtn').disabled = !m.can_cutover;
  $('startBtn').disabled = m.state !== 'done';
}

function renderScan() {
  const s = state.scan;
  if (!s || !s.has_run) {
    $('scanBox').innerHTML = '<p class="hint">尚未启动扫描。</p>';
    $('scanBatchBtn').disabled = true;
    return;
  }
  const cursors = (s.cursors || []).map((c) => `
    <div class="cell">
      <div class="k">${esc(c.shard_id)} ${c.done ? '✅' : '⏳'}</div>
      <div class="v mono">cursor=${c.last_order_id}</div>
    </div>`).join('');
  const dup = s.total_seen - s.distinct_count;
  $('scanBox').innerHTML = `
    <div class="kv">
      <div class="cell"><div class="k">run</div><div class="v">#${s.id} <span class="badge ${s.state}">${s.state}</span></div></div>
      <div class="cell"><div class="k">物理行数（含重复副本）</div><div class="v">${s.total_seen}</div></div>
      <div class="cell"><div class="k">去重后逻辑订单</div><div class="v">${s.distinct_count}</div></div>
      <div class="cell"><div class="k">被去重的副本行</div><div class="v">${dup}</div></div>
    </div>
    <div class="kv">${cursors}</div>
    <div class="stuck ${s.state === 'done' ? 'ok' : ''}">${s.state === 'done'
      ? '✅ 全部分片扫完：不漏（游标到尾）不重（主键去重 + 高版本覆盖）。'
      : '⏸ 卡住原因 / 下一步：' + esc(s.stuck_reason || '')}</div>`;
  $('scanBatchBtn').disabled = s.state === 'done';
}

function renderEvents() {
  $('eventsBox').innerHTML = state.events.map((e) => `
    <div class="event kind-${e.kind}">
      <span class="time">${fmtTime(e.ts)}</span>
      <span class="kind">${esc(e.kind)}</span>
      <span>${esc(e.message)}</span>
    </div>`).join('');
}

function renderSimControls() {
  $('failMig').checked = !!state.settings.fail_next_migration_batch;
  $('failScan').checked = !!state.settings.fail_next_scan_batch;
  $('delayInput').value = state.settings.write_delay_ms;
  $('offlineInput').value = state.offline_shards.join(',');
}

function render() {
  if (!state) return;
  $('healthDot').className = 'dot ok';
  $('healthText').textContent = `已连接 · 服务版本 ${state.version}`;
  renderRouter();
  renderMigration();
  renderScan();
  renderEvents();
  renderSimControls();
}

// ---------------- 交互 ----------------

async function refresh() {
  try {
    state = await api('/api/state');
    render();
  } catch (e) {
    $('healthDot').className = 'dot';
    $('healthText').textContent = '连接失败：' + e.message;
  }
}

function bind() {
  $('refreshBtn').onclick = refresh;

  $('writeBtn').onclick = async () => {
    try {
      const data = await api('/api/orders/write', {
        order_id: Number($('wOrderId').value),
        op: $('wOp').value,
        price: Number($('wPrice').value),
        auto_retry: $('wRetry').checked,
      });
      const w = data.write;
      if (w.ok) {
        const fenceNote = w.fenced ? `\n⚠ 曾被 fencing 拦截（旧分片），重定向到 ${w.shard} 重试成功，改动未丢` : '';
        toast(`写入成功：${w.op}#${w.order_id} → ${w.shard} v${w.version}${fenceNote}`, 'ok');
      } else {
        toast('写入失败：' + w.error, 'err');
      }
    } catch (e) { toast('写入失败：' + e.message, 'err'); }
    refresh();
  };

  $('startBtn').onclick = async () => {
    try {
      await api('/api/migration/start', {
        range_lo: Number($('mLo').value), range_hi: Number($('mHi').value),
        dst_shard: $('mDst').value || 'shard-b',
      });
      toast('拆分已发起：源分片继续接写，游标从 0 开始', 'ok');
    } catch (e) { toast(e.message, 'err'); }
    refresh();
  };
  $('backfillBtn').onclick = async () => {
    try {
      const d = await api('/api/migration/backfill');
      if (d.interrupted) toast(d.message, 'warn');
    } catch (e) { toast(e.message, 'err'); }
    refresh();
  };
  $('catchupBtn').onclick = async () => {
    try {
      const d = await api('/api/migration/catchup');
      if (d.interrupted) toast(d.message, 'warn');
    } catch (e) { toast(e.message, 'err'); }
    refresh();
  };
  $('cutoverBtn').onclick = async () => {
    try {
      await api('/api/migration/cutover');
      toast('归属已一次性切换：旧条目退休，新区间由目标分片接写（epoch+1）', 'ok');
    } catch (e) { toast(e.message, 'err'); }
    refresh();
  };

  $('lateBtn').onclick = async () => {
    try {
      const d = await api('/api/migration/late-replica', {
        order_id: Number($('lOrderId').value),
        version: Number($('lVersion').value),
        status: $('lStatus').value,
        price: Number($('lPrice').value),
        stale_epoch: Number($('lEpoch').value),
      });
      const map = {
        applied: '已合并（版本更高）',
        skipped_same: '同版本幂等忽略',
        skipped_stale: '旧版本副本已丢弃，删单不会复活',
        fenced_epoch: '被 epoch 任期 fence，拒绝写入',
      };
      toast('迟到副本处理结果：' + map[d.outcome.action], d.outcome.action === 'applied' ? 'warn' : 'ok');
    } catch (e) { toast(e.message, 'err'); }
    refresh();
  };

  $('scanStartBtn').onclick = async () => {
    try { await api('/api/scan/start'); toast('扫描已启动：每个分片独立游标', 'ok'); }
    catch (e) { toast(e.message, 'err'); }
    showResults = false; $('scanResultBox').classList.add('hidden');
    refresh();
  };
  $('scanBatchBtn').onclick = async () => {
    try {
      const d = await api('/api/scan/batch');
      if (d.interrupted) toast(d.message, 'warn');
    } catch (e) { toast(e.message, 'err'); }
    refresh();
  };
  $('scanResultBtn').onclick = async () => {
    showResults = !showResults;
    const box = $('scanResultBox');
    if (showResults) {
      const d = await api('/api/scan/results');
      box.innerHTML = `<table>
        <tr><th>order_id</th><th>保留副本所在分片</th><th>version</th><th>status</th><th>price</th></tr>
        ${d.results.map((r) => `<tr><td>${r.order_id}</td><td>${esc(r.shard_id)}</td>
          <td>${r.version}</td><td>${r.status === 'tombstone'
            ? '<span class="badge tombstone">tombstone</span>' : 'active'}</td>
          <td>${r.price ?? ''}</td></tr>`).join('')}</table>`;
      box.classList.remove('hidden');
    } else box.classList.add('hidden');
  };

  $('offlineBtn').onclick = async () => {
    const shards = $('offlineInput').value.split(',').map((s) => s.trim()).filter(Boolean);
    await api('/api/sim/offline', { shards });
    toast('离线设置已应用：该分片读/写将被拒绝，迁移与扫描会报告卡住原因', 'warn');
    refresh();
  };
  $('failMig').onchange = async (e) => {
    await api('/api/sim/fail-next', { target: 'migration', enabled: e.target.checked });
    refresh();
  };
  $('failScan').onchange = async (e) => {
    await api('/api/sim/fail-next', { target: 'scan', enabled: e.target.checked });
    refresh();
  };
  $('delayBtn').onclick = async () => {
    await api('/api/sim/delay', { write_delay_ms: Number($('delayInput').value) || 0 });
    toast('写入延迟窗口已更新：可在该窗口内点击切换以复现 fencing', 'ok');
    refresh();
  };
  $('reseedBtn').onclick = async () => {
    if (!confirm('清空所有订单/迁移/扫描数据并重新播种？')) return;
    await api('/api/sim/reseed');
    toast('已重新播种', 'ok');
    showResults = false; $('scanResultBox').classList.add('hidden');
    refresh();
  };
  $('resetBtn').onclick = async () => {
    if (!confirm('删除整个 SQLite 库文件并重建？此操作不可撤销。')) return;
    await api('/api/sim/reset');
    toast('沙盘已重置（模拟全新环境）', 'ok');
    refresh();
  };

  setInterval(() => { if ($('autoPoll').checked) refresh(); }, 2000);
}

bind();
refresh();
