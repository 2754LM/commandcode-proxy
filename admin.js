/* ============================================================
   Command Code Proxy · Key 池管理台（前端）
   纯原生实现，无构建步骤，直接由 proxy.mjs 静态托管。
   ============================================================ */
(function () {
  'use strict';

  const $ = (sel, el = document) => el.querySelector(sel);
  const $$ = (sel, el = document) => Array.from(el.querySelectorAll(sel));

  /** 内联 SVG 图标（精灵定义在 index.html 顶部） */
  const icon = (name, cls = '') =>
    `<svg class="ic${cls ? ' ' + cls : ''}" viewBox="0 0 24 24" aria-hidden="true"><use href="#i-${name}"/></svg>`;

  /* ── 主题 ────────────────────────────────────────────── */
  const themeBtn = $('#btnTheme');
  const metaTheme = document.querySelector('meta[name="theme-color"]');

  function applyTheme(t) {
    document.documentElement.dataset.theme = t;
    if (metaTheme) metaTheme.setAttribute('content', t === 'light' ? '#f3f4fa' : '#07080d');
    try { localStorage.setItem('cc-proxy-theme', t); } catch { /* 隐私模式忽略 */ }
  }
  function initialTheme() {
    // 支持 ?theme=dark|light 深链，便于分享/嵌入时固定主题
    try {
      const q = new URLSearchParams(location.search).get('theme');
      if (q === 'light' || q === 'dark') return q;
    } catch { /* 忽略 */ }
    try {
      const saved = localStorage.getItem('cc-proxy-theme');
      if (saved === 'light' || saved === 'dark') return saved;
    } catch { /* 忽略 */ }
    return window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
  }
  applyTheme(initialTheme());
  themeBtn.addEventListener('click', () => {
    applyTheme(document.documentElement.dataset.theme === 'light' ? 'dark' : 'light');
  });

  /* ── Toast ───────────────────────────────────────────── */
  const toastEl = $('#toast');
  let toastTimer = null;
  function toast(msg, kind = 'ok') {
    toastEl.className = 'toast ' + (kind === 'err' ? 'err' : 'ok');
    toastEl.innerHTML = icon(kind === 'err' ? 'alert' : 'check-circle') + '<span></span>';
    toastEl.lastElementChild.textContent = msg; // 文本节点，避免注入
    // 重启入场动画
    toastEl.style.animation = 'none';
    void toastEl.offsetWidth;
    toastEl.style.animation = '';
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => toastEl.classList.add('hidden'), 3000);
  }

  /* ── API ─────────────────────────────────────────────── */
  async function api(path, opts = {}) {
    const res = await fetch(path, {
      headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
      ...opts,
    });
    if (res.status === 204) return null;
    const text = await res.text();
    let data = null;
    try { data = text ? JSON.parse(text) : null; } catch { data = { raw: text }; }
    if (!res.ok) {
      const msg = data?.error?.message || data?.message || `HTTP ${res.status}`;
      throw new Error(msg);
    }
    return data;
  }

  /* ── 格式化 ──────────────────────────────────────────── */
  const pad = (n) => String(n).padStart(2, '0');

  function fmtTime(ts) {
    if (!ts) return '—';
    const d = new Date(ts);
    return `${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }
  function fmtClock(ts) {
    if (!ts) return '—';
    const d = new Date(ts);
    return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }
  function fmtNum(n) {
    if (n == null || !isFinite(n)) return '—';
    return Number(n).toLocaleString('zh-CN', { maximumFractionDigits: 2 });
  }
  /** 相对时间：3 分钟前 */
  function fmtAgo(ts) {
    if (!ts) return '从未使用';
    const diff = Date.now() - ts;
    if (diff < 0) return fmtTime(ts);
    const m = Math.floor(diff / 60000);
    if (m < 1) return '刚刚';
    if (m < 60) return `${m} 分钟前`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h} 小时前`;
    const d = Math.floor(h / 24);
    if (d < 30) return `${d} 天前`;
    return fmtTime(ts);
  }
  /** 倒计时：2 天 3 小时 / 38 分钟后 */
  function fmtCountdown(ts) {
    if (!ts) return '—';
    const diff = ts - serverNow();
    if (diff <= 0) return '已重置';
    const m = Math.ceil(diff / 60000);
    if (m < 60) return `${m} 分钟后`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h} 小时 ${m % 60} 分后`;
    return `${Math.floor(h / 24)} 天 ${h % 24} 小时后`;
  }
  /** 时长：45 秒 / 15 分 / 3 分 20 秒 / 2 小时 5 分 */
  function fmtDuration(ms) {
    const total = Math.max(0, Math.round(ms / 1000));
    if (total < 60) return `${total} 秒`;
    if (total < 3600) {
      const m = Math.floor(total / 60);
      const s = total % 60;
      return s ? `${m} 分 ${s} 秒` : `${m} 分钟`;
    }
    const h = Math.floor(total / 3600);
    const m = Math.floor((total % 3600) / 60);
    return m ? `${h} 小时 ${m} 分` : `${h} 小时`;
  }
  /** 用服务端时间校准的“现在”，避免前后端时钟偏差导致倒计时不准 */
  function serverNow() {
    return Date.now() + (state.serverSkew || 0);
  }
  function escapeHtml(s) {
    return String(s ?? '').replace(/[&<>"']/g, (c) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }
  function truncate(s, n) {
    const str = String(s ?? '');
    return str.length > n ? str.slice(0, n) + '…' : str;
  }
  const clampPct = (v) => Math.max(0, Math.min(100, Math.round(Number(v) || 0)));
  /** 用量等级：越高越危险 */
  const levelOf = (pct) => (pct >= 80 ? 'danger' : pct >= 50 ? 'warn' : 'ok');
  const WIN_NAMES = { '5h': '5 小时窗口', weekly: '本周窗口' };

  const STRATEGY_LABEL = {
    weighted: '加权轮询', 'round-robin': '轮询', 'weighted-random': '加权随机',
    random: '随机', sticky: '粘性会话', 'least-recent': '最久未用', failover: '主备故障转移',
  };
  const STRATEGY_HINT = {
    weighted: '按权重轮询分发，权重越高被选中的次数越多。',
    'round-robin': '所有 Key 依次轮流使用，忽略权重差异。',
    'weighted-random': '按权重随机抽取，权重越高命中概率越大。',
    random: '在可用 Key 中完全随机选择。',
    sticky: '同一客户端标识（或 IP）固定命中同一个 Key。',
    'least-recent': '优先使用最久没有被用到过的 Key。',
    failover: '按优先级主备切换，前面的 Key 不可用才轮到后面的。',
  };
  const strategyLabel = (v) => STRATEGY_LABEL[v] || v || '—';

  /* ── 状态 ────────────────────────────────────────────── */
  const state = {
    keys: [],
    lb: null,
    settings: {
      mode: 'session',
      failThreshold: 3,
      creditsRefreshMs: 0,
      autoDisableExhausted: true,
      onAllExhausted: 'error',
    },
    creditsRefresh: null,
    sessions: [],
    serverSkew: 0,
    tab: 'keys',
    filter: 'all',
    query: '',
    sort: 'default',
    busy: new Set(),   // 正在刷新额度的 Key id
    loading: true,
  };

  const FILTERS = ['all', 'usable', 'exhausted', 'cooling'];

  /* ── 过滤 / 排序 ─────────────────────────────────────── */
  function matchesFilter(k) {
    if (state.filter === 'usable' && !isUsableClient(k)) return false;
    if (state.filter === 'exhausted' && !k.autoDisabled) return false;
    if (state.filter === 'cooling' && !k.cooling) return false;
    const q = state.query.trim().toLowerCase();
    if (!q) return true;
    return [k.label, k.keyPrefix, k.id]
      .some((v) => String(v ?? '').toLowerCase().includes(q));
  }

  const SORTERS = {
    default: (a, b) => (Number(b.isDefault) - Number(a.isDefault)) || ((a.createdAt || 0) - (b.createdAt || 0)),
    'credits-desc': (a, b) => (b.credits?.creditsRemaining ?? -1) - (a.credits?.creditsRemaining ?? -1),
    'usage-desc': (a, b) => (b.credits?.worstPct ?? -1) - (a.credits?.worstPct ?? -1),
    'weight-desc': (a, b) => (b.weight ?? 0) - (a.weight ?? 0),
    'label-asc': (a, b) => String(a.label ?? '').localeCompare(String(b.label ?? ''), 'zh-Hans-CN'),
  };

  function visibleKeys() {
    const list = state.keys.filter(matchesFilter);
    const sorter = SORTERS[state.sort];
    return sorter ? list.slice().sort(sorter) : list;
  }

  /* ── 片段渲染 ────────────────────────────────────────── */

  /** 窗口剩余比例：100 - 最高已用（100 = 完全没用过，0 = 已用尽） */
  function remainingPct(k) {
    const c = k.credits;
    if (!c || c.error) return null;
    const worst = clampPct(c.worstPct);
    return clampPct(100 - worst);
  }
  /** 剩余额度等级：充裕=ok，偏低=warn，告急=danger */
  function levelByRemaining(pct) {
    if (pct == null) return 'none';
    if (pct <= 10) return 'danger';
    if (pct <= 35) return 'warn';
    return 'ok';
  }

  function ringHtml(k) {
    const remaining = remainingPct(k);
    if (remaining == null) {
      return `<span class="ring lvl-none" style="--p:0" title="还没有额度数据"><span>—</span></span>`;
    }
    return `<span class="ring lvl-${levelByRemaining(remaining)}" style="--p:${remaining}"
      title="窗口额度剩余 ${remaining}%（按已用最高的窗口计算）"><span>${remaining}%</span></span>`;
  }

  function creditsBlock(k) {
    const c = k.credits;
    let body;
    if (!c) {
      body = `<span class="kc-cred-label">剩余额度</span>
        <strong>未查询</strong>
        <small>点击 ⟳ 查询</small>`;
    } else if (c.error) {
      body = `<span class="kc-cred-label">剩余额度</span>
        <span class="badge danger" title="${escapeHtml(c.error)}">查询失败</span>
        <small title="${escapeHtml(c.error)}">${escapeHtml(truncate(c.error, 24))}</small>`;
    } else {
      body = `<span class="kc-cred-label">剩余额度</span>
        <strong>${fmtNum(c.creditsRemaining)}</strong>
        <small>${c.plan ? escapeHtml(c.plan) : '已同步'} · ${fmtAgo(c.fetchedAt)}</small>`;
    }
    return `<div class="kc-gauge">${ringHtml(k)}<div class="kc-cred">${body}</div></div>`;
  }

  function windowsBlock(k) {
    const c = k.credits;
    if (!c || c.error || !c.windows?.length) return '';
    return `<div class="kc-wins">${c.windows.slice(0, 2).map((w) => {
      const pct = clampPct(w.pct);
      const resets = w.resetsAt ? `<span class="sep">·</span><span>重置 ${fmtCountdown(w.resetsAt)}</span>` : '';
      return `<div class="win lvl-${levelOf(pct)}">
        <div class="win-top">
          <span class="win-name">${escapeHtml(WIN_NAMES[w.name] || w.name)}</span>
          <span class="win-pct">${pct}%</span>
        </div>
        <div class="win-bar"><i style="width:${pct}%"></i></div>
        <div class="win-foot"><span>${fmtNum(w.used)} / ${fmtNum(w.cap)}</span>${resets}</div>
      </div>`;
    }).join('')}</div>`;
  }

  /** 自动停用徽标（额度用尽等），带恢复时间 */
  function autoDisabledBadge(k) {
    const ad = k.autoDisabled;
    if (!ad) return '';
    const reason = ad.reason === 'quota-exhausted' ? '额度用尽' : ad.reason === 'disabled' ? '凭证失效' : '自动停用';
    if (ad.until && ad.until > Date.now()) {
      return `<span class="badge danger" title="自动停用原因：${escapeHtml(ad.reason)}；预计 ${fmtTime(ad.until)} 恢复">${icon('alert')}${reason} · ${fmtCountdown(ad.until)}恢复</span>`;
    }
    return `<span class="badge danger" title="自动停用原因：${escapeHtml(ad.reason)}">${icon('alert')}${reason}</span>`;
  }

  /** 前端侧的"可用"判断，与后端 isKeyUsable 保持一致 */
  function isUsableClient(k) {
    if (!k.enabled || k.autoDisabled || k.cooling) return false;
    return true;
  }

  function keyCard(k, index) {
    const busy = state.busy.has(k.id);
    const cls = [
      'key-card',
      k.enabled ? '' : 'is-off',
      k.cooling ? 'is-cooling' : '',
      k.errorCount ? 'is-error' : '',
      busy ? 'is-busy' : '',
    ].filter(Boolean).join(' ');

    return `<article class="${cls}" data-id="${escapeHtml(k.id)}">
      <label class="switch" title="${k.enabled ? '点击停用' : '点击启用'}">
        <input type="checkbox" data-act="toggle" ${k.enabled ? 'checked' : ''} aria-label="启用状态" />
        <span class="track"></span>
      </label>

      <div class="kc-main">
        <div class="kc-title">
          <span class="kc-index">${pad(index + 1)}</span>
          <h4 title="${escapeHtml(k.label)}">${escapeHtml(k.label)}</h4>
          ${k.isDefault ? `<span class="badge star">${icon('star-fill')}默认</span>` : ''}
          ${k.enabled ? '' : `<span class="badge off">停用</span>`}
          ${autoDisabledBadge(k)}
          ${k.cooling ? `<span class="badge warn" title="冷却至 ${fmtTime(k.cooldownUntil)}">${icon('clock')}冷却中</span>` : ''}
          ${k.consecutiveFailures ? `<span class="badge danger" title="连续失败 ${k.consecutiveFailures} 次，达到阈值后会换 Key">${icon('alert')}连败 ${k.consecutiveFailures}</span>` : ''}
          ${k.activeSessions ? `<span class="badge">${icon('sliders')}${k.activeSessions} 会话</span>` : ''}
        </div>
        <div class="kc-meta">
          <button type="button" class="kc-key" data-act="copy" title="点击复制 Key ID">${escapeHtml(k.keyPrefix)}${icon('copy')}</button>
          <span class="sep">·</span>
          <span>权重 <b>${escapeHtml(k.weight)}</b></span>
          <span class="sep">·</span>
          <span title="${fmtTime(k.lastUsedAt)}">最近使用 ${fmtAgo(k.lastUsedAt)}</span>
          ${k.errorCount ? `<span class="sep">·</span><span title="累计错误 ${k.errorCount} 次">错误 ${k.errorCount}</span>` : ''}
        </div>
      </div>

      <div class="kc-actions">
        <button type="button" class="icon-btn ${k.isDefault ? 'tone-default' : ''}" data-act="default"
          title="${k.isDefault ? '取消默认 Key' : '设为默认 Key'}"
          aria-label="${k.isDefault ? '取消默认 Key' : '设为默认 Key'}">${icon(k.isDefault ? 'star-fill' : 'star')}</button>
        <button type="button" class="icon-btn" data-act="credits" title="查询该 Key 额度" aria-label="查询该 Key 额度">${icon('refresh')}</button>
        <button type="button" class="icon-btn" data-act="edit" title="编辑" aria-label="编辑">${icon('pencil')}</button>
        <button type="button" class="icon-btn tone-danger" data-act="del" title="删除" aria-label="删除">${icon('trash')}</button>
      </div>

      <div class="kc-stats">
        ${creditsBlock(k)}
        ${windowsBlock(k)}
      </div>
    </article>`;
  }

  function skeletonCard() {
    return `<div class="skeleton">
      <span class="sk-bar sk-avatar"></span>
      <div class="sk-stack">
        <span class="sk-bar" style="width:42%"></span>
        <span class="sk-bar"></span>
      </div>
      <div class="sk-right"><span class="sk-bar" style="width:34px;height:34px;border-radius:10px"></span><span class="sk-bar" style="width:34px;height:34px;border-radius:10px"></span></div>
      <div class="sk-stack" style="grid-column:2 / -1"><span class="sk-bar" style="width:70%"></span></div>
    </div>`;
  }

  /* ── 列表渲染 ────────────────────────────────────────── */
  function renderKeys() {
    const listEl = $('#keyList');
    $('#emptyKeys').classList.toggle('hidden', state.loading || state.keys.length > 0);

    // 过滤芯片计数
    const counts = {
      all: state.keys.length,
      usable: state.keys.filter(isUsableClient).length,
      exhausted: state.keys.filter((k) => k.autoDisabled).length,
      cooling: state.keys.filter((k) => k.cooling).length,
    };
    $$('#filterChips .seg').forEach((btn) => {
      const f = btn.dataset.filter;
      const badge = btn.querySelector('b');
      if (badge) badge.textContent = String(counts[f] ?? 0);
      btn.classList.toggle('is-active', state.filter === f);
    });

    if (state.loading) {
      listEl.innerHTML = skeletonCard() + skeletonCard() + skeletonCard();
      return;
    }

    const visible = visibleKeys();
    if (!visible.length) {
      const noFilter = !state.query.trim() && state.filter === 'all';
      listEl.innerHTML = noFilter ? '' : `<p class="empty">
        <span class="empty-icon">${icon('search')}</span>
        <b>没有匹配的 Key</b>
        <span>试试其它关键词，或切换状态筛选。</span>
        <button class="btn ghost sm" type="button" data-act="reset-filter">重置筛选条件</button>
      </p>`;
      return;
    }
    listEl.innerHTML = visible.map(keyCard).join('');
  }

  function renderStats() {
    const keys = state.keys;
    const usable = keys.filter(isUsableClient);
    const exhausted = keys.filter((k) => k.autoDisabled).length;
    const notEnabled = keys.filter((k) => !k.enabled).length;
    const synced = keys.filter((k) => k.credits && !k.credits.error);
    const sum = synced.reduce((s, k) => s + (k.credits?.creditsRemaining || 0), 0);

    $('#statTotal').textContent = String(keys.length);
    $('#statTotalSub').textContent = keys.length
      ? `启用 ${keys.length - notEnabled} · 停用 ${notEnabled}`
      : '池内暂无凭证';

    $('#statUsable').textContent = String(usable.length);
    const blocked = [];
    if (exhausted) blocked.push(`用尽 ${exhausted}`);
    const cooling = keys.filter((k) => k.cooling).length;
    if (cooling) blocked.push(`冷却 ${cooling}`);
    if (notEnabled) blocked.push(`停用 ${notEnabled}`);
    $('#statUsableSub').textContent = keys.length
      ? (blocked.length ? blocked.join(' · ') : '全部可参与分发')
      : '参与请求分发';

    $('#statCredits').textContent = synced.length ? fmtNum(sum) : '—';
    $('#statCreditsSub').textContent = keys.length
      ? `已同步 ${synced.length}/${keys.length} 个 Key`
      : '尚未查询额度';

    const sessions = state.sessions || [];
    $('#statSessions').textContent = String(sessions.length);
    $('#statSessionsSub').textContent = state.settings.mode === 'session'
      ? (sessions.length
        ? `分布在 ${new Set(sessions.map((s) => s.keyId)).size} 个 Key`
        : '会话粘性 · 等待请求')
      : '每次请求模式';

    renderPoolBadge();
    renderTabBadges();
  }

  /** 池健康概览：额度告急 / 冷却 / 出错的 Key 数量 */
  function renderPoolBadge() {
    const badge = $('#poolBadge');
    const keys = state.keys;
    if (state.loading) {
      badge.className = 'hero-badge tone-idle';
      badge.innerHTML = icon('shield') + '<span>正在加载…</span>';
      return;
    }
    if (!keys.length) {
      badge.className = 'hero-badge tone-idle';
      badge.innerHTML = icon('inbox') + '<span>池内暂无 Key</span>';
      return;
    }
    const attention = keys.filter((k) =>
      k.autoDisabled
      || k.cooling
      || k.consecutiveFailures > 0
      || (k.credits && k.credits.error)
      || (!k.credits && k.enabled)
    ).length;

    if (attention) {
      badge.className = 'hero-badge tone-warn';
      badge.innerHTML = icon('alert') + `<span>${attention} 个 Key 需要关注</span>`;
    } else {
      const usableNow = keys.filter(isUsableClient).length;
      badge.className = 'hero-badge tone-ok';
      badge.innerHTML = icon('shield') + `<span>池状态良好 · ${usableNow} 个 Key 可用</span>`;
    }
  }

  function renderAll() {
    renderKeys();
    renderStats();
  }

  function fillLb(lb) {
    if (!lb) return;
    state.lb = lb;
    $('#lbStrategy').value = lb.strategy || 'weighted';
    $('#lbStickyBy').value = lb.stickyBy || 'client-key';
    $('#lbMaxRetries').value = lb.maxRetries ?? 2;
    $('#lbCooldownMs').value = lb.cooldownMs ?? 60000;
    $('#stickyByWrap').style.display = lb.strategy === 'sticky' ? '' : 'none';
    updateStrategyHint();
  }

  function updateStrategyHint() {
    const v = $('#lbStrategy').value;
    if (STRATEGY_HINT[v]) $('#lbStrategyHint').textContent = STRATEGY_HINT[v];
  }

  /* ── 标签页 ──────────────────────────────────────────── */
  const TABS = ['keys', 'routing', 'sessions'];
  const TAB_STORE_KEY = 'cc-proxy-tab';

  function setTab(name, { save = true } = {}) {
    const t = TABS.includes(name) ? name : 'keys';
    state.tab = t;
    $$('#tabs .tab').forEach((btn) => {
      const on = btn.dataset.tab === t;
      btn.classList.toggle('is-active', on);
      btn.setAttribute('aria-selected', on ? 'true' : 'false');
      btn.tabIndex = on ? 0 : -1;
    });
    $$('.tab-panel').forEach((panel) => {
      const on = panel.dataset.panel === t;
      panel.classList.toggle('is-active', on);
      if (on) panel.removeAttribute('hidden');
      else panel.setAttribute('hidden', '');
    });
    if (save) { try { localStorage.setItem(TAB_STORE_KEY, t); } catch { /* 忽略 */ } }
    // 让每个标签页都有自己的 URL，可直接分享 / 刷新停留在同一页
    if (location.hash.replace(/^#/, '') !== t) {
      try { history.replaceState(null, '', '#' + t); } catch { location.hash = t; }
    }
    if (t === 'sessions') refreshSessions();
  }

  function initialTab() {
    const fromHash = location.hash.replace(/^#/, '');
    if (TABS.includes(fromHash)) return fromHash;
    try {
      const saved = localStorage.getItem(TAB_STORE_KEY);
      if (TABS.includes(saved)) return saved;
    } catch { /* 忽略 */ }
    return 'keys';
  }

  $('#tabs').addEventListener('click', (e) => {
    const btn = e.target.closest('.tab[data-tab]');
    if (btn) setTab(btn.dataset.tab);
  });
  // role=tablist 的惯例：左右方向键切换
  $('#tabs').addEventListener('keydown', (e) => {
    if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
    e.preventDefault();
    const cur = Math.max(0, TABS.indexOf(state.tab));
    const next = (cur + (e.key === 'ArrowRight' ? 1 : TABS.length - 1)) % TABS.length;
    setTab(TABS[next]);
    const btn = $(`#tabs .tab[data-tab="${TABS[next]}"]`);
    if (btn) btn.focus();
  });
  window.addEventListener('hashchange', () => {
    const t = location.hash.replace(/^#/, '');
    if (TABS.includes(t) && t !== state.tab) setTab(t, { save: false });
  });

  function renderTabBadges() {
    const keysCount = $('#tabKeysCount');
    if (keysCount) keysCount.textContent = String(state.keys.length);
    const sessCount = $('#tabSessionsCount');
    if (sessCount) sessCount.textContent = String((state.sessions || []).length);
    // 调度标签上点一个绿点，表示服务端定时轮询开着
    const dot = $('#tabRouteDot');
    if (dot) dot.hidden = !(Number(state.settings.creditsRefreshMs) > 0);
  }

  /** 会话列表单独刷新（很轻，不动额度） */
  let lastSessionFetch = 0;
  async function refreshSessions() {
    lastSessionFetch = Date.now();
    try {
      const res = await api('/admin/api/sessions');
      state.sessions = res.sessions || [];
      renderSessions();
    } catch { /* 静默失败，下一轮再试 */ }
  }

  /* ── 调度设置（模式 / 失败阈值 / 额度轮询） ─────────── */
  const MODE_HINT = {
    session: '一个会话固定一个 Key：多开 agent 会话会自动分散到不同 Key，某个 Key 额度用尽或连续失败后，该会话自动换到下一个可用 Key。',
    request: '每次请求都按策略重新选 Key（旧行为），适合无会话概念的简单转发。',
  };

  function setMode(mode, { save = false } = {}) {
    const m = mode === 'request' ? 'request' : 'session';
    state.settings.mode = m;
    $$('#modeSwitch .seg').forEach((b) => b.classList.toggle('is-active', b.dataset.mode === m));
    $('#modeHint').textContent = MODE_HINT[m];
    $('#legacyLb').classList.toggle('hidden', m !== 'request');
    if (save) saveSettings({ mode: m }, '调度模式已切换为' + (m === 'session' ? '会话粘性' : '每次请求'));
  }

  function fillSettings(settings, credits) {
    if (!settings) return;
    state.settings = { ...state.settings, ...settings };
    state.creditsRefresh = credits || state.creditsRefresh;
    setMode(state.settings.mode);
    $('#failThreshold').value = state.settings.failThreshold ?? 3;

    const iv = Number(state.settings.creditsRefreshMs) || 0;
    const preset = $('#pollPreset');
    const custom = $('#pollCustom');
    const known = ['1', '5', '15', '30', '60'];
    const minutes = iv / 60000;
    if (!iv) {
      preset.value = '5';
      custom.value = '';
    } else if (known.includes(String(minutes))) {
      preset.value = String(minutes);
      custom.value = '';
    } else {
      preset.value = 'custom';
      custom.value = String(Math.max(1, Math.round(minutes)));
    }
    $('#pollEnabled').checked = iv > 0;
    $('#autoDisable').checked = !!state.settings.autoDisableExhausted;
    $('#onAllExhausted').value = state.settings.onAllExhausted || 'error';
    syncPollInputs();
    renderPollStatus();
  }

  function syncPollInputs() {
    const on = $('#pollEnabled').checked;
    $('#pollIntervalWrap').style.opacity = on ? '1' : '0.5';
    $('#pollPreset').disabled = !on;
    const custom = $('#pollPreset').value === 'custom';
    $('#pollCustom').disabled = !on || !custom;
    if (!custom) $('#pollCustom').value = '';
  }

  function pollIntervalMsFromForm() {
    if (!$('#pollEnabled').checked) return 0;
    const preset = $('#pollPreset').value;
    const minutes = preset === 'custom'
      ? Math.max(1, Math.min(1440, Number($('#pollCustom').value) || 5))
      : Number(preset) || 5;
    return minutes * 60000;
  }

  async function saveSettings(patch, okMessage) {
    try {
      const res = await api('/admin/api/settings', {
        method: 'PUT',
        body: JSON.stringify(patch),
      });
      if (res?.settings) state.settings = { ...state.settings, ...res.settings };
      if (res?.creditsRefresh) state.creditsRefresh = res.creditsRefresh;
      fillSettings(res.settings, res.creditsRefresh);
      renderStats();
      toast(okMessage || '设置已保存');
    } catch (e) {
      toast(e.message, 'err');
      await reload({ silent: true });
    }
  }

  /** 轮询状态 + 倒计时进度 */
  function renderPollStatus() {
    const box = $('#pollStatus');
    const cr = state.creditsRefresh || {};
    const iv = Number(state.settings.creditsRefreshMs) || 0;
    const on = iv > 0;

    let countdown = '—';
    let pct = 0;
    if (on && cr.nextAt) {
      const remain = Math.max(0, cr.nextAt - serverNow());
      countdown = fmtDuration(remain);
      pct = Math.max(0, Math.min(100, 100 - (remain / iv) * 100));
    }
    const lastTxt = cr.lastAt
      ? `${fmtClock(cr.lastAt)}${cr.lastDurationMs ? ` · 耗时 ${(cr.lastDurationMs / 1000).toFixed(1)}s` : ''}`
      : '尚未刷新';
    const resultTxt = cr.lastAt
      ? (cr.lastFailed ? `${cr.lastOk} 成功 · ${cr.lastFailed} 失败` : `${cr.lastOk} 个 Key 已刷新`)
      : '—';

    box.innerHTML = `
      <div class="ps-head">
        <span class="ps-dot ${on ? (cr.running ? 'warn' : 'on') : ''}"></span>
        <span>${on ? (cr.running ? '正在刷新额度…' : `每 ${fmtDuration(iv)}刷新一次`) : '定时刷新已关闭'}</span>
      </div>
      <div class="ps-bar"><i style="width:${pct}%"></i></div>
      <div class="ps-row"><span>上次刷新</span><b>${lastTxt}</b></div>
      <div class="ps-row"><span>刷新结果</span><b>${resultTxt}</b></div>
      <div class="ps-row"><span>下次刷新</span><b>${on ? countdown + ' 后' : '—'}</b></div>
    `;
  }

  /** 会话分布列表 */
  function renderSessions() {
    const box = $('#sessionList');
    const list = state.sessions || [];
    renderTabBadges();
    if (!list.length) {
      box.innerHTML = `<p class="empty sm">
        <span class="empty-icon" aria-hidden="true">${icon('sliders')}</span>
        <span>还没有会话绑定。等下游发起请求后，这里会显示每个会话正在使用的 Key。</span>
      </p>`;
      return;
    }
    box.innerHTML = list.map((s) => `
      <div class="session-row">
        <div class="sr-top">
          <code title="${escapeHtml(s.id)}">${escapeHtml(s.tag || s.shortId)}</code>
          <span class="sr-key">${escapeHtml(s.keyLabel || '—')}</span>
        </div>
        <div class="sr-meta">
          <span>使用中 ${fmtAgo(s.lastAt)}</span>
          ${s.failures ? `<span class="sep">·</span><span>连败 ${s.failures}</span>` : ''}
          <span class="sep">·</span><span>绑定 ${fmtAgo(s.boundAt)}</span>
        </div>
      </div>`).join('');
  }

  /* ── 额度明细 ────────────────────────────────────────── */
  function renderCreditsDetail(label, c) {
    const box = $('#creditsDetail');
    if (!c) {
      box.innerHTML = `<p class="empty sm"><span class="empty-icon">${icon('credits')}</span><span>暂无额度数据。</span></p>`;
      return;
    }
    if (c.error) {
      box.innerHTML = `<p class="empty sm">
        <span class="empty-icon">${icon('alert')}</span>
        <b>额度查询失败</b>
        <span>${escapeHtml(c.error)}</span>
      </p>`;
      return;
    }

    const worst = clampPct(c.worstPct);
    const wins = (c.windows || []).map((w) => {
      const pct = clampPct(w.pct);
      const resets = w.resetsAt ? `<span class="sep">·</span><span>重置 ${fmtCountdown(w.resetsAt)}</span>` : '';
      return `<div class="win lvl-${levelOf(pct)}">
        <div class="win-top">
          <span class="win-name">${escapeHtml(WIN_NAMES[w.name] || w.name)}</span>
          <span class="win-pct">${pct}%</span>
        </div>
        <div class="window-bar"><i style="width:${pct}%"></i></div>
        <div class="win-foot"><span>已用 ${fmtNum(w.used)} / ${fmtNum(w.cap)}</span>${resets}</div>
      </div>`;
    }).join('');

    box.innerHTML = `
      <div class="cred-head">
        <div class="who">
          <strong title="${escapeHtml(label)}">${escapeHtml(label)}</strong>
          ${c.plan ? `<span class="badge">${escapeHtml(c.plan)}</span>` : ''}
        </div>
        <span class="when">更新于 ${fmtTime(c.fetchedAt)}（${fmtAgo(c.fetchedAt)}）</span>
      </div>

      <div class="cred-total lvl-${levelByRemaining(100 - worst)}">
        <span class="ring-lg" style="--p:${100 - worst}" title="窗口额度剩余 ${100 - worst}%"><span><b>${100 - worst}%</b><small>剩余额度</small></span></span>
        <div class="cred-nums">
          <div class="cred-remaining"><span>剩余额度</span><strong>${fmtNum(c.creditsRemaining)}</strong></div>
          <div class="cred-pills">
            <span class="cred-pill">月度 <b>${fmtNum(c.credits?.monthly)}</b></span>
            <span class="cred-pill">购买 <b>${fmtNum(c.credits?.purchased)}</b></span>
            <span class="cred-pill">免费 <b>${fmtNum(c.credits?.free)}</b></span>
          </div>
        </div>
      </div>

      <div class="wins">${wins || `<p class="empty sm"><span>无窗口限额数据</span></p>`}</div>
    `;
  }

  /* ── 确认弹窗 ────────────────────────────────────────── */
  const confirmDlg = $('#dlgConfirm');
  let confirmResolve = null;

  function askConfirm(title, message, okText = '删除') {
    $('#confirmTitle').textContent = title;
    $('#confirmMsg').innerHTML = message;
    $('#confirmOk').textContent = okText;
    return new Promise((resolve) => {
      confirmResolve = resolve;
      confirmDlg.showModal();
    });
  }
  $('#confirmCancel').addEventListener('click', () => confirmDlg.close('cancel'));
  confirmDlg.addEventListener('close', () => {
    const resolve = confirmResolve;
    confirmResolve = null;
    if (resolve) resolve(confirmDlg.returnValue === 'ok');
  });
  $('#confirmOk').addEventListener('click', () => confirmDlg.close('ok'));

  /* ── Key 表单弹窗 ────────────────────────────────────── */
  const keyDlg = $('#dlgKey');

  function openAdd() {
    $('#dlgTitle').textContent = '添加 Key';
    $('#dlgSub').innerHTML = '录入一个 <code>user_*</code> Key，加入分发池。';
    $('#editId').value = '';
    $('#keyLabel').value = '';
    $('#keyValue').value = '';
    $('#keyValue').disabled = false;
    $('#wrapKeyValue').classList.remove('hidden');
    $('#keyWeight').value = '1';
    $('#keyPriority').value = '0';
    $('#keyEnabled').checked = true;
    $('#keyDefault').checked = false;
    keyDlg.showModal();
    setTimeout(() => $('#keyLabel').focus(), 30);
  }

  function openEdit(k) {
    $('#dlgTitle').textContent = '编辑 Key';
    $('#dlgSub').textContent = '修改标签、权重、优先级与启用状态。';
    $('#editId').value = k.id;
    $('#keyLabel').value = k.label || '';
    $('#keyValue').value = k.keyPrefix || '';
    $('#keyValue').disabled = true;
    $('#wrapKeyValue').classList.add('hidden');
    $('#keyWeight').value = String(k.weight ?? 1);
    $('#keyPriority').value = String(k.priority ?? 0);
    $('#keyEnabled').checked = !!k.enabled;
    $('#keyDefault').checked = !!k.isDefault;
    keyDlg.showModal();
    setTimeout(() => $('#keyLabel').focus(), 30);
  }

  const closeKeyDlg = () => keyDlg.close();
  $('#dlgClose').addEventListener('click', closeKeyDlg);
  $('#dlgCancel').addEventListener('click', closeKeyDlg);
  $('#btnAdd').addEventListener('click', openAdd);

  /* ── 加载 ────────────────────────────────────────────── */
  async function reload({ silent = false } = {}) {
    if (!silent && !state.keys.length) {
      state.loading = true;
      renderKeys();
    }
    try {
      const data = await api('/admin/api/keys');
      state.keys = data.keys || [];
      state.loading = false;
      state.sessions = data.sessions || [];
      if (typeof data.serverTime === 'number') state.serverSkew = data.serverTime - Date.now();
      fillLb(data.lb);
      fillSettings(data.settings, data.creditsRefresh);
      $('#apiBase').textContent = data.apiBase || 'api';
      $('#apiBase').title = data.apiBase || 'api';
      renderAll();
      renderSessions();
      $('#lastSync').textContent = `数据同步于 ${fmtClock(Date.now())}`;
    } catch (e) {
      state.loading = false;
      renderKeys();
      toast('加载失败：' + e.message, 'err');
    }
  }

  /* ── 统计按钮 ────────────────────────────────────────── */
  $('#btnReload').addEventListener('click', async () => {
    const btn = $('#btnReload');
    btn.classList.add('loading');
    await reload({ silent: true });
    btn.classList.remove('loading');
    toast('数据已刷新');
  });

  $('#btnRefreshCredits').addEventListener('click', async () => {
    const btn = $('#btnRefreshCredits');
    btn.disabled = true;
    btn.classList.add('loading');
    try {
      const res = await api('/admin/api/keys/refresh-credits', { method: 'POST' });
      if (res?.creditsRefresh) state.creditsRefresh = res.creditsRefresh;
      await reload({ silent: true });
      const exhausted = (res?.results || []).filter((r) => r.credits?.error).length;
      toast(exhausted ? `额度已刷新（${exhausted} 个查询失败）` : '全部额度已刷新');
    } catch (e) {
      toast(e.message, 'err');
    } finally {
      btn.disabled = false;
      btn.classList.remove('loading');
    }
  });

  /* ── 工具条 ──────────────────────────────────────────── */
  const searchInput = $('#keySearch');

  searchInput.addEventListener('input', () => {
    state.query = searchInput.value;
    $('#btnClearSearch').classList.toggle('hidden', !state.query);
    renderKeys();
  });
  searchInput.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      searchInput.value = '';
      state.query = '';
      $('#btnClearSearch').classList.add('hidden');
      renderKeys();
      searchInput.blur();
    }
  });
  $('#btnClearSearch').addEventListener('click', () => {
    searchInput.value = '';
    state.query = '';
    $('#btnClearSearch').classList.add('hidden');
    renderKeys();
    searchInput.focus();
  });

  $('#filterChips').addEventListener('click', (e) => {
    const btn = e.target.closest('.seg');
    if (!btn) return;
    state.filter = FILTERS.includes(btn.dataset.filter) ? btn.dataset.filter : 'all';
    renderKeys();
  });

  $('#keySort').addEventListener('change', () => {
    state.sort = $('#keySort').value;
    renderKeys();
  });

  // Ctrl/Cmd+K 聚焦搜索
  const isMac = /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent);
  const kbdHint = document.querySelector('.search-kbd');
  if (kbdHint && isMac) kbdHint.textContent = '⌘ K';
  document.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      searchInput.focus();
      searchInput.select();
    }
  });

  /* ── 调度 / 轮询 / 会话 ──────────────────────────────── */
  $('#lbStrategy').addEventListener('change', () => {
    $('#stickyByWrap').style.display = $('#lbStrategy').value === 'sticky' ? '' : 'none';
    updateStrategyHint();
  });

  $('#modeSwitch').addEventListener('click', (e) => {
    const btn = e.target.closest('.seg[data-mode]');
    if (!btn || btn.dataset.mode === state.settings.mode) return;
    setMode(btn.dataset.mode, { save: true });
  });

  $('#lbForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const btn = e.submitter || $('#lbForm button[type="submit"]');
    btn.disabled = true;
    try {
      const saved = await api('/admin/api/lb', {
        method: 'PUT',
        body: JSON.stringify({
          strategy: $('#lbStrategy').value,
          stickyBy: $('#lbStickyBy').value,
          maxRetries: Number($('#lbMaxRetries').value) || 0,
          cooldownMs: Number($('#lbCooldownMs').value) || 0,
        }),
      });
      if (saved) state.lb = saved;
      await saveSettings({
        mode: $('#modeSwitch .seg.is-active')?.dataset.mode || 'session',
        failThreshold: Number($('#failThreshold').value) || 3,
      }, '调度设置已保存');
    } catch (err) {
      toast(err.message, 'err');
    } finally {
      btn.disabled = false;
    }
  });

  // 轮询表单
  $('#pollEnabled').addEventListener('change', syncPollInputs);
  $('#pollPreset').addEventListener('change', syncPollInputs);

  $('#pollForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const btn = e.submitter || $('#pollForm button[type="submit"]');
    btn.disabled = true;
    try {
      await saveSettings({
        creditsRefreshMs: pollIntervalMsFromForm(),
        autoDisableExhausted: $('#autoDisable').checked,
        onAllExhausted: $('#onAllExhausted').value,
      }, $('#pollEnabled').checked
        ? `已按每 ${fmtDuration(pollIntervalMsFromForm())} 自动刷新额度`
        : '已关闭定时刷新额度');
    } finally {
      btn.disabled = false;
    }
  });

  $('#btnClearSessions').addEventListener('click', async () => {
    if (!state.sessions.length) {
      toast('当前没有会话绑定');
      return;
    }
    const ok = await askConfirm(
      '清空会话绑定？',
      `当前 <b>${state.sessions.length}</b> 个会话的 Key 绑定会被清掉，它们的下一次请求将重新分配到最空闲的 Key。`,
      '清空',
    );
    if (!ok) return;
    try {
      await api('/admin/api/sessions', { method: 'DELETE' });
      await reload({ silent: true });
      toast('会话绑定已清空');
    } catch (err) {
      toast(err.message, 'err');
    }
  });

  // 每秒心跳：轮询倒计时、到点后补一次状态同步、会话页停留时自动刷新会话列表
  let lastStaleSync = 0;
  setInterval(() => {
    const iv = Number(state.settings.creditsRefreshMs) || 0;
    if (iv > 0) {
      renderPollStatus();
      // 已经过了预定的刷新时间，但本地快照还没跟上 → 静默拉一次（带 15s 冷却，避免刷新期间反复触发）
      const cr = state.creditsRefresh;
      if (cr?.nextAt && serverNow() > cr.nextAt + 3000 && cr.lastAt < cr.nextAt && Date.now() - lastStaleSync > 15000) {
        lastStaleSync = Date.now();
        reload({ silent: true });
      }
    }
    // 停在会话页时自动刷新（只查会话，不动额度）
    if (state.tab === 'sessions' && Date.now() - lastSessionFetch > 15000) {
      refreshSessions();
    }
  }, 1000);

  /* ── 保存 Key ────────────────────────────────────────── */
  $('#keyForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const editId = $('#editId').value;
    const submitBtn = $('#keyForm button[type="submit"]');
    const payload = {
      label: $('#keyLabel').value.trim(),
      weight: Number($('#keyWeight').value) || 0,
      priority: Number($('#keyPriority').value) || 0,
      enabled: $('#keyEnabled').checked,
      default: $('#keyDefault').checked,
    };

    if (!editId) {
      const key = $('#keyValue').value.trim();
      if (!/^user_[A-Za-z0-9_-]+$/.test(key)) {
        toast('Key 格式不正确，需匹配 ^user_[A-Za-z0-9_-]+$', 'err');
        $('#keyValue').focus();
        return;
      }
      payload.key = key;
    }

    submitBtn.disabled = true;
    try {
      if (editId) {
        await api(`/admin/api/keys/${editId}`, { method: 'PATCH', body: JSON.stringify(payload) });
        toast('已保存');
      } else {
        await api('/admin/api/keys', { method: 'POST', body: JSON.stringify(payload) });
        toast('Key 已添加');
      }
      closeKeyDlg();
      await reload({ silent: true });
    } catch (err) {
      toast(err.message, 'err');
    } finally {
      submitBtn.disabled = false;
    }
  });

  /* ── 复制 ────────────────────────────────────────────── */
  async function copyText(text) {
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
        return true;
      }
    } catch { /* 回退方案 */ }
    try {
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.setAttribute('readonly', '');
      ta.style.cssText = 'position:fixed;top:-1000px;opacity:0';
      document.body.appendChild(ta);
      ta.select();
      const ok = document.execCommand('copy');
      document.body.removeChild(ta);
      return ok;
    } catch { return false; }
  }

  /* ── 列表交互 ────────────────────────────────────────── */
  $('#keyList').addEventListener('click', async (e) => {
    // 空状态 / 无结果里的按钮
    const special = e.target.closest('[data-act="reset-filter"], [data-act="add-key"]');
    if (special) {
      if (special.dataset.act === 'add-key') { openAdd(); return; }
      state.query = '';
      state.filter = 'all';
      searchInput.value = '';
      $('#btnClearSearch').classList.add('hidden');
      renderKeys();
      return;
    }

    const btn = e.target.closest('button[data-act]');
    if (!btn) return;
    const card = btn.closest('.key-card');
    const id = card?.dataset?.id;
    if (!id) return;
    const k = state.keys.find((x) => x.id === id);
    if (!k) return;
    const label = k.label || id;
    const act = btn.dataset.act;

    if (act === 'copy') {
      const ok = await copyText(k.id);
      toast(ok ? `已复制 Key ID：${k.id}` : '复制失败，请手动选择', ok ? 'ok' : 'err');
      return;
    }

    if (act === 'del') {
      const ok = await askConfirm(
        '删除这个 Key？',
        `将把「<b>${escapeHtml(label)}</b>」从分发池中永久移除，此操作不可撤销。`,
        '删除',
      );
      if (!ok) return;
      card.classList.add('is-busy');
      try {
        await api(`/admin/api/keys/${id}`, { method: 'DELETE' });
        toast('已删除');
        await reload({ silent: true });
      } catch (err) {
        card.classList.remove('is-busy');
        toast(err.message, 'err');
      }
      return;
    }

    if (act === 'credits') {
      state.busy.add(id);
      btn.classList.add('loading');
      renderKeys();
      try {
        const c = await api(`/admin/api/keys/${id}/credits`);
        state.busy.delete(id);
        renderCreditsDetail(label, c);
        await reload({ silent: true });
        toast('额度已更新');
        if (window.innerWidth < 1080) {
          $('#creditsDetail').scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
      } catch (err) {
        state.busy.delete(id);
        renderKeys();
        btn.classList.remove('loading');
        toast(err.message, 'err');
      }
      return;
    }

    if (act === 'edit') { openEdit(k); return; }

    if (act === 'default') {
      const next = !k.isDefault;
      card.classList.add('is-busy');
      try {
        await api(`/admin/api/keys/${id}`, {
          method: 'PATCH',
          body: JSON.stringify({ default: next }),
        });
        toast(next ? `已设为默认：${label}` : '已取消默认 Key');
        await reload({ silent: true });
      } catch (err) {
        card.classList.remove('is-busy');
        toast(err.message, 'err');
      }
    }
  });

  /* 快捷启停 */
  $('#keyList').addEventListener('change', async (e) => {
    const input = e.target.closest('input[data-act="toggle"]');
    if (!input) return;
    const card = input.closest('.key-card');
    const id = card?.dataset?.id;
    if (!id) return;
    input.disabled = true;
    try {
      await api(`/admin/api/keys/${id}`, {
        method: 'PATCH',
        body: JSON.stringify({ enabled: input.checked }),
      });
      toast(input.checked ? '已启用' : '已停用');
      await reload({ silent: true });
    } catch (err) {
      input.checked = !input.checked;
      input.disabled = false;
      toast(err.message, 'err');
    }
  });

  // 点击卡片空白处不触发任何操作，但 Enter 在按钮上由浏览器原生处理
  setTab(initialTab(), { save: false });
  reload();
})();
