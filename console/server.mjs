#!/usr/bin/env node
/**
 * console/server.mjs —— 形态 B：带 UI 的反代（多文件、零 npm 依赖、零构建）
 *
 * 用法：
 *   node console/server.mjs          # 等价于 `node proxy.mjs`，额外挂上 /console 与 /api/console
 *
 * 关键设计（见 HANDOFF-webui.md）：
 *  - **同进程**：直接 import 核心单例。设备伪装面板要读的 keyStateStore / sessionStore
 *    是模块级单例，只有跑在同一个进程里才拿得到真数据（D3 / §5 坑 1）。
 *  - **不复制协议**：只 import，不重写任何信封 / 伪装 / 流式逻辑（D4）。
 *  - **只读**（Q1）：console 路由只接受 GET，不写回 config.json，不触发重报指纹。
 *  - **回环限定**（Q2）：UI 与 /api/console 只响应 127.0.0.1 / ::1 的请求。
 *    CFG.host 默认是 0.0.0.0（要给局域网/容器用），所以这里**不能靠绑定地址**来收口，
 *    而是在路由层按 remoteAddress 拒绝非回环来源（403）。
 *  - 上游错误信息是**不可信输入**：UI 侧一律用 textContent 渲染，绝不用 innerHTML。
 */

import { readFileSync } from 'fs';
import { createHash } from 'crypto';
import { dirname } from 'path';
import { fileURLToPath } from 'url';

import {
  server, CFG, log, hooks,
  keyStateStore, sessionStore,
  MODELS, DEVICE_PROFILE,
  CC_VERSION, CC_PROTOCOL_VERSION,
  MAX_INFLIGHT, inflightCount,
} from '../proxy.mjs';

import * as stats from './stats.mjs';

// ── 账号池插件栈（可选）────────────────────────────────────────
// 只有设了 CC_ADMIN_PASSWORD 才启用；用**动态 import** 是为了让没装池的场景
// 在 Node < 22.5（没有 node:sqlite）上照样能起 console，而不是顶层 import 就崩。
// 池完全通过 proxy.mjs 导出的 hooks 接入 —— 核心不认识它。
let POOL = null;   // { db, pool, plugin, auth, access, quota, poller }

async function enablePool() {
  const [dbm, crypto, poolm, pluginm, authm, accessm, quotam] = await Promise.all([
    import('./db.mjs'), import('./crypto.mjs'), import('./pool.mjs'),
    import('./plugin.mjs'), import('./auth.mjs'), import('./access.mjs'), import('./quota.mjs'),
  ]);
  const { openDb, defaultDbPath, settings: kv } = dbm;
  const dir = dirname(fileURLToPath(import.meta.url));
  const db = openDb(defaultDbPath(dir));
  // 盐必须先落地再派生 KEK。反过来的话首次运行会用空盐派生、随后又写入新盐，
  // 下次启动盐变了 → KEK 变了 → 之前加密的账号全部解不开。
  let salt = kv.get(db, 'kek_salt', '');
  if (!salt) {
    salt = crypto.newSalt().toString('base64');
    kv.set(db, 'kek_salt', salt);
  }
  const kek = crypto.deriveKek(process.env.CC_ADMIN_PASSWORD, salt);
  const pool = poolm.createPool({ db, kek });
  const auth = authm.createAuth({ passphrase: process.env.CC_ADMIN_PASSWORD });
  const access = accessm.createAccess({ db });
  const quota = quotam.createQuota({ db, apiBase: CFG.apiBase });
  const plugin = pluginm.createPoolPlugin({ pool, hooks, access, quota, log });
  plugin.install();

  // P4「主动轮询」：定期把每个账号的额度拉回来（best-effort，unref 不影响退出）
  const refreshAll = async () => {
    for (const a of pool.listAccounts()) {
      const key = pool.revealKey(a.keyHash);
      if (key) await quota.refresh(a.keyHash, key).catch(() => {});
    }
  };
  const poller = setInterval(refreshAll, 10 * 60_000);
  if (poller.unref) poller.unref();
  setTimeout(refreshAll, 1500).unref?.();

  return { db, pool, plugin, auth, access, quota, poller, refreshAll, settings: kv };
}

// ── 冷却到点唤醒（punctual recovery）──────────────────────────
// 冷却的正确性不依赖唤醒：选号时比 cooldown_until，到点自然回候选。唤醒解决的是另外两件事：
//   1) 到点那一刻先问上游"到底恢复没有"，再决定放行还是续期 —— 否则 60s 兜底 / 账期这种
//      "猜出来的时间"一到就放行，下一个请求立刻又撞限流，来回抖；
//   2) 到点那一刻把额度刷新、状态清干净，控制台上的卡片和数字同时更新，不用等下一个请求。
// 定时器**对准最近的恢复时刻**（不是每 N 秒轮询），所以"准时"是真的准时。
const WAKE_CAP_MS = 24 * 3600_000;        // 单次 setTimeout 上限，更远的冷却分段唤醒
const WAKE_TICK_MS = Math.max(1000, Number.parseInt(process.env.CC_POOL_WAKE_TICK_MS ?? '', 10) || 60_000);
let wakeTimer = null;
let wakeFloor = 0;                        // 上次扫描的时间下界：每个到期时刻只复核一次
let wakeArmed = false;

/** 复核所有"上次扫描之后到点"的冷却。返回处理条数 */
async function sweepExpiredCooldowns() {
  const from = wakeFloor;
  wakeFloor = Date.now();
  const rows = POOL.pool.listExpiredCooldowns(from);
  for (const r of rows) {
    const key = POOL.pool.revealKey(r.keyHash);
    let verdict = null;
    if (key) {
      try {
        await POOL.quota.refresh(r.keyHash, key);
        verdict = POOL.quota.stillExhausted(r.keyHash);
      } catch (e) {
        log('warn', 'cooldown wake: quota re-check failed', { hint: r.hint, error: e.message });
      }
    }
    if (verdict) {
      POOL.pool.settleCooldown(r.keyHash, { recovered: false, untilMs: verdict.until, reason: 'rate_limit', note: `${verdict.window} 仍未恢复` });
      log('info', 'cooldown hit deadline but upstream still limited', { hint: r.hint, window: verdict.window, until: new Date(verdict.until).toISOString() });
    } else {
      POOL.pool.settleCooldown(r.keyHash, { recovered: true, note: key ? '到点复核：上游已恢复' : '到点自动恢复（拿不到明文 key，未复核）' });
      log('info', 'cooldown expired, account back in rotation', { hint: r.hint });
    }
  }
  return rows.length;
}

/** 把定时器对准最近的恢复时刻 */
function armWake() {
  if (wakeTimer) { clearTimeout(wakeTimer); wakeTimer = null; }
  if (!POOL) return;
  const a = POOL.pool.availability();
  if (!a.cooling || a.nextRecoverMs <= 0) return;
  const delay = Math.max(200, Math.min(a.nextRecoverMs, WAKE_CAP_MS));
  wakeTimer = setTimeout(() => {
    wakeTimer = null;
    sweepExpiredCooldowns().catch((e) => log('warn', 'cooldown wake failed', { error: e.message })).finally(armWake);
  }, delay);
  if (wakeTimer.unref) wakeTimer.unref();
}

/** 一次完整的唤醒周期：先把"已经到点但没被定时器覆盖"的补上，再对准下一个时刻 */
function wakeTick() {
  if (!wakeArmed) return;
  Promise.resolve()
    .then(sweepExpiredCooldowns)
    .catch((e) => log('warn', 'cooldown sweep failed', { error: e.message }))
    .finally(armWake);
}

function startWake() {
  if (wakeArmed || !POOL) return;
  if (process.env.CC_POOL_WAKE === '0') { log('info', 'cooldown wake disabled by CC_POOL_WAKE=0'); return; }
  wakeArmed = true;
  wakeFloor = Date.now();
  // 新建冷却时立刻对准它的到期时刻。池没有事件回调可用，而调度属于 console 层的职责，
  // 所以在这里包一层（插件拿的就是同一个对象），比让池去认识"定时器"要干净。
  const markCooldown = POOL.pool.markCooldown;
  POOL.pool.markCooldown = (...args) => { const r = markCooldown(...args); wakeTick(); return r; };
  const settleCooldown = POOL.pool.settleCooldown;
  POOL.pool.settleCooldown = (...args) => { const r = settleCooldown(...args); wakeTick(); return r; };
  wakeTick();
  const tick = setInterval(wakeTick, WAKE_TICK_MS);
  if (tick.unref) tick.unref();
}

if (process.env.CC_ADMIN_PASSWORD) {
  try {
    POOL = await enablePool();
    log('info', 'Account pool enabled', {
      accounts: POOL.pool.availability().total,
      accessMode: POOL.access.mode(),
      db: process.env.CC_POOL_DB || '(default console/pool.db)',
    });
    startWake();                       // 冷却到点唤醒：必须等 POOL 挂上之后
  } catch (e) {
    log('error', 'Failed to enable account pool, continuing in single-key mode', { error: e.message });
    POOL = null;
  }
}

// ── 常量 ────────────────────────────────────────────
const UI_PATH = '/console';
const API_PREFIX = '/api/console';
const SNIFF_MAX_BYTES = 4096;
const POLL_MS = 2000; // UI 轮询间隔（与 ui.html 保持一致，仅用于文案）

const UI_HTML = readFileSync(new URL('./ui.html', import.meta.url), 'utf8');

// 响应头：UI 是纯本地只读页，禁掉一切外部资源与外联，防止错误信息里夹带的东西回传
const SECURITY_HEADERS = {
  'Cache-Control': 'no-store',
  'X-Content-Type-Options': 'nosniff',
  'Referrer-Policy': 'no-referrer',
  'Content-Security-Policy':
    "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; form-action 'none'; base-uri 'none'",
};

// ── 小工具 ──────────────────────────────────────────

function mask(v) {
  if (typeof v !== 'string' || !v) return v;
  if (v.length <= 8) return `****(len ${v.length})`;
  return `${v.slice(0, 4)}…${v.slice(-2)}(len ${v.length})`;
}

const SECRET_KEY_RE = /(key|token|secret|password|salt)/i;

/** 配置回显：凡是名字里带 key/token/secret/password/salt 的字段一律打码 */
function redactConfig(cfg) {
  const out = {};
  for (const [k, v] of Object.entries(cfg)) out[k] = SECRET_KEY_RE.test(k) ? mask(v) : v;
  return out;
}

function shortDigest(obj) {
  try {
    return createHash('sha256').update(JSON.stringify(obj)).digest('hex').slice(0, 12);
  } catch {
    return null;
  }
}

/** 回环判定：绑 0.0.0.0 时这是 UI 唯一的访问控制（Q2） */
function isLoopback(req) {
  const addr = req.socket?.remoteAddress || '';
  if (addr === '::1') return true;
  // IPv4-mapped IPv6（绑 IPv6 socket 时 Node 会给 ::ffff:127.0.0.1）
  const v4 = addr.startsWith('::ffff:') ? addr.slice(7) : addr;
  // 整个 127.0.0.0/8 都是回环，不只是 127.0.0.1（127.0.0.2 等别名也算本机）
  const m = /^127\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(v4);
  return !!m && Number(m[1]) <= 255 && Number(m[2]) <= 255 && Number(m[3]) <= 255;
}

function isConsolePath(pathname) {
  return pathname === UI_PATH ||
    pathname.startsWith(UI_PATH + '/') ||
    pathname === API_PREFIX ||
    pathname.startsWith(API_PREFIX + '/');
}

function sendJSON(res, status, data, extraHeaders = null) {
  const body = JSON.stringify(data, null, 2);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    ...SECURITY_HEADERS,
    ...(extraHeaders || {}),
  });
  res.end(body);
}

// ── 账号池管理 API（只在 POOL 启用时存在；写口一律要会话） ─────────
function poolStatus() {
  if (!POOL) return { enabled: false };
  const a = POOL.pool.availability();
  return {
    enabled: true,
    mode: a,
    accessMode: POOL.access.mode(),
    sticky: POOL.pool.stickySize(),
    retryMax: POOL.plugin.maxRetries(),
    db: process.env.CC_POOL_DB || 'console/pool.db',
  };
}

/**
 * 统一鉴权闸门（全局）。
 *   设了 CC_ADMIN_PASSWORD  → 整个 /api/console/* 都要管理会话（只有登录端点本身公开）
 *   没设                    → 没有口令可验，退回「仅回环只读」的旧语义：核心只读口放行、其余 501
 * 之前是「读口公开、写口要口令」，那等于把 /overview（含每个 key 的设备指纹）、
 * /config、/errors 全裸在回环上，且两套心智模型；现在统一。
 */
const CORE_READ_PATHS = new Set(['/overview', '/device', '/stats', '/errors', '/config', '/models', '/health']
  .map((s) => API_PREFIX + s));

function requireSession(req, res, p = '') {
  const read = req.method === 'GET' || req.method === 'HEAD';
  if (!POOL) {
    // 没启用池：核心自己的只读路由照旧放行（仅回环），账号池相关的口一律 501（它们本来也不存在）
    if (read && CORE_READ_PATHS.has(p)) return true;
    sendJSON(res, 501, { error: 'pool_disabled', message: '设置 CC_ADMIN_PASSWORD 后才会启用账号池与写口' });
    return false;
  }
  if (!POOL.auth.check(POOL.auth.tokenOf(req))) {
    sendJSON(res, 401, { error: 'unauthorized', message: '控制台需要管理口令：先 POST /api/console/auth' });
    return false;
  }
  return true;
}

/** 读取 JSON 请求体（上限 64KB —— 这里只收小对象，收不到就报错） */
function readJson(req, limit = 65536) {
  return new Promise((resolve, reject) => {
    let n = 0; const chunks = [];
    req.on('data', (c) => {
      n += c.length;
      if (n > limit) { reject(new Error('body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      if (!chunks.length) return resolve({});
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
      catch (e) { reject(new Error('invalid JSON body')); }
    });
    req.on('error', reject);
  });
}

const accountView = (a) => {
  const q = POOL.quota.get(a.keyHash);
  return {
    keyHash: a.keyHash, hint: a.hint, label: a.label,
    enabled: a.enabled === 1, priority: a.priority, weight: a.weight, createdAt: a.createdAt,
    cooldownUntil: a.cooldownUntil, cooldownReason: a.cooldownReason,
    bannedAt: a.bannedAt, banReason: a.banReason, failStreak: a.failStreak,
    requests: a.requests, successes: a.successes, errors: a.errors, lastUsed: a.lastUsed,
    state: a.bannedAt ? 'banned' : (a.cooldownUntil > Date.now() ? 'cooling' : (a.enabled ? 'ready' : 'disabled')),
    quota: q ? {
      planId: q.planId, monthlyLeft: q.monthlyLeft, periodEnd: q.periodEnd,
      fiveHour: q.fiveHour, weekly: q.weekly, monthly: q.monthly, exceeded: q.exceeded,
      checkedAt: q.checkedAt, stale: q.stale, lastError: q.lastError,
    } : null,
  };
};

async function handleAdmin(req, res, url) {
  const p = url.pathname;
  const m = req.method;

  // 登录不需要会话（其余写口都要）
  if (p === API_PREFIX + '/auth' && m === 'POST') {
    if (!POOL) return sendJSON(res, 501, { error: 'pool_disabled', message: '未启用账号池' });
    let body;
    try { body = await readJson(req); } catch (e) { return sendJSON(res, 400, { error: 'bad_body', message: e.message }); }
    const verdict = POOL.auth.verify(body.password);
    if (!verdict.ok) {
      log('warn', 'console auth failed', { locked: !!verdict.locked, remaining: verdict.remaining });
      return sendJSON(res, 401, {
        error: verdict.locked ? 'locked' : 'bad_password',
        message: verdict.locked ? `口令错误次数过多，请 ${Math.ceil(verdict.retryAfterMs / 1000)}s 后重试` : '口令不正确',
        retryAfterMs: verdict.retryAfterMs || 0,
      });
    }
    const s = POOL.auth.issue();
    log('info', 'console session issued', { expiresAt: new Date(s.expiresAt).toISOString() });
    return sendJSON(res, 200, { ok: true, expiresAt: s.expiresAt }, {
      // HttpOnly + SameSite=Strict：JS 读不到，CSRF 也带不出去
      'Set-Cookie': `cc_admin=${encodeURIComponent(s.token)}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${Math.floor(POOL.auth.ttlMs / 1000)}`,
    });
  }

  // 读类：列表/审计/准入（也要会话 —— 账号列表属于敏感信息）
  if (!requireSession(req, res, p)) return;

  const hashOf = (prefix) => {
    const seg = p.slice(prefix.length).split('/').filter(Boolean);
    return seg[0] ? decodeURIComponent(seg[0]) : '';
  };

  if (p === API_PREFIX + '/auth' && m === 'DELETE') {
    POOL.auth.revoke(POOL.auth.tokenOf(req));
    return sendJSON(res, 200, { ok: true }, { 'Set-Cookie': 'cc_admin=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0' });
  }

  if (p === API_PREFIX + '/accounts' && m === 'GET') {
    return sendJSON(res, 200, {
      accounts: POOL.pool.listAccounts().map(accountView),
      availability: POOL.pool.availability(),
      accessMode: POOL.access.mode(),
      accessClients: POOL.access.list(),
    });
  }

  if (p === API_PREFIX + '/accounts' && m === 'POST') {
    let body;
    try { body = await readJson(req); } catch (e) { return sendJSON(res, 400, { error: 'bad_body', message: e.message }); }
    if (!body.key || typeof body.key !== 'string') return sendJSON(res, 400, { error: 'bad_key', message: 'key 必填' });
    const r = POOL.pool.addAccount({
      key: body.key.trim(), label: String(body.label || ''),
      priority: Number.isFinite(+body.priority) ? +body.priority : 0,
      weight: Number.isFinite(+body.weight) ? +body.weight : 1,
      enabled: body.enabled !== false,
    });
    log('info', 'account added via console', { hint: r.hint, created: r.created });
    // 加完顺手拉一次额度，UI 立刻有数
    POOL.quota.refresh(r.keyHash, body.key.trim()).catch(() => {});
    return sendJSON(res, 200, { ok: true, ...r });
  }

  if (p.startsWith(API_PREFIX + '/accounts/')) {
    const hash = hashOf(API_PREFIX + '/accounts/');
    const tail = p.slice((API_PREFIX + '/accounts/').length).split('/').filter(Boolean)[1];
    if (!hash) return sendJSON(res, 400, { error: 'bad_hash' });

    if (m === 'PATCH' && !tail) {
      let body;
      try { body = await readJson(req); } catch (e) { return sendJSON(res, 400, { error: 'bad_body', message: e.message }); }
      const ok = POOL.pool.setAccount(hash, body);
      if (!ok) return sendJSON(res, 404, { error: 'not_found' });
      POOL.pool.clearCooldown(hash);
      return sendJSON(res, 200, { ok: true });
    }
    if (m === 'DELETE' && !tail) {
      const ok = POOL.pool.removeAccount(hash);
      return sendJSON(res, ok ? 200 : 404, { ok });
    }
    if (m === 'POST' && tail === 'unban') {
      const ok = POOL.pool.unban(hash);
      return sendJSON(res, ok ? 200 : 404, { ok });
    }
    if (m === 'POST' && tail === 'cooldown') {
      const ok = POOL.pool.clearCooldown(hash);
      return sendJSON(res, ok ? 200 : 404, { ok });
    }
    if (m === 'POST' && tail === 'quota') {
      const key = POOL.pool.revealKey(hash);
      if (!key) return sendJSON(res, 404, { error: 'not_found' });
      const r = await POOL.quota.refresh(hash, key);
      return sendJSON(res, 200, { ok: r.ok, quota: POOL.quota.get(hash), error: r.error || null });
    }
    if (m === 'POST' && tail === 'test') {
      // 连通性测试：拿这个账号打一次 /alpha/whoami（不消耗配额、不动冷却状态）
      const key = POOL.pool.revealKey(hash);
      if (!key) return sendJSON(res, 404, { error: 'not_found' });
      try {
        const all = await POOL.quota.fetchAll(key);
        return sendJSON(res, 200, { ok: true, whoami: all.whoami, credits: all.credits });
      } catch (e) {
        return sendJSON(res, 200, { ok: false, error: e.message });
      }
    }
    return sendJSON(res, 404, { error: 'not_found', path: p });
  }

  if (p === API_PREFIX + '/access' && m === 'GET') {
    return sendJSON(res, 200, { mode: POOL.access.mode(), clients: POOL.access.list() });
  }
  if (p === API_PREFIX + '/access' && m === 'POST') {
    let body;
    try { body = await readJson(req); } catch (e) { return sendJSON(res, 400, { error: 'bad_body', message: e.message }); }
    if (body.mode) {
      try { POOL.access.setMode(body.mode); } catch (e) { return sendJSON(res, 400, { error: 'bad_mode', message: e.message }); }
      return sendJSON(res, 200, { ok: true, mode: POOL.access.mode() });
    }
    if (body.key) {
      const r = POOL.access.add(body.key.trim(), String(body.label || ''));
      return sendJSON(res, 200, { ok: true, ...r });
    }
    return sendJSON(res, 400, { error: 'bad_request' });
  }
  if (p.startsWith(API_PREFIX + '/access/') && m === 'DELETE') {
    const hash = hashOf(API_PREFIX + '/access/');
    const ok = POOL.access.remove(hash);
    return sendJSON(res, ok ? 200 : 404, { ok });
  }

  if (p === API_PREFIX + '/audit' && m === 'GET') {
    const limit = Math.min(500, Math.max(1, Number.parseInt(url.searchParams.get('limit') || '100', 10) || 100));
    const rows = POOL.db.prepare('SELECT at, action, target, outcome, ip FROM audit ORDER BY id DESC LIMIT ?').all(limit);
    return sendJSON(res, 200, { rows });
  }

  // ── 只读数据接口（也在会话闸门之后：/overview 里有每个 key 的设备指纹，不是公开信息）──
  if (p === API_PREFIX + '/overview' && m === 'GET') return sendJSON(res, 200, buildOverview());
  if (p === API_PREFIX + '/device' && m === 'GET') return sendJSON(res, 200, buildDevice());
  if (p === API_PREFIX + '/stats' && m === 'GET') return sendJSON(res, 200, stats.snapshot());
  if (p === API_PREFIX + '/errors' && m === 'GET') return sendJSON(res, 200, stats.snapshot().errors);
  if (p === API_PREFIX + '/config' && m === 'GET') return sendJSON(res, 200, redactConfig(CFG));
  if (p === API_PREFIX + '/models' && m === 'GET') return sendJSON(res, 200, { models: MODELS.map(x => ({ id: x.id, name: x.name || null })) });
  if (p === API_PREFIX + '/health' && m === 'GET') return sendJSON(res, 200, { ok: true, at: Date.now() });

  return sendJSON(res, 404, { error: 'not_found', path: p });
}


// ── 响应嗅探：从响应流里捞上游 error.code ────────────
// 只读观测，不改写任何字节：嗅探在 res.write/res.end 里只做检查，原样透传。
// 非 2xx → 缓冲前 4KB JSON 体；SSE → 只在出现 error 事件时抽一次。
function createSniffer(res) {
  const state = { mode: null, buf: '', err: null };

  const onChunk = (chunk) => {
    if (state.err) return;
    if (state.mode === null) {
      const ct = String(res.getHeader('content-type') || '');
      const isSse = ct.includes('event-stream');
      if (res.statusCode >= 400) state.mode = isSse ? 'skip' : 'json';
      else if (isSse) state.mode = 'sse';
      else state.mode = 'skip';
    }
    if (state.mode === 'skip') return;

    let text = '';
    if (typeof chunk === 'string') text = chunk;
    else if (chunk instanceof Uint8Array) text = Buffer.from(chunk).toString('utf8');
    else return;

    if (state.mode === 'json') {
      if (state.buf.length < SNIFF_MAX_BYTES) state.buf += text.slice(0, SNIFF_MAX_BYTES - state.buf.length);
    } else if (state.mode === 'sse' && text.includes('"type":"error"')) {
      state.err = extractStreamError(text);
    }
  };

  const origWrite = res.write;
  const origEnd = res.end;
  res.write = function (chunk, ...rest) {
    try { onChunk(chunk); } catch { /* 观测失败绝不影响转发 */ }
    return origWrite.apply(this, [chunk, ...rest]);
  };
  res.end = function (chunk, ...rest) {
    if (chunk !== undefined && typeof chunk !== 'function') {
      try { onChunk(chunk); } catch { /* 同上 */ }
    }
    return origEnd.apply(this, [chunk, ...rest]);
  };

  return {
    finalize() {
      if (state.err) return state.err;
      if (state.mode === 'json' && state.buf) {
        try {
          const o = JSON.parse(state.buf);
          const e = o?.error || o;
          return { code: e?.code ?? null, type: e?.type ?? null, message: String(e?.message ?? '') };
        } catch {
          return { code: null, type: null, message: state.buf.slice(0, 200) };
        }
      }
      return null;
    },
  };
}

/** 从 `event: error\ndata: {...}` 的 SSE 分片里抽错误对象 */
function extractStreamError(text) {
  let idx = text.indexOf('data: ');
  while (idx !== -1) {
    const nl = text.indexOf('\n', idx);
    const line = text.slice(idx + 6, nl === -1 ? text.length : nl).trim();
    if (line.includes('"error"')) {
      try {
        const obj = JSON.parse(line);
        if (obj?.error) return { code: obj.error.code ?? null, type: obj.error.type ?? null, message: String(obj.error.message ?? '') };
        if (obj?.type === 'error') return { code: obj.code ?? null, type: obj.type, message: String(obj.message ?? '') };
      } catch { /* 不是完整 JSON 行，跳过 */ }
    }
    if (nl === -1) break;
    idx = text.indexOf('data: ', nl);
  }
  return null;
}

// ── 代理流量：计数 + 透传给核心 ──────────────────────

function handleProxyTraffic(req, res) {
  const t0 = process.hrtime.bigint();
  const path = (() => {
    try { return new URL(req.url, 'http://x').pathname; } catch { return '?'; }
  })();

  // 探活端点不计入统计：容器每 30s 打一次 /health，算进去会把延迟分位与成功率整体带偏
  // （核心自己也在准入控制里豁免了 /health，这里与它保持一致）。
  const counted = path !== '/health' && path !== '/';
  if (!counted) return coreHandler(req, res);

  stats.enter();
  const sniffer = createSniffer(res);

  let settled = false;
  const settle = () => {
    if (settled) return;
    settled = true;
    stats.leave();
    const latencyMs = Number(process.hrtime.bigint() - t0) / 1e6;
    stats.record({
      path,
      status: res.statusCode || 0,
      latencyMs,
      finished: res.writableFinished === true,
      error: sniffer.finalize(),
    });
  };
  res.once('finish', settle);
  res.once('close', settle);

  return coreHandler(req, res);
}

// ── 数据装配（全部只读） ─────────────────────────────

function buildCore() {
  return {
    url: `http://${CFG.host}:${CFG.port}`,
    host: CFG.host,
    // 绑定 0.0.0.0 时给人看 127.0.0.1（console 本来就只认回环，0.0.0.0 对人没意义）
    displayHost: (CFG.host === '0.0.0.0' || CFG.host === '::') ? '127.0.0.1' : CFG.host,
    port: CFG.port,
    apiBase: CFG.apiBase,
    projectSlug: CFG.projectSlug,
    commandCodeVersion: CC_VERSION,
    protocolVersion: CC_PROTOCOL_VERSION,
    protocolDriftNote: CC_VERSION === CC_PROTOCOL_VERSION ? '与已实现方言一致（不跟随 npm）' : '版本已漂移',
    inflight: inflightCount,
    maxInflight: MAX_INFLIGHT,
    cliMode: CFG.cliMode,
    cliSessionMode: CFG.cliSessionMode,
    zdr: !!CFG.zdr,
    emptySystemPlaceholder: !!CFG.emptySystemPlaceholder,
    useProviderModels: !!CFG.useProviderModels,
    modelRefreshIntervalMs: CFG.modelRefreshIntervalMs,
    logLevel: CFG.logLevel,
    logFile: CFG.logFile || null,
    pid: process.pid,
    node: process.version,
    startedAt: stats.snapshot().startedAt,
    pollMs: POLL_MS,
  };
}

function buildDevice() {
  const now = Date.now();
  const keys = [...keyStateStore.entries()].map(([apiKey, state]) => {
    const fp = state?.fingerprint || null;
    const sess = sessionStore.get(apiKey);
    return {
      keyPrefix: `${apiKey.slice(0, 8)}…`,
      keyDigest: shortDigest(apiKey),
      fingerprintDigest: fp ? shortDigest(fp) : null,
      nextInitAt: state?.nextInitAt || 0,
      nextInitInMs: state?.nextInitAt ? Math.max(0, state.nextInitAt - now) : null,
      initialized: !!state?.nextInitAt,
      components: fp?.components || null,
      session: sess ? { sessionId: `${sess.sessionId.slice(0, 8)}…`, expiresInMs: Math.max(0, sess.expiresAt - now) } : null,
    };
  });
  keys.sort((a, b) => (a.keyPrefix < b.keyPrefix ? -1 : 1));
  return {
    keys,
    sessionCount: sessionStore.size,
    keyCount: keyStateStore.size,
    // 伪装档案的“真源”，UI 用来对照每个 key 的 components 是否自洽
    profile: DEVICE_PROFILE,
    componentsOrder: Object.keys(DEVICE_PROFILE || {}),
  };
}

function buildOverview() {
  return {
    core: buildCore(),
    stats: stats.snapshot(),
    device: buildDevice(),
    models: MODELS.map(m => ({ id: m.id, name: m.name || null })),
    config: redactConfig(CFG),
    pool: poolStatus(),
    generatedAt: Date.now(),
  };
}

// ── console 路由 ─────────────────────────────────────

async function handleConsole(req, res, url) {
  // Q2：回环限定。放在最前面 —— 未通过检查的请求连方法/路径都不必知道。
  if (!isLoopback(req)) {
    log('warn', 'Console access denied (non-loopback)', {
      remote: req.socket?.remoteAddress, path: url.pathname,
    });
    sendJSON(res, 403, {
      error: 'console_local_only',
      message: 'Console is bound to loopback only. Open it from 127.0.0.1 (or an SSH tunnel).',
      remote: req.socket?.remoteAddress || null,
    });
    return;
  }

  const p = url.pathname;
  const isApi = p === API_PREFIX || p.startsWith(API_PREFIX + '/');

  // 非 API 路径：只服务 UI 静态外壳。
  // 外壳本身不含任何数据（数据全靠 /api/console/* 取），所以公开 —— 否则连登录框都渲染不出来。
  if (!isApi) {
    if (p === UI_PATH || p === UI_PATH + '/' || p === UI_PATH + '/index.html') {
      if (req.method !== 'GET' && req.method !== 'HEAD') {
        res.writeHead(405, { 'Content-Type': 'application/json; charset=utf-8', Allow: 'GET, HEAD', ...SECURITY_HEADERS });
        res.end(JSON.stringify({ error: 'read_only', message: 'Console shell is read-only.' }));
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', ...SECURITY_HEADERS });
      res.end(UI_HTML);
      return;
    }
    // 其它路径一律 405/404 —— 绝不落到核心的写路径上
    res.writeHead(405, { 'Content-Type': 'application/json; charset=utf-8', Allow: 'GET, HEAD', ...SECURITY_HEADERS });
    res.end(JSON.stringify({ error: 'read_only', message: 'Only GET/HEAD on /console, and /api/console/*.' }));
    return;
  }

  // 登录端点本身公开（否则没法换取会话）
  if (p === API_PREFIX + '/auth' && req.method === 'POST') return handleAdmin(req, res, url);

  // 闸门探针：只回答"要不要口令、当前会话还行不行、池里有几个账号"。
  // 不含任何 key / 指纹 / 配置 —— 目的是让登录页本身不是一片空白，同时不泄漏池内明细。
  if (p === API_PREFIX + '/gate' && req.method === 'GET') {
    const a = POOL ? POOL.pool.availability() : null;
    return sendJSON(res, 200, {
      pool: !!POOL,
      authed: POOL ? POOL.auth.check(POOL.auth.tokenOf(req)) : true,
      total: a ? a.total : 0,
      usable: a ? a.usable : 0,
    }, { 'Cache-Control': 'no-store' });
  }

  // ← 全局鉴权闸门：以下**全部** /api/console/*（读与写）都要会话
  if (!requireSession(req, res, p)) return;
  return handleAdmin(req, res, url);
}

// ── 接管 request 事件 ────────────────────────────────
// 核心把自己的 handler 注册成了唯一的 'request' 监听器（http.createServer(handler)）。
// 这里把它取下来、换成我们的分发器：console 路径自己处理，其余原样交还给核心。
// 这样 proxy.mjs 一行都不用为 UI 让步（R5）。
const listeners = server.listeners('request');
if (listeners.length !== 1 || typeof listeners[0] !== 'function') {
  console.error(`[console] 预期核心 server 上恰好 1 个 'request' 监听器，实际 ${listeners.length} 个 —— 拒绝接管。`);
  process.exit(1);
}
const coreHandler = listeners[0];
server.removeAllListeners('request');
server.on('request', function consoleDispatcher(req, res) {
  let url;
  try {
    url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  } catch {
    url = { pathname: req.url || '/' };
  }

  // 形态 B 的便利：浏览器直接敲 ip:port 时把人送到 UI。
  // 核心的 '/' 是探活端点（返回纯文本 OK），直接访问会让人以为"没页面"。
  // 只对**回环来源**重定向：非回环保持核心原有的 OK，远端探活语义不变。
  if (url.pathname === '/' && (req.method === 'GET' || req.method === 'HEAD') && isLoopback(req)) {
    res.writeHead(302, { Location: UI_PATH, ...SECURITY_HEADERS });
    res.end();
    return;
  }

  if (isConsolePath(url.pathname)) {
    // handleConsole 现在是 async（管理 API 要读请求体），错误必须自己兜住
    return handleConsole(req, res, url).catch((e) => {
      log('error', 'console handler failed', { path: url.pathname, error: e.message });
      if (!res.headersSent) sendJSON(res, 500, { error: 'internal_error', message: e.message });
      else try { res.end(); } catch {}
    });
  }
  return handleProxyTraffic(req, res);
});

// ── 启动 ────────────────────────────────────────────

server.on('error', (e) => {
  if (e.code === 'EADDRINUSE') {
    console.error(`[console] 端口 ${CFG.port} 已被占用（${CFG.host}）。改 config.json 的 port，或设 PORT 环境变量。`);
  } else {
    console.error(`[console] server error: ${e.message}`);
  }
  process.exit(1);
});

server.listen(CFG.port, CFG.host, () => {
  log('info', 'CC Proxy + console started', {
    url: `http://${CFG.host}:${CFG.port}`,
    console: `http://127.0.0.1:${CFG.port}${UI_PATH}`,
    access: 'console loopback-only (127.0.0.1 / ::1); 读口 GET，写口需管理会话',
    pool: POOL
      ? `enabled (${POOL.pool.availability().total} accounts, access=${POOL.access.mode()}, retryMax=${POOL.plugin.maxRetries()})`
      : 'disabled (set CC_ADMIN_PASSWORD to enable multi-account)',
    api: CFG.apiBase,
    models: MODELS.length,
    protocol: CC_PROTOCOL_VERSION,
    maxInflight: MAX_INFLIGHT > 0 ? `${MAX_INFLIGHT} (global, /health exempt)` : 'unlimited (CC_MAX_INFLIGHT=0)',
    pid: process.pid,
  });
  if (CFG.host !== '127.0.0.1' && CFG.host !== 'localhost') {
    log('info', 'Console is not reachable from outside this machine even though the proxy binds elsewhere', {
      bindHost: CFG.host,
      hint: 'UI 只认回环来源；要从别的机器看，用 SSH 端口转发到 127.0.0.1',
    });
  }
});
