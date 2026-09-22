/**
 * Command Code → OpenAI 兼容代理
 * 基于真实 CLI 流量抓包数据构建
 */
import http from 'http';
import crypto from 'crypto';
import { randomUUID } from 'crypto';
import { readFileSync, existsSync, appendFileSync, writeFileSync, renameSync, mkdirSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

// ── 配置加载 ──────────────────────────────────────
const __dirname = dirname(fileURLToPath(import.meta.url));

function loadConfig() {
  const defaults = {
    port: 3000,
    host: '0.0.0.0',
    apiBase: 'https://api.commandcode.ai',
    projectSlug: 'cc-proxy',
    logFile: '',
    logLevel: 'info',
    useProviderModels: true,
    modelRefreshIntervalMs: 5 * 60 * 1000,  // 5 minutes
    zdr: false,
    emptySystemPlaceholder: true, // 无 system prompt 时发空格占位，阻止 CC 上游注入 ~7.5K token 默认提示词（issue #17）
  };

  const configPath = resolve(__dirname, 'config.json');
  if (existsSync(configPath)) {
    try {
      const user = JSON.parse(readFileSync(configPath, 'utf-8'));
      Object.assign(defaults, user);
    } catch (e) {
      console.error('[config] Failed to parse config.json:', e.message);
    }
  }

  // 环境变量覆写
  if (process.env.PORT) defaults.port = parseInt(process.env.PORT);
  if (process.env.HOST) defaults.host = process.env.HOST;
  if (process.env.CC_API_BASE) defaults.apiBase = process.env.CC_API_BASE;
  if (process.env.PROJECT_SLUG) defaults.projectSlug = process.env.PROJECT_SLUG;
  if (process.env.LOG_FILE) defaults.logFile = process.env.LOG_FILE;
  if (process.env.CC_USE_PROVIDER_MODELS) defaults.useProviderModels = process.env.CC_USE_PROVIDER_MODELS !== 'false';
  if (process.env.CMD_ZDR !== undefined) defaults.zdr = process.env.CMD_ZDR === '1';
  if (process.env.CC_EMPTY_SYSTEM_PLACEHOLDER) defaults.emptySystemPlaceholder = process.env.CC_EMPTY_SYSTEM_PLACEHOLDER !== 'false';

  return defaults;
}

const CFG = loadConfig();

// ── Key 池 + 负载均衡 ─────────────────────────────────
// keys.json（gitignore）：{ lb: {...}, keys: [{ id, label, key, weight, enabled, priority, ... }] }
// 客户端 header 里的 user_* 仍可直通（BYOK）；未带 key 时从池里按策略选取。

// 默认放在 proxy.mjs 同目录；容器里用 CC_KEYS_FILE 指到挂载卷（如 /app/data/keys.json），
// 否则 docker compose 重建容器会把 Key 池一起丢掉。
const KEYS_PATH = process.env.CC_KEYS_FILE
  ? resolve(process.env.CC_KEYS_FILE)
  : resolve(__dirname, 'keys.json');

const LB_DEFAULTS = {
  strategy: 'weighted',          // round-robin | weighted | random | weighted-random | sticky | least-recent | failover
  stickyBy: 'client-key',        // none | ip | client-key
  maxRetries: 2,                 // 可重试失败时最多再换几个 key
  cooldownMs: 60000,             // 失败后暂时跳过
};

// 调度设置（中转站场景）：会话粘性 + 失败/额度触发转移 + 额度轮询
const SETTINGS_DEFAULTS = {
  mode: 'session',               // session（会话粘性，默认）| request（每次请求按策略选）
  failThreshold: 3,              // 同一会话在同一 Key 上连续失败几次后换 Key
  sessionTtlMs: 6 * 60 * 60 * 1000, // 会话绑定有效期（超时回收）
  creditsRefreshMs: 15 * 60 * 1000, // 服务端定时刷新额度间隔（默认 15 分钟），0 = 关闭
  autoDisableExhausted: true,    // 额度用尽自动停用（额度恢复后自动恢复）
  onAllExhausted: 'error',       // 池内全部不可用时：error（明确报错）| best-effort（仍然尝试）
};

let keyStore = {
  lb: { ...LB_DEFAULTS },
  settings: { ...SETTINGS_DEFAULTS },
  keys: [],
  defaultId: null,
};
let rrIndex = 0;
const wrrCurrent = new Map();

/** 会话 → Key 绑定（会话粘性模式的核心状态，仅内存，重启后按需重建） */
const sessionRoutes = new Map(); // sessionId → { keyId, boundAt, lastAt, failures, moved }

function newKeyId() {
  return 'key_' + crypto.randomBytes(4).toString('hex');
}

function findKey(id) {
  return keyStore.keys.find((k) => k.id === id) || null;
}

/** 设置项（带范围收敛，防止前端传脏数据） */
function normalizeSettings(raw) {
  const s = { ...SETTINGS_DEFAULTS, ...(raw || {}) };
  s.mode = s.mode === 'request' ? 'request' : 'session';
  s.failThreshold = Math.max(1, Math.min(20, Number(s.failThreshold) || SETTINGS_DEFAULTS.failThreshold));
  s.sessionTtlMs = Math.max(60000, Math.min(7 * 24 * 3600 * 1000, Number(s.sessionTtlMs) || SETTINGS_DEFAULTS.sessionTtlMs));
  s.creditsRefreshMs = Math.max(0, Math.min(24 * 3600 * 1000, Number(s.creditsRefreshMs) || 0));
  s.autoDisableExhausted = s.autoDisableExhausted !== false;
  s.onAllExhausted = s.onAllExhausted === 'best-effort' ? 'best-effort' : 'error';
  return s;
}

function loadKeyStore() {
  try {
    const raw = JSON.parse(readFileSync(KEYS_PATH, 'utf-8'));
    keyStore.lb = { ...LB_DEFAULTS, ...(raw?.lb || {}) };
    keyStore.settings = normalizeSettings(raw?.settings);
    keyStore.keys = (raw?.keys || []).filter((k) => k?.key).map((k) => ({
      id: k.id || newKeyId(),
      label: k.label || '',
      key: k.key,
      weight: Number.isFinite(+k.weight) ? Math.max(0, +k.weight) : 1,
      enabled: k.enabled !== false,
      priority: Number.isFinite(+k.priority) ? +k.priority : 0,
      createdAt: k.createdAt || Date.now(),
      lastUsedAt: k.lastUsedAt || null,
      lastErrorAt: k.lastErrorAt || null,
      errorCount: k.errorCount || 0,
      cooldownUntil: k.cooldownUntil || 0,
      consecutiveFailures: k.consecutiveFailures || 0,
      autoDisabled: k.autoDisabled && k.autoDisabled.at
        ? { reason: String(k.autoDisabled.reason || 'unknown'), at: k.autoDisabled.at, until: k.autoDisabled.until || null }
        : null,
      credits: k.credits && typeof k.credits === 'object' ? k.credits : null,
    }));
    keyStore.defaultId = findKey(raw?.defaultId) ? raw.defaultId : null;
    rrIndex = 0;
    wrrCurrent.clear();
    sessionRoutes.clear();
  } catch {
    keyStore = {
      lb: { ...LB_DEFAULTS },
      settings: { ...SETTINGS_DEFAULTS },
      keys: [],
      defaultId: null,
    };
  }
}

function saveKeyStore() {
  const tmp = KEYS_PATH + '.tmp';
  // 目录可能还不存在（首次挂载空卷）；tmp 与目标同目录，保证 rename 仍是原子替换
  mkdirSync(dirname(KEYS_PATH), { recursive: true });
  writeFileSync(tmp, JSON.stringify({
    lb: keyStore.lb,
    settings: keyStore.settings,
    keys: keyStore.keys,
    defaultId: keyStore.defaultId,
  }, null, 2), 'utf-8');
  renameSync(tmp, KEYS_PATH);
}

function extractClientApiKey(headers) {
  const auth = headers['authorization'] || headers['Authorization'] || '';
  if (auth.startsWith('Bearer ')) {
    const match = auth.slice(7).match(/user_[a-zA-Z0-9_-]+/);
    if (match) return match[0];
  }
  const xKey = headers['x-api-key'] || headers['X-Api-Key'] || '';
  if (xKey) {
    const match = xKey.match(/user_[a-zA-Z0-9_-]+/);
    if (match) return match[0];
  }
  return null;
}

function getFallbackApiKey() {
  if (CFG.apiKey) return CFG.apiKey;
  if (process.env.CC_API_KEY) return process.env.CC_API_KEY.trim();
  return null;
}

function isKeyReady(k, now = Date.now()) {
  return !!(k && k.enabled && k.key && (!k.cooldownUntil || k.cooldownUntil <= now));
}

function poolCandidates(excludeTried, now = Date.now()) {
  const notTried = (k) => !excludeTried || !excludeTried.has(k.id);
  const live = keyStore.keys.filter((k) => notTried(k) && isKeyReady(k, now));
  if (live.length) return live;
  // 全在冷却时放行，避免无 key 可用
  return keyStore.keys.filter((k) => notTried(k) && k.enabled && k.key);
}

function stickyValue(req, clientKey) {
  const by = keyStore.lb.stickyBy || 'none';
  if (by === 'ip') return req?.socket?.remoteAddress || req?.headers?.['x-forwarded-for'] || 'ip';
  if (by === 'client-key') return clientKey || req?.socket?.remoteAddress || 'anon';
  return null;
}

function pickByStrategy(candidates, req, clientKey) {
  if (!candidates.length) return null;
  const strategy = keyStore.lb.strategy || 'weighted';

  if (strategy === 'sticky') {
    const sv = stickyValue(req, clientKey);
    if (sv) {
      let h = 0;
      for (let i = 0; i < sv.length; i++) h = (h * 31 + sv.charCodeAt(i)) | 0;
      return candidates[Math.abs(h) % candidates.length];
    }
  }

  if (strategy === 'least-recent') {
    return candidates.reduce((a, b) => ((a.lastUsedAt || 0) <= (b.lastUsedAt || 0) ? a : b));
  }

  if (strategy === 'failover') {
    return [...candidates].sort((a, b) =>
      (a.priority || 0) - (b.priority || 0) || (a.lastUsedAt || 0) - (b.lastUsedAt || 0)
    )[0];
  }

  if (strategy === 'random') {
    return candidates[Math.floor(Math.random() * candidates.length)];
  }

  if (strategy === 'round-robin') {
    rrIndex = (rrIndex + 1) % candidates.length;
    return candidates[rrIndex];
  }

  if (strategy === 'weighted-random') {
    const total = candidates.reduce((s, k) => s + Math.max(0, k.weight || 0), 0);
    if (total <= 0) return candidates[Math.floor(Math.random() * candidates.length)];
    let r = Math.random() * total;
    for (const k of candidates) {
      r -= Math.max(0, k.weight || 0);
      if (r <= 0) return k;
    }
    return candidates[candidates.length - 1];
  }

  // weighted — nginx 平滑加权轮询
  let total = 0;
  let best = null;
  for (const k of candidates) {
    const w = Math.max(0, k.weight || 0);
    total += w;
    const cur = (wrrCurrent.get(k.id) || 0) + w;
    wrrCurrent.set(k.id, cur);
    if (!best || cur > wrrCurrent.get(best.id)) best = k;
  }
  if (best && total > 0) wrrCurrent.set(best.id, wrrCurrent.get(best.id) - total);
  return best;
}

function markKeyUsed(entry) {
  if (entry) entry.lastUsedAt = Date.now();
}

function markKeySuccess(pick) {
  if (!pick?.id) return;
  const k = findKey(pick.id);
  if (!k) return;
  k.errorCount = 0;
  k.consecutiveFailures = 0;
  k.cooldownUntil = 0;
  k.lastErrorAt = null;
}

/**
 * 记录一次上游失败。
 * trigger: quota-exhausted（额度/限流）| disabled（凭证失效）| forbidden（403，可能是套餐/权限）
 *          | any-error（5xx/网络等）
 * 会话模式下，普通错误要连续失败达到 failThreshold 才弃用该 Key；
 * 额度类错误立即冷却（并按设置自动停用）；403 只换 Key，不惩罚该 Key。
 */
function markKeyFailure(pick, status, retryable, trigger = 'any-error', bodyText = '') {
  if (!pick?.id) return;
  const k = findKey(pick.id);
  if (!k) return;
  const now = Date.now();
  k.lastErrorAt = now;
  k.errorCount = (k.errorCount || 0) + 1;

  // 请求级限制（例如套餐不含该模型）：换个 Key 试试即可，不要把 Key 标记成坏 Key
  if (trigger === 'forbidden') {
    saveKeyStore();
    return;
  }

  k.consecutiveFailures = (k.consecutiveFailures || 0) + 1;

  const quotaLike = trigger === 'quota-exhausted' || trigger === 'disabled';
  if (quotaLike) {
    const cool = Math.max(0, keyStore.lb.cooldownMs || 0);
    k.cooldownUntil = now + cool;
    // 额度/限流：优先用上游给的窗口重置时间，拿不到就用冷却时长兜底
    if (trigger === 'quota-exhausted') {
      const until = quotaResetFromError(bodyText) || now + Math.max(cool, 5 * 60 * 1000);
      setAutoDisabled(k, 'quota-exhausted', until);
    }
  } else if (retryable) {
    const sessionMode = keyStore.settings.mode === 'session';
    const threshold = Math.max(1, keyStore.settings.failThreshold || 3);
    if (!sessionMode || k.consecutiveFailures >= threshold) {
      k.cooldownUntil = now + Math.max(0, keyStore.lb.cooldownMs || 0);
    }
  }
  saveKeyStore();
}

function isRetryableCcFailure(status, bodyText) {
  if (status === 401 || status === 402 || status === 403 || status === 429) return true;
  return /insufficient\s+(?:credits|balance)|usage_limit_reached|quota\s+(?:exceeded|reached)|USAGE_EXCEEDED|rate.?limit|free.?usage.?limit/i.test(String(bodyText || ''));
}

/**
 * 始终从 GUI 配置的 Key 池里选取上游 Key，忽略请求头里的 Authorization / x-api-key。
 * excludeTried: Set<keyId>，failover 重试时排除已试过的。
 * sticky 策略仍可用客户端 Key/IP 做哈希，但只影响落到哪个池内 Key，不会直通该 Key。
 */
function pickUpstreamKey(clientKey, req, excludeTried) {
  // 默认 Key 优先（未禁用、未冷却、本轮未试过）
  if (keyStore.defaultId) {
    const def = findKey(keyStore.defaultId);
    if (def && def.enabled && def.key && !excludeTried?.has(def.id) && isKeyReady(def)) {
      markKeyUsed(def);
      return { apiKey: def.key, id: def.id, label: def.label };
    }
  }

  const candidates = poolCandidates(excludeTried);
  if (!candidates.length) return null;

  const entry = pickByStrategy(candidates, req, clientKey);
  if (!entry) return null;
  markKeyUsed(entry);
  return { apiKey: entry.key, id: entry.id, label: entry.label };
}

// ══════════════════════════════════════════════════════════
// 中转站调度：额度健康 + 会话粘性分配
// 目标：多会话（多个 agent）自然分散到不同 Key；某个 Key 额度用尽或连续
// 失败后自动换 Key，正在跑的会话不会被打断。
// ══════════════════════════════════════════════════════════

/** 额度是否用尽：剩余 <= 0，或任一窗口已满 */
function isQuotaExhausted(k) {
  const c = k?.credits;
  if (!c || c.error) return false;
  if (typeof c.creditsRemaining === 'number' && c.creditsRemaining <= 0.000001) return true;
  return Array.isArray(c.windows) && c.windows.some((w) => Number(w.pct) >= 100);
}

/**
 * 预计多久后额度恢复：
 * 窗口类限额 → 取已满窗口最晚的重置时间；余额为 0 或时间未知 → null（需等额度刷新或充值）。
 */
function quotaRecoveryAt(k) {
  const c = k?.credits;
  if (!c || c.error) return null;
  const full = (c.windows || []).filter((w) => Number(w.pct) >= 100);
  if (!full.length) return null;
  const resets = full.map((w) => Number(w.resetsAt)).filter((t) => t > Date.now());
  return resets.length ? Math.max(...resets) : null;
}

/** 自动停用是否仍然生效（到点自动失效，避免"停用后再也起不来"） */
function autoDisableActive(k, now = Date.now()) {
  if (!k?.autoDisabled) return false;
  if (k.autoDisabled.until && k.autoDisabled.until <= now) return false;
  return true;
}

/** 是否可作为上游候选：启用、有 key、未冷却、未被自动停用 */
function isKeyUsable(k, now = Date.now()) {
  if (!k || !k.key || !k.enabled) return false;
  if (autoDisableActive(k, now)) return false;
  if (k.cooldownUntil && k.cooldownUntil > now) return false;
  return true;
}

function keyUnusableReason(k, now = Date.now()) {
  if (!k || !k.key) return 'removed';
  if (!k.enabled) return 'disabled';
  if (autoDisableActive(k, now)) return 'auto-disabled:' + k.autoDisabled.reason;
  if (k.cooldownUntil && k.cooldownUntil > now) return 'cooldown';
  return 'unknown';
}

/**
 * 标记自动停用。
 * until：到点自动恢复（窗口限额用窗口重置时间；上游报错用冷却时长兜底）
 */
function setAutoDisabled(k, reason, until = null) {
  if (!keyStore.settings.autoDisableExhausted) return;
  const sameUntil = k.autoDisabled && k.autoDisabled.reason === reason && k.autoDisabled.until === until;
  if (sameUntil) return;
  k.autoDisabled = { reason, at: Date.now(), until };
  log('warn', 'Key auto-disabled', {
    keyId: k.id, label: k.label, reason,
    until: until ? new Date(until).toISOString() : null,
  });
}

/** 清掉已到期的自动停用标记 */
function expireAutoDisables(now = Date.now()) {
  let cleared = 0;
  for (const k of keyStore.keys) {
    if (k.autoDisabled && k.autoDisabled.until && k.autoDisabled.until <= now) {
      k.autoDisabled = null;
      cleared++;
      log('info', 'Key auto-enable (recovery time reached)', { keyId: k.id, label: k.label });
    }
  }
  return cleared;
}

/** 额度刷新后同步自动停用状态：用尽则停用（带恢复时间），恢复则重新启用 */
function syncAutoDisable(k, now = Date.now()) {
  if (!keyStore.settings.autoDisableExhausted) {
    if (k.autoDisabled) k.autoDisabled = null;
    return;
  }
  if (isQuotaExhausted(k)) {
    const until = quotaRecoveryAt(k);
    if (!k.autoDisabled || k.autoDisabled.reason !== 'quota-exhausted') {
      k.autoDisabled = { reason: 'quota-exhausted', at: now, until };
      log('warn', 'Key exhausted, auto-disabled', {
        keyId: k.id, label: k.label,
        until: until ? new Date(until).toISOString() : null,
      });
    } else if (k.autoDisabled.until !== until) {
      k.autoDisabled.until = until; // 窗口重置时间会变，跟随更新
    }
    return;
  }
  // 未用尽：额度数据比停用时刻新，或已到恢复时间 → 认定恢复
  if (k.autoDisabled
    && ((k.credits?.fetchedAt || 0) > k.autoDisabled.at
      || (k.autoDisabled.until && k.autoDisabled.until <= now))) {
    k.autoDisabled = null;
    log('info', 'Key recovered, auto-enabled', { keyId: k.id, label: k.label });
  }
}

/** 会话指纹用的文本提取（支持 string / 内容块数组） */
function textOfContent(v) {
  if (!v) return '';
  if (typeof v === 'string') return v;
  if (Array.isArray(v)) return v.map(textOfContent).filter(Boolean).join('\n');
  if (typeof v === 'object') return textOfContent(v.text || v.content || v.input || '');
  return '';
}

/** 会话指纹：模型 + system + 首条 user 消息（同一会话多轮请求保持稳定） */
function sessionSeed(body, clientKey) {
  if (!body || typeof body !== 'object') return '';
  const p = body.params && typeof body.params === 'object' ? body.params : {};
  const sys = textOfContent(body.system) || textOfContent(body.instructions) || textOfContent(p.system);
  const msgs = Array.isArray(body.messages) ? body.messages
    : Array.isArray(p.messages) ? p.messages
      : Array.isArray(body.input) ? body.input : [];
  let firstUser = '';
  for (const m of msgs) {
    if (!m) continue;
    if (typeof m === 'string') { firstUser = m; break; }
    if (m.role === 'user' || m.role === 'human') { firstUser = textOfContent(m.content ?? m.input); break; }
  }
  if (!sys && !firstUser) return '';
  return `${clientKey || ''}|${body.model || p.model || ''}|${sys.slice(0, 1500)}|${firstUser.slice(0, 3000)}`;
}

/**
 * 推导"会话"标识：显式 header > metadata > 内容指纹。
 * 无法识别时返回 null，调用方退化为按请求选 Key。
 */
function deriveSessionId(req, body, clientKey) {
  const h = req?.headers || {};
  const direct = h['x-cc-session'] || h['x-session-id'] || h['x-conversation-id'] || h['conversation-id'];
  if (direct && String(direct).trim().length >= 6) return 'h_' + String(direct).trim().slice(0, 100);
  const meta = body?.metadata || {};
  const uid = meta.user_id || meta.session_id || meta.conversation_id || body?.user;
  if (uid && typeof uid === 'string' && uid.length >= 6) return 'm_' + uid.slice(0, 100);
  const seed = sessionSeed(body, clientKey);
  if (!seed) return null;
  return 'f_' + crypto.createHash('sha1').update(seed).digest('hex').slice(0, 16);
}

function pruneSessionRoutes(now = Date.now()) {
  if (!sessionRoutes.size) return;
  const ttl = keyStore.settings.sessionTtlMs;
  for (const [id, rec] of sessionRoutes) {
    if (now - rec.lastAt > ttl) sessionRoutes.delete(id);
  }
}

function activeSessionCounts(now = Date.now()) {
  const counts = new Map();
  const ttl = keyStore.settings.sessionTtlMs;
  for (const rec of sessionRoutes.values()) {
    if (now - rec.lastAt > ttl) continue;
    counts.set(rec.keyId, (counts.get(rec.keyId) || 0) + 1);
  }
  return counts;
}

/** 分散选 Key：活跃会话最少 → 默认 Key 优先 → 最久未用 → 权重最大 */
function pickBySpreading(pool, now) {
  if (!pool.length) return null;
  const counts = activeSessionCounts(now);
  return [...pool].sort((a, b) =>
    (counts.get(a.id) || 0) - (counts.get(b.id) || 0)
    || Number(b.id === keyStore.defaultId) - Number(a.id === keyStore.defaultId)
    || (a.lastUsedAt || 0) - (b.lastUsedAt || 0)
    || (b.weight || 0) - (a.weight || 0)
  )[0];
}

/** 会话模式下选 Key；abandoned 为本轮已放弃的 Key id */
function pickSessionKey(sessionId, clientKey, req, body, abandoned) {
  const now = Date.now();
  if (sessionRoutes.size > 256) pruneSessionRoutes(now);
  const tried = abandoned || new Set();

  // 1) 已有绑定且仍可用 → 继续用同一个 Key（会话粘性）
  if (sessionId) {
    const rec = sessionRoutes.get(sessionId);
    if (rec && !tried.has(rec.keyId)) {
      const bound = findKey(rec.keyId);
      if (bound && isKeyUsable(bound, now)) {
        rec.lastAt = now;
        markKeyUsed(bound);
        return { apiKey: bound.key, id: bound.id, label: bound.label, sessionId };
      }
      if (bound) {
        log('info', 'Session rebind', {
          session: sessTag(sessionId), from: rec.keyId, reason: keyUnusableReason(bound, now),
        });
      }
      sessionRoutes.delete(sessionId);
    }
  }

  // 2) 新会话 / 需要换 Key → 分散到活跃会话最少的可用 Key
  const pool = keyStore.keys.filter((k) => isKeyUsable(k, now) && !tried.has(k.id));
  const chosen = pickBySpreading(pool, now);
  if (!chosen) return null;
  markKeyUsed(chosen);
  if (sessionId) {
    const counts = activeSessionCounts(now);
    sessionRoutes.set(sessionId, { keyId: chosen.id, boundAt: now, lastAt: now, failures: 0 });
    log('info', 'Session bound', {
      session: sessTag(sessionId),
      keyId: chosen.id,
      label: chosen.label,
      activeSessionsBefore: counts.get(chosen.id) || 0,
      poolSize: pool.length,
    });
  }
  return { apiKey: chosen.key, id: chosen.id, label: chosen.label, sessionId };
}

/** 兜底：所有 Key 都不可用时（onAllExhausted=best-effort）仍然挑一个 */
function pickBestEffort(abandoned) {
  const tried = abandoned || new Set();
  const pool = keyStore.keys.filter((k) => k.key && k.enabled && !tried.has(k.id));
  if (!pool.length) return null;
  const fresh = pool.filter((k) => !isQuotaExhausted(k));
  const target = (fresh.length ? fresh : pool)
    .slice()
    .sort((a, b) => (a.cooldownUntil || 0) - (b.cooldownUntil || 0) || (b.weight || 0) - (a.weight || 0))[0];
  if (!target) return null;
  markKeyUsed(target);
  return { apiKey: target.key, id: target.id, label: target.label, bestEffort: true };
}

/** 上游失败归类，决定是"立即换"还是"再试几次" */
function classifyFailure(status, bodyText) {
  const text = String(bodyText || '');
  if (status === 401) return 'disabled';
  if (status === 402 || status === 429) return 'quota-exhausted';
  // 403：可能是 Key 无权限，也可能是"套餐不含该模型"这类请求级限制。
  // 后者不该惩罚 Key（否则一次请求就能把整个池冷却掉），所以单独归类，只换 Key 不冷却。
  if (status === 403) return 'forbidden';
  if (/insufficient\s+(?:credits|balance)|usage_limit_reached|quota\s+(?:exceeded|reached)|USAGE_EXCEEDED|rate.?limit|free.?usage.?limit/i
    .test(text)) return 'quota-exhausted';
  if (/UNAUTHORIZED|invalid.{0,20}(api.?key|token)/i.test(text)) return 'disabled';
  return 'any-error';
}

function recordSessionFailure(sessionId, trigger) {
  if (!sessionId) return;
  const rec = sessionRoutes.get(sessionId);
  if (!rec) return;
  rec.failures = (rec.failures || 0) + 1;
  rec.lastAt = Date.now();
  log('warn', 'Session failure', {
    session: sessTag(sessionId), keyId: rec.keyId, failures: rec.failures, trigger,
  });
}

/** 会话日志/界面用的短标识：稳定、唯一，且不泄露客户端原始 session id */
function sessTag(sid) {
  return 'sess#' + crypto.createHash('sha1').update(String(sid)).digest('hex').slice(0, 7);
}

/**
 * 从上游错误体里解析"额度何时恢复"。
 * 例：{"rateLimit":{"limit":6,"remaining":0,"reset":1790171713,"window":"weekly"}}
 *     "You've reached your weekly usage limit ... resets at 2026-09-23T13:55:13.843Z"
 * 解析得到就能让 Key 一直停用到窗口真正重置，避免每 5 分钟重试一次的来回抖动。
 */
function quotaResetFromError(bodyText) {
  const s = String(bodyText || '');
  if (!s) return null;
  const reset = s.match(/"reset"\s*:\s*(\d{9,13})/);
  if (reset) {
    const n = Number(reset[1]);
    const ms = n > 1e11 ? n : n * 1000; // 秒 / 毫秒都兼容
    if (isFinite(ms) && ms > Date.now()) return ms;
  }
  const iso = s.match(/(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)/);
  if (iso) {
    const t = Date.parse(iso[1]);
    if (isFinite(t) && t > Date.now()) return t;
  }
  return null;
}

function recordSessionSuccess(sessionId) {
  if (!sessionId) return;
  const rec = sessionRoutes.get(sessionId);
  if (rec) rec.failures = 0;
}

function hasAnyUsableKey() {
  return keyStore.keys.some((k) => isKeyUsable(k));
}

/** 池整体不可用时的错误响应（区分"空池"和"全部用尽/冷却"） */
function poolUnavailableResponse() {
  const now = Date.now();
  const total = keyStore.keys.length;
  if (!total) {
    return { status: 401, body: { error: { message: 'Key 池为空，请先在管理页 http://<host>:<port>/ 配置至少一个 user_* Key', type: 'auth_error' } } };
  }
  const exhausted = keyStore.keys.filter((k) => k.autoDisabled).length;
  const cooling = keyStore.keys.filter((k) => k.cooldownUntil > now).length;
  const off = keyStore.keys.filter((k) => !k.enabled).length;
  return {
    status: 429,
    body: {
      error: {
        message: `池内 ${total} 个 Key 当前都不可用（额度用尽 ${exhausted} · 冷却中 ${cooling} · 已停用 ${off}），请补充 Key 或在管理页查看额度`,
        type: 'rate_limit_error',
      },
    },
  };
}

// 兼容旧函数名（handleModels 等仍在用）——同样只走池
function getApiKey(headers) {
  const req = { headers, socket: { remoteAddress: '' } };
  const picked = keyStore.settings.mode === 'session'
    ? pickSessionKey(null, extractClientApiKey(headers), req, null, null)
    : pickUpstreamKey(extractClientApiKey(headers), req, null);
  return picked?.apiKey || null;
}

loadKeyStore();

// ── 指纹生成（首次运行自动生成，写回 config.json） ──────
// CPU 型号与核心数对应表（仅 Windows x64）
const FINGERPRINT_CPUS = [
  { model: '12th Gen Intel(R) Core(TM) i7-12650H', cores: 10 },
  { model: '12th Gen Intel(R) Core(TM) i5-12400F', cores: 6 },
  { model: '12th Gen Intel(R) Core(TM) i9-12900K', cores: 16 },
  { model: '13th Gen Intel(R) Core(TM) i7-13700K', cores: 16 },
  { model: '13th Gen Intel(R) Core(TM) i5-13600K', cores: 14 },
  { model: '13th Gen Intel(R) Core(TM) i9-13900K', cores: 24 },
  { model: 'Intel(R) Core(TM) Ultra 7 155H', cores: 16 },
  { model: 'Intel(R) Core(TM) Ultra 9 285H', cores: 16 },
  { model: 'Intel(R) Core(TM) i9-14900K', cores: 24 },
  { model: 'Intel(R) Core(TM) i7-14700K', cores: 20 },
  { model: 'AMD Ryzen 7 7800X3D', cores: 8 },
  { model: 'AMD Ryzen 9 7950X', cores: 16 },
  { model: 'AMD Ryzen 5 7600', cores: 6 },
  { model: 'AMD Ryzen 9 7900X', cores: 12 },
  { model: 'AMD Ryzen 7 5800X3D', cores: 8 },
];
const FINGERPRINT_MEMS = [8, 16, 24, 32, 48, 64];
const FINGERPRINT_TZS = [
  'America/New_York', 'America/Chicago', 'America/Los_Angeles', 'America/Toronto',
  'Europe/London', 'Europe/Berlin', 'Europe/Paris', 'Europe/Moscow',
  'Asia/Shanghai', 'Asia/Tokyo', 'Asia/Singapore', 'Asia/Seoul', 'Asia/Hong_Kong',
  'Australia/Sydney', 'Pacific/Auckland',
];
const FINGERPRINT_MAC_COUNT_RANGE = [2, 3, 4, 5]; // 随机 2~5 个 MAC

function generateFingerprint() {
  const cpuEntry = FINGERPRINT_CPUS[Math.floor(Math.random() * FINGERPRINT_CPUS.length)];
  const memGiB = FINGERPRINT_MEMS[Math.floor(Math.random() * FINGERPRINT_MEMS.length)];
  const tz = FINGERPRINT_TZS[Math.floor(Math.random() * FINGERPRINT_TZS.length)];
  const macCount = FINGERPRINT_MAC_COUNT_RANGE[Math.floor(Math.random() * FINGERPRINT_MAC_COUNT_RANGE.length)];

  function sha256(s) { return crypto.createHash('sha256').update(s).digest('hex'); }
  function randHex(n) { return crypto.randomBytes(n).toString('hex'); }

  const macHashes = [];
  for (let i = 0; i < macCount; i++) macHashes.push(sha256(randHex(32)));

  const machineIdHash = sha256(randHex(32));
  const osUserHash = sha256(randHex(16));
  const hostnameHash = sha256(randHex(16));
  const gitEmailHash = sha256(randHex(16));

  // thumbmark = 所有组件的联合哈希
  const thumbData = [machineIdHash, ...macHashes, osUserHash, hostnameHash, gitEmailHash, 'win32', '10.0.22631', cpuEntry.model, String(cpuEntry.cores), String(memGiB)].join('|');
  const thumbmark = sha256(thumbData);

  return {
    thumbmark,
    components: {
      machineIdHash,
      macHashes,
      osUserHash,
      hostnameHash,
      gitEmailHash,
      platform: 'win32',
      arch: 'x64',
      osRelease: '10.0.22631',
      cpuModel: cpuEntry.model,
      cpuCount: cpuEntry.cores,
      memGiB,
      isContainer: false,
      timezone: tz,
      runtime: 'cli',
      collectorVersion: 1,
    },
  };
}

let CC_VERSION = '0.32.3';
const CC_VERSION_FALLBACK = '0.32.3';
const CC_VERSION_REFRESH_MS = 24 * 60 * 60 * 1000; // 24h — npm registry 刷新间隔

// ── 动态 CC 版本号（从 npm registry 拉取） ─────────────
async function refreshCCVersion() {
  try {
    const url = 'https://registry.npmjs.org/command-code/latest';
    const res = await fetch(url, { signal: AbortSignal.timeout(10000) });
    if (!res.ok) throw new Error(`npm responded with ${res.status}`);
    const pkg = await res.json();
    if (pkg.version && typeof pkg.version === 'string') {
      CC_VERSION = pkg.version;
      log('info', 'CC Version refreshed from npm', { version: CC_VERSION });
    }
  } catch (e) {
    log('warn', 'CC Version fetch failed, using current', { version: CC_VERSION, error: e.message });
  }
}
refreshCCVersion(); // 启动时立即拉取
setInterval(refreshCCVersion, CC_VERSION_REFRESH_MS);

// 请求体大小上限：默认 100MB，可用环境变量 CC_MAX_BODY_MB 覆盖（正整数，单位 MB）
// ⚠️ 内存特性（issue #20 实测）：请求体在转发到上游前会同时存在多份副本 ——
//    chunks[] / Buffer.concat / utf8 字符串 / JSON.parse 对象树 / buildCcRequest 重建对象树 / JSON.stringify 序列化体。
//    实测峰值 ≈ body 大小 × 5.1~7.4（7MB→+52MB，20MB→+116MB；而 413 拒绝路径只要 ×1.05）。
//    故 100MB 上限意味着「单个请求」最坏可吃 ~550MB，且该上限是每请求的、不是全局的。
//    公网/多用户部署请在反向代理层同时限制 body 大小与在途请求数（见 README「内存与部署」）。
const MAX_BODY_SIZE = (() => {
  const mb = Number.parseInt(process.env.CC_MAX_BODY_MB ?? '', 10);
  return Number.isFinite(mb) && mb > 0 ? mb * 1024 * 1024 : 100 * 1024 * 1024;
})();
// 上游读空闲超时（issue #19）：只计「reader.read() 的等待」，每收到一个 chunk 重置，
// 不是整个请求的总时长。默认值保持不变（30s / 90s），可用环境变量覆盖 ——
// 官方 CLI 对上游没有任何 idle timeout（反编译 command-code@1.50.0 已验证，
// createApiClient 调用点均未传 timeout），合法的长思考停顿可达数百秒，
// 遇到推理模型被 30s 误杀 / 触发 429 重试放大时，调大这两个值即可。
const STREAM_IDLE_TIMEOUT_MS = (() => {
  const ms = Number.parseInt(process.env.CC_STREAM_IDLE_MS ?? '', 10);
  return Number.isFinite(ms) && ms > 0 ? ms : 30000;   // 默认 30s — 流式无新数据中断
})();
const NONSTREAM_IDLE_TIMEOUT_MS = (() => {
  const ms = Number.parseInt(process.env.CC_NONSTREAM_IDLE_MS ?? '', 10);
  return Number.isFinite(ms) && ms > 0 ? ms : 90000;   // 默认 90s — 非流式超时更宽容
})();

// 客户端「僵死」保护：既不读也不断开时，该请求会连带上游连接一直挂着（背压修复后的残留）。
// 实测残留在途成本约 5MB/连接 —— 有界、不泄漏、断开即回收，但连接数本身无上限。
// 默认 0 = 禁用，保持既有行为不变：僵死客户端与「卡在工具执行的合法客户端」在协议层无法
// 区分，而官方 CLI 对上游没有任何 idle timeout（issue #19），贸然加超时会误杀健康请求。
// 在途请求上限（可选，默认关闭）。项目定位是纯反代层，并发控制属于下游（nginx
// limit_conn，per-IP / per-key）；本项仅为「不挂反代裸跑」的场景提供一个可选的
// 进程内全局兜底，不替代下游方案，也不感知客户端身份。
// 内存 = 在途数 × (0.13MB + 5.5 × body_MB)：body 上限只管住单请求量级，乘数由本项封顶。
// 超限返回 503 + Retry-After（SDK 会自行退避重试），而不是放任进程被 OOM 杀掉。
// 默认 0 = 关闭，不限制并发（既有的反代层定位不变，行为零变化）；需要时按需开启：
//   CC_MAX_INFLIGHT=32 npm start
// 注意：body 上限只管住单请求量级，乘数由本项封顶。默认 body 上限 100MB 时，
// N × 最坏 550MB —— 要硬性内存上界需同时下调 CC_MAX_BODY_MB。
const MAX_INFLIGHT = (() => {
  const n = Number.parseInt(process.env.CC_MAX_INFLIGHT ?? '', 10);
  return Number.isFinite(n) && n > 0 ? n : 0;            // 默认 0 = 不限
})();

let inflightCount = 0;   // 当前在途请求数（不含 /health）

const CLIENT_DRAIN_TIMEOUT_MS = (() => {
  const ms = Number.parseInt(process.env.CC_CLIENT_DRAIN_TIMEOUT_MS ?? '', 10);
  return Number.isFinite(ms) && ms > 0 ? ms : 0;
})();

// 连续超时计数：连续 3 次超时才提醒压缩上下文，任意成功请求后重置
let consecutiveTimeouts = 0;
const TIMEOUT_REDUCE_CONTEXT_THRESHOLD = 3;

// ── 日志 ─────────────────────────────────────────────
function log(level, msg, data) {
  const line = `[${new Date().toISOString()}] [${level}] ${msg}${data ? ' ' + JSON.stringify(data) : ''}`;
  console.log(line);
  if (CFG.logFile) {
    try { appendFileSync(CFG.logFile, line + '\n', 'utf-8'); } catch {}
  }
}

log('info', 'Key pool loaded', {
  path: KEYS_PATH,
  keys: keyStore.keys.length,
  enabled: keyStore.keys.filter((k) => k.enabled).length,
  strategy: keyStore.lb.strategy,
});

// 把上游错误体摘要成单行，便于日志排查。
// 之前 CC API error 只记 status，不记 body —— 遇到 400 只能靠猜（问题来源见 hk_sji 排查）。
// 截断到 500 字符，避免异常大的 body 刷爆日志；同时压掉换行，保证一条日志一行。
function summarizeUpstreamError(text, limit = 500) {
  if (!text) return '';
  const flat = String(text).replace(/\s+/g, ' ').trim();
  return flat.length > limit ? flat.slice(0, limit) + '…(' + (flat.length - limit) + ' more)' : flat;
}

// ── 会话管理 ───────────────────────────────────────
// 每个 API Key 独立一个 session，12h 过期 + 1h 随机抖动
// 同一 Key 在同一周期内复用，到期自动换新
const SESSION_DURATION_MS = 12 * 60 * 60 * 1000;    // 12h
const SESSION_JITTER_MS  = 60 * 60 * 1000;           // 1h 抖动范围

const sessionStore = new Map(); // apiKey → { sessionId, expiresAt }

function ensureSession(apiKey) {
  const now = Date.now();
  const entry = sessionStore.get(apiKey);

  if (entry && now < entry.expiresAt) {
    return entry.sessionId;
  }

  // 过期或第一次：生成新 session
  const jitter = Math.floor(Math.random() * SESSION_JITTER_MS);
  const sessionId = randomUUID();
  sessionStore.set(apiKey, { sessionId, expiresAt: now + SESSION_DURATION_MS + jitter });
      log('info', 'Session created', { sessionId: sessionId.slice(0, 8), storeSize: sessionStore.size });
  return sessionId;
}

// 定期清理过期 session 和 key 状态，防止 Map 无限增长
setInterval(() => {
  const now = Date.now();
  let cleaned = 0;
  for (const [key, entry] of sessionStore) {
    if (now >= entry.expiresAt) {
      sessionStore.delete(key);
      keyStateStore.delete(key); // 同时清理该 key 的指纹状态
      cleaned++;
    }
  }
  if (cleaned > 0) log('info', 'Session cleanup', { cleaned, remaining: sessionStore.size });
}, 60 * 60 * 1000); // 每小时

function getSessionId(incomingHeaders, apiKey, promptCacheKey) {
  // 优先从客户端传来的 session 类 header 获取
  const candidates = [
    incomingHeaders['x-session-id'],
    incomingHeaders['x-claude-code-session-id'],
    incomingHeaders['session_id'],
    promptCacheKey,
  ];
  for (const id of candidates) {
    if (id && typeof id === 'string' && id.length >= 8) return id;
  }
  // 按 API Key 分 session
  return ensureSession(apiKey);
}

// 每个请求独立 thread ID
function newThreadId() { return randomUUID(); }

// ── 每 Key 独立状态（fingerprint + 初始化节流） ──
// 每个 API Key 拥有自己的设备指纹和初始化定时器
const keyStateStore = new Map(); // apiKey → { fingerprint, nextInitAt }

function getOrCreateKeyState(apiKey) {
  let state = keyStateStore.get(apiKey);
  if (!state) {
    state = {
      fingerprint: generateFingerprint(),
      nextInitAt: 0,
    };
    keyStateStore.set(apiKey, state);
    log('info', 'Fingerprint generated for key', { keyPrefix: apiKey.slice(0, 8) });
  }
  return state;
}

// ── 初始化预请求（fingerprint + lifecycle，首次 + 每 8h+2h 抖动） ────
const INIT_REFRESH_MS = 8 * 60 * 60 * 1000;    // 8h
const INIT_JITTER_MS  = 2 * 60 * 60 * 1000;    // 2h 抖动

async function ensureInitialized(apiKey, signal) {
  const state = getOrCreateKeyState(apiKey);
  const now = Date.now();
  if (now < state.nextInitAt) return;

  try {
    // 并行发两个预请求
    const headers = {
      'Content-Type': 'application/json',
      'x-cli-environment': 'production',
      'Authorization': `Bearer ${apiKey}`,
      'x-command-code-version': CC_VERSION,
      ...(CFG.zdr ? { 'x-cmd-zdr': '1' } : {}),
    };
    const fingerprint = state.fingerprint || {};

    await Promise.all([
      fetch(`${CFG.apiBase}/alpha/fingerprint/record`, {
        method: 'POST', headers, signal,
        body: JSON.stringify(fingerprint),
      }).then(r => {
        if (!r.ok) log('warn', 'Fingerprint record failed', { status: r.status });
        else log('info', 'Fingerprint recorded');
      }).catch(e => {
        if (e.name !== 'AbortError') log('warn', 'Fingerprint record error', { error: e.message });
      }),

      fetch(`${CFG.apiBase}/alpha/lifecycle-events`, {
        method: 'POST', headers, signal,
        body: JSON.stringify({
          eventType: 'cli_session_exists',
          metadata: {
            sessionId: `sess_${crypto.randomBytes(8).toString('hex')}`,
            cliVersion: CC_VERSION,
            mode: 'interactive',
            os: `${fingerprint.components.platform}-${fingerprint.components.arch}`,
          },
        }),
      }).then(r => {
        if (!r.ok) log('warn', 'Lifecycle event failed', { status: r.status });
        else log('info', 'Lifecycle event sent');
      }).catch(e => {
        if (e.name !== 'AbortError') log('warn', 'Lifecycle event error', { error: e.message });
      }),
    ]);

    // 成功：8h + 2h 随机抖动
    const jitter = Math.floor(Math.random() * INIT_JITTER_MS);
    state.nextInitAt = Date.now() + INIT_REFRESH_MS + jitter;
    log('info', 'Fingerprint/lifecycle next refresh', { nextIn: `${(INIT_REFRESH_MS + jitter) / 3600000}h` });
  } catch (e) {
    if (e.name !== 'AbortError') log('warn', 'Fingerprint/lifecycle refresh error, will retry next request', { error: e.message });
  }
}

// ── 模型列表 ───────────────────────────────────────
const MODELS = [
  // Anthropic
  { id: 'claude-sonnet-4-6', name: 'Claude Sonnet 4.6' },
  { id: 'claude-opus-4-8', name: 'Claude Opus 4.8' },
  { id: 'claude-opus-4-7', name: 'Claude Opus 4.7' },
  { id: 'claude-haiku-4-5-20251001', name: 'Claude Haiku 4.5' },
  // OpenAI
  { id: 'gpt-5.5', name: 'GPT-5.5' },
  { id: 'gpt-5.4', name: 'GPT-5.4' },
  { id: 'gpt-5.4-mini', name: 'GPT-5.4 Mini' },
  { id: 'gpt-5.3-codex', name: 'GPT-5.3 Codex' },
  // DeepSeek
  { id: 'deepseek/deepseek-v4-pro', name: 'DeepSeek V4 Pro' },
  { id: 'deepseek/deepseek-v4-flash', name: 'DeepSeek V4 Flash' },
  // Kimi
  { id: 'moonshotai/Kimi-K2.6', name: 'Kimi K2.6' },
  { id: 'moonshotai/Kimi-K2.5', name: 'Kimi K2.5' },
  // GLM
  { id: 'zai-org/GLM-5.1', name: 'GLM 5.1' },
  { id: 'zai-org/GLM-5', name: 'GLM 5' },
  // MiniMax
  { id: 'MiniMaxAI/MiniMax-M3', name: 'MiniMax M3' },
  { id: 'MiniMaxAI/MiniMax-M2.7', name: 'MiniMax M2.7' },
  { id: 'MiniMaxAI/MiniMax-M2.5', name: 'MiniMax M2.5' },
  // Qwen
  { id: 'Qwen/Qwen3.6-Max-Preview', name: 'Qwen 3.6 Max Preview' },
  { id: 'Qwen/Qwen3.6-Plus', name: 'Qwen 3.6 Plus' },
  { id: 'Qwen/Qwen3.7-Max', name: 'Qwen 3.7 Max' },
  // Step
  { id: 'stepfun/Step-3.7-Flash', name: 'Step 3.7 Flash' },
  { id: 'stepfun/Step-3.5-Flash', name: 'Step 3.5 Flash' },
  // Xiaomi
  { id: 'xiaomi/mimo-v2.5-pro', name: 'MiMo V2.5 Pro' },
  { id: 'xiaomi/mimo-v2.5', name: 'MiMo V2.5' },
  // Gemini
  { id: 'google/gemini-3.5-flash', name: 'Gemini 3.5 Flash' },
  { id: 'google/gemini-3.1-flash-lite', name: 'Gemini 3.1 Flash Lite' },
];

// ── 工具函数 ───────────────────────────────────────

// 从 sessionId 构造一个假的工作目录路径，再按真实 CLI 规则生成 slug
// 结果形如 "d-users-dev-projects-web-app-a3f2" (和真实 CLI 的 slug 格式一致)
function fakeProjectSlug(sessionId) {
  const names = ['app', 'api', 'backend', 'bot', 'cli', 'core', 'data', 'frontend',
    'lib', 'plugin', 'proxy', 'server', 'service', 'tool', 'web', 'worker'];
  const id = String(sessionId || '');
  const head = id.slice(0, 4);
  // sessionId 既可能是随机 UUID（前 4 位十六进制），也可能是客户端自定义的
  // prompt_cache_key（如 "my-stable-cache-key-001"）。后者按 16 进制解析得 NaN，
  // 会让 slug 变成 "…-undefined-my-s"。失败时退化为确定性字符哈希。
  let idx = parseInt(head, 16);
  if (!Number.isFinite(idx)) {
    let h = 0;
    for (let i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) >>> 0;
    idx = h;
  }
  const name = names[idx % names.length];
  const suffix = head || '0000';
  // 模拟一个类似 C:\Users\dev\projects\{name}-{suffix} 的路径
  const path = `C:\\Users\\dev\\projects\\${name}-${suffix}`;
  return path
    .toLowerCase()
    .replace(/^[a-z]:/i, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function generateTraceparent() {
  const traceId = crypto.randomBytes(16).toString('hex');
  const parentId = crypto.randomBytes(8).toString('hex');
  return `00-${traceId}-${parentId}-01`;
}

function nowUnix() {
  return Math.floor(Date.now() / 1000);
}

function getDateStr() {
  return new Date().toISOString().slice(0, 10);
}

function getEnvironment() {
  return `${process.platform}-${process.arch}, Node.js ${process.version.slice(1)}`;
}

// ── CC 请求体构建 ─────────────────────────────────

function buildCcRequest(openaiReq) {
  const { model, messages, max_tokens, temperature, tools, stream, reasoning_effort, tool_choice, parallel_tool_calls, prompt_cache_key } = openaiReq;

  // 提取系统提示，OpenAI 的 system 与 developer 均映射为系统提示
  // 数组型 content 必须展开取 text 后拼成「字符串」，而不是转成 JSON 字符串，
  // 更不能输出 Anthropic 风格的 content 块数组：CC 上游要求 params.system 恒为
  // 字符串，传数组会被直接拒绝（真机验证：
  // Validation error: Invalid input: expected string, received array at "params.system"）。
  const systemMsgs = messages.filter(m => m.role === 'system' || m.role === 'developer');
  const systemPrompt = systemMsgs.map(m => {
    if (typeof m.content === 'string') return m.content;
    if (Array.isArray(m.content)) return m.content.map(c => c?.text ?? c?.content ?? '').join('\n');
    return m.content == null ? '' : String(m.content);
  }).join('\n');
  const chatMessages = messages.filter(m => m.role !== 'system' && m.role !== 'developer');

  // Build tool_call_id → tool_name reverse lookup
  const toolNameMap = {};
  for (const msg of chatMessages) {
    if (msg.role === 'assistant' && msg.tool_calls) {
      for (const tc of msg.tool_calls) {
        if (tc.id) {
          toolNameMap[tc.id] = tc.function?.name || '';
        }
      }
    }
  }

  // 转换 messages 为 CC 格式
  const ccMessages = chatMessages.map(msg => {
    if (msg.role === 'user') {
      if (typeof msg.content === 'string') {
        return { role: 'user', content: [{ type: 'text', text: msg.content }] };
      }
      // 多模态：数组 content 原样透传（text + image_url → CC image 格式）
      if (Array.isArray(msg.content)) {
        const parts = msg.content.map(part => {
          if (part.type === 'image_url') {
            const url = part.image_url?.url || '';
            // CC CLI 真实格式: { type: "image", image: "data:image/jpeg;base64,..." }
            return { type: 'image', image: url };
          }
          return part;
        }).filter(Boolean);
        return { role: 'user', content: parts };
      }
      return { role: 'user', content: [{ type: 'text', text: String(msg.content) }] };
    }
    if (msg.role === 'assistant') {
      const parts = [];
      // 思考内容必须回传：CC 在 thinking 模式下校验 reasoning 是否随历史带回，
      // 丢弃会让上游直接拒绝。次序也必须与 CC CLI 的抓包格式一致 ——
      // [reasoning, text, tool-call]，reasoning 在最前。
      if (msg.reasoning_content) {
        parts.push({ type: 'reasoning', text: msg.reasoning_content });
      }
      if (msg.content && typeof msg.content === 'string') {
        if (msg.content) parts.push({ type: 'text', text: msg.content });
      } else if (msg.content && Array.isArray(msg.content)) {
        for (const part of msg.content) {
          if (!part) continue;
          if (part.type === 'text') parts.push(part);
          // 客户端直接把 reasoning 放在 content 数组里时同样透传；
          // 已有 reasoning_content 字段则不重复
          else if (part.type === 'reasoning' && !msg.reasoning_content) parts.push(part);
        }
      }
      if (msg.tool_calls) {
        for (const tc of msg.tool_calls) {
          parts.push({
            type: 'tool-call',
            toolCallId: tc.id,
            toolName: tc.function?.name || '',
            input: (typeof tc.function?.arguments === 'string' ? tryParseJSON(tc.function.arguments) : (tc.function?.arguments || {})),
          });
        }
      }
      return { role: 'assistant', content: parts };
    }
    if (msg.role === 'tool') {
      return {
        role: 'tool',
        content: [{
          type: 'tool-result',
          toolCallId: msg.tool_call_id,
          toolName: toolNameMap[msg.tool_call_id] || msg.name || '',
          output: { type: 'text', value: typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content) },
        }],
      };
    }
    // 未知 role 兜底：归一化为 user 并保证 content 为数组，避免 CC 校验拒绝
    return { role: 'user', content: [{ type: 'text', text: String(msg.content ?? '') }] };
  });

  const hasMessageCacheMarker = ccMessages.some(msg =>
    Array.isArray(msg.content) && msg.content.some(part => part?.cache_control));
  if (prompt_cache_key && !hasMessageCacheMarker) {
    const firstUserMessage = ccMessages.find(msg => msg.role === 'user' && Array.isArray(msg.content));
    const cacheBoundary = firstUserMessage?.content.findLast(part => part?.type === 'text');
    if (cacheBoundary) cacheBoundary.cache_control = { type: 'ephemeral' };
  }

  const threadId = newThreadId();

  const body = {
    config: {
      workingDir: process.cwd(),
      date: getDateStr(),
      environment: getEnvironment(),
      structure: [],
      isGitRepo: false,
      currentBranch: '',
      mainBranch: '',
      gitStatus: '',
      recentCommits: [],
    },
    memory: null,
    taste: null,
    skills: '',
    permissionMode: 'standard',
    params: {
      model: model || 'deepseek/deepseek-v4-flash',
      messages: ccMessages,
      max_tokens: Math.min(max_tokens || 64000, 200000),
      stream: true,  // CC API 总是 stream
    },
  };

  // 条件字段
  if (systemPrompt) {
    body.params.system = systemPrompt;
  } else if (CFG.emptySystemPlaceholder) {
    // CC 上游在 params.system 缺省时会注入自身约 7.5K token 的默认提示词（进入
    // 默认上下文/前缀路径），既产生大量 cached tokens 又污染对话（模型会以为
    // 自己在 CC 的可执行目录里，见 issue #17）。发一个空格占位即可绕过，
    // 真机验证 prompt_tokens 从 7653 降到 85。
    // 默认开启；config.json 设 "emptySystemPlaceholder": false 或环境变量
    // CC_EMPTY_SYSTEM_PLACEHOLDER=false 可关闭（回到原生的缺省行为）。
    body.params.system = ' ';
  }
  if (temperature !== undefined) {
    body.params.temperature = temperature;
  }
  if (reasoning_effort !== undefined) {
    body.params.reasoning_effort = reasoning_effort;
  }
  if (tools && tools.length > 0) {
    body.params.tools = tools.map(t => ({
      type: t.type || 'function',
      name: t.function?.name || t.name || '',
      description: t.function?.description || t.description || '',
      input_schema: t.function?.parameters || t.input_schema || { type: 'object', properties: {} },
    }));
  }
  if (tool_choice !== undefined) {
    // OpenAI 格式 → CC (Anthropic 风格) 格式
    if (typeof tool_choice === 'string') {
      const map = { 'auto': 'auto', 'none': 'none', 'required': 'any' };
      body.params.tool_choice = { type: map[tool_choice] || 'auto' };
    } else if (tool_choice.type === 'function') {
      // OpenAI object → Anthropic object
      body.params.tool_choice = { type: 'tool', name: tool_choice.function?.name };
    } else {
      body.params.tool_choice = tool_choice;
    }
  }
  if (parallel_tool_calls !== undefined) {
    body.params.parallel_tool_calls = parallel_tool_calls;
  }

  return body;
}

function tryParseJSON(str) {
  try { return JSON.parse(str); } catch { return {}; }
}

// ── CC NDJSON → OpenAI SSE 转换 ────────────────────

function createSseTranslator(model, completionId, created) {
  let chunkIndex = 0;
  let sentRole = false;
  let finishReason = null;
  let usage = null;
  let toolCallIndex = 0;

  return {
    lastCcEvent: '',
    upstreamError: null,
    inputTokens: 0,
    outputTokens: 0,
    cachedInputTokens: 0,
    /** 解析一行 NDJSON，返回 OpenAI chunk 数组 */
    parseLine(line) {
      const trimmed = line.trim();
      if (!trimmed || trimmed === '[DONE]' || trimmed.startsWith(':')) return null;

      let event;
      try { event = JSON.parse(trimmed); } catch { return null; }
      if (!event.type) return null;
      this.lastCcEvent = event.type;

      const out = [];

      switch (event.type) {
        case 'text-start':
        case 'reasoning-start':
        case 'start':
        case 'start-step':
          // 忽略，无用户可见内容
          break;

        case 'text-delta': {
          const text = event.text || event.delta || '';
          if (!text) break;
          const delta = chunkIndex === 0 ? { role: 'assistant', content: text } : { content: text };
          chunkIndex++;
          sentRole = true;
          out.push(makeChunk(completionId, created, model, delta, null, null));
          break;
        }

        case 'reasoning-delta': {
          const text = event.text || '';
          if (!text) break;
          const delta = chunkIndex === 0
            ? { role: 'assistant', reasoning_content: text }
            : { reasoning_content: text };
          chunkIndex++;
          out.push(makeChunk(completionId, created, model, delta, null, null));
          break;
        }

        case 'tool-call': {
          const id = event.toolCallId || `call_${Date.now()}_${toolCallIndex}`;
          const name = event.toolName || '';
          const args = typeof event.input === 'string' ? event.input : JSON.stringify(event.input || {});
          const tcEntry = { index: toolCallIndex, id, type: 'function', function: { name, arguments: args } };
          const delta = chunkIndex === 0
            ? { role: 'assistant', content: null, tool_calls: [tcEntry] }
            : { tool_calls: [tcEntry] };
          chunkIndex++;
          toolCallIndex++;
          out.push(makeChunk(completionId, created, model, delta, null, null));
          break;
        }

        case 'finish-step': {
          if (event.finishReason) finishReason = mapFinishReason(event.finishReason);
          if (event.usage) {
            usage = event.usage;
            this.inputTokens = event.usage.inputTokens ?? 0;
            this.outputTokens = event.usage.outputTokens ?? 0;
            this.cachedInputTokens = event.usage.cachedInputTokens ?? 0;
          }
          break;
        }

        case 'finish': {
          const fr = finishReason || mapFinishReason(event.finishReason || 'stop');
          const u = event.totalUsage || usage || {};
          normalizeUsage(u);
          this.inputTokens = u.inputTokens ?? 0;
          this.outputTokens = u.outputTokens ?? 0;
          this.cachedInputTokens = u.cachedInputTokens ?? 0;
          const openaiUsage = u ? {
            prompt_tokens: u.inputTokens ?? 0,
            completion_tokens: u.outputTokens ?? 0,
            total_tokens: (u.inputTokens ?? 0) + (u.outputTokens ?? 0),
            prompt_tokens_details: { cached_tokens: u.cachedInputTokens ?? 0 },
          } : undefined;
          out.push(makeChunk(completionId, created, model, {}, fr, openaiUsage));
          break;
        }

        case 'error': {
          const msg = event.error?.message || event.message || 'Unknown error';
          log('warn', 'CC stream error', { message: msg });
          this.upstreamError = mapCcEventError(event);
          // Don't emit a finish_reason chunk — let the natural stream termination
          // handle it. Otherwise a subsequent finish(tool_calls) would be ignored
          // by downstream agent loops that stop at the first finish_reason.
          break;
        }

        case 'reasoning-end': case 'provider-metadata': case 'tool-input-start': case 'tool-input-delta': case 'tool-input-end': case 'tool-error': case 'text-end':
          // Silent - no user-visible content
          break;
        default:
          log('warn', 'Unknown CC event type', { type: event.type });
          break;
      }

      return out.length > 0 ? out : null;
    },

    /** 获取 SSE 结束标记 */
    getDoneEvent() {
      return 'data: [DONE]\n\n';
    },
  };
}

function makeChunk(id, created, model, delta, finishReason, usage) {
  const chunk = {
    id,
    object: 'chat.completion.chunk',
    created,
    model,
    choices: [{ index: 0, delta, finish_reason: finishReason || null }],
  };
  if (usage) chunk.usage = usage;
  return `data: ${JSON.stringify(chunk)}\n\n`;
}

// normalize CC usage stats:
// - outputTokens=0 → zero everything (anti false billing)
function normalizeUsage(u) {
  if (!u) return;
  const ot = Number(u.outputTokens);
  if (!ot) {  // 0, null, undefined, NaN → zero input + cached (anti false billing)
    u.inputTokens = 0;
    u.cachedInputTokens = 0;
  }
}

// CC 的 inputTokens 是「总数」（含缓存命中部分），而 Anthropic 的 input_tokens 只计
// 非缓存部分 —— 官方 SDK 注释：Total input tokens in a request is the summation of
// `input_tokens`, `cache_creation_input_tokens`, and `cache_read_input_tokens`。
// 直接把 CC 的 inputTokens 当 input_tokens 转发，会让下游把两者当成互不重叠的两部分，
// 相加后约为真实输入的两倍（issue #25）。
//
// CC 实际已经算好：inputTokenDetails.noCacheTokens（实测 noCacheTokens + cacheReadTokens
// === inputTokens）。优先采用该字段；缺失时回退到减法，保证老版本上游也能得到正确值。
function anthropicInputTokens(usage, noCacheOverride) {
  const u = usage || {};
  if (typeof noCacheOverride === 'number' && noCacheOverride >= 0) return noCacheOverride;
  const noCache = u.inputTokenDetails && u.inputTokenDetails.noCacheTokens;
  if (typeof noCache === 'number' && noCache >= 0) return noCache;
  const cacheRead = u.cachedInputTokens || (u.inputTokenDetails && u.inputTokenDetails.cacheReadTokens) || 0;
  const cacheWrite = (u.inputTokenDetails && u.inputTokenDetails.cacheWriteTokens) || 0;
  return Math.max(0, (u.inputTokens || 0) - cacheRead - cacheWrite);
}

function mapFinishReason(reason) {
  switch (reason) {
    case 'tool-calls': return 'tool_calls';
    case 'length': return 'length';
    case 'stop': return 'stop';
    default: return reason || 'stop';
  }
}

// ── 错误映射 ───────────────────────────────────────
const CC_STATUS_MAP = {
  400: { status: 400, type: 'invalid_request_error' },
  401: { status: 401, type: 'authentication_error' },
  402: { status: 429, type: 'rate_limit_error' },       // payment required → rate limit
  403: { status: 401, type: 'authentication_error' },
  404: { status: 404, type: 'not_found' },
  422: { status: 400, type: 'invalid_request_error' },
  429: { status: 429, type: 'rate_limit_error' },
  500: { status: 502, type: 'upstream_error' },
  502: { status: 502, type: 'upstream_error' },
  503: { status: 503, type: 'temporarily_unavailable' },
};

function mapCcError(ccStatus, ccBody) {
  const mapped = CC_STATUS_MAP[ccStatus] || { status: 502, type: 'upstream_error' };
  let message = `CC API error (${ccStatus})`;

  if (ccBody) {
    try {
      const parsed = JSON.parse(ccBody);
      message = parsed.error?.message || parsed.message || message;
    } catch {
      message = ccBody.slice(0, 200) || message;
    }
  }

  // CC 429 响应可能带 retry-after
  if (ccStatus === 429) {
    return {
      status: 429,
      body: {
        error: { message, type: 'rate_limit_error' },
        retry_after: 30,
      },
    };
  }

  return { status: mapped.status, body: { error: { message, type: mapped.type } } };
}

function mapCcEventError(event) {
  const message = event.error?.message || event.message || 'Unknown CC error';
  const statusMatch = message.match(/^<(\d{3})>/);
  const ccStatus = statusMatch ? Number(statusMatch[1]) : 502;
  const mapped = CC_STATUS_MAP[ccStatus] || { status: 502, type: 'upstream_error' };

  // 与 mapCcError 保持一致：终态为 429 时带上 retry_after，
  // 否则客户端 SDK 拿不到退避提示（402 也映射成 429，一视同仁）
  if (mapped.status === 429) {
    return {
      status: 429,
      body: { error: { message, type: 'rate_limit_error' }, retry_after: 30 },
    };
  }

  return { status: mapped.status, body: { error: { message, type: mapped.type } } };
}

// ── HTTP 请求处理 ──────────────────────────────────

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let totalSize = 0;
    let settled = false;
    let drained = 0;
    // 413 拒绝后转入排空模式：继续读取并丢弃剩余请求体，保持 keep-alive 连接可复用，
    // 让客户端明确收到 413 而不是 Connection reset（issue #7）。
    // 但若客户端无视 413 持续上传超过 DRAIN_LIMIT，则强制掐断，不无限吞带宽。
    const DRAIN_LIMIT = 32 * 1024 * 1024;
    req.on('data', c => {
      if (settled) {
        drained += c.length;
        if (drained > DRAIN_LIMIT) { try { req.destroy(); } catch {} }
        return;
      }
      totalSize += c.length;
      if (totalSize > MAX_BODY_SIZE) {
        settled = true;
        chunks.length = 0;
        const mb = Math.round(MAX_BODY_SIZE / 1024 / 1024);
        const err = new Error(`Request body exceeds ${mb}MB limit`);
        err.statusCode = 413;
        reject(err);
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      if (settled) return;
      settled = true;
      try { resolve(JSON.parse(Buffer.concat(chunks).toString())); }
      catch { reject(new Error('Invalid JSON')); }
    });
    req.on('error', e => { if (!settled) { settled = true; reject(e); } });
  });
}

// 下游背压：res.write() 返回 false 表示 socket 写缓冲已超 highWaterMark（消费者跟不上）。
// 忽略它会让整个上游流在内存中无界堆积 —— 客户端不读时 RSS 随上游流一起增长（issue #20）。
// 必须同时监听 close/error，否则客户端断连会让请求协程永久挂起。
// CLIENT_DRAIN_TIMEOUT_MS > 0 时额外加一道空闲看门狗：超时则 destroy 该响应，
// 由此触发既有的 res 'close' 处理器 → aborted=true → 中止 CC 上游，无需改动各调用点。
function waitDrain(res) {
  if (!res.writableNeedDrain) return Promise.resolve();
  return new Promise((resolve) => {
    let timer = null;
    const done = () => {
      res.off('drain', done); res.off('close', done); res.off('error', done);
      if (timer) { clearTimeout(timer); timer = null; }
      resolve();
    };
    res.once('drain', done); res.once('close', done); res.once('error', done);
    if (CLIENT_DRAIN_TIMEOUT_MS > 0) {
      timer = setTimeout(() => {
        log('warn', 'Client stalled on backpressure, dropping connection', {
          path: res.req?.url || '(unknown)',
          timeoutMs: CLIENT_DRAIN_TIMEOUT_MS,
          bufferedBytes: res.writableLength,
        });
        try { res.destroy(); } catch {}
        done();
      }, CLIENT_DRAIN_TIMEOUT_MS);
    }
  });
}

// 上游读空闲看门狗：复用单个定时器，避免「每个 chunk 新建一个 setTimeout 且从不清理」。
// 实测每个待触发定时器滞留约 225B；稳态滞留 = 吞吐 × 超时窗口 × 每响应 chunk 数 × 225B
// （50 rps × 2000 chunk × 30s ≈ 644MB，非流式 90s 窗口约为其三倍）。
// arm() 用 refresh() 把窗口重置为「本轮 read 开始」，与原实现语义一致：超时只计 reader.read() 的等待。
function createIdleWatchdog(timeoutMs) {
  let rejectFn = null;
  const expired = new Promise((_, reject) => { rejectFn = reject; });
  expired.catch(() => {}); // 读循环退出后定时器才触发时，避免 unhandledRejection
  const timer = setTimeout(() => rejectFn(new Error('STREAM_IDLE_TIMEOUT')), timeoutMs);
  return {
    arm() { timer.refresh(); return expired; },
    dispose() { clearTimeout(timer); },
  };
}

function sendJSON(res, status, data) {
  const headers = { 'Content-Type': 'application/json' };
  if (data && data.retry_after !== undefined) {
    headers['Retry-After'] = String(data.retry_after);
  }
  res.writeHead(status, headers);
  res.end(JSON.stringify(data));
}

// ── 流式转发 ────────────────────────────────────────

async function forwardToCC(body, apiKey, incomingHeaders = {}, signal, promptCacheKey) {
  const url = `${CFG.apiBase}/alpha/generate`;
  const traceparent = generateTraceparent();
  const sessionId = getSessionId(incomingHeaders, apiKey, promptCacheKey);

  const headers = {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${apiKey}`,
    'x-cli-environment': 'production',
    'x-command-code-version': CC_VERSION,
    'x-session-id': sessionId,
    'x-co-flag': 'false',
    'x-taste-learning': 'false',
    'x-project-slug': fakeProjectSlug(sessionId),
    'traceparent': traceparent,
  };

  if (CFG.zdr || incomingHeaders['x-cmd-zdr'] === '1') {
    headers['x-cmd-zdr'] = '1';
  }

  const response = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
    signal,
  });

  return response;
}

/**
 * 带 Key 池 failover 的转发。
 *
 * request 模式（旧行为）：可重试失败即换下一个 Key，最多 1 + maxRetries 次。
 * session 模式（默认）：同一会话固定用一个 Key；普通错误在本 Key 上连续重试到
 * failThreshold 次才换 Key，额度/凭证类错误立即换 Key，会话自动重绑到新 Key。
 */
async function forwardToCCWithFailover(body, clientKey, req, signal, promptCacheKey, sessionBody) {
  const settings = keyStore.settings;
  const sessionMode = settings.mode === 'session';
  // 会话指纹要用「客户端原始请求体」推导：body 已是 CC 上游格式（字段在 params 下）
  const sessionId = sessionMode ? deriveSessionId(req, sessionBody || body, clientKey) : null;
  const maxKeys = Math.max(1, 1 + Math.max(0, keyStore.lb.maxRetries || 0));
  const perKeyAttempts = sessionMode ? Math.max(1, settings.failThreshold || 3) : 1;
  const budget = Math.max(1, Math.min(20, maxKeys * perKeyAttempts));

  const abandoned = new Set();   // 本轮已放弃的 Key
  let pick = null;
  let keysUsed = 0;
  let lastMapped = null;
  let lastPick = null;

  for (let attempt = 0; attempt < budget; attempt++) {
    if (!pick) {
      pick = sessionMode
        ? pickSessionKey(sessionId, clientKey, req, body, abandoned)
        : pickUpstreamKey(clientKey, req, abandoned);
      if (!pick && settings.onAllExhausted === 'best-effort') pick = pickBestEffort(abandoned);
      if (!pick) return { error: poolUnavailableResponse(), pick: null };
      keysUsed++;
    }
    lastPick = pick;

    let failure = null; // { mapped, retryable, trigger }
    try {
      await ensureInitialized(pick.apiKey, signal);
      const response = await forwardToCC(body, pick.apiKey, req.headers, signal, promptCacheKey);

      if (response.ok) {
        markKeySuccess(pick);
        recordSessionSuccess(sessionId);
        return { response, pick };
      }

      const errorText = await response.text().catch(() => '');
      log('error', 'CC API error', {
        status: response.status,
        body: summarizeUpstreamError(errorText),
        keyId: pick.id,
        attempt: attempt + 1,
      });
      const trigger = classifyFailure(response.status, errorText);
      const retryable = isRetryableCcFailure(response.status, errorText);
      markKeyFailure(pick, response.status, retryable, trigger, errorText);
      if (retryable) recordSessionFailure(sessionId, trigger);
      failure = { mapped: mapCcError(response.status, errorText), retryable, trigger };
    } catch (e) {
      if (signal?.aborted) throw e;
      log('error', 'CC forward failed', { message: e.message, keyId: pick.id, attempt: attempt + 1 });
      markKeyFailure(pick, 0, true, 'any-error');
      recordSessionFailure(sessionId, 'any-error');
      failure = { mapped: mapCcError(502, e.message), retryable: true, trigger: 'any-error' };
    }

    lastMapped = failure.mapped;
    if (!failure.retryable || attempt >= budget - 1) return { error: lastMapped, pick };

    // ── 决定：继续用同一个 Key 重试，还是换 Key ──
    // 额度/限流、凭证失效、403（可能换了 Key 就能用）都立即换 Key；
    // 其余错误先在本 Key 上重试，连续失败到阈值再换。
    const immediate = failure.trigger === 'quota-exhausted'
      || failure.trigger === 'disabled'
      || failure.trigger === 'forbidden';
    let transfer = true;
    if (sessionMode && !immediate) {
      const rec = sessionId ? sessionRoutes.get(sessionId) : null;
      const sessionFails = rec?.failures || 0;
      const keyFails = findKey(pick.id)?.consecutiveFailures || 0;
      const threshold = Math.max(1, settings.failThreshold || 3);
      transfer = sessionFails >= threshold || keyFails >= threshold;
    }

    if (!transfer) continue;              // 同一 Key 再试一次
    if (keysUsed >= maxKeys) return { error: lastMapped, pick };
    abandoned.add(pick.id);
    pick = null;
    if (sessionId) sessionRoutes.delete(sessionId); // 让下次选取重绑到新 Key
  }

  return { error: lastMapped || mapCcError(502, 'All upstream keys failed'), pick: lastPick };
}

// ── 路由 ────────────────────────────────────────────

async function handleChatCompletions(req, res) {
  let openaiReq;
  try {
    openaiReq = await readBody(req);
  } catch (e) {
    if (e.statusCode === 413) {
      sendJSON(res, 413, { error: { message: e.message, type: 'invalid_request_error' } });
      return;
    }
    sendJSON(res, 400, { error: { message: 'Invalid JSON body', type: 'invalid_request_error' } });
    return;
  }

  const clientKey = extractClientApiKey(req.headers);
  if (!hasAnyUsableKey() && keyStore.settings.onAllExhausted !== 'best-effort') {
    const unavailable = poolUnavailableResponse();
    sendJSON(res, unavailable.status, unavailable.body);
    return;
  }

  const stream = openaiReq.stream === true;
  const model = openaiReq.model || 'deepseek/deepseek-v4-flash';
  const completionId = `chatcmpl-${randomUUID().slice(0, 12)}`;
  const created = nowUnix();

  // 构建 CC 请求体
  const ccBody = buildCcRequest(openaiReq);

  // AbortController 用于客户端断连时真正打断 CC 上游（pi-commandcode-provider 模式）
  const abortController = new AbortController();
  let aborted = false;
  // 提前初始化，断连回调/超时 catch 安全引用（避免块级作用域 ReferenceError）
  const startTime = Date.now();
  let bytesReceived = 0; let lastCcEvent = ''; let keepaliveCount = 0; let fullText = '';
  let reader = null;
  let translator = null;

  try {
    // Key 池选取 + 可重试失败自动换 key（fingerprint/lifecycle 在 failover 内完成）
    const fwd = await forwardToCCWithFailover(ccBody, clientKey, req, abortController.signal, openaiReq.prompt_cache_key, openaiReq);
    if (fwd.error) {
      sendJSON(res, fwd.error.status, fwd.error.body);
      return;
    }
    const ccResponse = fwd.response;

    // 下游断连检测：打断 CC 上游 + 记录日志
    res.on('close', () => {
      if (res.writableEnded) return; // Normal completion, not a disconnect
      aborted = true;
      const reason = lastCcEvent?.startsWith('tool-input') ? 'tool-input-silent-timeout'
        : lastCcEvent?.includes('delta') ? 'streaming-active-disconnect'
        : 'client-hangup';
      abortController.signal.aborted || log('warn', 'Client disconnected', {
        path: '/v1/chat/completions',
        model, completionId, reason,
        streaming: stream,
        elapsedMs: Date.now() - startTime,
        bytesSent: bytesReceived,
        lastCcEvent: lastCcEvent || '(none)',
        keepaliveCount,
        inputTokens: translator?.inputTokens ?? 0,
        outputTokens: translator?.outputTokens ?? 0,
        cachedInputTokens: translator?.cachedInputTokens ?? 0,
      });
      if (!abortController.signal.aborted) {
        // 断连前抢发 usage=0 终止 chunk，避免下游自行估算 token
        try {
          res.write(`data: ${JSON.stringify({
            id: completionId,
            object: 'chat.completion.chunk',
            created,
            model,
            choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
            usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0, prompt_tokens_details: { cached_tokens: 0 } },
          })}\n\n`);
          res.write('data: [DONE]\n\n');
        } catch {}
        try { abortController.abort(); } catch {}
      }
    });

    if (stream) {
      // ── 流式响应 ──
      translator = createSseTranslator(model, completionId, created);
      let buffer = '';
      let started = false; // 延迟写 200 header，超时/output=0 时返回 JSON 429/502 让 SDK 自动重试
      const decoder = new TextDecoder();
      reader = ccResponse.body.getReader();

      const idle = createIdleWatchdog(STREAM_IDLE_TIMEOUT_MS);
      try {
        while (true) {
          const result = await Promise.race([reader.read(), idle.arm()]);
          const { done, value } = result;
          if (done) break;
          if (aborted) break;
          bytesReceived += value.length;

          const chunkText = decoder.decode(value, { stream: true });
          buffer += chunkText;
          // 仅在新到数据含换行时才切分：buffer 中永不残留 '\n'，故无换行即无完整行。
          // 避免对增长中的超长单行（大 tool-call / tool_result）反复做全量 split —— O(n²) → O(n)。
          let lines = [];
          if (chunkText.indexOf('\n') !== -1) {
            lines = buffer.split('\n');
            buffer = lines.pop() || '';
          }

          let hadOutput = false;
          for (const line of lines) {
            const events = translator.parseLine(line);
            if (events) {
              if (!started) {
                res.writeHead(200, {
                  'Content-Type': 'text/event-stream',
                  'Cache-Control': 'no-cache',
                  'Connection': 'keep-alive',
                  'X-Accel-Buffering': 'no',
                });
                started = true;
              }
              for (const evt of events) res.write(evt);
              await waitDrain(res);
              hadOutput = true;
            }
            if (translator.lastCcEvent) lastCcEvent = translator.lastCcEvent;
          }
          // silent events 期间发 keepalive，防止客户端超时断开
          if (started && !hadOutput) {
            try { res.write(': keepalive\n\n'); keepaliveCount++; } catch {}
            await waitDrain(res);
          }
        }

        if (!aborted) {
          // 成功完成一次请求，重置连续超时计数
          consecutiveTimeouts = 0;
          // 处理剩余 buffer
          if (buffer.trim()) {
            const events = translator.parseLine(buffer);
            if (events) {
              if (!started) started = true;
              for (const evt of events) res.write(evt);
              await waitDrain(res);
            }
          }
          if (translator.upstreamError) {
            if (!started) {
              sendJSON(res, translator.upstreamError.status, translator.upstreamError.body);
              return;
            }
            try { res.write(`data: ${JSON.stringify(translator.upstreamError.body)}\n\n`); } catch {}
          // 输出 token 为 0 时记为错误，避免下游异常计费
          } else if (translator.outputTokens === 0) {
            try { if (!abortController.signal.aborted) abortController.abort(); } catch {}
            if (!started) {
              sendJSON(res, 429, { error: { message: 'Empty response from upstream (zero output tokens)', type: 'rate_limit_error' }, retry_after: 10 });
              return;
            }
            try { res.write(`data: ${JSON.stringify({ error: { message: 'Empty response from upstream (zero output tokens)', type: 'rate_limit_error' }, retry_after: 10 })}\n\n`); } catch {}
          } else {
            if (!started) {
              res.writeHead(200, {
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'X-Accel-Buffering': 'no',
              });
              started = true;
            }
            res.write(translator.getDoneEvent());
          }
        }
      } catch (e) {
        if (aborted) {
          // 客户端已断连，只清理（close handler 已调用 abortController.abort()）
          try { reader.cancel(); } catch {}
        } else if (e.message === 'STREAM_IDLE_TIMEOUT') {
          log('warn', 'Stream idle timeout', {
            path: '/v1/chat/completions',
            model,
            streaming: true,
            timeoutMs: STREAM_IDLE_TIMEOUT_MS,
            elapsedMs: Date.now() - startTime,
            id: completionId,
            bytesReceived,
            lastCcEvent: lastCcEvent || '(none)',
            inputTokens: translator.inputTokens,
            outputTokens: translator.outputTokens,
            cachedInputTokens: translator.cachedInputTokens,
          });
          try { reader.cancel(); } catch {}
          try { abortController.abort(); } catch {} // 打断 CC 上游，避免浪费 token
          consecutiveTimeouts++;
          const timeoutMsg = consecutiveTimeouts >= TIMEOUT_REDUCE_CONTEXT_THRESHOLD
            ? 'Response timeout - try reducing context length (summarize earlier messages)'
            : 'Response timeout - request timed out';
          if (!started) {
            sendJSON(res, 429, { error: { message: timeoutMsg, type: 'rate_limit_error', input_tokens: 0 }, retry_after: 5 });
            return;
          }
          if (!res.writableEnded) {
            try { res.write(`data: ${JSON.stringify({ error: { message: timeoutMsg, type: 'rate_limit_error' }, retry_after: 5 })}\n\n`); } catch {}
            try { res.destroy(); } catch {}
          }
        } else {
          log('error', 'Stream error', { message: e.message });
          try { abortController.abort(); } catch {} // 打断 CC 上游
          if (!started) {
            sendJSON(res, 502, { error: { message: `Upstream error: ${e.message}`, type: 'proxy_error', input_tokens: 0 }, retry_after: 10 });
            return;
          }
          if (!res.writableEnded) {
            try { res.write(`data: ${JSON.stringify({ error: { message: e.message, type: 'proxy_error' } })}\n\n`); } catch {}
          }
        }
      } finally {
        idle.dispose();
      }

      if (!res.writableEnded) res.end();
    } else {
      // ── 非流式响应（缓冲完整 NDJSON）──
      let reasoningContent = '';
      let finishReason = 'stop';
      let usage = null;
      let toolCalls = null;
      let upstreamError = null;

      reader = ccResponse.body.getReader();
      const decoder = new TextDecoder();
      let buf = '';

      const processLines = () => {
        const lines = buf.split('\n');
        buf = lines.pop() || '';
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed || trimmed === '[DONE]' || trimmed.startsWith(':')) continue;
          try {
            const event = JSON.parse(trimmed);
            switch (event.type) {
              case 'text-delta': lastCcEvent = event.type; fullText += event.text || ''; break;
              case 'reasoning-delta': lastCcEvent = event.type; reasoningContent += event.text || ''; break;
              case 'tool-call':
                lastCcEvent = event.type;
                toolCalls = toolCalls || [];
                toolCalls.push({
                  id: event.toolCallId || ('call_' + randomUUID().slice(0, 8)),
                  type: 'function',
                  function: {
                    name: event.toolName || '',
                    arguments: typeof event.input === 'string' ? event.input : JSON.stringify(event.input || {}),
                  },
                });
                break;
              case 'finish':
                lastCcEvent = event.type;
                finishReason = mapFinishReason(event.finishReason || 'stop');
                if (event.totalUsage) usage = event.totalUsage;
                break;
              case 'error':
                lastCcEvent = event.type;
                log('warn', 'CC stream error (non-stream)', { message: event.error?.message || event.message });
                upstreamError = mapCcEventError(event);
                break;
              case 'reasoning-end': case 'provider-metadata': case 'tool-input-start': case 'tool-input-delta': case 'tool-input-end': case 'tool-error': case 'text-end':
                // Silent - no user-visible content
                break;
              default:
                log('warn', 'Unknown CC event type', { type: event.type });
                break;
            }
          } catch {}
        }
      };

      const idle = createIdleWatchdog(NONSTREAM_IDLE_TIMEOUT_MS);
      while (true) {
        const result = await Promise.race([reader.read(), idle.arm()]);
        const { done, value } = result;
        if (done) break;
        bytesReceived += value.length;
        const chunkText = decoder.decode(value, { stream: true });
        buf += chunkText;
        // 无换行则不可能产生完整行，跳过全量 split（见 handleChatCompletions 流式段同处说明）
        if (chunkText.indexOf('\n') !== -1) processLines();
      }
      idle.dispose();
      processLines();

      if (upstreamError) {
        sendJSON(res, upstreamError.status, upstreamError.body);
        return;
      }

      // 输出 token 为 0 时记为错误，避免下游异常计费
      if ((usage?.outputTokens ?? 0) === 0) {
        try { if (!abortController.signal.aborted) abortController.abort(); } catch {}
        sendJSON(res, 429, { error: { message: 'Empty response from upstream (zero output tokens)', type: 'rate_limit_error' }, retry_after: 10 });
        return;
      }

      consecutiveTimeouts = 0;
      sendJSON(res, 200, {
        id: completionId,
        object: 'chat.completion',
        created,
        model,
        choices: [{
          index: 0,
          message: Object.assign(
            { role: 'assistant', content: fullText || null },
            toolCalls ? { tool_calls: toolCalls } : {},
            reasoningContent ? { reasoning_content: reasoningContent } : {},
          ),
          finish_reason: finishReason,
        }],
    usage: (() => {
      if (!usage) usage = {};
      normalizeUsage(usage);
      return {
        prompt_tokens: usage.inputTokens ?? 0,
        completion_tokens: usage.outputTokens ?? 0,
        total_tokens: (usage.inputTokens ?? 0) + (usage.outputTokens ?? 0),
        prompt_tokens_details: { cached_tokens: usage.cachedInputTokens ?? 0 },
      };
    })(),
      });
    }
  } catch (e) {
    if (abortController.signal.aborted) {
      log('warn', 'Request cancelled (client disconnected before CC response)', {
        path: '/v1/chat/completions',
        model,
        completionId,
      });
    } else if (e.message === 'STREAM_IDLE_TIMEOUT') {
      log('warn', 'Stream idle timeout', {
        path: '/v1/chat/completions',
        model,
        streaming: false,
        timeoutMs: NONSTREAM_IDLE_TIMEOUT_MS,
        elapsedMs: Date.now() - startTime,
        id: completionId,
        bytesReceived,
        lastCcEvent: lastCcEvent || '(none)',
        partialLen: fullText ? fullText.length : 0,
      });
      try { reader?.cancel(); } catch {}
      try { abortController.abort(); } catch {} // 打断 CC 上游
      consecutiveTimeouts++;
      const timeoutMsg = consecutiveTimeouts >= TIMEOUT_REDUCE_CONTEXT_THRESHOLD
        ? 'Response timeout - try reducing context length (summarize earlier messages)'
        : 'Response timeout - request timed out';
      res.setHeader('Retry-After', '5');
      sendJSON(res, 429, { error: { message: timeoutMsg, type: 'rate_limit_error', input_tokens: 0 }, retry_after: 5 });
    } else {
      log('error', 'Upstream error', { message: e.message });
      try { abortController.abort(); } catch {} // 打断 CC 上游
      sendJSON(res, 502, { error: { message: `Upstream error: ${e.message}`, type: 'proxy_error', input_tokens: 0 }, retry_after: 10 });
    }
  }
}

// ── Anthropic /v1/messages 协议转换 ─────────────────

function mapAnthropicStopReason(finishReason) {
  switch (finishReason) {
    case 'tool_calls': return 'tool_use';
    case 'length': return 'max_tokens';
    case 'stop': return 'end_turn';
    default: return 'end_turn';
  }
}

// Generate a Claude-format fake signature for thinking blocks.
// Anthropic validates thinking signatures cryptographically; third-party
// proxies cannot mint valid ones. Claude Code's shallow check only requires
// base64 starting with 'E' (single-layer) / 'R' (double-layer) with payload
// first byte 0x12 — this satisfies that, letting CC display thinking.
// The payload is derived from the thinking text so each block's signature
// differs (closer to spec, avoids identical-signature quirks).
function fakeThinkingSignature(thinkingText) {
  const seed = crypto.createHash('sha256').update(thinkingText || 'dsh-proxy-thinking').digest().subarray(0, 64);
  const raw = Buffer.concat([Buffer.from([0x12, seed.length]), seed]);
  return raw.toString('base64');
}

function buildAnthropicResponse(model, fullText, toolCalls, finishReason, usage, thinkingText) {
  const content = [];
  if (thinkingText) content.push({ type: 'thinking', thinking: thinkingText, signature: fakeThinkingSignature(thinkingText) });
  if (fullText) content.push({ type: 'text', text: fullText });
  if (toolCalls) {
    for (const tc of toolCalls) {
      let input = {};
      try { input = JSON.parse(tc.function.arguments); } catch { input = {}; }
      content.push({ type: 'tool_use', id: tc.id, name: tc.function.name, input });
    }
  }
  return {
    id: `msg_${randomUUID().slice(0, 12)}`,
    type: 'message',
    role: 'assistant',
    model,
    content,
    stop_reason: mapAnthropicStopReason(finishReason || 'stop'),
    stop_sequence: null,
    usage: (() => {
      normalizeUsage(usage || {});
      // CC 未回报 usage 时按内容长度估算输出 token，避免客户端展示/记账为 0
      const estOut = Math.max(1,
        Math.ceil(((fullText || '').length + (thinkingText || '').length) / 4) + (toolCalls ? toolCalls.length * 20 : 0));
      return {
        // input_tokens 只计非缓存部分（Anthropic 语义），与 cache_* 相加才等于总输入
        input_tokens: anthropicInputTokens(usage),
        output_tokens: usage?.outputTokens || estOut,
        cache_creation_input_tokens: usage?.inputTokenDetails?.cacheWriteTokens ?? 0,
        cache_read_input_tokens: usage?.cachedInputTokens ?? 0,
      };
    })(),
  };
}

function convertAnthropicToOpenAI(anthropicReq) {
  // 1. Extract system prompt (top-level, not in messages array)
  let systemPrompt = '';
  if (anthropicReq.system) {
    if (typeof anthropicReq.system === 'string') {
      systemPrompt = anthropicReq.system;
    } else if (Array.isArray(anthropicReq.system)) {
      systemPrompt = anthropicReq.system
        .filter(b => b.type === 'text')
        .map(b => b.text)
        .join('\n');
    }
  }

  // 2. Build tool name map + convert messages
  const toolNameFromId = {};
  const openaiMessages = [];

  if (systemPrompt) {
    openaiMessages.push({ role: 'system', content: systemPrompt });
  }

  const messages = anthropicReq.messages || [];
  for (const msg of messages) {
    if (msg.role === 'assistant') {
      let textContent = '';
      // Anthropic 的 thinking block 承载思考内容，需转成 reasoning_content
      // 交给 buildCcRequest 回传，否则 CC 会因缺少 reasoning 而拒绝
      let thinkingContent = '';
      const toolCalls = [];
      const blocks = Array.isArray(msg.content) ? msg.content : [{ type: 'text', text: msg.content || '' }];
      for (const block of blocks) {
        if (block.type === 'text') {
          textContent += block.text || '';
        } else if (block.type === 'thinking') {
          thinkingContent += block.thinking || '';
        } else if (block.type === 'tool_use') {
          toolNameFromId[block.id] = block.name;
          toolCalls.push({
            id: block.id,
            type: 'function',
            function: {
              name: block.name,
              arguments: JSON.stringify(block.input || {}),
            },
          });
        }
      }
      const assistantMsg = { role: 'assistant', content: textContent || null };
      if (thinkingContent) assistantMsg.reasoning_content = thinkingContent;
      if (toolCalls.length > 0) assistantMsg.tool_calls = toolCalls;
      openaiMessages.push(assistantMsg);
    } else if (msg.role === 'user') {
      let textContent = '';
      const toolResults = [];
      if (typeof msg.content === 'string') {
        textContent = msg.content;
      } else if (Array.isArray(msg.content)) {
        for (const block of msg.content) {
          if (block.type === 'text') {
            textContent += block.text || '';
          } else if (block.type === 'tool_result') {
            toolResults.push(block);
          }
        }
      }
      if (textContent) {
        // 暂存，tool_result 优先入队：OpenAI 语义要求 tool 消息紧跟 assistant 的
        // tool_calls，同一条 user 消息里的文本要排在 tool 结果之后
      }
      for (const tr of toolResults) {
        const toolContent = typeof tr.content === 'string' ? tr.content
          : Array.isArray(tr.content) ? tr.content.map(c => c.text || '').join('')
          : String(tr.content || '');
        // OpenAI 语义里 tool 消息的 name 是可选的；会话恢复等场景下 tool_use_id 可能
        // 找不到对应 assistant tool_use（历史被客户端裁剪），此时不硬塞空 name，
        // 避免 CC 上游报 "Tool result is missing"（issue #15）
        const toolMsg = { role: 'tool', tool_call_id: tr.tool_use_id, content: toolContent };
        if (toolNameFromId[tr.tool_use_id]) toolMsg.name = toolNameFromId[tr.tool_use_id];
        openaiMessages.push(toolMsg);
      }
      if (textContent) {
        openaiMessages.push({ role: 'user', content: textContent });
      }
    }
  }

  // 3. Build OpenAI request
  const openaiReq = {
    model: anthropicReq.model || 'deepseek/deepseek-v4-flash',
    messages: openaiMessages,
    max_tokens: anthropicReq.max_tokens || 64000,
    stream: anthropicReq.stream === true,
  };

  // 4. Map tools
  if (anthropicReq.tools && anthropicReq.tools.length > 0) {
    openaiReq.tools = anthropicReq.tools.map(t => ({
      type: 'function',
      function: {
        name: t.name,
        description: t.description || '',
        parameters: t.input_schema || { type: 'object', properties: {} },
      },
    }));
  }

  // 5. Map tool_choice
  if (anthropicReq.tool_choice) {
    const tc = anthropicReq.tool_choice;
    if (tc.type === 'auto' || tc.type === undefined) {
      openaiReq.tool_choice = 'auto';
    } else if (tc.type === 'any') {
      openaiReq.tool_choice = 'required';
    } else if (tc.type === 'tool') {
      openaiReq.tool_choice = { type: 'function', function: { name: tc.name } };
    } else if (tc.type === 'none') {
      openaiReq.tool_choice = 'none';
    }
  }

  // 6. Optional params
  if (anthropicReq.temperature !== undefined) openaiReq.temperature = anthropicReq.temperature;
  if (anthropicReq.top_p !== undefined) openaiReq.top_p = anthropicReq.top_p;
  if (anthropicReq.stop_sequences) openaiReq.stop = anthropicReq.stop_sequences;
  if (anthropicReq.metadata?.user_id) openaiReq.user = anthropicReq.metadata.user_id;

  // 7. Anthropic thinking → reasoning_effort（LiteLLM 标准映射）
  if (anthropicReq.thinking) {
    const t = anthropicReq.thinking;
    if (t.type === 'disabled' || t.type === 'none') {
      // 不发送 reasoning_effort
    } else if (t.type === 'adaptive') {
      openaiReq.reasoning_effort = t.effort ?? 'medium';
    } else if (t.budget_tokens !== undefined) {
      if (t.budget_tokens >= 10000) openaiReq.reasoning_effort = 'high';
      else if (t.budget_tokens >= 5000) openaiReq.reasoning_effort = 'medium';
      else if (t.budget_tokens >= 2000) openaiReq.reasoning_effort = 'low';
      else openaiReq.reasoning_effort = 'low'; // <2000 → low
    }
  }

  return openaiReq;
}

/**
 * Async generator that reads CC NDJSON response body and yields
 * Anthropic SSE events for streaming.
 */
async function* createAnthropicSseTranslator(response, model, messageId, ctx) {
  let nextBlockIndex = 0;
  let currentBlockIndex = -1;
  let currentBlockType = null;
  let blockStarted = false;
  let inputTokens = 0;
  let outputTokens = 0;
  let cachedInputTokens = 0;
  let cacheWriteTokens = 0;
  let noCacheTokens = -1;   // -1 = 上游未提供该字段，改用减法兜底
  let stopReason = null;
  let hasError = false;
  let currentThinkingText = ''; // accumulated thinking text for the open block

  // Close the current block (text or thinking) if one is active.
  // For thinking blocks, emit a signature_delta (Anthropic standard) before stop.
  function closeBlock() {
    if (blockStarted) {
      const idx = currentBlockIndex;
      const type = currentBlockType;
      let out = '';
      if (type === 'thinking') {
        out += `event: content_block_delta\ndata: ${JSON.stringify({ type: 'content_block_delta', index: idx, delta: { type: 'signature_delta', signature: fakeThinkingSignature(currentThinkingText) } })}\n\n`;
        currentThinkingText = '';
      }
      blockStarted = false;
      currentBlockType = null;
      return out + `event: content_block_stop\ndata: ${JSON.stringify({ type: 'content_block_stop', index: idx })}\n\n`;
    }
    return '';
  }
  const closeTextBlock = closeBlock;

  // Open a new block of the given type (closing any previous block first)
  function startBlock(type, contentBlock) {
    if (!blockStarted || currentBlockType !== type) {
      const close = closeBlock();
      currentBlockIndex = nextBlockIndex++;
      currentBlockType = type;
      blockStarted = true;
      return close + `event: content_block_start\ndata: ${JSON.stringify({ type: 'content_block_start', index: currentBlockIndex, content_block: contentBlock })}\n\n`;
    }
    return '';
  }

  // Open a new text block (closing any previous block first)
  function startTextBlock() {
    return startBlock('text', { type: 'text', text: '' });
  }

  // Open a new thinking block (closing any previous block first)
  function startThinkingBlock() {
    return startBlock('thinking', { type: 'thinking', thinking: '' });
  }

  // Emit message_start (always the first event)
  yield `event: message_start\ndata: ${JSON.stringify({
    type: 'message_start',
    message: {
      id: messageId,
      type: 'message',
      role: 'assistant',
      content: [],
      model,
      usage: { input_tokens: 0, output_tokens: 0 },
    }
  })}\n\n`;

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  const idle = createIdleWatchdog(STREAM_IDLE_TIMEOUT_MS);

  try {
    while (true) {
      const result = await Promise.race([reader.read(), idle.arm()]);
      const { done, value } = result;
      if (done) break;
      ctx.bytesReceived += value.length;
      const chunkText = decoder.decode(value, { stream: true });
      buffer += chunkText;
      // 同 handleChatCompletions：无换行即无完整行，跳过全量 split
      let lines = [];
      if (chunkText.indexOf('\n') !== -1) {
        lines = buffer.split('\n');
        buffer = lines.pop() || '';
      }

      let hadOutput = false;
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || trimmed === '[DONE]') continue;
        let event;
        try { event = JSON.parse(trimmed); } catch { continue; }
        if (!event.type) continue;
        ctx.lastCcEvent = event.type;

        switch (event.type) {
          case 'start': case 'start-step': case 'text-start': case 'reasoning-start':
            // Signal events, no user-visible data
            break;

          case 'reasoning-delta': {
            // CC reasoning → Anthropic thinking block (Claude Code shows this as thinking)
            const text = event.text || '';
            if (!text) break;
            const startBlock = startThinkingBlock();
            currentThinkingText += text;
            yield startBlock + `event: content_block_delta\ndata: ${JSON.stringify({ type: 'content_block_delta', index: currentBlockIndex, delta: { type: 'thinking_delta', thinking: text } })}\n\n`;
            hadOutput = true;
            break;
          }

          case 'text-delta': {
            const text = event.text || '';
            const startBlock = startTextBlock();
            yield startBlock + `event: content_block_delta\ndata: ${JSON.stringify({ type: 'content_block_delta', index: currentBlockIndex, delta: { type: 'text_delta', text } })}\n\n`;
            outputTokens += 1;
            hadOutput = true;
            break;
          }

          case 'tool-call': {
            // Close any pending text block
            const closeBlock = closeTextBlock();
            if (closeBlock) yield closeBlock;

            const id = event.toolCallId || `toolu_${randomUUID().slice(0, 12)}`;
            const name = event.toolName || '';
            const input = typeof event.input === 'string' ? event.input : JSON.stringify(event.input || {});

            const tcIndex = nextBlockIndex++;
            yield `event: content_block_start\ndata: ${JSON.stringify({ type: 'content_block_start', index: tcIndex, content_block: { type: 'tool_use', id, name, input: {} } })}\n\n`;
            yield `event: content_block_delta\ndata: ${JSON.stringify({ type: 'content_block_delta', index: tcIndex, delta: { type: 'input_json_delta', partial_json: input } })}\n\n`;
            yield `event: content_block_stop\ndata: ${JSON.stringify({ type: 'content_block_stop', index: tcIndex })}\n\n`;
            outputTokens += 20;
            break;
          }

          case 'finish-step':
          case 'finish': {
            if (event.finishReason) stopReason = mapAnthropicStopReason(event.finishReason);
            const u = event.totalUsage || event.usage;
            if (u) {
              normalizeUsage(u);
              inputTokens = u.inputTokens ?? inputTokens;
              outputTokens = u.outputTokens ?? outputTokens;
              cachedInputTokens = u.cachedInputTokens ?? cachedInputTokens;
              cacheWriteTokens = u.inputTokenDetails?.cacheWriteTokens ?? cacheWriteTokens;
              if (typeof u.inputTokenDetails?.noCacheTokens === 'number') {
                noCacheTokens = u.inputTokenDetails.noCacheTokens;
              }
              ctx.inputTokens = inputTokens;
              ctx.outputTokens = outputTokens;
              ctx.cachedInputTokens = cachedInputTokens;
            }
            // 上游未回报 usage 时保留本地按 delta 计数的估算值——清零会把有内容的
            // 响应误判成零输出（触发 429）。未知字段保持原值即可。
            break;
          }

          case 'error': {
            hasError = true;
            const upstreamError = mapCcEventError(event);
            ctx.upstreamError = upstreamError;
            yield `event: error\ndata: ${JSON.stringify({ type: 'error', error: upstreamError.body.error })}\n\n`;
            break;
          }

          case 'reasoning-end': case 'provider-metadata': case 'tool-input-start': case 'tool-input-delta': case 'tool-input-end': case 'tool-error': case 'text-end':
            // Silent - no user-visible content
            break;
          default:
            log('warn', 'Unknown CC event type', { type: event.type });
            break;
        }
      }
    }

    // 无论上游是否回报 usage，都把本地计数同步进 ctx（零输出判定与超时日志依赖它）。
    // 注意：ctx.inputTokens 保存的是上游原始总数，仅供日志排查；
    // message_delta 的 input_tokens 走 anthropicInputTokens / noCacheTokens 换算，不读它。
    ctx.inputTokens = inputTokens;
    ctx.outputTokens = outputTokens;
    ctx.cachedInputTokens = cachedInputTokens;
    ctx.cacheWriteTokens = cacheWriteTokens;

    // Finalize — close pending text block, emit message_delta + message_stop
    if (!hasError) {
      const closeBlock = closeTextBlock();
      if (closeBlock) yield closeBlock;

      // 输出 token 为 0 时记为错误，避免下游异常计费
      if (outputTokens === 0) {
        yield `event: error\ndata: ${JSON.stringify({ type: 'error', error: { type: 'rate_limit_error', message: 'Empty response from upstream (zero output tokens)' }, retry_after: 10 })}\n\n`;
      } else {
        yield `event: message_delta\ndata: ${JSON.stringify({
          type: 'message_delta',
          delta: { stop_reason: stopReason || 'end_turn' },
          usage: {
            output_tokens: outputTokens,
            cache_read_input_tokens: cachedInputTokens,
            cache_creation_input_tokens: cacheWriteTokens || 0,
            // 只计非缓存部分；否则下游把 input 与 cache_read 相加会得到约两倍（issue #25）
            input_tokens: noCacheTokens >= 0
              ? noCacheTokens
              : Math.max(0, inputTokens - cachedInputTokens - (cacheWriteTokens || 0)),
          },
        })}\n\n`;

        yield `event: message_stop\ndata: ${JSON.stringify({ type: 'message_stop' })}\n\n`;
      }
    }
  } finally {
    // 确保流中断时通知上游
    idle.dispose();
    try { reader.cancel(); } catch {}
  }
}

function sendAnthropicError(res, status, type, message, retryAfter) {
  const body = { type: 'error', error: { type, message } };
  const headers = { 'Content-Type': 'application/json' };
  if (retryAfter !== undefined) {
    body.retry_after = retryAfter;
    headers['Retry-After'] = String(retryAfter);
  }
  res.writeHead(status, headers);
  res.end(JSON.stringify(body));
}

async function handleMessages(req, res) {
  let anthropicReq;
  try {
    anthropicReq = await readBody(req);
  } catch (e) {
    if (e.statusCode === 413) {
      sendAnthropicError(res, 413, 'invalid_request_error', e.message);
      return;
    }
    sendAnthropicError(res, 400, 'invalid_request_error', 'Invalid JSON body');
    return;
  }

  const clientKey = extractClientApiKey(req.headers);
  if (!hasAnyUsableKey() && keyStore.settings.onAllExhausted !== 'best-effort') {
    const unavailable = poolUnavailableResponse();
    sendJSON(res, unavailable.status, {
      type: 'error',
      error: {
        type: unavailable.status === 401 ? 'authentication_error' : 'rate_limit_error',
        message: unavailable.body.error.message,
      },
    });
    return;
  }

  const stream = anthropicReq.stream === true;
  const model = anthropicReq.model || 'claude-sonnet-4-6';

  // Convert Anthropic → OpenAI → CC
  const openaiReq = convertAnthropicToOpenAI(anthropicReq);
  const ccBody = buildCcRequest(openaiReq);

  const abortController = new AbortController();
  let aborted = false;
  // 提前初始化，断连回调/超时 catch 安全引用（避免块级作用域 ReferenceError）
  const startTime = Date.now();
  let messageId = '';
  let reader = null;
  let bytesReceived = 0; let lastCcEvent = ''; let fullText = '';

  try {
    const fwd = await forwardToCCWithFailover(ccBody, clientKey, req, abortController.signal, undefined, anthropicReq);
    if (fwd.error) {
      const mapped = fwd.error;
      sendAnthropicError(res, mapped.status, mapped.body.error.type, mapped.body.error.message);
      return;
    }
    const ccResponse = fwd.response;

    // 下游断连检测：打断 CC 上游 + 记录日志
    res.on('close', () => {
      if (res.writableEnded) return; // Normal completion, not a disconnect
      aborted = true;
      if (!abortController.signal.aborted) {
        // 断连前抢发 usage=0 终止事件，避免下游自行估算 token
        try {
          res.write(`event: message_delta\ndata: ${JSON.stringify({
            type: 'message_delta',
            delta: { stop_reason: 'end_turn' },
            usage: { output_tokens: 0, input_tokens: 0, cache_read_input_tokens: 0 },
          })}\n\n`);
          res.write(`event: message_stop\ndata: ${JSON.stringify({ type: 'message_stop' })}\n\n`);
        } catch {}
        try { abortController.abort(); } catch {}
      }
      log('warn', 'Client disconnected', {
        path: '/v1/messages',
        model,
        messageId,
        streaming: stream,
        elapsedMs: Date.now() - startTime,
      });
    });

    if (stream) {
      // ── 流式 Anthropic SSE ──
      // 行为与 /v1/chat/completions 对齐：首个上游事件（thinking/text/tool_use）到达即
      // 发 header——之前扣到 text_delta 才发，推理模型 thinking 阶段客户端收不到任何
      // 字节，触发下游 60s 首字节超时（context canceled）。message_start 仍缓冲：
      // 完全无输出时还能回 JSON 429/502 让 SDK 自动重试（同 chat 端点）。
      let started = false;
      const buf = [];
      const SSE_HEADERS = {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no',
      };
      const flushBuf = async () => {
        if (!started) {
          res.writeHead(200, SSE_HEADERS);
          started = true;
        }
        for (const ev of buf) { try { res.write(ev); } catch {} }
        buf.length = 0;
        await waitDrain(res);
      };

      // 心跳：等价于 chat 端点的 ': keepalive'——chat 在每轮读到静默事件时发注释行，
      // Anthropic 翻译器会吞掉 signal 事件，这里改用空闲计时发 ping（Anthropic 标准
      // 事件，官方 SDK 会忽略），覆盖上游排队/长 thinking 的静默窗口
      let lastSentAt = Date.now();
      const heartbeat = setInterval(() => {
        // 不向已积压的下游继续塞数据：定时器回调是同步的，无法 await waitDrain，
        // 因此用 writableNeedDrain 直接跳过本轮心跳（背压场景下少发一个 ping 无副作用）
        if (started && !aborted && !res.writableEnded && !res.writableNeedDrain && Date.now() - lastSentAt > 15000) {
          try { res.write('event: ping\ndata: {"type":"ping"}\n\n'); lastSentAt = Date.now(); } catch {}
        }
      }, 5000);

      let ctx;
      try {
        messageId = 'msg_' + randomUUID().slice(0, 12);
        ctx = { bytesReceived: 0, lastCcEvent: '', inputTokens: 0, outputTokens: 0, cachedInputTokens: 0, upstreamError: null };
        const generator = createAnthropicSseTranslator(ccResponse, model, messageId, ctx);
        for await (const event of generator) {
          if (aborted) break;
          if (!started && !event.startsWith('event: message_start')) {
            await flushBuf();
          }
          if (started) {
            try { res.write(event); } catch {}
            lastSentAt = Date.now();
            await waitDrain(res);
          } else {
            buf.push(event);
          }
        }

        if (!aborted) {
          consecutiveTimeouts = 0;
          if (ctx.upstreamError) {
            if (!started) {
              sendAnthropicError(
                res,
                ctx.upstreamError.status,
                ctx.upstreamError.body.error.type,
                ctx.upstreamError.body.error.message,
              );
            }
            // started 时 error 事件已在循环中经 SSE 下发，按规范 error 事件即终结
          } else if (ctx.outputTokens === 0) {
            try { abortController.abort(); } catch {}
            if (!started) {
              sendAnthropicError(res, 429, 'rate_limit_error', 'Empty response from upstream (zero output tokens)', 10);
              return;
            }
            await flushBuf();
          } else {
            await flushBuf();
          }
        }
      } catch (e) {
        if (aborted) {
          // 客户端已断连，只清理（close handler 已调用 abortController.abort()）
        } else if (e.message === 'STREAM_IDLE_TIMEOUT') {
          log('warn', 'Stream idle timeout', {
            path: '/v1/messages',
            model,
            streaming: true,
            timeoutMs: STREAM_IDLE_TIMEOUT_MS,
            elapsedMs: Date.now() - startTime,
            id: messageId,
            bytesReceived: ctx.bytesReceived,
            lastCcEvent: ctx.lastCcEvent || '(none)',
            inputTokens: ctx.inputTokens,
            outputTokens: ctx.outputTokens,
            cachedInputTokens: ctx.cachedInputTokens,
          });
          try { abortController.abort(); } catch {} // 打断 CC 上游
          if (!started) {
            consecutiveTimeouts++;
            const timeoutMsg = consecutiveTimeouts >= TIMEOUT_REDUCE_CONTEXT_THRESHOLD
              ? 'Response timeout - try reducing context length (summarize earlier messages)'
              : 'Response timeout - request timed out';
            sendAnthropicError(res, 429, 'rate_limit_error', timeoutMsg);
            return;
          }
          if (!res.writableEnded) {
            consecutiveTimeouts++;
            const timeoutMsg = consecutiveTimeouts >= TIMEOUT_REDUCE_CONTEXT_THRESHOLD
              ? 'Response timeout - try reducing context length (summarize earlier messages)'
              : 'Response timeout - request timed out';
            try { res.write(`event: error\ndata: ${JSON.stringify({ type: 'error', error: { type: 'rate_limit_error', message: timeoutMsg }, retry_after: 5 })}\n\n`); } catch {}
            try { res.destroy(); } catch {}
          }
        } else {
          log('error', 'Anthropic stream error', { message: e.message });
          try { abortController.abort(); } catch {} // 打断 CC 上游
          if (!started) {
            sendAnthropicError(res, 502, 'proxy_error', `Upstream error: ${e.message}`, 10);
            return;
          }
          if (!res.writableEnded) {
            try {
              res.write(`event: error\ndata: ${JSON.stringify({ type: 'error', error: { type: 'internal_error', message: e.message } })}\n\n`);
            } catch {}
          }
        }
      } finally {
        clearInterval(heartbeat);
      }

      if (!res.writableEnded) res.end();
    } else {
      // ── 非流式 Anthropic JSON ──
      const messageId = 'msg_' + randomUUID().slice(0, 12);
      let finishReason = 'stop';
      let usage = null;
      let toolCalls = null;
      let thinkingText = ''; // CC reasoning → Anthropic thinking block
      let upstreamError = null;

      reader = ccResponse.body.getReader();
      const decoder = new TextDecoder();
      let buf = '';

      const processLines = () => {
        const lines = buf.split('\n');
        buf = lines.pop() || '';
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed || trimmed === '[DONE]') continue;
          try {
            const event = JSON.parse(trimmed);
            switch (event.type) {
              case 'text-delta': lastCcEvent = event.type; fullText += event.text || ''; break;
              case 'reasoning-delta': lastCcEvent = event.type; thinkingText += event.text || ''; break;
              case 'tool-call':
                lastCcEvent = event.type;
                (toolCalls = toolCalls || []).push({
                  id: event.toolCallId || ('call_' + randomUUID().slice(0, 8)),
                  type: 'function',
                  function: {
                    name: event.toolName || '',
                    arguments: typeof event.input === 'string' ? event.input : JSON.stringify(event.input || {}),
                  },
                });
                break;
              case 'finish':
                lastCcEvent = event.type;
                finishReason = mapFinishReason(event.finishReason || 'stop');
                if (event.totalUsage || event.usage) usage = event.totalUsage || event.usage;
                break;
              case 'error':
                lastCcEvent = event.type;
                log('warn', 'CC error (Anthropic non-stream)', { message: event.error?.message || event.message });
                upstreamError = mapCcEventError(event);
                break;
              case 'reasoning-end': case 'provider-metadata': case 'tool-input-start': case 'tool-input-delta': case 'tool-input-end': case 'tool-error': case 'text-end':
                // Silent - no user-visible content
                break;
              default:
                log('warn', 'Unknown CC event type', { type: event.type });
                break;
            }
          } catch {}
        }
      };

      const idle = createIdleWatchdog(NONSTREAM_IDLE_TIMEOUT_MS);
      while (true) {
        const result = await Promise.race([reader.read(), idle.arm()]);
        const { done, value } = result;
        if (done) break;
        bytesReceived += value.length;
        const chunkText = decoder.decode(value, { stream: true });
        buf += chunkText;
        // 无换行则不可能产生完整行，跳过全量 split
        if (chunkText.indexOf('\n') !== -1) processLines();
      }
      idle.dispose();
      processLines();

      if (upstreamError) {
        sendAnthropicError(res, upstreamError.status, upstreamError.body.error.type, upstreamError.body.error.message);
        return;
      }

      // 零输出判定改为按实际内容：上游偶发不回 totalUsage 时，旧逻辑（usage?.outputTokens ?? 0 === 0）
      // 会把有完整文本的响应误杀成 429
      if (!fullText && !thinkingText && !toolCalls) {
        try { if (!abortController.signal.aborted) abortController.abort(); } catch {}
        sendAnthropicError(res, 429, 'rate_limit_error', 'Empty response from upstream (zero output tokens)', 10);
        return;
      }

      consecutiveTimeouts = 0;
      sendJSON(res, 200, buildAnthropicResponse(model, fullText, toolCalls, finishReason, usage, thinkingText));
    }
  } catch (e) {
    if (abortController.signal.aborted) {
      log('warn', 'Request cancelled (client disconnected before CC response)', {
        path: '/v1/messages',
        model,
        messageId,
      });
    } else if (e.message === 'STREAM_IDLE_TIMEOUT') {
      log('warn', 'Stream idle timeout', {
        path: '/v1/messages',
        model,
        streaming: false,
        timeoutMs: NONSTREAM_IDLE_TIMEOUT_MS,
        elapsedMs: Date.now() - startTime,
        id: messageId,
        bytesReceived,
        lastCcEvent: lastCcEvent || '(none)',
        partialLen: fullText ? fullText.length : 0,
      });
      try { reader?.cancel(); } catch {}
      try { abortController.abort(); } catch {} // 打断 CC 上游
      consecutiveTimeouts++;
      const timeoutMsg = consecutiveTimeouts >= TIMEOUT_REDUCE_CONTEXT_THRESHOLD
        ? 'Response timeout - try reducing context length (summarize earlier messages)'
        : 'Response timeout - request timed out';
      res.setHeader('Retry-After', '5');
      sendAnthropicError(res, 429, 'rate_limit_error', timeoutMsg);
    } else {
      log('error', 'Upstream error', { message: e.message });
      try { abortController.abort(); } catch {} // 打断 CC 上游
      sendAnthropicError(res, 502, 'proxy_error', `Upstream error: ${e.message}`, 10);
    }
  }
}

// ── 动态模型列表 ────────────────────────────────────

let dynamicModels = null;
let modelsLastFetch = 0;

async function fetchModels(apiKey) {
  const now = Date.now();
  if (dynamicModels && (now - modelsLastFetch) < CFG.modelRefreshIntervalMs) {
    return dynamicModels;
  }

  try {
    if (!apiKey || !CFG.useProviderModels) throw new Error('Provider models disabled');

    const response = await fetch(`${CFG.apiBase}/provider/v1/models`, {
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'x-cli-environment': 'production',
        'x-command-code-version': CC_VERSION,
      },
      signal: AbortSignal.timeout(10000),
    });

    if (response.ok) {
      const data = await response.json();
      if (Array.isArray(data.data)) {
        dynamicModels = data.data.map(m => ({
          id: m.id,
          name: m.id,
        }));
        modelsLastFetch = now;
        log('info', 'Fetched models from Provider API', { count: dynamicModels.length });
        return dynamicModels;
      }
    }
    log('warn', 'Provider models fetch failed, using hardcoded list', { status: response.status });
  } catch (e) {
    log('warn', 'Provider models fetch error, using hardcoded list', { error: e.message });
  }

  // Fallback to hardcoded MODELS
  return MODELS;
}

// ── OpenAI Responses API（/v1/responses）──────────────
// 供 Codex 等使用 Responses 协议的客户端接入。代理仍是无状态转换层：
// 把 input 翻译成内部 Chat 格式，复用同一套 CC 转发管线。
// 不支持 previous_response_id / store（需要服务端保存会话，与无状态定位冲突），
// 收到直接 400，避免静默降级成错误答案。

function responsesTextOf(content) {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content.map(p => (p && typeof p === 'object' ? (p.text || '') : '')).join('');
}

function responsesReasoningOf(item) {
  if (!item) return '';
  if (Array.isArray(item.summary) && item.summary.length) return item.summary.map(p => (p && p.text) || '').join('');
  if (Array.isArray(item.content) && item.content.length) return item.content.map(p => (p && p.text) || '').join('');
  return typeof item.text === 'string' ? item.text : '';
}

function newResponsesId(prefix) {
  return prefix + randomUUID().replace(/-/g, '').slice(0, 24);
}

function convertResponsesToChat(respReq) {
  const messages = [];

  if (respReq.instructions !== undefined && respReq.instructions !== null) {
    const sys = responsesTextOf(respReq.instructions);
    if (sys) messages.push({ role: 'system', content: sys });
  }

  // Responses 把 reasoning / message / function_call 拆成并列 item，
  // Chat 要求它们挂在同一条 assistant 消息上，故先累积再冲刷。
  let pending = null;
  const ensurePending = () => (pending = pending || { role: 'assistant', content: null, tool_calls: [] });
  const flushPending = () => {
    if (!pending) return;
    if (!pending.tool_calls.length) delete pending.tool_calls;
    if (!pending.reasoning_content) delete pending.reasoning_content;
    if (pending.content === null && !pending.tool_calls) { pending = null; return; }
    messages.push(pending);
    pending = null;
  };

  const input = respReq.input;
  if (typeof input === 'string') {
    messages.push({ role: 'user', content: input });
  } else if (Array.isArray(input)) {
    for (const item of input) {
      if (!item || typeof item !== 'object') continue;
      switch (item.type) {
        case 'reasoning': {
          const t = responsesReasoningOf(item);
          if (t) ensurePending().reasoning_content = t;
          break;
        }
        case 'message': {
          const text = responsesTextOf(item.content);
          if (item.role === 'assistant') {
            if (text) ensurePending().content = text;
          } else if (item.role === 'system' || item.role === 'developer') {
            flushPending();
            messages.push({ role: 'system', content: text });
          } else {
            flushPending();
            messages.push({ role: 'user', content: text });
          }
          break;
        }
        case 'function_call': {
          ensurePending().tool_calls.push({
            id: item.call_id || item.id || ('call_' + randomUUID().slice(0, 8)),
            type: 'function',
            function: { name: item.name || '', arguments: item.arguments || '{}' },
          });
          break;
        }
        case 'function_call_output': {
          flushPending();
          messages.push({
            role: 'tool',
            tool_call_id: item.call_id || '',
            content: typeof item.output === 'string' ? item.output : JSON.stringify(item.output === undefined ? '' : item.output),
          });
          break;
        }
        default: break;
      }
    }
  }
  flushPending();

  let tools;
  if (Array.isArray(respReq.tools) && respReq.tools.length) {
    tools = respReq.tools.filter(t => t && (t.type === 'function' || t.name)).map(t => ({
      type: 'function',
      function: {
        name: t.name || '',
        description: t.description || '',
        parameters: t.parameters || { type: 'object', properties: {} },
      },
    }));
    if (!tools.length) tools = undefined;
  }

  let toolChoice;
  const tc = respReq.tool_choice;
  if (typeof tc === 'string') toolChoice = tc;
  else if (tc && typeof tc === 'object' && tc.name) toolChoice = { type: 'function', function: { name: tc.name } };

  const out = { model: respReq.model, messages, stream: respReq.stream === true };
  if (tools) out.tools = tools;
  if (toolChoice) out.tool_choice = toolChoice;
  if (respReq.max_output_tokens !== undefined) out.max_tokens = respReq.max_output_tokens;
  if (respReq.temperature !== undefined) out.temperature = respReq.temperature;
  if (respReq.top_p !== undefined) out.top_p = respReq.top_p;
  if (respReq.parallel_tool_calls !== undefined) out.parallel_tool_calls = respReq.parallel_tool_calls;
  const eff = respReq.reasoning && typeof respReq.reasoning === 'object' ? respReq.reasoning.effort : undefined;
  if (eff) out.reasoning_effort = eff;
  return out;
}

// Responses 的 input_tokens 是总数，cached / cache_write 均为其子集 ——
// 与 Anthropic 相反（那里 cache_read 是独立增量，必须做减法，见 issue #25）。
// 本代理上游 CC 的 inputTokens 同样已含缓存，故此处直接沿用、不做减法。
// 实测：total_tokens === input_tokens + output_tokens（即使 cached 占绝大多数）。
function buildResponsesUsage(usage, fallbackOutputTokens) {
  const u = usage || {};
  normalizeUsage(u);
  const inTok = u.inputTokens || 0;
  const outTok = u.outputTokens || fallbackOutputTokens || 0;
  return {
    input_tokens: inTok,
    // 规范里 cached_tokens 与 cache_write_tokens 都是 required
    input_tokens_details: {
      cached_tokens: u.cachedInputTokens || 0,
      cache_write_tokens: (u.inputTokenDetails && u.inputTokenDetails.cacheWriteTokens) || 0,
    },
    output_tokens: outTok,
    output_tokens_details: { reasoning_tokens: 0 },
    total_tokens: inTok + outTok,
  };
}

function buildResponsesOutput(fullText, thinkingText, toolCalls) {
  const output = [];
  if (thinkingText) {
    output.push({ type: 'reasoning', id: newResponsesId('rs_'), summary: [{ type: 'summary_text', text: thinkingText }] });
  }
  if (fullText) {
    output.push({
      type: 'message', id: newResponsesId('msg_'), status: 'completed', role: 'assistant',
      content: [{ type: 'output_text', text: fullText, annotations: [] }],
    });
  }
  for (const tc of (toolCalls || [])) {
    const rawArgs = tc.function ? tc.function.arguments : '{}';
    output.push({
      type: 'function_call', id: newResponsesId('fc_'), call_id: tc.id,
      name: tc.function ? (tc.function.name || '') : '',
      arguments: typeof rawArgs === 'string' ? rawArgs : JSON.stringify(rawArgs || {}),
      status: 'completed',
    });
  }
  return output;
}

function buildResponsesObject(responseId, model, created, fullText, thinkingText, toolCalls, usage, opts) {
  const o = opts || {};
  const truncated = o.finishReason === 'length';
  return {
    id: responseId,
    object: 'response',
    created_at: created,
    status: truncated ? 'incomplete' : 'completed',
    completed_at: nowUnix(),
    error: null,
    incomplete_details: truncated ? { reason: 'max_output_tokens' } : null,
    input: o.input || [],
    instructions: o.instructions === undefined ? null : o.instructions,
    max_output_tokens: o.max_output_tokens === undefined ? null : o.max_output_tokens,
    model,
    output: buildResponsesOutput(fullText, thinkingText, toolCalls),
    output_text: fullText || '',
    parallel_tool_calls: true,
    previous_response_id: null,
    reasoning: o.reasoning || null,
    store: false,
    temperature: o.temperature === undefined ? 1 : o.temperature,
    text: { format: { type: 'text' } },
    tool_choice: o.tool_choice || 'auto',
    tools: o.tools || [],
    top_p: o.top_p === undefined ? 1 : o.top_p,
    truncation: 'disabled',
    usage: buildResponsesUsage(usage, 0),
    user: null,
    metadata: {},
  };
}

function sendResponsesError(res, status, type, message, retryAfter) {
  const body = { error: { message, type, code: null, param: null } };
  if (retryAfter !== undefined) body.retry_after = retryAfter;
  sendJSON(res, status, body);
}

// CC NDJSON → Responses 具名 SSE 事件（每个事件都必需的 sequence_number 递增发送）
function createResponsesSseTranslator(model, responseId, created) {
  let seq = 0;
  const sse = (type, data) => 'event: ' + type + '\ndata: ' + JSON.stringify(Object.assign({ type, sequence_number: seq++ }, data)) + '\n\n';
  let createdSent = false;
  let current = null;
  let outputIndex = 0;
  const doneItems = [];
  let usage = null;
  let textAcc = '';
  let finishReason = null;

  const baseResponse = (status, output) => ({
    id: responseId, object: 'response', created_at: created, status,
    output: output || [], output_text: '', model, error: null, incomplete_details: null,
    parallel_tool_calls: true, previous_response_id: null, store: false, tools: [], metadata: {},
  });

  function startResponse() {
    createdSent = true;
    return [
      sse('response.created', { response: baseResponse('in_progress') }),
      sse('response.in_progress', { response: baseResponse('in_progress') }),
    ];
  }

  function closeItem() {
    if (!current) return [];
    const out = [];
    const item = current.item;
    const idx = current.index;
    if (current.kind === 'message') {
      out.push(sse('response.output_text.done', { item_id: item.id, output_index: idx, content_index: 0, text: current.textBuf, logprobs: [] }));
      out.push(sse('response.content_part.done', {
        item_id: item.id, output_index: idx, content_index: 0,
        part: { type: 'output_text', text: current.textBuf, annotations: [] },
      }));
      item.content = [{ type: 'output_text', text: current.textBuf, annotations: [] }];
      item.status = 'completed';
    } else if (current.kind === 'function_call') {
      out.push(sse('response.function_call_arguments.done', { item_id: item.id, output_index: idx, arguments: item.arguments }));
      item.status = 'completed';
    } else if (current.kind === 'reasoning') {
      out.push(sse('response.reasoning_summary_text.done', { item_id: item.id, output_index: idx, summary_index: 0, text: current.textBuf }));
      out.push(sse('response.reasoning_summary_part.done', {
        item_id: item.id, output_index: idx, summary_index: 0,
        part: { type: 'summary_text', text: current.textBuf },
      }));
      item.summary = [{ type: 'summary_text', text: current.textBuf }];
      item.status = 'completed';
    }
    out.push(sse('response.output_item.done', { output_index: idx, item }));
    doneItems.push(item);
    current = null;
    return out;
  }

  function openItem(kind, item) {
    const out = closeItem();
    current = { kind, index: outputIndex++, item, textBuf: '' };
    out.push(sse('response.output_item.added', { output_index: current.index, item }));
    if (kind === 'message') {
      out.push(sse('response.content_part.added', {
        item_id: item.id, output_index: current.index, content_index: 0,
        part: { type: 'output_text', text: '', annotations: [] },
      }));
    } else if (kind === 'reasoning') {
      out.push(sse('response.reasoning_summary_part.added', {
        item_id: item.id, output_index: current.index, summary_index: 0,
        part: { type: 'summary_text', text: '' },
      }));
    }
    return out;
  }

  return {
    lastCcEvent: '',
    upstreamError: null,
    inputTokens: 0,
    outputTokens: 0,
    cachedInputTokens: 0,
    get started() { return createdSent; },
    get stopReason() { return finishReason; },
    parseLine(line) {
      const trimmed = line.trim();
      if (!trimmed || trimmed === '[DONE]' || trimmed.startsWith(':')) return null;
      let event;
      try { event = JSON.parse(trimmed); } catch { return null; }
      if (!event.type) return null;
      this.lastCcEvent = event.type;
      const out = [];

      switch (event.type) {
        case 'text-start': case 'reasoning-start': case 'start': case 'start-step':
          break;

        case 'text-delta': {
          const text = event.text || event.delta || '';
          if (!text) break;
          if (!createdSent) out.push.apply(out, startResponse());
          if (!current || current.kind !== 'message') {
            out.push.apply(out, openItem('message', { type: 'message', id: newResponsesId('msg_'), status: 'in_progress', role: 'assistant', content: [] }));
          }
          current.textBuf += text;
          textAcc += text;
          out.push(sse('response.output_text.delta', { item_id: current.item.id, output_index: current.index, content_index: 0, delta: text, logprobs: [] }));
          break;
        }

        case 'reasoning-delta': {
          const text = event.text || '';
          if (!text) break;
          if (!createdSent) out.push.apply(out, startResponse());
          if (!current || current.kind !== 'reasoning') {
            out.push.apply(out, openItem('reasoning', { type: 'reasoning', id: newResponsesId('rs_'), summary: [], status: 'in_progress' }));
          }
          current.textBuf += text;
          out.push(sse('response.reasoning_summary_text.delta', {
            item_id: current.item.id, output_index: current.index, summary_index: 0, delta: text,
          }));
          break;
        }

        case 'tool-call': {
          if (!createdSent) out.push.apply(out, startResponse());
          const callId = event.toolCallId || newResponsesId('call_');
          const args = typeof event.input === 'string' ? event.input : JSON.stringify(event.input || {});
          out.push.apply(out, openItem('function_call', {
            type: 'function_call', id: newResponsesId('fc_'), call_id: callId,
            name: event.toolName || '', arguments: '', status: 'in_progress',
          }));
          current.item.arguments = args;
          out.push(sse('response.function_call_arguments.delta', { item_id: current.item.id, output_index: current.index, delta: args }));
          break;
        }

        case 'finish': {
          finishReason = event.finishReason || null;
          const u = event.totalUsage || event.usage || null;
          if (u) {
            normalizeUsage(u);
            usage = u;
            this.inputTokens = u.inputTokens || 0;
            this.outputTokens = u.outputTokens || 0;
            this.cachedInputTokens = u.cachedInputTokens || 0;
          }
          break;
        }

        case 'error': {
          this.upstreamError = mapCcEventError(event);
          break;
        }

        default: break;
      }
      return out.length ? out : null;
    },
    finish() {
      if (!createdSent) return [];
      const out = closeItem();
      // finishReason=length 表示被 max_output_tokens 截断：规范要求 status=incomplete
      const truncated = finishReason === 'length';
      out.push(sse(truncated ? 'response.incomplete' : 'response.completed', {
        response: Object.assign(baseResponse(truncated ? 'incomplete' : 'completed', doneItems.slice()), {
          output_text: textAcc,
          incomplete_details: truncated ? { reason: 'max_output_tokens' } : null,
          usage: buildResponsesUsage(usage, this.outputTokens),
        }),
      }));
      return out;
    },
    fail(message) {
      if (!createdSent) return [];
      return [sse('response.failed', {
        response: Object.assign(baseResponse('failed'), {
          error: { code: 'upstream_error', message: message || 'Upstream error' },
        }),
      })];
    },
    errorEvent(message) {
      return sse('error', { code: null, message: message || 'Upstream error', param: null });
    },
  };
}

async function handleResponses(req, res) {
  let respReq;
  try {
    respReq = await readBody(req);
  } catch (e) {
    if (e.statusCode === 413) { sendResponsesError(res, 413, 'invalid_request_error', e.message); return; }
    sendResponsesError(res, 400, 'invalid_request_error', 'Invalid JSON body');
    return;
  }

  if (respReq.previous_response_id) {
    sendResponsesError(res, 400, 'invalid_request_error',
      'previous_response_id is not supported (this proxy is stateless); send the full input each turn');
    return;
  }

  const clientKey = extractClientApiKey(req.headers);
  if (!hasAnyUsableKey() && keyStore.settings.onAllExhausted !== 'best-effort') {
    const unavailable = poolUnavailableResponse();
    sendResponsesError(res, unavailable.status,
      unavailable.status === 401 ? 'authentication_error' : 'rate_limit_error',
      unavailable.body.error.message);
    return;
  }

  let chatReq = convertResponsesToChat(respReq);
  if (!chatReq.messages.length) {
    sendResponsesError(res, 400, 'invalid_request_error', 'input is required');
    return;
  }

  const stream = chatReq.stream === true;
  const model = chatReq.model || 'deepseek/deepseek-v4-flash';
  const responseId = newResponsesId('resp_');
  const created = nowUnix();
  const echoOpts = {
    instructions: respReq.instructions === undefined ? null : respReq.instructions,
    max_output_tokens: respReq.max_output_tokens === undefined ? null : respReq.max_output_tokens,
    temperature: respReq.temperature,
    top_p: respReq.top_p,
    reasoning: respReq.reasoning || null,
    tool_choice: typeof respReq.tool_choice === 'string' ? respReq.tool_choice : 'auto',
    tools: respReq.tools || [],
  };
  const ccBody = buildCcRequest(chatReq);
  const promptCacheKey = chatReq.prompt_cache_key;
  chatReq = null;

  const abortController = new AbortController();
  let aborted = false;
  const startTime = Date.now();
  let bytesReceived = 0;
  let lastCcEvent = '';
  let reader = null;
  let translator = null;

  res.on('close', () => {
    if (res.writableEnded) return;
    aborted = true;
    log('warn', 'Client disconnected', {
      path: '/v1/responses', model, responseId, elapsedMs: Date.now() - startTime,
      bytesSent: bytesReceived, lastCcEvent: lastCcEvent || '(none)',
    });
    if (!abortController.signal.aborted) { try { abortController.abort(); } catch (e2) {} }
  });

  try {
    const fwd = await forwardToCCWithFailover(ccBody, clientKey, req, abortController.signal, promptCacheKey, chatReq);
    if (fwd.error) {
      const mapped = fwd.error;
      sendResponsesError(res, mapped.status, mapped.body.error.type, mapped.body.error.message, mapped.body.retry_after);
      return;
    }
    const ccResponse = fwd.response;

    if (stream) {
      translator = createResponsesSseTranslator(model, responseId, created);
      let buffer = '';
      let started = false;
      const decoder = new TextDecoder();
      reader = ccResponse.body.getReader();
      const idle = createIdleWatchdog(STREAM_IDLE_TIMEOUT_MS);
      const SSE_HEADERS = {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no',
      };
      const writeEvents = async (evts) => {
        if (!started) { res.writeHead(200, SSE_HEADERS); started = true; }
        for (const e2 of evts) res.write(e2);
        await waitDrain(res);
      };

      try {
        while (true) {
          const result = await Promise.race([reader.read(), idle.arm()]);
          const done = result.done;
          const value = result.value;
          if (done) break;
          if (aborted || res.destroyed) break;
          bytesReceived += value.length;

          const chunkText = decoder.decode(value, { stream: true });
          buffer += chunkText;
          let lines = [];
          if (chunkText.indexOf('\n') !== -1) {
            lines = buffer.split('\n');
            buffer = lines.pop() || '';
          }

          for (const line of lines) {
            const evts = translator.parseLine(line);
            if (evts) await writeEvents(evts);
            if (translator.lastCcEvent) lastCcEvent = translator.lastCcEvent;
          }
        }

        if (!aborted) {
          if (buffer.trim()) {
            const evts = translator.parseLine(buffer);
            if (evts) await writeEvents(evts);
          }
          if (translator.upstreamError) {
            if (!started) {
              sendResponsesError(res, translator.upstreamError.status,
                translator.upstreamError.body.error.type, translator.upstreamError.body.error.message,
                translator.upstreamError.body.retry_after);
              return;
            }
            const failed = translator.fail(translator.upstreamError.body.error.message);
            if (failed.length) await writeEvents(failed);
          } else if (translator.outputTokens === 0 && !translator.started) {
            try { if (!abortController.signal.aborted) abortController.abort(); } catch (e2) {}
            sendResponsesError(res, 429, 'rate_limit_error',
              'Empty response from upstream (zero output tokens)', 10);
            return;
          } else {
            if (!started) { res.writeHead(200, SSE_HEADERS); started = true; }
            for (const e2 of translator.finish()) res.write(e2);
          }
          consecutiveTimeouts = 0;
        }
      } catch (e) {
        if (aborted) {
          try { reader.cancel(); } catch (e2) {}
        } else if (e.message === 'STREAM_IDLE_TIMEOUT') {
          log('warn', 'Stream idle timeout', {
            path: '/v1/responses', model, streaming: true, timeoutMs: STREAM_IDLE_TIMEOUT_MS,
            elapsedMs: Date.now() - startTime, bytesReceived, lastCcEvent: lastCcEvent || '(none)',
          });
          consecutiveTimeouts++;
          const timeoutMsg = consecutiveTimeouts >= TIMEOUT_REDUCE_CONTEXT_THRESHOLD
            ? 'Response timeout - try reducing context length (summarize earlier messages)'
            : 'Response timeout - request timed out';
          if (!started) { sendResponsesError(res, 429, 'rate_limit_error', timeoutMsg, 5); return; }
          if (!res.writableEnded) {
            try { res.write(translator.errorEvent(timeoutMsg)); } catch (e2) {}
            try { res.destroy(); } catch (e2) {}
          }
        } else {
          log('error', 'Stream error', { message: e.message, path: '/v1/responses' });
          try { abortController.abort(); } catch (e2) {}
          if (!started) {
            sendResponsesError(res, 502, 'proxy_error', 'Upstream error: ' + e.message, 10);
            return;
          }
          if (!res.writableEnded) {
            try { res.write(translator.errorEvent(e.message)); } catch (e2) {}
          }
        }
      } finally {
        idle.dispose();
      }

      if (!res.writableEnded) res.end();
    } else {
      // ── 非流式：缓冲完整 NDJSON 后一次性构造 Responses 对象 ──
      let fullText = '';
      let thinkingText = '';
      let usage = null;
      let finishReason = 'stop';
      let upstreamError = null;
      const toolCalls = [];
      reader = ccResponse.body.getReader();
      const decoder = new TextDecoder();
      let buf = '';

      const processLines = () => {
        const lines = buf.split('\n');
        buf = lines.pop() || '';
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed || trimmed === '[DONE]' || trimmed.startsWith(':')) continue;
          let event;
          try { event = JSON.parse(trimmed); } catch (e2) { continue; }
          switch (event.type) {
            case 'text-delta': lastCcEvent = event.type; fullText += event.text || ''; break;
            case 'reasoning-delta': lastCcEvent = event.type; thinkingText += event.text || ''; break;
            case 'tool-call': {
              lastCcEvent = event.type;
              toolCalls.push({
                id: event.toolCallId || ('call_' + randomUUID().slice(0, 8)),
                type: 'function',
                function: {
                  name: event.toolName || '',
                  arguments: typeof event.input === 'string' ? event.input : JSON.stringify(event.input || {}),
                },
              });
              break;
            }
            case 'finish':
              lastCcEvent = event.type;
              finishReason = mapFinishReason(event.finishReason || 'stop');
              if (event.totalUsage || event.usage) usage = event.totalUsage || event.usage;
              break;
            case 'error':
              lastCcEvent = event.type;
              log('warn', 'CC stream error (non-stream)', { message: event.error ? event.error.message : event.message });
              upstreamError = mapCcEventError(event);
              break;
            default:
              log('warn', 'Unknown CC event type', { type: event.type });
              break;
          }
        }
      };

      const idle = createIdleWatchdog(NONSTREAM_IDLE_TIMEOUT_MS);
      while (true) {
        const result = await Promise.race([reader.read(), idle.arm()]);
        const done = result.done;
        const value = result.value;
        if (done) break;
        bytesReceived += value.length;
        const chunkText = decoder.decode(value, { stream: true });
        buf += chunkText;
        if (chunkText.indexOf('\n') !== -1) processLines();
      }
      idle.dispose();
      processLines();

      if (upstreamError) {
        sendResponsesError(res, upstreamError.status, upstreamError.body.error.type,
          upstreamError.body.error.message, upstreamError.body.retry_after);
        return;
      }

      if (!fullText && !thinkingText && !toolCalls.length) {
        try { if (!abortController.signal.aborted) abortController.abort(); } catch (e2) {}
        sendResponsesError(res, 429, 'rate_limit_error',
          'Empty response from upstream (zero output tokens)', 10);
        return;
      }

      consecutiveTimeouts = 0;
      echoOpts.finishReason = finishReason;
      sendJSON(res, 200, buildResponsesObject(
        responseId, model, created, fullText, thinkingText, toolCalls, usage, echoOpts));
    }
  } catch (e) {
    if (e.name === 'AbortError' || e.code === 'ABORT_ERR') return;
    log('error', 'Responses handler error', { message: e.message });
    if (!res.headersSent) {
      sendResponsesError(res, 502, 'proxy_error', 'Upstream error: ' + e.message, 10);
    } else if (!res.writableEnded) {
      try { res.write(translator ? translator.errorEvent(e.message) : ''); } catch (e2) {}
      try { res.end(); } catch (e2) {}
    }
  }
}

async function handleModels(req, res) {
  const apiKey = getApiKey(req.headers);
  const models = await fetchModels(apiKey);
  const now = nowUnix();
  sendJSON(res, 200, {
    object: 'list',
    data: models.map(m => ({
      id: m.id,
      object: 'model',
      created: now,
      owned_by: 'command-code',
    })),
  });
}

function handleHealth(req, res) {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('OK');
}

// ── 管理面板 / Key 池 API ───────────────────────────
// GET  /  /admin              → index.html
// GET  /admin.css /admin.js   → 静态资源
// GET/POST /admin/api/keys    → 列出 / 新增
// PATCH/DELETE /admin/api/keys/:id
// GET  /admin/api/keys/:id/credits
// POST /admin/api/keys/refresh-credits
// GET/PUT /admin/api/lb       → 负载均衡配置

const UI_FILES = {
  '/': { path: resolve(__dirname, 'index.html'), type: 'text/html; charset=utf-8' },
  '/index.html': { path: resolve(__dirname, 'index.html'), type: 'text/html; charset=utf-8' },
  '/admin': { path: resolve(__dirname, 'index.html'), type: 'text/html; charset=utf-8' },
  '/admin/': { path: resolve(__dirname, 'index.html'), type: 'text/html; charset=utf-8' },
  '/admin.css': { path: resolve(__dirname, 'admin.css'), type: 'text/css; charset=utf-8' },
  '/admin.js': { path: resolve(__dirname, 'admin.js'), type: 'application/javascript; charset=utf-8' },
};

function publicKeyView(k) {
  const cooling = k.cooldownUntil && k.cooldownUntil > Date.now();
  return {
    id: k.id,
    label: k.label,
    keyPrefix: k.key.slice(0, 12) + '…',
    weight: k.weight,
    enabled: k.enabled,
    priority: k.priority,
    createdAt: k.createdAt,
    lastUsedAt: k.lastUsedAt,
    lastErrorAt: k.lastErrorAt,
    errorCount: k.errorCount,
    consecutiveFailures: k.consecutiveFailures || 0,
    autoDisabled: k.autoDisabled || null,
    cooldownUntil: cooling ? k.cooldownUntil : 0,
    cooling,
    isDefault: k.id === keyStore.defaultId,
    credits: k.credits,
    activeSessions: activeSessionCounts().get(k.id) || 0,
  };
}

/** 服务端轮询状态（前端用来显示上次/下次刷新与倒计时） */
function creditsRefreshState() {
  return {
    intervalMs: keyStore.settings.creditsRefreshMs,
    running: creditsRefresh.running,
    lastAt: creditsRefresh.lastAt,
    nextAt: creditsRefresh.nextAt,
    lastDurationMs: creditsRefresh.lastDurationMs,
    lastOk: creditsRefresh.lastOk,
    lastFailed: creditsRefresh.lastFailed,
    lastError: creditsRefresh.lastError,
  };
}

/** 当前会话 → Key 绑定（只暴露不可逆的短标识） */
function sessionRoutesView() {
  const now = Date.now();
  const ttl = keyStore.settings.sessionTtlMs;
  const out = [];
  for (const [sid, rec] of sessionRoutes) {
    if (now - rec.lastAt > ttl) continue;
    const k = findKey(rec.keyId);
    out.push({
      id: sid,
      tag: sessTag(sid),
      shortId: sid.slice(0, 18),
      kind: sid.slice(0, 2).replace('_', ''),
      keyId: rec.keyId,
      keyLabel: k ? k.label : '(已删除)',
      boundAt: rec.boundAt,
      lastAt: rec.lastAt,
      failures: rec.failures || 0,
      idleMs: now - rec.lastAt,
    });
  }
  return out.sort((a, b) => b.lastAt - a.lastAt).slice(0, 200);
}

async function ccGet(path, key, orgId) {
  const url = new URL(CFG.apiBase + path);
  if (orgId) url.searchParams.set('orgId', orgId);
  const r = await fetch(url, {
    headers: {
      'Authorization': `Bearer ${key}`,
      'Accept': 'application/json',
      'x-cli-environment': 'production',
      'x-command-code-version': CC_VERSION,
    },
    signal: AbortSignal.timeout(10000),
  });
  if (!r.ok) throw new Error(`${path} HTTP ${r.status}`);
  return r.json();
}

// credits.{monthlyCredits,purchasedCredits,freeCredits} + windowLimits.{fiveHour,weekly}
async function fetchCreditsForKey(k) {
  const fetchedAt = Date.now();
  let orgId = null;
  try { orgId = (await ccGet('/alpha/whoami', k.key)).org?.id || null; } catch {}

  let root;
  try { root = await ccGet('/alpha/billing/credits', k.key, orgId); }
  catch (e) { return { error: e.message, fetchedAt }; }
  if (root?.error) return { error: String(root.error), fetchedAt };

  let plan = null;
  try { plan = (await ccGet('/alpha/billing/subscriptions', k.key, orgId)).data?.planId || null; } catch {}

  const ledger = root.credits || {};
  const num = (v) => Number(v) || 0;
  const credits = {
    monthly: num(ledger.monthlyCredits),
    purchased: num(ledger.purchasedCredits),
    free: num(ledger.freeCredits),
  };
  credits.remaining = credits.monthly + credits.purchased + credits.free;

  const wl = root.windowLimits || ledger.windowLimits || {};
  const windows = [['5h', wl.fiveHour], ['weekly', wl.weekly]]
    .filter(([, w]) => w && isFinite(Number(w.used)) && isFinite(Number(w.cap)))
    .map(([name, w]) => {
      const used = Number(w.used), cap = Number(w.cap);
      const resetAt = typeof w.resetAt === 'number' ? w.resetAt : Date.parse(w.resetAt);
      return {
        name, used, cap,
        resetsAt: isFinite(resetAt) ? resetAt : null,
        pct: cap > 0 ? Math.min(100, Math.round((used / cap) * 100)) : 0,
      };
    });

  return {
    fetchedAt, plan, credits,
    creditsRemaining: credits.remaining,
    windows,
    worstPct: windows.reduce((m, w) => Math.max(m, w.pct), 0),
  };
}

// ── 额度轮询（服务端定时刷新） ───────────────────────
// 中转站要靠额度数据判断某个 Key 还能不能用，所以轮询必须跑在服务端，
// 间隔由管理页配置并持久化到 keys.json。
const creditsRefresh = {
  running: false,
  lastAt: 0,
  nextAt: 0,
  lastDurationMs: 0,
  lastOk: 0,
  lastFailed: 0,
  lastError: null,
};
let creditsRefreshTimer = null;

async function refreshAllCredits() {
  if (creditsRefresh.running) return creditsRefresh;
  creditsRefresh.running = true;
  const t0 = Date.now();
  let ok = 0;
  let failed = 0;
  try {
    expireAutoDisables();
    // 并发刷新，避免 Key 多时一轮跑太久（每个 Key 要打 3 个上游接口）
    const list = [...keyStore.keys];
    let cursor = 0;
    const worker = async () => {
      while (cursor < list.length) {
        const k = list[cursor++];
        try {
          k.credits = await fetchCreditsForKey(k);
          if (k.credits?.error) failed++; else ok++;
        } catch (e) {
          failed++;
          log('warn', 'Credits refresh failed', { keyId: k.id, message: e.message });
        }
        syncAutoDisable(k);
      }
    };
    await Promise.all(Array.from({ length: Math.min(4, list.length) }, worker));
    saveKeyStore();
    creditsRefresh.lastOk = ok;
    creditsRefresh.lastFailed = failed;
    creditsRefresh.lastError = failed ? `${failed} 个 Key 查询失败` : null;
    creditsRefresh.lastAt = Date.now();
    creditsRefresh.lastDurationMs = creditsRefresh.lastAt - t0;
    log('info', 'Credits refreshed', { ok, failed, durationMs: creditsRefresh.lastDurationMs });
  } finally {
    creditsRefresh.running = false;
  }
  return creditsRefresh;
}

/** 按当前设置重排轮询定时器（自调度，避免刷新未结束时叠加） */
function scheduleCreditsRefresh() {
  if (creditsRefreshTimer) {
    clearTimeout(creditsRefreshTimer);
    creditsRefreshTimer = null;
  }
  const ms = Math.max(0, Number(keyStore.settings.creditsRefreshMs) || 0);
  if (!ms) {
    creditsRefresh.nextAt = 0;
    log('info', 'Credits auto-refresh disabled');
    return;
  }
  creditsRefreshTimer = setTimeout(async () => {
    try {
      await refreshAllCredits();
    } catch (e) {
      log('warn', 'Credits refresh loop error', { message: e.message });
    }
    scheduleCreditsRefresh();
  }, ms);
  if (creditsRefreshTimer.unref) creditsRefreshTimer.unref();
  creditsRefresh.nextAt = Date.now() + ms;
  log('info', 'Credits auto-refresh scheduled', { intervalMs: ms });
}

async function readJsonOr400(req, res) {
  try {
    return await readBody(req);
  } catch (e) {
    sendJSON(res, e.statusCode === 413 ? 413 : 400, { error: { message: e.message, type: 'invalid_request_error' } });
    return null;
  }
}

function serveUiFile(req, res, pathname) {
  const f = UI_FILES[pathname];
  if (!f || !existsSync(f.path)) {
    sendJSON(res, 404, { error: { message: 'UI file not found: ' + pathname, type: 'not_found' } });
    return;
  }
  res.writeHead(200, { 'Content-Type': f.type, 'Cache-Control': 'no-store' });
  res.end(readFileSync(f.path));
}

const LB_FIELDS = ['strategy', 'stickyBy', 'maxRetries', 'cooldownMs'];

function currentLbConfig() {
  return {
    strategy: keyStore.lb.strategy,
    stickyBy: keyStore.lb.stickyBy,
    maxRetries: keyStore.lb.maxRetries,
    cooldownMs: keyStore.lb.cooldownMs,
  };
}

async function handleAdminApi(req, res, url) {
  const path = url.pathname.replace(/^\/admin\/api/, '') || '/';

  if (path === '/lb') {
    if (req.method === 'GET') return sendJSON(res, 200, currentLbConfig());
    if (req.method === 'PUT') {
      const body = await readJsonOr400(req, res);
      if (!body) return;
      for (const f of LB_FIELDS) {
        if (body[f] === undefined) continue;
        if (f === 'strategy' || f === 'stickyBy') keyStore.lb[f] = String(body[f]);
        else keyStore.lb[f] = Number(body[f]) || 0;
      }
      const strategies = ['round-robin', 'weighted', 'random', 'weighted-random', 'sticky', 'least-recent', 'failover'];
      const stickies = ['none', 'ip', 'client-key'];
      if (!strategies.includes(keyStore.lb.strategy)) keyStore.lb.strategy = 'weighted';
      if (!stickies.includes(keyStore.lb.stickyBy)) keyStore.lb.stickyBy = 'client-key';
      keyStore.lb.maxRetries = Math.max(0, Math.min(5, keyStore.lb.maxRetries | 0));
      keyStore.lb.cooldownMs = Math.max(0, Math.min(3600000, keyStore.lb.cooldownMs | 0));
      saveKeyStore();
      return sendJSON(res, 200, currentLbConfig());
    }
  }

  if (path === '/settings') {
    if (req.method === 'GET') {
      return sendJSON(res, 200, { settings: keyStore.settings, creditsRefresh: creditsRefreshState() });
    }
    if (req.method === 'PUT') {
      const body = await readJsonOr400(req, res);
      if (!body) return;
      const before = keyStore.settings.creditsRefreshMs;
      keyStore.settings = normalizeSettings({ ...keyStore.settings, ...body });
      saveKeyStore();
      if (keyStore.settings.creditsRefreshMs !== before) scheduleCreditsRefresh();
      // 设置变化后立刻重算自动停用状态（例如刚打开/关闭"额度用尽自动停用"）
      for (const k of keyStore.keys) syncAutoDisable(k);
      saveKeyStore();
      return sendJSON(res, 200, { settings: keyStore.settings, creditsRefresh: creditsRefreshState() });
    }
  }

  if (path === '/sessions') {
    if (req.method === 'GET') return sendJSON(res, 200, { sessions: sessionRoutesView() });
    if (req.method === 'DELETE') {
      const count = sessionRoutes.size;
      sessionRoutes.clear();
      log('info', 'Session bindings cleared from admin', { count });
      return sendJSON(res, 200, { cleared: count, sessions: [] });
    }
  }

  if (path === '/keys/refresh-credits' && req.method === 'POST') {
    await refreshAllCredits();
    return sendJSON(res, 200, {
      results: keyStore.keys.map((k) => ({ id: k.id, credits: k.credits })),
      creditsRefresh: creditsRefreshState(),
    });
  }

  if (path === '/keys') {
    if (req.method === 'GET') {
      // 顺手清理已到恢复时间的自动停用，保证界面状态与调度一致
      if (expireAutoDisables()) saveKeyStore();
      return sendJSON(res, 200, {
        apiBase: CFG.apiBase,
        lb: currentLbConfig(),
        settings: keyStore.settings,
        creditsRefresh: creditsRefreshState(),
        serverTime: Date.now(),
        defaultId: keyStore.defaultId,
        sessions: sessionRoutesView(),
        keys: keyStore.keys.map(publicKeyView),
      });
    }
    if (req.method === 'POST') {
      const body = await readJsonOr400(req, res);
      if (!body) return;
      const key = String(body.key || '').trim();
      const label = String(body.label || '').trim();
      if (!/^user_[a-zA-Z0-9_-]+$/.test(key)) {
        return sendJSON(res, 400, { error: { message: 'key must match /^user_[a-zA-Z0-9_-]+$/', type: 'invalid_request_error' } });
      }
      if (keyStore.keys.some((k) => k.key === key)) {
        return sendJSON(res, 409, { error: { message: 'key already exists', type: 'conflict' } });
      }
      const entry = {
        id: newKeyId(),
        label: label || '未命名',
        key,
        weight: Number.isFinite(+body.weight) ? Math.max(0, +body.weight) : 1,
        enabled: body.enabled !== false,
        priority: Number.isFinite(+body.priority) ? +body.priority : 0,
        createdAt: Date.now(),
        lastUsedAt: null,
        lastErrorAt: null,
        errorCount: 0,
        cooldownUntil: 0,
        credits: null,
      };
      keyStore.keys.push(entry);
      saveKeyStore();
      return sendJSON(res, 201, { ...publicKeyView(entry), key: entry.key });
    }
  }

  const m = path.match(/^\/keys\/([A-Za-z0-9_]+)(\/credits)?$/);
  const entry = m ? findKey(m[1]) : null;
  if (entry) {
    if (m[2] && req.method === 'GET') {
      entry.credits = await fetchCreditsForKey(entry);
      saveKeyStore();
      return sendJSON(res, 200, entry.credits);
    }
    if (req.method === 'PATCH') {
      const body = await readJsonOr400(req, res);
      if (!body) return;
      if (typeof body.label === 'string' && body.label.trim()) entry.label = body.label.trim();
      if (body.weight !== undefined) entry.weight = Math.max(0, Number(body.weight) || 0);
      if (body.enabled !== undefined) entry.enabled = !!body.enabled;
      if (body.priority !== undefined) entry.priority = Number(body.priority) || 0;
      if (body.default === true) keyStore.defaultId = entry.id;
      else if (body.default === false && keyStore.defaultId === entry.id) keyStore.defaultId = null;
      saveKeyStore();
      return sendJSON(res, 200, publicKeyView(entry));
    }
    if (req.method === 'DELETE') {
      keyStore.keys = keyStore.keys.filter((k) => k.id !== entry.id);
      wrrCurrent.delete(entry.id);
      if (keyStore.defaultId === entry.id) keyStore.defaultId = null;
      // 清掉指向该 Key 的会话绑定，让相关会话下次请求自动重绑
      for (const [sid, rec] of sessionRoutes) {
        if (rec.keyId === entry.id) sessionRoutes.delete(sid);
      }
      saveKeyStore();
      res.writeHead(204);
      return res.end();
    }
  }

  return entry
    ? sendJSON(res, 405, { error: { message: 'method not allowed', type: 'invalid_request_error' } })
    : sendJSON(res, 404, { error: { message: 'admin route not found', type: 'not_found' } });
}

// ── 服务器 ──────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const host = req.headers.host || 'localhost';
  const url = new URL(req.url, `http://${host}`);

  // 在途上限准入。/health 与管理 UI 例外：探活与编排器不该因业务繁忙而收 503。
  const isLiveness = url.pathname === '/health'
    || url.pathname === '/'
    || url.pathname === '/index.html'
    || url.pathname === '/admin'
    || url.pathname === '/admin/'
    || url.pathname === '/admin.css'
    || url.pathname === '/admin.js';
  if (!isLiveness && MAX_INFLIGHT > 0) {
    if (inflightCount >= MAX_INFLIGHT) {
      log('warn', 'In-flight limit reached, rejecting request', {
        maxInflight: MAX_INFLIGHT, inflight: inflightCount, path: url.pathname,
      });
      sendJSON(res, 503, {
        error: { message: `Too many concurrent requests (limit ${MAX_INFLIGHT}), retry shortly`, type: 'server_busy' },
        retry_after: 5,
      });
      return;
    }
    inflightCount++;
    // 释放时机：响应写完（finish）或连接终止（close）—— 取先到者，且幂等，
    // 保证任何退出路径（成功/出错/客户端断连/超时）都不会泄漏槽位。
    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      if (inflightCount > 0) inflightCount--;
    };
    res.once('finish', release);
    res.once('close', release);
  }

  try {
    // 管理面板与 Key 池 API
    if (url.pathname.startsWith('/admin/api/') || url.pathname === '/admin/api') {
      await handleAdminApi(req, res, url);
      return;
    }
    if (UI_FILES[url.pathname]) {
      serveUiFile(req, res, url.pathname);
      return;
    }

    if (url.pathname === '/v1/chat/completions' && req.method === 'POST') {
      await handleChatCompletions(req, res);
    } else if (url.pathname === '/v1/messages' && req.method === 'POST') {
      await handleMessages(req, res);
    } else if (url.pathname === '/v1/responses' && req.method === 'POST') {
      await handleResponses(req, res);
    } else if (url.pathname === '/v1/models' && req.method === 'GET') {
      await handleModels(req, res);
    } else if (url.pathname === '/health') {
      handleHealth(req, res);
    } else {
      sendJSON(res, 404, { error: { message: 'Not found', type: 'not_found' } });
    }
  } catch (e) {
    sendJSON(res, 500, { error: { message: e.message, type: 'internal_error' } });
  }
});

// 全局兜底：abort 触发的异步 rejection 不会让进程崩溃
process.on('unhandledRejection', (reason) => {
  if (reason?.name === 'AbortError' || reason?.code === 'ABORT_ERR') {
    // 客户端断连触发的 abort — 预期行为，静默处理
    log('info', 'Aborted request cleaned up');
  } else {
    log('error', 'Unhandled rejection', { message: reason?.message || String(reason), stack: reason?.stack?.split('\n')[0] });
  }
});

// 启动时按已持久化的额度数据同步一次自动停用状态，并开启额度轮询
for (const k of keyStore.keys) syncAutoDisable(k);
scheduleCreditsRefresh();
if (keyStore.settings.creditsRefreshMs > 0) {
  // 启动后先拉一次，让管理页与调度尽快拿到真实额度
  setTimeout(() => { refreshAllCredits().catch(() => {}); }, 3000).unref?.();
}

server.listen(CFG.port, CFG.host, () => {
  log('info', 'CC Proxy started', {
    url: `http://${CFG.host}:${CFG.port}`,
    api: CFG.apiBase,
    models: MODELS.length,
    session: '12h + 1h jitter, per API key',
    routing: `${keyStore.settings.mode} mode, failThreshold=${keyStore.settings.failThreshold}, keys=${keyStore.keys.length}`,
    creditsRefresh: keyStore.settings.creditsRefreshMs > 0
      ? `every ${Math.round(keyStore.settings.creditsRefreshMs / 1000)}s (auto-disable exhausted: ${keyStore.settings.autoDisableExhausted ? 'on' : 'off'})`
      : 'off',
    zdr: CFG.zdr ? 'enabled (x-cmd-zdr: 1 on generation/init requests)' : 'off (CMD_ZDR=1 or per-request x-cmd-zdr: 1 to enable)',
    emptySystemPlaceholder: CFG.emptySystemPlaceholder ? 'on (space placeholder for requests without system prompt, issue #17)' : 'off',
    logFile: CFG.logFile || '(console only)',
    clientDrainTimeout: CLIENT_DRAIN_TIMEOUT_MS > 0 ? `${CLIENT_DRAIN_TIMEOUT_MS}ms` : 'disabled',
    idleTimeouts: `stream ${STREAM_IDLE_TIMEOUT_MS}ms / nonstream ${NONSTREAM_IDLE_TIMEOUT_MS}ms`,
    maxInflight: MAX_INFLIGHT > 0 ? `${MAX_INFLIGHT} (global, /health exempt)` : 'unlimited (CC_MAX_INFLIGHT=0)',
  });
  if (CLIENT_DRAIN_TIMEOUT_MS > 0) {
    log('info', 'Client drain timeout enabled', { timeoutMs: CLIENT_DRAIN_TIMEOUT_MS });
  }
  // 内存提示：body 上限隐含的最坏内存 = 上限 × 实测放大系数（见 MAX_BODY_SIZE 注释 / issue #20）
  const bodyCapMB = Math.round(MAX_BODY_SIZE / 1048576);
  const worstCaseMB = Math.round(bodyCapMB * 5.5);
  if (worstCaseMB >= 500) {
    log('warn', 'Request body limit implies high per-request worst-case memory', {
      maxBodyMB: bodyCapMB,
      worstCaseRSSPerRequestMB: worstCaseMB,
      hint: 'lower CC_MAX_BODY_MB and/or cap in-flight requests at the reverse proxy (see README)',
    });
  }
  if (!CFG.apiKey) {
    log('info', 'No API key in config. API key must be sent in Authorization: Bearer <key> header per request.');
  }
});
