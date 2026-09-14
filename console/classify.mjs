/**
 * console/classify.mjs — 上游错误的分类器（P2a）
 *
 * 纯函数：**不做任何 I/O**（不查库、不查额度、不发请求），只吃 (status, 原始 body 文本)
 * 吐出一个动作。挂在核心的 hooks.forward 里用，核心对此一无所知。
 *
 * 判定顺序照 DESIGN-multiaccount-v4 §4.2，**顺序不能换**：
 *   1. 401 / 403                 → 封禁（凭据被拒，不会自己好）
 *   2. 额度耗尽（400 + insufficient / 或已知额度 code）→ 冷却至账期
 *   3. 429                       → 冷却至重置时间
 *   4. 5xx                       → 计数（连续 N 次由池决定是否软封）
 *   5. 其他 4xx（含 422）        → 透传（请求本身的错，换号也一样失败）
 *
 * 相对设计的两处强化（都有理由，见各自注释）：
 *   A. **code 优先、文本兜底**：上游 error.code 是机器可读的（本会话实测抓到过
 *      USAGE_EXCEEDED / RATE_LIMITED / BAD_REQUEST），比 /insufficient/i 文本匹配抗改词。
 *      文本匹配保留为兜底，因为 §4.1 的真实样本里 code 是 null。
 *   B. **重置时间扫描加安全窗口**：§4.4 说「取所有候选中的最大值（最晚恢复=安全）」，
 *      但无边界取最大值有真实风险 —— 429 body 里若混进 currentPeriodEnd 之类的远期字段，
 *      一个 5 小时限流会被判成冷却一整月。这里限定在 now + 8 天（周窗口实测约 69h），
 *      超出窗口的候选直接丢弃并记 note。
 */
import { debuglog } from 'util';
export const debug = debuglog('cc-classify');

export const ACTIONS = ['transparent', 'cooldown', 'ban', 'count'];

// 额度耗尽类 code（服务端权威标记，优先于文本）
const QUOTA_CODES = new Set([
  'USAGE_EXCEEDED', 'QUOTA_EXCEEDED', 'INSUFFICIENT_CREDITS', 'INSUFFICIENT_QUOTA',
  'CREDITS_EXHAUSTED', 'OUT_OF_CREDITS', 'MONTHLY_LIMIT_REACHED',
]);
const RATE_CODES = new Set([
  'RATE_LIMITED', 'RATE_LIMIT_EXCEEDED', 'TOO_MANY_REQUESTS', 'WINDOW_EXCEEDED',
]);

const DEFAULT_FALLBACK_MS = 60_000;            // §4.4 第 3 级：拿不到重置时间就保守冷却 60s
const RESET_WINDOW_MS = 8 * 24 * 3600_000;     // 见上文 B

/** 单个值 → 毫秒时间戳。宽容：秒/毫秒数字、纯数字字符串、ISO 字符串；认不出返回 null */
export function toMs(v) {
  if (typeof v === 'number') {
    if (v >= 1e12) return v;                    // 毫秒
    if (v >= 1e9) return v * 1000;              // 秒（§4.4：< 1e12 视为秒）
    return null;
  }
  if (typeof v === 'string') {
    if (/^\d{10}$/.test(v)) return Number(v) * 1000;
    if (/^\d{13}$/.test(v)) return Number(v);
    if (/^\d{4}-\d{2}-\d{2}T/.test(v)) {
      const t = Date.parse(v);
      return Number.isFinite(t) ? t : null;
    }
  }
  return null;
}

/** 宽容地把 JSON 里所有「像时间戳」的值挖出来 */
function collectTimestamps(node, out, depth = 0) {
  if (depth > 6 || node == null) return out;
  if (Array.isArray(node)) { for (const v of node) collectTimestamps(v, out, depth + 1); return out; }
  if (typeof node === 'object') { for (const v of Object.values(node)) collectTimestamps(v, out, depth + 1); return out; }
  const ms = toMs(node);
  if (ms != null) out.push(ms);
  return out;
}

/**
 * 从 body 文本里挑一个「重置时间」。
 * @returns {{at:number|null, rejected:number[]}} rejected = 被安全窗口丢掉的那些
 */
export function parseResetTime(bodyText, { now = Date.now(), windowMs = RESET_WINDOW_MS } = {}) {
  if (!bodyText) return { at: null, rejected: [] };
  let parsed;
  try { parsed = JSON.parse(bodyText); } catch { parsed = null; }
  const all = parsed == null ? [] : collectTimestamps(parsed, []);
  const future = all.filter((t) => t > now);                 // 过去的重置时间没有意义
  const inWindow = future.filter((t) => t <= now + windowMs);
  const rejected = future.filter((t) => t > now + windowMs);
  return { at: inWindow.length ? Math.max(...inWindow) : null, rejected };
}

/** 从 body 里取 code / message / type，兼容三种信封 */
export function inspect(bodyText) {
  const out = { code: null, message: '', type: null, parsed: null };
  if (!bodyText) return out;
  try { out.parsed = JSON.parse(bodyText); } catch { out.message = String(bodyText).slice(0, 300); return out; }
  const p = out.parsed;
  const err = p?.error && typeof p.error === 'object' ? p.error : null;
  out.code = err?.code ?? p?.code ?? null;
  out.message = String(err?.message ?? p?.message ?? p?.error ?? '').slice(0, 500);
  out.type = err?.type ?? p?.type ?? null;
  return out;
}

/**
 * 分类一次上游失败。
 *
 * @param {number} status 上游 HTTP 状态
 * @param {string} bodyText 上游**原始**响应体（务必是透传之前的那份）
 * @param {{now?:number, fallbackMs?:number, windowMs?:number}} opts
 * @returns {{
 *   status:number, action:'transparent'|'cooldown'|'ban'|'count', retryable:boolean,
 *   reason:string|null, cooldownUntil:number|null, needsQuotaQuery:boolean,
 *   code:string|null, type:string|null, message:string, source:string, note:string|null
 * }}
 */
export function classify(status, bodyText, opts = {}) {
  const now = opts.now ?? Date.now();
  const fallbackMs = opts.fallbackMs ?? DEFAULT_FALLBACK_MS;
  const windowMs = opts.windowMs ?? RESET_WINDOW_MS;
  const info = inspect(bodyText);
  const base = {
    status, code: info.code, type: info.type, message: info.message,
    reason: null, cooldownUntil: null, needsQuotaQuery: false, source: 'status', note: null,
  };

  // 1. 凭据被拒 —— 永久封禁（换号重试，但原账号要人处理）
  if (status === 401 || status === 403) {
    return { ...base, action: 'ban', retryable: true, source: 'auth',
      note: `凭据被拒（HTTP ${status}），需手动解封` };
  }

  // 2. 额度耗尽（月度）—— 冷却至账期
  //    优先看 code（机器可读），文本匹配只作兜底（§4.1 真实样本的 code 是 null）
  const looksQuota = (info.code && QUOTA_CODES.has(info.code)) || /insufficient|quota|credits/i.test(info.message);
  if (status === 400 && looksQuota) {
    const q = parseResetTime(bodyText, { now, windowMs });
    return { ...base, action: 'cooldown', retryable: true, reason: 'monthly',
      cooldownUntil: q.at, needsQuotaQuery: q.at == null,
      source: info.code && QUOTA_CODES.has(info.code) ? 'code' : 'text',
      note: q.at ? null : '账期结束时间未知，需查 /alpha/billing/credits 或走 60s 兜底' };
  }
  if (status === 402) {
    const q = parseResetTime(bodyText, { now, windowMs });
    return { ...base, action: 'cooldown', retryable: true, reason: 'monthly',
      cooldownUntil: q.at, needsQuotaQuery: q.at == null, source: 'status',
      note: q.at ? null : '账期结束时间未知，需查 /alpha/billing/credits 或走 60s 兜底' };
  }

  // 3. 窗口耗尽（5h / weekly 不区分：动作相同，重置时间上游会给）
  if (status === 429 || (info.code && RATE_CODES.has(info.code))) {
    const q = parseResetTime(bodyText, { now, windowMs });
    const note = q.at
      ? (q.rejected.length ? `丢弃了 ${q.rejected.length} 个超出 ${Math.round(windowMs / 86400000)} 天窗口的时间戳` : null)
      : '上游未给出可用的重置时间，走 60s 保守冷却并标记待查额度';
    return { ...base, action: 'cooldown', retryable: true, reason: q.at ? 'rate_limit' : 'fallback',
      cooldownUntil: q.at ?? now + fallbackMs, needsQuotaQuery: q.at == null,
      source: q.at ? 'reset-time' : 'fallback', note };
  }

  // 4. 上游自身故障 —— 计数，连续 N 次由池决定软封
  if (status >= 500 && status <= 599) {
    return { ...base, action: 'count', retryable: true,
      note: '上游 5xx，计入 fail_streak（达阈值由池软封）' };
  }

  // 5. 其他 4xx / 422 —— 请求本身的问题，换号也一样失败
  return { ...base, action: 'transparent', retryable: false,
    note: '请求本身的错误，直接透传，不冷却不换号' };
}
