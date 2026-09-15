/**
 * console/classify.mjs — 上游错误的分类器（P2a）
 *
 * 纯函数：**不做任何 I/O**（不查库、不查额度、不发请求），只吃 (status, 原始 body 文本)
 * 吐出一个动作。挂在核心的 hooks.forward 里用，核心对此一无所知。
 *
 * 判定顺序**不能换**：
 *   1. 额度不足（400 + insufficient / 额度 code，或 402）→ 冷却至账期 → 换号
 *   2. 429（或限流 code）                                → 冷却至重置时间 → 换号
 *   3. 401 / 403                                        → 记账，原样透传，**不换号**
 *   4. 5xx                                              → 记账，原样透传，**不换号**
 *   5. 其他 4xx（含 422）                                → 原样透传，连账都不记
 *
 * 两条产品决策（业主 2026-09 明确）：
 *   - **只有「这个号暂时没额度了」才换号**：额度不足与 429 都是"换一个号就能继续服务"
 *     的情形。其余错误换号也不能成功（401 是 key 废了、5xx 是上游炸了、400 是请求本身
 *     的问题），所以一律透传给客户端，让它自己看见真实的上游错误。
 *   - **没有硬封（永久封禁）**：401/403 不再把账号踢出候选。代价是废号的 key 会一直留在
 *     候选里、每次请求有 1/N 概率白试一次；换来的是"不需要人工解封"这一条运维上的简洁。
 *     池里仍保留 markBan/unban 作为**人工**手段，但没有任何自动路径会调用它们。
 *
 * 保留的设计要点：
 *   A. **code 优先、文本兜底**：上游 error.code 是机器可读的（实测抓到过 USAGE_EXCEEDED /
 *      RATE_LIMITED），比 /insufficient/i 文本匹配抗改词；文本兜底保留，因为 §4.1 的真实
 *      额度不足样本里 code 是 null。
 *   B. **重置时间扫描加安全窗口**：无边界取最大值有真实风险 —— 429 body 里若混进
 *      currentPeriodEnd 之类的远期字段，一个 5 小时限流会被判成冷却一整月。这里限定在
 *      now + 8 天（周窗口实测约 69h），超出窗口的候选直接丢弃并记 note。
 *   C. **正则只看 code 与 message**，不扫整个 body：额度不足的返回体是固定的
 *      （400 + "You have insufficient credits..."），扫全文只会引入误判。
 */
import { debuglog } from 'util';
export const debug = debuglog('cc-classify');

export const ACTIONS = ['transparent', 'record', 'cooldown'];

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
 * 把上游各种窗口写法归一化。用途：决定冷却原因与"该看哪个窗口的重置时间"。
 * @returns {'fiveHour'|'weekly'|'monthly'|''}
 */
export function normalizeWindow(raw) {
  const s = String(raw || '').toLowerCase();
  if (!s) return '';
  if (s.includes('hour') || s.includes('5h') || s.includes('h5')) return 'fiveHour';
  if (s.includes('week') || s.includes('7d') || s.includes('7-day')) return 'weekly';
  if (s.includes('month') || s.includes('period') || s.includes('credit') || s.includes('billing')) return 'monthly';
  return '';
}

/**
 * 从**文案**里认窗口。上游纯文本样本（业主提供）：
 *   "You've reached your 5-hour usage limit for your plan. Your limit resets at 3:00 PM."
 * 顺序：先认小时，再认周，最后才认月度 —— 否则 "5-hour usage limit for your plan"
 * 里的 plan/credit 类词会把 5 小时限流误判成月度账期（那会把账号冷却几周）。
 */
export function detectWindow(text) {
  const s = String(text || '');
  if (!s) return '';
  if (/5[\s-]?hours?|five[\s-]?hours?|hourly|\b5h\b/i.test(s)) return 'fiveHour';
  if (/week|7[\s-]?days?/i.test(s)) return 'weekly';
  if (/month|billing period|credits?\b|balance/i.test(s)) return 'monthly';
  return '';
}

/**
 * 纯文本里的钟点时间："... resets at 3:00 PM." / "until 15:00"。
 * 没有日期也没有时区，所以只能按**本机本地时间**解释：
 * 取今天该时刻；若已过去（超过 1 分钟），取明天。调用方必须按窗口上限再夹一次
 * （5 小时窗口的冷却不可能超过 5 小时），见 classify 里的 cap。
 */
export function clockTimeIn(text, now = Date.now()) {
  const m = /(?:at|until|resets?\s+at)\s+(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(am|pm)?/i.exec(String(text || ''));
  if (!m) return null;
  let h = Number(m[1]);
  const min = Number(m[2]), sec = Number(m[3] || 0);
  const ap = (m[4] || '').toLowerCase();
  if (h > 23 || min > 59) return null;
  if (ap) { if (h < 1 || h > 12) return null; if (ap === 'pm' && h !== 12) h += 12; if (ap === 'am' && h === 12) h = 0; }
  const d = new Date(now);
  const at = new Date(d.getFullYear(), d.getMonth(), d.getDate(), h, min, sec, 0).getTime();
  return at > now + 60_000 ? at : at + 24 * 3600_000;    // 已经过点 → 认为是明天的那次
}

/**
 * 从 body 文本里挑一个「重置时间」。
 * @returns {{at:number|null, rejected:number[], fuzzy:boolean}} fuzzy = 来自纯文本钟点（不精确，
 *          调用方应标记 needsQuotaQuery 用额度接口校正）
 */
export function parseResetTime(bodyText, { now = Date.now(), windowMs = RESET_WINDOW_MS } = {}) {
  if (!bodyText) return { at: null, rejected: [], fuzzy: false };
  let parsed;
  try { parsed = JSON.parse(bodyText); } catch { parsed = null; }
  const all = parsed == null ? [] : collectTimestamps(parsed, []);
  const future = all.filter((t) => t > now);                 // 过去的重置时间没有意义
  const inWindow = future.filter((t) => t <= now + windowMs);
  const rejected = future.filter((t) => t > now + windowMs);
  if (inWindow.length) return { at: Math.max(...inWindow), rejected, fuzzy: false };
  // JSON 里没有可用时间戳（或根本不是 JSON）→ 退一步认纯文本里的钟点
  const clock = clockTimeIn(bodyText, now);
  return { at: clock, rejected, fuzzy: clock != null };
}

/** 从 body 里取 code / message / type，兼容三种信封 */
export function inspect(bodyText) {
  const out = { code: null, message: '', type: null, parsed: null, window: '' };
  if (!bodyText) return out;
  try { out.parsed = JSON.parse(bodyText); } catch { out.message = String(bodyText).slice(0, 300); return out; }
  const p = out.parsed;
  const err = p?.error && typeof p.error === 'object' ? p.error : null;
  out.code = err?.code ?? p?.code ?? null;
  out.message = String(err?.message ?? p?.message ?? p?.error ?? '').slice(0, 500);
  out.type = err?.type ?? p?.type ?? null;
  out.window = windowOf(p);
  return out;
}

/**
 * 从真实的限流信封里取"哪个窗口被拦"与"还剩多少额度"。
 *
 * 上游固定返回体（业主 2026-09 提供，逐字样本）：
 *   { "success": false,
 *     "error": { "code": "RATE_LIMITED", "status": 429,
 *                "message": "You've reached your weekly usage limit for your plan. Your limit
 *                            resets at 2026-09-21T10:50:25.673Z. ...",
 *                "docs": "https://commandcode.ai/docs/reference/errors/rate_limited",
 *                "rateLimit": { "limit": 6, "remaining": 0, "reset": 1789987825, "window": "weekly" } } }
 *
 * 关键点：**`rateLimit` 对象本身就是"这个号没额度了"的权威标记**，比状态码可靠 ——
 * 万一上游哪天把这条换个状态码（400/403）返回，只要 err.rateLimit 在，就照样能识别。
 */
export function windowOf(parsed) {
  const rl = findNode(parsed, ['rateLimit', 'ratelimit', 'rate_limit']);
  if (!rl || typeof rl !== 'object') return null;
  const w = String(findNode(rl, ['window', 'type', 'scope']) ?? '').toLowerCase();
  return {
    name: w,
    limit: Number(findNode(rl, ['limit', 'cap', 'total'])) || null,
    remaining: (() => { const v = findNode(rl, ['remaining', 'left']); return v == null ? null : Number(v); })(),
    reset: toMs(findNode(rl, ['reset', 'resetAt', 'resetsAt'])),
  };
}

/** 在对象里按（归一化）键名找第一个值，只往下钻两层：限流信封是固定形状，不需要深挖 */
function findNode(node, names, depth = 0) {
  if (!node || typeof node !== 'object' || depth > 2) return undefined;
  const want = new Set(names.map((n) => String(n).toLowerCase().replace(/[_\-\s]/g, '')));
  for (const [k, v] of Object.entries(node)) {
    if (want.has(String(k).toLowerCase().replace(/[_\-\s]/g, ''))) return v;
  }
  for (const v of Object.values(node)) {
    const r = findNode(v, names, depth + 1);
    if (r !== undefined) return r;
  }
  return undefined;
}

/**
 * 分类一次上游失败。
 *
 * @param {number} status 上游 HTTP 状态
 * @param {string} bodyText 上游**原始**响应体（务必是透传之前的那份）
 * @param {{now?:number, fallbackMs?:number, windowMs?:number}} opts
 * @returns {{
 *   status:number, action:'transparent'|'record'|'cooldown', retryable:boolean,
 *   reason:string|null, cooldownUntil:number|null, needsQuotaQuery:boolean,
 *   code:string|null, type:string|null, message:string, source:string, note:string|null
 * }}
 */
export function classify(status, bodyText, opts = {}) {
  const now = opts.now ?? Date.now();
  const fallbackMs = opts.fallbackMs ?? DEFAULT_FALLBACK_MS;
  const windowMs = opts.windowMs ?? RESET_WINDOW_MS;
  const info = inspect(bodyText);
  // 哪个窗口被拦：优先信信封里的 rateLimit.window，其次从文案里认（纯文本样本也得认出来）
  const win = normalizeWindow(info.window?.name) || detectWindow(info.message);
  // 只有**月度额度**才冷却到账期；5 小时 / 周窗口有自己的重置时间，
  // 判成 monthly 会让一次 5 小时限流冷却到账期结束（实测会差出好几周）
  const reason = win === 'monthly' ? 'monthly' : 'rate_limit';
  const base = {
    status, code: info.code, type: info.type, message: info.message, window: win,
    reason: null, cooldownUntil: null, needsQuotaQuery: false, source: 'status', note: null,
  };
  /** 窗口自身的上限：5 小时窗口不可能冷却超过 5 小时，周窗口不可能超过 7 天 */
  const capFor = (ms) => {
    if (ms == null) return null;
    const cap = win === 'fiveHour' ? 5 * 3600_000 : win === 'weekly' ? 7 * 24 * 3600_000 : null;
    return cap && ms > now + cap ? now + cap : ms;
  };

  // 1. 上游固定限流信封优先：error.rateLimit 在 = 这个号就是没额度了，比状态码可靠
  //    （万一哪天它换个状态码返回，只要 rateLimit 还在就照样识别）
  if (info.window) {
    const w = info.window;
    const reset = w.reset && w.reset > now ? w.reset : null;
    const desc = [w.name && `window=${w.name}`, w.limit != null && `limit=${w.limit}`,
      w.remaining != null && `remaining=${w.remaining}`].filter(Boolean).join(' ');
    return { ...base, action: 'cooldown', retryable: true, reason,
      cooldownUntil: reset, needsQuotaQuery: reset == null, source: 'rate-limit-object',
      note: `上游限流信封（${desc || '无细节'}）` + (reset ? '，已取到重置时间' : '，未给重置时间，需查额度接口') };
  }

  // 2. 额度语义 —— **不看状态码**：只要 code 或文案说"没额度了"，就换号。
  //    业主的要求是"只对额度不足做轮换"，判据就该是语义：上游给 400 还是 402/403 都是
  //    同一件事（实测月度不足返 400 且 code=null，只能靠文案）。
  const looksQuota = (info.code && QUOTA_CODES.has(info.code))
    || /insufficient|quota|credits|usage limit|reached your|upgrade your plan|spend cap/i.test(info.message);
  if (looksQuota) {
    const q = parseResetTime(bodyText, { now, windowMs });
    return { ...base, action: 'cooldown', retryable: true, reason,
      cooldownUntil: capFor(q.at), needsQuotaQuery: q.at == null || q.fuzzy,
      source: info.code && QUOTA_CODES.has(info.code) ? 'code' : (q.fuzzy ? 'text-clock' : 'text'),
      note: (q.at == null ? '重置时间未知，需查额度接口或走 60s 兜底'
        : (q.fuzzy ? '重置时间来自文案里的钟点（不精确），用额度接口校正' : null))
        + (status !== 400 && status !== 402 ? `（HTTP ${status}，按额度语义处理）` : '') || null };
  }

  // 3. 付款/账期类状态码 —— 与额度同义，即使文案没命中
  if (status === 402) {
    const q = parseResetTime(bodyText, { now, windowMs });
    return { ...base, reason: 'monthly', action: 'cooldown', retryable: true,
      cooldownUntil: q.at, needsQuotaQuery: q.at == null, source: 'status',
      note: q.at ? null : '账期结束时间未知，需查 /alpha/billing/credits 或走 60s 兜底' };
  }

  // 4. 窗口耗尽（5h / weekly 不区分：动作相同，重置时间上游会给）
  if (status === 429 || (info.code && RATE_CODES.has(info.code))) {
    const q = parseResetTime(bodyText, { now, windowMs });
    const note = q.at
      ? (q.rejected.length ? `丢弃了 ${q.rejected.length} 个超出 ${Math.round(windowMs / 86400000)} 天窗口的时间戳` : null)
      : '上游未给出可用的重置时间，走 60s 保守冷却并标记待查额度';
    return { ...base, action: 'cooldown', retryable: true, reason: q.at ? reason : 'fallback',
      cooldownUntil: capFor(q.at) ?? now + fallbackMs, needsQuotaQuery: q.at == null || q.fuzzy,
      source: q.at ? (q.fuzzy ? 'text-clock' : 'reset-time') : 'fallback', note };
  }

  // 5. 凭据被拒 —— 记账后原样透传。**不换号、不封禁**（换号也救不了这个请求：
  //    账号是随机选的，客户端需要知道到底是哪个 key 出了什么事）
  if (status === 401 || status === 403) {
    return { ...base, action: 'record', retryable: false, source: 'auth',
      note: `凭据被拒（HTTP ${status}）：原样透传，不换号也不封禁` };
  }

  // 6. 上游自身故障 —— 同样只记账不换号：5xx 通常是上游整体故障，
  //    把请求换个号重打一遍既救不了它，还会让同一个请求在故障期间放大上游压力
  if (status >= 500 && status <= 599) {
    return { ...base, action: 'record', retryable: false,
      note: `上游 ${status}：原样透传，只计入该账号的失败数` };
  }

  // 7. 其他 4xx / 422 —— 请求本身的问题，原样透传
  return { ...base, action: 'transparent', retryable: false,
    note: '请求本身的错误，直接透传，不冷却不换号' };
}
