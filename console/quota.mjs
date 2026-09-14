/**
 * console/quota.mjs — 额度查询与缓存（P4）
 *
 * 依据 PLAN §5.3（四个上游端点）与 D11（借鉴 commandcode-usage 的字段容错，但**不学**它的明文存储/无鉴权）。
 *
 * 四个端点（GET + `Authorization: Bearer <key>`）：
 *   /alpha/whoami                            账户身份、orgId
 *   /alpha/billing/credits                   余额 + windowLimits.fiveHour/.weekly（used/cap/exceeded/resetAt）
 *   /alpha/billing/subscriptions?orgId=      套餐、账期 currentPeriodEnd
 *   /alpha/usage/summary                     累计统计
 *
 * 关键语义（§5.3 实测样本）：`windowLimits.limited:true` 只表示"存在限制"，
 * **`exceeded` 才是权威的「哪个窗口被拦」** —— §4.4 第 2 级回退就靠它。
 *
 * 一切 best-effort：查不到就返回 null 并把原因写进 quota.last_error，绝不抛给转发路径。
 */
import { toMs } from './classify.mjs';

const TIMEOUT_MS = 8000;

// ── 字段容错：同一个语义在不同版本/端点里可能叫不同名字 ──────────
const norm = (s) => String(s).toLowerCase().replace(/[_\-\s]/g, '');

/** 在嵌套对象里按「归一化后的键名」找一个值（先本层，再往下钻） */
export function find(node, names, depth = 0) {
  if (!node || typeof node !== 'object' || depth > 6) return undefined;
  const want = new Set(names.map(norm));
  for (const [k, v] of Object.entries(node)) if (want.has(norm(k))) return v;
  for (const v of Object.values(node)) {
    const r = find(v, names, depth + 1);
    if (r !== undefined) return r;
  }
  return undefined;
}

const numOf = (v) => {
  const n = typeof v === 'number' ? v : Number.parseFloat(String(v ?? ''));
  return Number.isFinite(n) ? n : null;
};
const boolOf = (v) => (typeof v === 'boolean' ? v : v === 'true' ? true : v === 'false' ? false : null);

const WINDOW_NAMES = {
  fiveHour: ['fiveHour', 'fiveHours', 'fiveHourLimit', 'five_hour', 'hour5', 'fivelimit', 'hourly'],
  weekly: ['weekly', 'week', 'weeklyLimit', 'weekLimit', 'sevenDay', 'sevenDayLimit'],
};

/** 把 /alpha/billing/credits 的响应归一化成一行 quota */
export function normalizeCredits(raw) {
  const credits = find(raw, ['credits']) || {};
  const wl = find(raw, ['windowLimits', 'windows', 'limits']) || {};
  const pickWindow = (names) => {
    const w = find(wl, names) || find(raw, names) || {};
    return {
      used: numOf(find(w, ['used', 'usage', 'consumed', 'current'])),
      cap: numOf(find(w, ['cap', 'limit', 'quota', 'max', 'total'])),
      reset: toMs(find(w, ['resetAt', 'resetsAt', 'resetTime', 'reset', 'recoverAt', 'recoverTime'])),
      exceeded: boolOf(find(w, ['exceeded', 'isExceeded', 'blocked', 'hit'])),
    };
  };
  // exceeded 可能落在 windowLimits 里（§5.3 真实样本），也可能落在顶层（exceeded_window），两处都找
  const EXCEEDED_NAMES = ['exceeded', 'exceededWindow', 'exceededLimit', 'blockedWindow', 'blocked'];
  const exceededRaw = find(wl, EXCEEDED_NAMES) ?? find(raw, EXCEEDED_NAMES);
  let exceeded = '';
  if (typeof exceededRaw === 'string') {
    const n = norm(exceededRaw);
    if (n.includes('five') || n.includes('hour')) exceeded = 'fiveHour';
    else if (n.includes('week')) exceeded = 'weekly';
    else if (n.includes('month') || n.includes('period') || n.includes('credit')) exceeded = 'monthly';
    else exceeded = exceededRaw;
  }
  return {
    planId: String(find(raw, ['planId', 'plan', 'planName', 'tier']) ?? ''),
    monthlyLeft: numOf(find(credits, ['monthlyCredits', 'monthly', 'monthlyLeft', 'remaining'])),
    purchased: numOf(find(credits, ['purchasedCredits', 'purchased'])),
    free: numOf(find(credits, ['freeCredits', 'free'])),
    fiveHour: pickWindow(WINDOW_NAMES.fiveHour),
    weekly: pickWindow(WINDOW_NAMES.weekly),
    exceeded,
  };
}

export function createQuota({ db, apiBase, now = Date.now, fetchImpl = fetch }) {
  const authHeaders = (apiKey) => ({ Authorization: `Bearer ${apiKey}`, Accept: 'application/json' });

  async function getJson(path, apiKey) {
    const res = await fetchImpl(`${apiBase}${path}`, {
      headers: authHeaders(apiKey), signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (!res.ok) throw new Error(`GET ${path} → ${res.status}`);
    return res.json();
  }

  /** 拉一次四个端点（后面两个失败不致命） */
  async function fetchAll(apiKey) {
    const out = { whoami: null, credits: null, subscription: null, usage: null, orgId: '' };
    out.credits = await getJson('/alpha/billing/credits', apiKey);
    out.whoami = await getJson('/alpha/whoami', apiKey).catch(() => null);
    out.orgId = String(find(out.whoami, ['orgId', 'organizationId', 'org']) ?? '');
    if (out.orgId) {
      out.subscription = await getJson(`/alpha/billing/subscriptions?orgId=${encodeURIComponent(out.orgId)}`, apiKey).catch(() => null);
    } else {
      out.subscription = await getJson('/alpha/billing/subscriptions', apiKey).catch(() => null);
    }
    out.usage = await getJson('/alpha/usage/summary', apiKey).catch(() => null);
    return out;
  }

  /** 查询并写入 quota 表 */
  async function refresh(keyHash, apiKey) {
    const t = now();
    try {
      const all = await fetchAll(apiKey);
      const n = normalizeCredits(all.credits);
      const periodEnd = toMs(find(all.subscription, ['currentPeriodEnd', 'periodEnd', 'renewsAt', 'expiresAt'])) ?? 0;
      db.prepare(`INSERT INTO quota (key_hash, plan_id, monthly_left, period_end, five_used, five_cap, five_reset,
                    week_used, week_cap, week_reset, exceeded, checked_at, last_error)
                  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'')
                  ON CONFLICT(key_hash) DO UPDATE SET plan_id=excluded.plan_id, monthly_left=excluded.monthly_left,
                    period_end=excluded.period_end, five_used=excluded.five_used, five_cap=excluded.five_cap,
                    five_reset=excluded.five_reset, week_used=excluded.week_used, week_cap=excluded.week_cap,
                    week_reset=excluded.week_reset, exceeded=excluded.exceeded, checked_at=excluded.checked_at,
                    last_error=''`)
        .run(keyHash, n.planId, n.monthlyLeft, periodEnd, n.fiveHour.used, n.fiveHour.cap, n.fiveHour.reset ?? 0,
          n.weekly.used, n.weekly.cap, n.weekly.reset ?? 0, n.exceeded, t);
      return { ok: true, ...n, periodEnd, checkedAt: t };
    } catch (e) {
      // 失败只记原因，不清空旧值（旧值还有参考价值）
      db.prepare(`INSERT INTO quota (key_hash, checked_at, last_error) VALUES (?,?,?)
                  ON CONFLICT(key_hash) DO UPDATE SET checked_at=excluded.checked_at, last_error=excluded.last_error`)
        .run(keyHash, t, String(e.message).slice(0, 200));
      return { ok: false, error: e.message, checkedAt: t };
    }
  }

  function get(keyHash) {
    const r = db.prepare('SELECT * FROM quota WHERE key_hash = ?').get(keyHash);
    if (!r) return null;
    const t = now();
    return {
      planId: r.plan_id, monthlyLeft: r.monthly_left, periodEnd: r.period_end,
      fiveHour: { used: r.five_used, cap: r.five_cap, reset: r.five_reset, exceeded: r.exceeded === 'fiveHour' },
      weekly: { used: r.week_used, cap: r.week_cap, reset: r.week_reset, exceeded: r.exceeded === 'weekly' },
      exceeded: r.exceeded, checkedAt: r.checked_at, stale: !r.checked_at || t - r.checked_at > 10 * 60_000,
      lastError: r.last_error || '',
    };
  }

  /**
   * §4.4 第 2 级回退：上游 body 没给重置时间时，查额度接口拿权威答案。
   * @returns {{at:number|null, source:string, detail?:string}}
   */
  async function cooldownUntilFor(keyHash, apiKey, reason = 'fallback') {
    let snap = get(keyHash);
    if (!snap || snap.stale) await refresh(keyHash, apiKey);
    snap = get(keyHash);
    if (!snap) return { at: null, source: 'none' };
    if (reason === 'monthly') {
      const at = snap.periodEnd > now() ? snap.periodEnd : null;
      return { at, source: at ? 'quota:periodEnd' : 'none', detail: snap.planId };
    }
    // 限流：看服务端权威标记的 exceeded 指向哪个窗口
    const which = snap.exceeded;
    if (which === 'fiveHour' && snap.fiveHour.reset) return { at: snap.fiveHour.reset, source: 'quota:fiveHour' };
    if (which === 'weekly' && snap.weekly.reset) return { at: snap.weekly.reset, source: 'quota:weekly' };
    // exceeded 没标或认不出：取两个窗口里更晚的重置时间（安全）
    const cands = [snap.fiveHour.reset, snap.weekly.reset].filter((x) => x && x > now());
    if (cands.length) return { at: Math.max(...cands), source: 'quota:max-window' };
    return { at: null, source: 'none', detail: snap.lastError || 'no window resetAt' };
  }

  return { refresh, get, fetchAll, cooldownUntilFor, normalizeCredits };
}
