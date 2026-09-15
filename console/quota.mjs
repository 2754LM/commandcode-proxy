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
import { toMs, find } from './classify.mjs';

const TIMEOUT_MS = 8000;

// ── 字段容错：同一个语义在不同版本/端点里可能叫不同名字 ──────────
const norm = (s) => String(s).toLowerCase().replace(/[_\-\s]/g, '');

const numOf = (v) => {
  const n = typeof v === 'number' ? v : Number.parseFloat(String(v ?? ''));
  return Number.isFinite(n) ? n : null;
};
const boolOf = (v) => (typeof v === 'boolean' ? v : v === 'true' ? true : v === 'false' ? false : null);

const WINDOW_NAMES = {
  fiveHour: ['fiveHour', 'fiveHours', 'fiveHourLimit', 'five_hour', 'hour5', 'fivelimit', 'hourly'],
  weekly: ['weekly', 'week', 'weeklyLimit', 'weekLimit', 'sevenDay', 'sevenDayLimit'],
  monthly: ['monthly', 'monthlyLimit', 'monthWindow', 'month'],
};

/**
 * 把 /alpha/billing/credits 的响应归一化成一行 quota。
 * @param raw     /alpha/billing/credits 的 body
 * @param sources 其它三个端点的 body（都可缺）：{ subscription, whoami, usage }
 *                月总额的分母与本期已用要从它们里取，见下方注释。
 */
export function normalizeCredits(raw, sources = {}) {
  const { subscription: planRaw = null, whoami: orgRaw = null, usage: usageRaw = null } = sources || {};
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

  const monthlyLeft = numOf(find(credits, ['monthlyCredits', 'monthly', 'monthlyLeft', 'remaining']));
  const purchased = numOf(find(credits, ['purchasedCredits', 'purchased'])) ?? 0;
  const free = numOf(find(credits, ['freeCredits', 'free'])) ?? 0;

  /*
   * 月额度是唯一"只有余额、没有上限"的窗口：上游 credits 里只有 monthlyCredits（余额）。
   * 分母按可靠性依次取三个来源，拿不到就不给分母（cap = null，UI 不画进度条），
   * 绝不用别的窗口的 cap 硬凑：
   *   ① windowLimits.monthly.cap —— 服务端自己算的（若存在，最权威）
   *   ② 余额 + 本期已用           —— 实测可推导：individual-go 的 8.2468545324（余额）
   *                                  + 1.7483896696（usage.totalMonthlyCredits）= 10.0
   *                                  这正是 §14.2 公式里 plan.monthlyCredits 的取值，
   *                                  而我们没有那张套餐目录表，只能这样等价推算。
   *   ③ plan.monthlyCredits       —— §14.2 的原文口径（实测 subscriptions 不带该字段，
   *                                  保留是为了兼容将来/组织账号）
   */
  const monthWindow = pickWindow(WINDOW_NAMES.monthly);
  let monthCap = monthWindow.cap;
  let monthCapSource = monthCap != null ? 'window' : null;
  const usedThisPeriod = numOf(find(usageRaw, ['totalMonthlyCredits', 'monthlyCredits', 'totalCredits']));
  if (monthCap == null && usedThisPeriod != null && monthlyLeft != null) {
    monthCap = monthlyLeft + usedThisPeriod + purchased + free;
    monthCapSource = 'balance+usage';
  }
  if (monthCap == null) {
    const planMonthly = numOf(find(planRaw, ['monthlyCredits', 'monthlyAllowance', 'monthlyLimit']))
      ?? numOf(find(orgRaw, ['monthlyCredits', 'monthlyAllowance', 'monthlyLimit']));
    if (planMonthly != null) { monthCap = Math.max(planMonthly, monthlyLeft ?? 0) + purchased + free; monthCapSource = 'plan'; }
  }
  const monthly = {
    // 本期已用：usage 给的就是权威值，否则用 总额 − 余额 反推
    used: usedThisPeriod ?? (monthCap != null && monthlyLeft != null ? Math.max(0, monthCap - monthlyLeft) : null),
    cap: monthCap,
    capSource: monthCapSource,
    reset: monthWindow.reset,          // 为空时由调用方用账期 currentPeriodEnd 补
    exceeded: monthWindow.exceeded,
  };

  return {
    planId: String(find(raw, ['planId', 'plan', 'planName', 'tier']) ?? ''),
    monthlyLeft,
    purchased,
    free,
    fiveHour: pickWindow(WINDOW_NAMES.fiveHour),
    weekly: pickWindow(WINDOW_NAMES.weekly),
    monthly,
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

  /**
   * 拉一次四个端点。顺序按 §14.2 的依赖关系：
   *   whoami(`?limits=1`) → 拿 orgId 与 orgLimits
   *   credits(`?orgId=`)  ← 必需，失败即整次 refresh 失败
   *   subscriptions(`?orgId=`) → 账期 currentPeriodStart/End（ISO 字符串）
   *   usage(`?orgId=&since=<ISO>`) → 本期已用（since 必须 ISO，传 0 会被上游拒）
   * 除 credits 外全部 best-effort：少一个来源只会让月额度分母取不到，不影响冷却。
   */
  async function fetchAll(apiKey) {
    const out = { whoami: null, credits: null, subscription: null, usage: null, orgId: '' };
    out.whoami = await getJson('/alpha/whoami?limits=1', apiKey).catch(() => null);
    out.orgId = String(find(out.whoami, ['orgId', 'organizationId', 'org']) ?? '');
    const org = out.orgId ? `?orgId=${encodeURIComponent(out.orgId)}` : '';
    out.credits = await getJson(`/alpha/billing/credits${org}`, apiKey);
    out.subscription = await getJson(`/alpha/billing/subscriptions${org}`, apiKey).catch(() => null);
    const since = find(out.subscription, ['currentPeriodStart', 'periodStart', 'startsAt']);
    const sinceIso = typeof since === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(since) ? since : null;
    out.usage = await getJson(`/alpha/usage/summary${org}${sinceIso ? `${org ? '&' : '?'}since=${encodeURIComponent(sinceIso)}` : ''}`, apiKey)
      .catch(() => null);
    return out;
  }

  /** 查询并写入 quota 表 */
  async function refresh(keyHash, apiKey) {
    const t = now();
    try {
      const all = await fetchAll(apiKey);
      const n = normalizeCredits(all.credits, { subscription: all.subscription, whoami: all.whoami, usage: all.usage });
      const periodEnd = toMs(find(all.subscription, ['currentPeriodEnd', 'periodEnd', 'renewsAt', 'expiresAt'])) ?? 0;
      // 月窗口没有自己的 resetAt 时，账期结束就是它的重置时刻
      const monthReset = n.monthly.reset ?? periodEnd;
      db.prepare(`INSERT INTO quota (key_hash, plan_id, monthly_left, month_cap, period_end, five_used, five_cap, five_reset,
                    week_used, week_cap, week_reset, exceeded, checked_at, last_error)
                  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'')
                  ON CONFLICT(key_hash) DO UPDATE SET plan_id=excluded.plan_id, monthly_left=excluded.monthly_left,
                    month_cap=excluded.month_cap, period_end=excluded.period_end,
                    five_used=excluded.five_used, five_cap=excluded.five_cap,
                    five_reset=excluded.five_reset, week_used=excluded.week_used, week_cap=excluded.week_cap,
                    week_reset=excluded.week_reset, exceeded=excluded.exceeded, checked_at=excluded.checked_at,
                    last_error=''`)
        .run(keyHash, n.planId, n.monthlyLeft, n.monthly.cap, periodEnd,
          n.fiveHour.used, n.fiveHour.cap, n.fiveHour.reset ?? 0,
          n.weekly.used, n.weekly.cap, n.weekly.reset ?? 0, n.exceeded, t);
      return { ok: true, ...n, monthly: { ...n.monthly, reset: monthReset }, periodEnd, checkedAt: t };
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
      monthly: {
        // 分母为 null 就不要 used（宁可 UI 只显示余额，也不编一个数）
        used: r.month_cap == null || r.monthly_left == null ? null : Math.max(0, r.month_cap - r.monthly_left),
        cap: r.month_cap, left: r.monthly_left, reset: r.period_end, exceeded: r.exceeded === 'monthly',
      },
      exceeded: r.exceeded, checkedAt: r.checked_at, stale: !r.checked_at || t - r.checked_at > 10 * 60_000,
      lastError: r.last_error || '',
    };
  }

  /**
   * 第 2 级回退：上游 body 没给（可信的）重置时间时，查额度接口拿权威答案。
   * @param reason     'monthly' | 'rate_limit' | 'fallback'
   * @param windowHint 'fiveHour' | 'weekly' | '' —— body 里认出来的窗口。
   *   有这个提示时必须**优先取该窗口自己的重置时间**：否则"取所有窗口里最晚的"
   *   会把一次 5 小时限流冷却成周窗口的 6 天。
   * @returns {{at:number|null, source:string, detail?:string}}
   */
  async function cooldownUntilFor(keyHash, apiKey, reason = 'fallback', windowHint = '') {
    let snap = get(keyHash);
    if (!snap || snap.stale) await refresh(keyHash, apiKey);
    snap = get(keyHash);
    if (!snap) return { at: null, source: 'none' };
    if (reason === 'monthly') {
      const at = snap.periodEnd > now() ? snap.periodEnd : null;
      return { at, source: at ? 'quota:periodEnd' : 'none', detail: snap.planId };
    }
    // body 说清了是哪个窗口 → 直接取该窗口的重置时间
    if (windowHint === 'fiveHour' || windowHint === 'weekly') {
      const w = windowHint === 'fiveHour' ? snap.fiveHour : snap.weekly;
      if (w && w.reset > now()) return { at: w.reset, source: `quota:${windowHint}` };
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

  /**
   * 该账号此刻是否仍然"没额度"—— 冷却到点复核用。
   *
   * 为什么需要它：冷却到点只是"我们猜的时间到了"，不代表上游真的放行。常见两种猜法：
   *   - 上游没给重置时间 → 保守兜 60s
   *   - 月度 → 账期结束（那是账期边界，不一定是额度恢复点）
   * 到点先复核一次，真没恢复就按服务端给的时间续上，避免"放行 → 又撞限流"来回抖。
   *
   * @returns {{until:number, window:string}|null} 仍然耗尽则给出权威恢复时刻，否则 null
   */
  function stillExhausted(keyHash) {
    const s = get(keyHash);
    if (!s) return null;
    const t = now();
    const ok = (w, name) => (w && w.reset > t ? { until: w.reset, window: name } : null);
    if (s.exceeded === 'fiveHour') return ok(s.fiveHour, 'fiveHour');
    if (s.exceeded === 'weekly') return ok(s.weekly, 'weekly');
    if (s.exceeded === 'monthly') return s.periodEnd > t ? { until: s.periodEnd, window: 'monthly' } : null;
    // exceeded 没标（上游有时不打）：用用量兜底判断，但只在"确实顶到上限"时才算耗尽
    for (const [w, name] of [[s.fiveHour, 'fiveHour'], [s.weekly, 'weekly']]) {
      if (w && w.cap != null && w.used != null && w.used >= w.cap) {
        const f = ok(w, name);
        if (f) return f;
      }
    }
    return null;
  }

  return { refresh, get, fetchAll, cooldownUntilFor, stillExhausted, normalizeCredits };
}
