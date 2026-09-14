/**
 * console/plugin.mjs — 把账号池 + 错误分类器装进核心那两个钩子（P2b/P3）
 *
 * 这一层是**插件**：核心完全不知道它的存在，它也只通过 hooks 与核心交互。
 * 装配后整条链变成：
 *
 *   客户端请求 → 核心 resolveKey 钩子 → 池选账号（sticky / priority / weight）
 *              → 核心 forwardVia 钩子 → 发上游 → 非 2xx 就分类
 *                  ├─ transparent（普通 400/422）→ 原样返回，不换号
 *                  ├─ ban（401/403）            → 封禁该账号 → 换号重试
 *                  ├─ cooldown（额度/限流）      → 冷却该账号 → 换号重试
 *                  └─ count（5xx）              → fail_streak++ → 换号重试
 *              换号后仍失败 / 池空 → 返回一个「像上游响应」的 503，让核心既有的错误映射
 *              把说明原样带给客户端（带 code=NO_AVAILABLE_ACCOUNT）
 *
 * 策略：
 *   - 池里**一个账号都没有** → 不接管（resolveKey 返回 undefined），单 key 模式照旧可用
 *   - 池里有账号但全部不可用 → 503，**绝不悄悄退回客户端 key**（否则用户以为在走池）
 */
import { classify } from './classify.mjs';

const DEFAULT_RETRY_MAX = 2;          // PLAN Q2 建议值：最多换 2 次
const NO_ACCOUNT_CODE = 'NO_AVAILABLE_ACCOUNT';

export function createPoolPlugin({ pool, hooks, access = null, quota = null, log = () => {}, retryMax = null, now = Date.now }) {
  const picked = new WeakMap();       // req -> { keyHash, hint, label } | { none } | { deny }：钩子之间传请求级状态
  const configuredRetryMax = retryMax;

  const maxRetries = () => configuredRetryMax ?? 2;
  // 每次选号都打一行会刷屏（核心的 log 不过滤级别），所以单独给个开关
  const vlog = (...a) => { if (process.env.CC_POOL_VERBOSE === '1') log(...a); };

  /** 客户端的 key（用于准入与 sticky 粒度） */
  const clientKeyOf = (req, fallback) => fallback || req.headers?.['authorization'] || 'anon';

  async function resolveKey(req, fallback) {
    const clientKey = clientKeyOf(req, fallback);

    // §5.4：准入判定必须在**消耗任何上游账号之前** —— 被拒的请求不该冷却/封禁任何账号，
    // 也不该给上游留痕。所以这里直接拒绝，连 pick 都不做。
    if (access) {
      const verdict = access.allows(fallback);
      if (!verdict.allowed) {
        picked.set(req, { deny: verdict });
        return fallback || 'none';
      }
    }

    const avail = pool.availability();
    if (avail.total === 0) return undefined;                  // 空池：不接管，单 key 模式照旧
    const got = pool.pick({ stickyKey: clientKey });          // PLAN Q4 待定：暂用 client key 粒度
    if (!got.ok) {
      picked.set(req, { none: got });                         // 交给 forward 出 503
      return fallback || 'none';
    }
    picked.set(req, { keyHash: got.keyHash, hint: got.hint, label: got.label });
    vlog('info', 'pool picked account', { kind: 'resolve', hint: got.hint, sticky: got.sticky });
    return got.key;
  }

  /** 准入被拒 → 401，且不碰任何账号（复刻核心既有的 401 文案形状） */
  function denyResponse(deny) {
    const message = deny.reason === 'missing_key'
      ? 'Missing API key. Send in Authorization: Bearer <key> or x-api-key header'
      : 'This API key is not allowed by this proxy (access mode = whitelist).';
    return new Response(JSON.stringify({ error: { code: 'ACCESS_DENIED', message } }),
      { status: 401, headers: { 'content-type': 'application/json' } });
  }

  function unavailableResponse(got) {
    const headers = { 'content-type': 'application/json' };
    if (got.retryAfterMs > 0) headers['retry-after'] = String(Math.ceil(got.retryAfterMs / 1000));
    // 伪装成「上游给的错误体」交给核心：既有的 mapCcError 会保留 message 并透出 code
    return new Response(JSON.stringify({
      success: false,
      error: { code: NO_ACCOUNT_CODE, message: got.summary },
    }), { status: 503, headers });
  }

  /**
   * 只有 chat/messages/responses 三条路径回传的是 fetch 的 Response。
   * `/v1/models` 走的是核心的 fetchModels()，它返回**已经解析好的模型数组**，
   * 不是 Response（内部自己处理错误、失败时退回内置列表）。
   * 所以这里必须先判类型，否则会在非 Response 上访问 .ok/.clone 而炸成 500。
   */
  const isResponse = (r) => !!r && typeof r === 'object'
    && typeof r.ok === 'boolean' && typeof r.clone === 'function';

  async function forward(ctx, firstKey, fn) {
    const state = picked.get(ctx.req);
    // 没有本请求的选号记录 = resolveKey 没接管（例如池是空的）→ 本钩子也必须让路，
    // 用客户端自带的 key 原样转发，保持单 key 模式可用。
    if (!state) return fn(firstKey);
    if (state.deny) return denyResponse(state.deny);
    if (state.none) return unavailableResponse(state.none);

    let current = state;                    // { keyHash, hint, label }
    let key = firstKey;
    let attempt = 0;
    let last = null;

    for (;;) {
      let res;
      try {
        res = await fn(key);
      } catch (e) {
        // 网络层错误：算这个账号一次失败，换号再试
        pool.noteFailure(current.keyHash, { reason: `转发异常：${e.message}` });
        log('warn', 'forward threw, will try next account', { hint: current.hint, error: e.message });
        if (attempt >= maxRetries()) throw e;
        const next = pool.pick({ stickyKey: null });
        if (!next.ok) return unavailableResponse(next);
        ({ key } = next); current = next; attempt++; continue;
      }

      // 非 Response（/v1/models 的数组）：核心自己已经处理过错误并可能退回内置列表，
      // 这里当成功原样交回，不参与分类/换号。上游真故障由上面 catch 的路径兜。
      if (!isResponse(res)) {
        pool.noteSuccess(current.keyHash);
        if (ctx.kind === 'models' && process.env.CC_POOL_VERBOSE === '1') vlog('info', 'models list served via account', { hint: current.hint });
        return res;
      }

      if (res.ok) {
        pool.noteSuccess(current.keyHash);
        return res;                          // 成功：原样交给核心（含流式）
      }

      last = res;
      const body = await res.clone().text().catch(() => '');   // 只读副本，不影响交给调用方的流
      const cls = classify(res.status, body, { now: now() });
      log('info', 'upstream error classified', {
        hint: current.hint, status: res.status, action: cls.action, reason: cls.reason,
        code: cls.code, source: cls.source, retryable: cls.retryable,
        needsQuotaQuery: cls.needsQuotaQuery,
      });

      if (cls.action === 'ban') {
        pool.noteFailure(current.keyHash, { hardBan: true, reason: cls.note || `HTTP ${res.status}` });
      } else if (cls.action === 'cooldown') {
        let until = cls.cooldownUntil;
        let source = cls.source;
        // §4.4 第 2 级回退：上游没给重置时间 → 查额度接口拿权威答案（best-effort，失败就退回兜底）
        if (cls.needsQuotaQuery && quota) {
          try {
            const q = await quota.cooldownUntilFor(current.keyHash, key, cls.reason);
            if (q.at) { until = q.at; source = q.source; }
          } catch (e) {
            log('warn', 'quota fallback failed', { hint: current.hint, error: e.message });
          }
        }
        const finalUntil = until ?? (now() + 60_000);
        pool.markCooldown(current.keyHash, { untilMs: finalUntil, reason: cls.reason || 'fallback' });
        pool.noteFailure(current.keyHash, { reason: `冷却：${cls.reason}` });
        log('info', 'account cooled down', { hint: current.hint, reason: cls.reason, source,
          until: new Date(finalUntil).toISOString() });
      } else if (cls.action === 'count') {
        const r = pool.noteFailure(current.keyHash, { reason: cls.note || `HTTP ${res.status}` });
        if (r.banned) log('warn', 'account soft-banned by fail_streak', { hint: current.hint, streak: r.failStreak });
      } else {
        return res;                          // transparent：请求本身的问题，原样透传
      }

      if (!cls.retryable || attempt >= maxRetries()) return last;
      const next = pool.pick({ stickyKey: null });
      if (!next.ok) return attempt === 0 ? unavailableResponse(next) : last;
      log('info', 'switching account and retrying', { from: current.hint, to: next.hint, attempt: attempt + 1 });
      current = next; key = next.key; attempt++;
    }
  }

  function install() {
    hooks.resolveKey = resolveKey;
    hooks.forward = forward;
    return api;
  }
  function uninstall() {
    if (hooks.resolveKey === resolveKey) hooks.resolveKey = null;
    if (hooks.forward === forward) hooks.forward = null;
  }

  const api = { install, uninstall, resolveKey, forward, maxRetries: () => maxRetries() };
  return api;
}

export { NO_ACCOUNT_CODE, DEFAULT_RETRY_MAX };
