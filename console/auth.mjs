/**
 * console/auth.mjs — 管理口令与会话（P5）
 *
 * 用途：console 的**写口**（增删账号、解封、改配置）必须验人。
 * 依据 HANDOFF-webui 的 Q2：读口可以只靠回环，一旦出现写口就必须有口令。
 *
 * 设计要点：
 *   - 口令来自环境变量 `CC_ADMIN_PASSWORD`（≥8 位），**不落盘**；同一个口令也用于派生凭证 KEK
 *   - 校验走**常数时间**比较（比较 sha256，避免长度/前缀早退泄漏）
 *   - 会话 token 只存**内存**（重启即失效，符合"读口+写口都在本机"的定位）；
 *     落盘只存 token 的 sha256，即使内存 dump 也拿不到原 token
 *   - **防猜解**：连续失败达阈值即锁定一段时间（本地端口也不放行暴力枚举）
 *
 * 零依赖：只用 node:crypto。
 */
import { keyHash, safeEqual, newToken } from './crypto.mjs';

const DEFAULT_TTL_MS = 12 * 3600_000;      // 会话 12h
const DEFAULT_MAX_FAILS = 5;               // 连续失败 5 次
const DEFAULT_LOCKOUT_MS = 60_000;         // 锁定 60s

export function createAuth({ passphrase, ttlMs = DEFAULT_TTL_MS, maxFails = DEFAULT_MAX_FAILS,
  lockoutMs = DEFAULT_LOCKOUT_MS, now = Date.now, token = newToken } = {}) {
  if (typeof passphrase !== 'string' || passphrase.length < 8) {
    throw new Error('CC_ADMIN_PASSWORD 至少 8 位');
  }
  const expected = keyHash(passphrase);          // sha256(口令)
  const sessions = new Map();                    // tokenHash -> expiresAt
  let fails = 0;
  let lockedUntil = 0;

  const prune = () => { const t = now(); for (const [k, exp] of sessions) if (exp <= t) sessions.delete(k); };

  /** 口令是否正确。连续失败会锁定（返回 {ok:false, locked:true, retryAfterMs}） */
  function verify(password) {
    const t = now();
    if (t < lockedUntil) {
      return { ok: false, locked: true, retryAfterMs: lockedUntil - t };
    }
    const given = keyHash(String(password ?? ''));
    // 常数时间比较（两者都是 64 字节 hex）
    const ok = safeEqual(given, expected);
    if (!ok) {
      fails++;
      if (fails >= maxFails) {
        lockedUntil = t + lockoutMs;
        fails = 0;
        return { ok: false, locked: true, retryAfterMs: lockoutMs };
      }
      return { ok: false, locked: false, remaining: maxFails - fails };
    }
    fails = 0;
    return { ok: true };
  }

  /** 口令通过后签发会话；返回 {token, expiresAt}（token 只在此处出现一次） */
  function issue() {
    const raw = token();
    const expiresAt = now() + ttlMs;
    sessions.set(keyHash(raw), expiresAt);
    prune();
    return { token: raw, expiresAt };
  }

  /** 校验会话 token（header 或 cookie 都走这里） */
  function check(raw) {
    if (!raw) return false;
    const exp = sessions.get(keyHash(String(raw)));
    if (!exp) return false;
    if (exp <= now()) { sessions.delete(keyHash(String(raw))); return false; }
    return true;
  }

  function revoke(raw) {
    if (!raw) return false;
    return sessions.delete(keyHash(String(raw)));
  }

  /** 从请求里取 token：优先 `x-admin-token`，其次 cookie `cc_admin` */
  function tokenOf(req) {
    const h = req.headers?.['x-admin-token'];
    if (h) return String(h);
    const cookie = req.headers?.cookie || '';
    const m = cookie.match(/(?:^|;\s*)cc_admin=([^;]+)/);
    return m ? decodeURIComponent(m[1]) : null;
  }

  const status = () => ({ sessions: sessions.size, locked: now() < lockedUntil,
    retryAfterMs: Math.max(0, lockedUntil - now()) });

  return { verify, issue, check, revoke, tokenOf, status, ttlMs };
}
