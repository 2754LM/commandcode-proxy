/**
 * console/access.mjs — 客户端准入（P5）
 *
 * 依据 PLAN §5.4：
 *   access.mode = open       任何携带有效 key 的客户端可用（现状语义）
 *   access.mode = whitelist  仅白名单内的客户端 key 可用
 *
 * 判定发生在**消耗任何上游账号之前**（§5.4 明确要求），所以被拒的请求
 * 不会冷却/封禁任何账号，也不会给上游留痕。
 *
 * 说明：客户端 key 与上游账号 key 是两套东西。准入判定用客户端 key 的 sha256，
 * 与 accounts 表互不影响。
 */
import { keyHash, keyHint } from './crypto.mjs';
import { settings, audit } from './db.mjs';

const MODE_KEY = 'access_mode';
export const MODES = ['open', 'whitelist'];

export function createAccess({ db, now = Date.now }) {
  const mode = () => {
    const m = settings.get(db, MODE_KEY, 'open');
    return MODES.includes(m) ? m : 'open';
  };
  const setMode = (m) => {
    if (!MODES.includes(m)) throw new Error(`access mode 只能是 ${MODES.join(' / ')}`);
    settings.set(db, MODE_KEY, m);
    audit(db, { at: now(), action: 'access.mode', outcome: m });
    return m;
  };

  function add(key, label = '') {
    const h = keyHash(String(key));
    const t = now();
    db.prepare(`INSERT INTO access (key_hash, label, enabled, created_at) VALUES (?, ?, 1, ?)
                ON CONFLICT(key_hash) DO UPDATE SET label = excluded.label`)
      .run(h, label, t);
    audit(db, { at: t, action: 'access.add', target: keyHint(key), outcome: 'ok' });
    return { keyHash: h, hint: keyHint(key) };
  }
  function remove(hash) {
    const changed = db.prepare('DELETE FROM access WHERE key_hash = ?').run(hash).changes;
    if (changed) audit(db, { at: now(), action: 'access.remove', target: String(hash).slice(0, 12), outcome: 'ok' });
    return !!changed;
  }
  function setEnabled(hash, on) {
    const changed = db.prepare('UPDATE access SET enabled = ? WHERE key_hash = ?').run(on ? 1 : 0, hash).changes;
    if (changed) audit(db, { at: now(), action: 'access.toggle', target: String(hash).slice(0, 12), outcome: on ? 'on' : 'off' });
    return !!changed;
  }
  function list() {
    return db.prepare('SELECT key_hash AS keyHash, label, enabled, created_at AS createdAt FROM access ORDER BY created_at DESC').all();
  }

  /**
   * 是否放行。
   * @returns {{allowed:boolean, reason?:string}} reason = 'missing_key' | 'not_whitelisted'
   */
  function allows(clientKey) {
    if (!clientKey) return { allowed: false, reason: 'missing_key' };
    if (mode() === 'open') return { allowed: true };
    const row = db.prepare('SELECT enabled FROM access WHERE key_hash = ?').get(keyHash(String(clientKey)));
    if (!row) return { allowed: false, reason: 'not_whitelisted' };
    if (row.enabled !== 1) return { allowed: false, reason: 'not_whitelisted' };
    return { allowed: true };
  }

  return { mode, setMode, add, remove, setEnabled, list, allows };
}
