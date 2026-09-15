/**
 * console/pool.mjs — 上游账号池：选择 / 冷却 / 封禁
 *
 * 依据 DESIGN-multiaccount-v4 §4（冷却 vs 封禁）、§8（选择算法）与 PLAN 的 D1/D7/D8。
 *
 * 刻意做成**显式工厂**（`createPool({db, kek})`），不是模块级单例：
 * 池要能用 ':memory:' 库 + 固定时钟反复测选择/冷却/封禁，单例会把这些都锁死。
 * （注意区分：proxy.mjs 那边是**既成事实**的模块级单例，所以导出单例；新模块没有这个包袱。）
 *
 * 明文 key 只在 pick() 返回时解密一次，不在内存里缓存。
 */
import { encrypt, decrypt, keyHash, keyHint } from './crypto.mjs';
import { settings, audit } from './db.mjs';

const STICKY_MAX = 1000;                // sticky 绑定表上限，防无界增长（超出按插入顺序淘汰）

const COOLDOWN_REASONS = new Set(['monthly', 'rate_limit', 'fallback']);

const fmtWhen = (ms) => {
  const d = new Date(ms);
  const p = (n) => String(n).padStart(2, '0');
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
};

/**
 * @param {{db:object, kek:Buffer, now?:()=>number, random?:()=>number}} opts
 */
export function createPool({ db, kek, now = Date.now, random = Math.random }) {
  const sticky = new Map();   // stickyKey -> key_hash（内存态：重启后重新选，无信息损失）

  // ── 凭证 ────────────────────────────────────────────────
  function addAccount({ key, label = '', priority = 0, weight = 1, enabled = true }) {
    if (!key || typeof key !== 'string') throw new Error('key 必须是非空字符串');
    const hash = keyHash(key);
    const enc = encrypt(kek, key);
    const hint = keyHint(key);
    const t = now();
    const exists = db.prepare('SELECT id FROM accounts WHERE key_hash = ?').get(hash);
    if (exists) {
      db.prepare(`UPDATE accounts SET label = ?, priority = ?, weight = ?, enabled = ? WHERE key_hash = ?`)
        .run(label, priority, weight, enabled ? 1 : 0, hash);
      audit(db, { at: t, action: 'account.update', target: hint, outcome: 'ok' });
      return { keyHash: hash, hint, created: false };
    }
    db.prepare(`INSERT INTO accounts
        (key_hash, key_enc, key_iv, key_tag, key_hint, label, enabled, priority, weight, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(hash, enc.enc, enc.iv, enc.tag, hint, label, enabled ? 1 : 0, priority, weight, t);
    db.prepare('INSERT OR IGNORE INTO state (key_hash) VALUES (?)').run(hash);
    audit(db, { at: t, action: 'account.add', target: hint, outcome: 'ok' });
    return { keyHash: hash, hint, created: true };
  }

  function removeAccount(hash) {
    const row = db.prepare('SELECT key_hint FROM accounts WHERE key_hash = ?').get(hash);
    if (!row) return false;
    db.prepare('DELETE FROM accounts WHERE key_hash = ?').run(hash);
    db.prepare('DELETE FROM state WHERE key_hash = ?').run(hash);
    db.prepare('DELETE FROM quota WHERE key_hash = ?').run(hash);
    db.prepare('DELETE FROM models_cache WHERE key_hash = ?').run(hash);
    for (const [k, v] of sticky) if (v === hash) sticky.delete(k);
    audit(db, { at: now(), action: 'account.remove', target: row.key_hint, outcome: 'ok' });
    return true;
  }

  /** 只允许改这几个字段（key 本身要先删再加，避免"悄悄换 key"这种不可审计的操作） */
  function setAccount(hash, patch = {}) {
    const row = db.prepare('SELECT key_hint, label, enabled, priority, weight FROM accounts WHERE key_hash = ?').get(hash);
    if (!row) return false;
    const next = {
      label: patch.label ?? row.label,
      enabled: patch.enabled == null ? row.enabled : (patch.enabled ? 1 : 0),
      priority: patch.priority ?? row.priority,
      weight: patch.weight ?? row.weight,
    };
    db.prepare('UPDATE accounts SET label = ?, enabled = ?, priority = ?, weight = ? WHERE key_hash = ?')
      .run(next.label, next.enabled, next.priority, next.weight, hash);
    audit(db, {
      at: now(), action: 'account.update', target: row.key_hint,
      outcome: `enabled=${next.enabled} priority=${next.priority} weight=${next.weight}`,
    });
    return true;
  }

  /** 列表：绝不返回明文 key，只给 hint + 运行态 */
  function listAccounts() {
    return db.prepare(`
      SELECT a.key_hash AS keyHash, a.key_hint AS hint, a.label, a.enabled, a.priority, a.weight,
             a.created_at AS createdAt,
             COALESCE(s.cooldown_until, 0) AS cooldownUntil,
             COALESCE(s.cooldown_reason, '') AS cooldownReason,
             COALESCE(s.banned_at, 0) AS bannedAt,
             COALESCE(s.ban_reason, '') AS banReason,
             COALESCE(s.fail_streak, 0) AS failStreak,
             COALESCE(s.requests, 0) AS requests,
             COALESCE(s.successes, 0) AS successes,
             COALESCE(s.errors, 0) AS errors,
             COALESCE(s.last_used, 0) AS lastUsed
      FROM accounts a LEFT JOIN state s ON s.key_hash = a.key_hash
      ORDER BY a.priority ASC, a.id ASC
    `).all();
  }

  // ── 冷却 / 封禁（§4.5：临时 vs 永久，必须分开） ──────────
  function markCooldown(hash, { untilMs, reason = 'fallback' }) {
    const r = COOLDOWN_REASONS.has(reason) ? reason : 'fallback';
    db.prepare(`INSERT INTO state (key_hash, cooldown_until, cooldown_reason, last_used)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key_hash) DO UPDATE SET cooldown_until = excluded.cooldown_until,
                  cooldown_reason = excluded.cooldown_reason`)
      .run(hash, untilMs, r, now());
    const hint = db.prepare('SELECT key_hint FROM accounts WHERE key_hash = ?').get(hash)?.key_hint || hash.slice(0, 8);
    audit(db, { at: now(), action: 'pool.cooldown', target: hint, outcome: `${r} until ${fmtWhen(untilMs)}` });
    return { cooldownUntil: untilMs, reason: r };
  }

  function markBan(hash, reason) {
    const t = now();
    db.prepare(`INSERT INTO state (key_hash, banned_at, ban_reason, last_used)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key_hash) DO UPDATE SET banned_at = excluded.banned_at,
                  ban_reason = excluded.ban_reason`)
      .run(hash, t, String(reason || '未注明'), t);
    const hint = db.prepare('SELECT key_hint FROM accounts WHERE key_hash = ?').get(hash)?.key_hint || hash.slice(0, 8);
    audit(db, { at: t, action: 'pool.ban', target: hint, outcome: String(reason || '') });
    for (const [k, v] of sticky) if (v === hash) sticky.delete(k);
    return { bannedAt: t, banReason: reason };
  }

  /** 手动解封（§4.5：封禁只能手动解除） */
  function unban(hash) {
    const row = db.prepare('SELECT ban_reason FROM state WHERE key_hash = ?').get(hash);
    if (!row) return false;
    db.prepare(`UPDATE state SET banned_at = 0, ban_reason = '', fail_streak = 0 WHERE key_hash = ?`).run(hash);
    const hint = db.prepare('SELECT key_hint FROM accounts WHERE key_hash = ?').get(hash)?.key_hint || hash.slice(0, 8);
    audit(db, { at: now(), action: 'pool.unban', target: hint, outcome: row.ban_reason || '' });
    return true;
  }

  function clearCooldown(hash) {
    const changed = db.prepare('UPDATE state SET cooldown_until = 0, cooldown_reason = \'\' WHERE key_hash = ?').run(hash);
    if (changed.changes) {
      const hint = db.prepare('SELECT key_hint FROM accounts WHERE key_hash = ?').get(hash)?.key_hint || hash.slice(0, 8);
      audit(db, { at: now(), action: 'pool.cooldown.clear', target: hint, outcome: 'ok' });
    }
    return !!changed.changes;
  }

  // ── 计数 ────────────────────────────────────────────────
  function noteSuccess(hash) {
    db.prepare(`UPDATE state SET requests = requests + 1, successes = successes + 1,
                fail_streak = 0, last_used = ? WHERE key_hash = ?`).run(now(), hash);
  }

  /**
   * 记一次失败：**只计数**（requests / errors / fail_streak）。
   *
   * 这里原本有"连续 N 次自动软封"和 hardBan 参数，已按业主决定移除 —— 自动路径不再
   * 产生任何封禁。markBan / unban 仍作为**人工**手段保留（控制台/API 调用），但没有任何
   * 自动流程会调它们，所以 banned 计数在实践中恒为 0。
   */
  function noteFailure(hash, { reason = '' } = {}) {
    const t = now();
    db.prepare(`UPDATE state SET requests = requests + 1, errors = errors + 1,
                fail_streak = fail_streak + 1, last_used = ? WHERE key_hash = ?`).run(t, hash);
    void reason;   // 失败原因由调用方打日志，这里不落库（高频写不放大）
    const streak = db.prepare('SELECT fail_streak FROM state WHERE key_hash = ?').get(hash)?.fail_streak || 0;
    return { banned: false, failStreak: streak };
  }

  // ── 选择（§8） ──────────────────────────────────────────
  function candidates() {
    const t = now();
    return db.prepare(`
      SELECT a.key_hash AS keyHash, a.key_hint AS hint, a.label, a.priority, a.weight,
             COALESCE(s.cooldown_until, 0) AS cooldownUntil,
             COALESCE(s.banned_at, 0) AS bannedAt
      FROM accounts a LEFT JOIN state s ON s.key_hash = a.key_hash
      WHERE a.enabled = 1
        AND COALESCE(s.banned_at, 0) = 0
        AND COALESCE(s.cooldown_until, 0) <= ?
      ORDER BY a.priority ASC, a.id ASC
    `).all(t);
  }

  function availability() {
    const t = now();
    const all = db.prepare(`
      SELECT a.enabled, COALESCE(s.banned_at, 0) AS bannedAt,
             COALESCE(s.cooldown_until, 0) AS cooldownUntil, COALESCE(s.cooldown_reason, '') AS reason,
             COALESCE(s.ban_reason, '') AS banReason
      FROM accounts a LEFT JOIN state s ON s.key_hash = a.key_hash
    `).all();
    const enabled = all.filter((r) => r.enabled === 1);
    const banned = enabled.filter((r) => r.bannedAt > 0);
    const cooling = enabled.filter((r) => r.bannedAt === 0 && r.cooldownUntil > t);
    const usable = enabled.filter((r) => r.bannedAt === 0 && r.cooldownUntil <= t);
    return {
      total: all.length,
      enabled: enabled.length,
      banned: banned.length,
      cooling: cooling.length,
      usable: usable.length,
      disabled: all.length - enabled.length,
      banReasons: [...new Set(banned.map((r) => r.banReason).filter(Boolean))],
      nextRecoverMs: cooling.length ? Math.min(...cooling.map((r) => r.cooldownUntil)) - t : 0,
      nextRecoverAt: cooling.length ? Math.min(...cooling.map((r) => r.cooldownUntil)) : 0,
    };
  }

  /** 候选为空时，按 §4.5 生成"不伪装成上游错误"的说明文本 */
  function unavailable(reasonOverride) {
    const a = availability();
    let reason = reasonOverride;
    if (!reason) {
      if (a.total === 0) reason = 'no_accounts';
      else if (a.enabled === 0) reason = 'all_disabled';
      else if (a.banned > 0) reason = 'banned';        // 有封禁优先报封禁：那需要人去处理
      else reason = 'cooling';
    }
    const parts = [`总计 ${a.total} 个`];
    if (a.banned) parts.push(`已封禁 ${a.banned} 个（${a.banReasons.join('、') || '原因未注明'}，需手动解封）`);
    if (a.cooling) parts.push(`额度冷却中 ${a.cooling} 个（最早 ${fmtWhen(a.nextRecoverAt)} 恢复）`);
    if (a.disabled) parts.push(`已停用 ${a.disabled} 个`);
    return {
      ok: false,
      reason,
      counts: a,
      retryAfterMs: reason === 'cooling' ? a.nextRecoverMs : 0,
      // 注意：只有「仅冷却」才给 Retry-After。有封禁时等多久都不会自己好，
      // 给了 Retry-After 反而误导客户端去重试（§8：有 banned 就只提示解封）。
      summary: `所有账号不可用：${parts.join('，')}`,
    };
  }

  /** priority 最小层内按 weight 加权随机 */
  function weightedPick(rows) {
    const topPriority = rows[0].priority;
    const tier = rows.filter((r) => r.priority === topPriority);
    const total = tier.reduce((s, r) => s + Math.max(1, r.weight), 0);
    let n = random() * total;
    for (const r of tier) {
      n -= Math.max(1, r.weight);
      if (n < 0) return r;
    }
    return tier[tier.length - 1];
  }

  /**
   * 选一个可用账号。
   * @param {{stickyKey?:string, exclude?:Set<string>}} opts
   *   stickyKey 由调用方拼（PLAN Q4 建议 `client key + prompt_cache_key`）；
   *   exclude 是"这次请求已经试过、别再给"的 keyHash 集合。
   * @returns {{ok:true,keyHash,key,hint,label,sticky:boolean}|{ok:false,...}}
   */
  function pick({ stickyKey, exclude } = {}) {
    let rows = candidates();
    // 软失败（5xx 计数）不会把账号踢出候选，若不排除，"换号"就会原地重试同一个号。
    // 只在**还有别的号**时才排除：池里只剩它自己就照旧用它，不误报"无可用账号"。
    if (exclude && exclude.size) {
      const rest = rows.filter((r) => !exclude.has(r.keyHash));
      if (rest.length) rows = rest;
    }
    if (!rows.length) return unavailable();
    const byHash = new Map(rows.map((r) => [r.keyHash, r]));

    let chosen = null;
    let stickyHit = false;
    if (stickyKey) {
      const bound = sticky.get(stickyKey);
      if (bound && byHash.has(bound)) { chosen = byHash.get(bound); stickyHit = true; }
    }
    if (!chosen) chosen = weightedPick(rows);

    if (stickyKey && chosen) {
      sticky.delete(stickyKey);           // 重新插入以维持 LRU 顺序
      sticky.set(stickyKey, chosen.keyHash);
      if (sticky.size > STICKY_MAX) sticky.delete(sticky.keys().next().value);
    }

    const rec = db.prepare('SELECT key_enc, key_iv, key_tag FROM accounts WHERE key_hash = ?').get(chosen.keyHash);
    if (!rec) return unavailable('no_accounts');
    const key = decrypt(kek, { enc: rec.key_enc, iv: rec.key_iv, tag: rec.key_tag });
    db.prepare('UPDATE state SET last_used = ? WHERE key_hash = ?').run(now(), chosen.keyHash);
    return {
      ok: true, keyHash: chosen.keyHash, key, hint: chosen.hint, label: chosen.label,
      priority: chosen.priority, weight: chosen.weight, sticky: stickyHit,
    };
  }

  /**
   * 取出某个账号的明文 key（解密一次）。
   * 只给管理侧用：额度查询、连通性测试、以及 server 里的配额轮询。
   * **不要**在请求转发路径上调用它绕开选择算法（那样会破坏冷却/封禁语义）。
   */
  function revealKey(hash) {
    const rec = db.prepare('SELECT key_enc, key_iv, key_tag FROM accounts WHERE key_hash = ?').get(hash);
    if (!rec) return null;
    return decrypt(kek, { enc: rec.key_enc, iv: rec.key_iv, tag: rec.key_tag });
  }

  /** 诊断用：sticky 绑定表规模 */
  const stickySize = () => sticky.size;

  return {
    addAccount, removeAccount, setAccount, listAccounts, revealKey,
    markCooldown, markBan, unban, clearCooldown,
    noteSuccess, noteFailure,
    pick, availability, unavailable, stickySize,
  };
}
