/**
 * console/db.mjs — SQLite 存储层
 *
 * 依据 DESIGN-multiaccount-v4 §7。schema 逐列照抄设计，不改名：
 * 后面 P4（quota 轮询）与 P6（账号视图）都要按这套列名读写。
 *
 * 关键设计取舍（照抄设计，不要"优化"掉）：
 *   - accounts（凭证，低频写）与 state（运行态，高频写）**分表** —— 高频写不拖累凭证
 *   - 明文 key 不存在任何列里：只有 key_hash / key_enc+iv+tag / key_hint
 *   - models_cache **按账号分**（不同账号可见模型可能不同）
 *
 * 零 npm 依赖：node:sqlite 内置（Node ≥22.5；本机 24.16 实测免旗标可用）。
 */
import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'fs';
import { dirname } from 'path';

const SCHEMA = `
CREATE TABLE IF NOT EXISTS accounts (            -- 凭证（低频写）
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  key_hash TEXT NOT NULL UNIQUE,                 -- sha256(key)
  key_enc TEXT NOT NULL, key_iv TEXT NOT NULL, key_tag TEXT NOT NULL,
  key_hint TEXT NOT NULL,                        -- user_ab…cd12
  label TEXT NOT NULL DEFAULT '',
  enabled INTEGER NOT NULL DEFAULT 1,
  priority INTEGER NOT NULL DEFAULT 0,
  weight INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS state (               -- 运行态（高频写）
  key_hash TEXT PRIMARY KEY,
  cooldown_until INTEGER NOT NULL DEFAULT 0,
  cooldown_reason TEXT NOT NULL DEFAULT '',      -- 'monthly' | 'rate_limit' | 'fallback'
  banned_at INTEGER NOT NULL DEFAULT 0,
  ban_reason TEXT NOT NULL DEFAULT '',
  fail_streak INTEGER NOT NULL DEFAULT 0,
  requests INTEGER NOT NULL DEFAULT 0,
  successes INTEGER NOT NULL DEFAULT 0,
  errors INTEGER NOT NULL DEFAULT 0,
  last_used INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS quota (               -- 额度缓存（中频，P4 写入）
  key_hash TEXT PRIMARY KEY, plan_id TEXT NOT NULL DEFAULT '',
  monthly_left REAL, period_end INTEGER NOT NULL DEFAULT 0,
  five_used REAL, five_cap REAL, five_reset INTEGER NOT NULL DEFAULT 0,
  week_used REAL, week_cap REAL, week_reset INTEGER NOT NULL DEFAULT 0,
  exceeded TEXT NOT NULL DEFAULT '',             -- 服务端权威：哪个窗口被拦
  checked_at INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS access (              -- 客户端准入白名单（P5）
  key_hash TEXT PRIMARY KEY, label TEXT NOT NULL DEFAULT '',
  enabled INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS models_cache (        -- 按账号分（D10：/v1/models 也走池）
  key_hash TEXT PRIMARY KEY, models TEXT NOT NULL, fetched_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL,
  action TEXT NOT NULL, target TEXT NOT NULL DEFAULT '',
  outcome TEXT NOT NULL DEFAULT '', ip TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT NOT NULL);

CREATE INDEX IF NOT EXISTS idx_accounts_priority ON accounts(priority, enabled);
CREATE INDEX IF NOT EXISTS idx_state_cooldown ON state(cooldown_until);
CREATE INDEX IF NOT EXISTS idx_audit_at ON audit(at DESC);
`;

/**
 * 打开（必要时创建）数据库。
 * @param {string} path 文件路径，或 ':memory:'（测试用）
 */
export function openDb(path) {
  if (path !== ':memory:') {
    try { mkdirSync(dirname(path), { recursive: true }); } catch { /* 目录已存在 */ }
  }
  const db = new DatabaseSync(path);
  // WAL：读不阻塞写；本地单进程也值得开，UI 轮询读与池写入会并发
  try { db.exec('PRAGMA journal_mode = WAL'); } catch { /* :memory: 上不支持就跳过 */ }
  db.exec('PRAGMA synchronous = NORMAL');
  db.exec(SCHEMA);
  return db;
}

/** 简单的 key/value 设置读写（阈值之类，避免散落的魔法数） */
export const settings = {
  get(db, k, dflt) {
    const row = db.prepare('SELECT v FROM settings WHERE k = ?').get(k);
    return row ? row.v : dflt;
  },
  getInt(db, k, dflt) {
    const v = settings.get(db, k, null);
    const n = Number.parseInt(v, 10);
    return Number.isFinite(n) ? n : dflt;
  },
  set(db, k, v) {
    db.prepare('INSERT INTO settings (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v')
      .run(k, String(v));
  },
};

/** 审计写入。所有状态变更都过这里，P6 的审计视图直接读它 */
export function audit(db, { at, action, target = '', outcome = '', ip = '' }) {
  db.prepare('INSERT INTO audit (at, action, target, outcome, ip) VALUES (?, ?, ?, ?, ?)')
    .run(at, action, target, outcome, ip);
}

/** 默认库路径：可用 CC_POOL_DB 覆盖（真机验证时把它指到仓库外） */
export function defaultDbPath(dirnameOfModule) {
  return process.env.CC_POOL_DB || `${dirnameOfModule}/pool.db`;
}
