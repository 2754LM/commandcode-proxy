#!/usr/bin/env node
/**
 * console/accounts.mjs — headless 账号管理 CLI（P6 的命令行入口）
 *
 * 有些场景不该开浏览器（服务器上初始化、脚本化加号、故障时解封），所以管理能力和 UI 共用同一套
 * db/crypto/pool —— 不重复实现、也不会出现"UI 能改但 CLI 改不了"的分叉。
 *
 * 用法（CC_ADMIN_PASSWORD 必须与 console 启动时一致，否则解密不了已有账号）：
 *   CC_ADMIN_PASSWORD=xxx node console/accounts.mjs list
 *   CC_ADMIN_PASSWORD=xxx node console/accounts.mjs add user_xxx --label 主号 --priority 0 --weight 1
 *   CC_ADMIN_PASSWORD=xxx node console/accounts.mjs remove <keyHash|hint>
 *   CC_ADMIN_PASSWORD=xxx node console/accounts.mjs enable|disable <keyHash|hint>
 *   CC_ADMIN_PASSWORD=xxx node console/accounts.mjs unban <keyHash|hint>
 *   CC_ADMIN_PASSWORD=xxx node console/accounts.mjs cooldown <keyHash|hint>      # 清冷却
 *   CC_ADMIN_PASSWORD=xxx node console/accounts.mjs quota <keyHash|hint>         # 查额度
 *   CC_ADMIN_PASSWORD=xxx node console/accounts.mjs access [open|whitelist|add <key>|rm <hash>]
 *   CC_ADMIN_PASSWORD=xxx node console/accounts.mjs audit [limit]
 *
 * 明文 key 只出现在 add 的参数里，落盘即加密；list 只显示 hint。
 */
import { dirname } from 'path';
import { fileURLToPath } from 'url';
import { openDb, defaultDbPath, settings } from './db.mjs';
import { deriveKek, newSalt } from './crypto.mjs';
import { createPool } from './pool.mjs';
import { createAccess } from './access.mjs';
import { createQuota } from './quota.mjs';
import { readFileSync, existsSync } from 'fs';

const dir = dirname(fileURLToPath(import.meta.url));
const die = (msg) => { console.error(msg); process.exit(1); };

const pw = process.env.CC_ADMIN_PASSWORD;
if (!pw || pw.length < 8) die('需要 CC_ADMIN_PASSWORD（≥8 位，且要与 console 启动时一致）');

const db = openDb(defaultDbPath(dir));
let salt = settings.get(db, 'kek_salt', '');
if (!salt) { salt = newSalt().toString('base64'); settings.set(db, 'kek_salt', salt); }
const kek = deriveKek(pw, salt);
const pool = createPool({ db, kek });
const access = createAccess({ db });

// apiBase 从 config.json 读（CLI 也要能查额度）
const cfgPath = `${dir}/../config.json`;
let apiBase = 'https://api.commandcode.ai';
if (existsSync(cfgPath)) {
  try { apiBase = JSON.parse(readFileSync(cfgPath, 'utf8')).apiBase || apiBase; } catch {}
}
if (process.env.CC_API_BASE) apiBase = process.env.CC_API_BASE;
const quota = createQuota({ db, apiBase });

/**
 * 用 keyHash / hint / 备注名 定位账号。
 * hint 里带省略号（user_cl…bbbb），人不会照着打，所以按「前缀 + 后缀」匹配；
 * 全都有歧义时要求用完整 keyHash。
 */
function resolveAccount(idOrHint) {
  if (!idOrHint) die('缺少账号标识（keyHash / hint / 备注名）');
  const all = pool.listAccounts();
  const exact = all.find((a) => a.keyHash === idOrHint);
  if (exact) return exact;

  const q = String(idOrHint);
  const byHint = all.filter((a) => {
    const [pre, post] = String(a.hint).split('…');
    return post ? (q.startsWith(pre) && q.endsWith(post)) : q === a.hint;
  });
  if (byHint.length === 1) return byHint[0];
  if (byHint.length > 1) die(`「${idOrHint}」匹配到多个账号（${byHint.map((a) => a.hint).join(', ')}），请用完整 keyHash`);

  const byLabel = all.filter((a) => a.label && a.label === q);
  if (byLabel.length === 1) return byLabel[0];
  if (byLabel.length > 1) die(`备注名「${idOrHint}」有 ${byLabel.length} 个账号同名，请用 keyHash`);

  die(`找不到账号：${idOrHint}（可用 list 查看 keyHash）`);
}

const fmtDur = (ms) => {
  if (!ms || ms <= 0) return '-';
  const s = Math.floor(ms / 1000);
  if (s < 60) return s + 's';
  if (s < 3600) return Math.floor(s / 60) + 'm';
  if (s < 86400) return (s / 3600).toFixed(1) + 'h';
  return (s / 86400).toFixed(1) + 'd';
};

const [cmd, ...args] = process.argv.slice(2);
const flag = (name, dflt) => {
  const i = args.indexOf('--' + name);
  return i >= 0 ? args[i + 1] : dflt;
};

const stateOf = (a) => (a.bannedAt ? '封禁' : a.cooldownUntil > Date.now() ? '冷却' : a.enabled ? '可用' : '停用');

switch (cmd) {
  case 'list':
  case undefined: {
    const all = pool.listAccounts();
    const av = pool.availability();
    console.log(`账号池：总计 ${av.total} / 可用 ${av.usable} / 冷却 ${av.cooling} / 封禁 ${av.banned} / 停用 ${av.disabled}` +
      `　准入 ${access.mode()}`);
    if (!all.length) break;
    for (const a of all) {
      const q = quota.get(a.keyHash);
      const win = q ? `5h ${q.fiveHour.used ?? '-'}/${q.fiveHour.cap ?? '-'} 周 ${q.weekly.used ?? '-'}/${q.weekly.cap ?? '-'}` : '未查询';
      console.log(`  ${a.hint}  [${stateOf(a)}]  prio=${a.priority} w=${a.weight}` +
        `  请求 ${a.requests}(成 ${a.successes}/败 ${a.errors})  ${win}` +
        (a.bannedAt ? `  原因: ${a.banReason}` : a.cooldownUntil > Date.now() ? `  至 ${new Date(a.cooldownUntil).toLocaleString()} (${a.cooldownReason})` : ''));
      console.log(`    keyHash ${a.keyHash}`);
    }
    break;
  }
  case 'add': {
    const key = args[0];
    if (!key) die('用法：add <key> [--label x] [--priority n] [--weight n]');
    const r = pool.addAccount({
      key, label: flag('label', ''), priority: Number(flag('priority', 0)), weight: Number(flag('weight', 1)),
    });
    console.log(`${r.created ? '已添加' : '已更新'} ${r.hint}（keyHash ${r.keyHash}）`);
    const q = await quota.refresh(r.keyHash, key);
    console.log(q.ok ? `额度：5h ${q.fiveHour.used}/${q.fiveHour.cap}，周 ${q.weekly.used}/${q.weekly.cap}，余额 ${q.monthlyLeft}` : `额度查询失败（不影响使用）：${q.error}`);
    break;
  }
  case 'remove': {
    const a = resolveAccount(args[0]);
    pool.removeAccount(a.keyHash);
    console.log(`已删除 ${a.hint}`);
    break;
  }
  case 'enable':
  case 'disable': {
    const a = resolveAccount(args[0]);
    pool.setAccount(a.keyHash, { enabled: cmd === 'enable' });
    if (cmd === 'enable') pool.clearCooldown(a.keyHash);
    console.log(`${a.hint} → ${cmd === 'enable' ? '已启用（并清除冷却）' : '已停用'}`);
    break;
  }
  case 'unban': {
    const a = resolveAccount(args[0]);
    console.log(pool.unban(a.keyHash) ? `${a.hint} 已解封` : `${a.hint} 不存在`);
    break;
  }
  case 'cooldown': {
    const a = resolveAccount(args[0]);
    console.log(pool.clearCooldown(a.keyHash) ? `${a.hint} 冷却已清除` : `${a.hint} 本来就不在冷却`);
    break;
  }
  case 'quota': {
    const a = resolveAccount(args[0]);
    const key = pool.revealKey(a.keyHash);
    const q = await quota.refresh(a.keyHash, key);
    if (!q.ok) { console.log(`查询失败：${q.error}`); break; }
    console.log(`${a.hint}：套餐 ${q.planId || '-'}，余额 ${q.monthlyLeft}` +
      `\n  5h 窗口 ${q.fiveHour.used}/${q.fiveHour.cap}，重置 ${q.fiveHour.reset ? new Date(q.fiveHour.reset).toLocaleString() : '-'}` +
      `\n  周窗口 ${q.weekly.used}/${q.weekly.cap}，重置 ${q.weekly.reset ? new Date(q.weekly.reset).toLocaleString() : '-'}` +
      `\n  被拦窗口 ${q.exceeded || '无'}，账期至 ${q.periodEnd ? new Date(q.periodEnd).toLocaleString() : '-'}`);
    break;
  }
  case 'access': {
    const sub = args[0];
    if (!sub) { console.log(`准入模式：${access.mode()}\n客户端白名单：`); for (const c of access.list()) console.log(`  ${c.keyHash}  ${c.label || '-'}  ${c.enabled ? '启用' : '停用'}`); break; }
    if (sub === 'open' || sub === 'whitelist') { console.log(`准入模式 → ${access.setMode(sub)}`); break; }
    if (sub === 'add') { const r = access.add(args[1], flag('label', '')); console.log(`已加入白名单 ${r.hint}`); break; }
    if (sub === 'rm') { console.log(access.remove(args[1]) ? '已移除' : '未找到'); break; }
    die('用法：access [open|whitelist|add <key>|rm <keyHash>]');
    break;
  }
  case 'audit': {
    const limit = Number(args[0] || 30);
    for (const r of db.prepare('SELECT at, action, target, outcome FROM audit ORDER BY id DESC LIMIT ?').all(limit)) {
      console.log(`  ${new Date(r.at).toLocaleString()}  ${r.action.padEnd(20)} ${r.target}  ${r.outcome}`);
    }
    break;
  }
  case 'prune-sticky':
    console.log('sticky 绑定是内存态，进程重启即清空，无需清理');
    break;
  default:
    die(`未知子命令：${cmd}\n可用：list | add | remove | enable | disable | unban | cooldown | quota | access | audit`);
}
db.close();
