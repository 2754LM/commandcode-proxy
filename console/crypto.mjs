/**
 * console/crypto.mjs — 凭证加密与口令派生
 *
 * 依据 DESIGN-multiaccount-v4 §7（存储）与 PLAN-multiaccount D6：
 *   - 上游 key 落盘一律 AES-256-GCM 加密，KEK 由管理口令经 scrypt 派生
 *   - **口令丢失 = 数据不可解密**（接受这个代价，刻意不做恢复后门）
 *   - `key_hash` = sha256(明文 key)，用于唯一性与索引；它本身不可逆推
 *   - `key_hint` = 形如 `user_ab…cd12`，只够人眼对号，不足以还原
 *
 * 零 npm 依赖：只用 node:crypto（D13）。
 */
import {
  randomBytes, scryptSync, createCipheriv, createDecipheriv,
  createHash, timingSafeEqual,
} from 'crypto';

// scrypt 参数：N=2^14 时内存 128*N*r ≈ 16MB，默认 maxmem(32MB) 会卡边界，故显式放宽
const SCRYPT = { N: 16384, r: 8, p: 1, keylen: 32 };
const MAXMEM = 64 * 1024 * 1024;
const IV_BYTES = 12;   // GCM 推荐 96-bit nonce
const SALT_BYTES = 16;

/** 新建盐（首次初始化时生成一次，之后与 KEK 一起持久化） */
export const newSalt = () => randomBytes(SALT_BYTES);

/**
 * 由管理口令派生 KEK。
 * @param {string} passphrase 管理口令（至少 8 位，弱口令直接拒绝而不是默默接受）
 * @param {Buffer|string} salt Buffer 或 base64 字符串
 */
export function deriveKek(passphrase, salt) {
  if (typeof passphrase !== 'string' || passphrase.length < 8) {
    throw new Error('管理口令至少 8 位');
  }
  const buf = Buffer.isBuffer(salt) ? salt : Buffer.from(String(salt), 'base64');
  return scryptSync(passphrase, buf, SCRYPT.keylen, {
    N: SCRYPT.N, r: SCRYPT.r, p: SCRYPT.p, maxmem: MAXMEM,
  });
}

/**
 * 加密一个字符串（上游 API key）。
 * @returns {{enc:string, iv:string, tag:string}} 三段都是 base64，直接进 SQLite 三个列
 */
export function encrypt(kek, plaintext) {
  const iv = randomBytes(IV_BYTES);
  const cipher = createCipheriv('aes-256-gcm', kek, iv);
  const body = Buffer.concat([cipher.update(String(plaintext), 'utf8'), cipher.final()]);
  return {
    enc: body.toString('base64'),
    iv: iv.toString('base64'),
    tag: cipher.getAuthTag().toString('base64'),
  };
}

/**
 * 解密。GCM 的 tag 校验失败会抛错 —— 这正是我们要的：
 * 口令错了必须硬失败，不能悄悄返回一段垃圾去请求上游。
 */
export function decrypt(kek, rec) {
  if (!rec || !rec.enc || !rec.iv || !rec.tag) throw new Error('密文记录不完整');
  const decipher = createDecipheriv('aes-256-gcm', kek, Buffer.from(rec.iv, 'base64'));
  decipher.setAuthTag(Buffer.from(rec.tag, 'base64'));
  return Buffer.concat([
    decipher.update(Buffer.from(rec.enc, 'base64')),
    decipher.final(),
  ]).toString('utf8');
}

/** sha256(明文 key) —— 唯一键、索引、以及「不存明文也能判重」 */
export const keyHash = (key) => createHash('sha256').update(String(key)).digest('hex');

/** 形如 `user_ab…cd12` 的提示串：够人眼辨认，不足以还原 */
export function keyHint(key) {
  const k = String(key);
  if (k.length <= 12) return k.slice(0, 2) + '…' + k.slice(-2);
  return k.slice(0, 7) + '…' + k.slice(-4);
}

/** 常数时间比较（管理口令校验用，避免逐字节早退泄漏长度/前缀） */
export function safeEqual(a, b) {
  const x = Buffer.from(String(a));
  const y = Buffer.from(String(b));
  if (x.length !== y.length) return false;
  return timingSafeEqual(x, y);
}

/** 任意串的 digest，给 session/token 之类用 */
export const digest = (s) => createHash('sha256').update(String(s)).digest();

/** 会话 token（不可预测；只在内存里存 hash，不落盘） */
export const newToken = (bytes = 32) => randomBytes(bytes).toString('hex');
