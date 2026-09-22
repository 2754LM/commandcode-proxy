// 本分支自研能力（Key 池 / 会话级调度 / 额度自动停用 / 轮询 / 数据卷）的回归测试。
//
// 上游测试假设「请求头带 key 直通（BYOK）」，而本分支固定从本地 Key 池取 key，
// 所以这里都用临时工作目录自己造一份 keys.json 来驱动调度逻辑。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, copyFileSync, existsSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { REPO, startMockUpstream, startProxy } from './helpers.mjs';

const CHAT = { model: 'm', messages: [{ role: 'user', content: 'hi' }] };

/** 造一个只属于本次测试的工作目录（自带 config.json 与 keys.json） */
function makeCwd(keys, settings = {}, lb = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'ccp-pool-'));
  copyFileSync(join(REPO, 'config.json'), join(dir, 'config.json'));
  writeFileSync(join(dir, 'keys.json'), JSON.stringify({
    lb: { strategy: 'weighted', stickyBy: 'client-key', maxRetries: 2, cooldownMs: 60000, ...lb },
    settings: {
      mode: 'session', failThreshold: 3, sessionTtlMs: 21600000,
      creditsRefreshMs: 0, autoDisableExhausted: true, onAllExhausted: 'error', ...settings,
    },
    keys, defaultId: null,
  }, null, 2));
  return dir;
}

/** 造一个 Key 池条目 */
function mkKey(id, label, key, extra = {}) {
  return {
    id, label, key, weight: 1, enabled: true, priority: 0, createdAt: Date.now(),
    lastUsedAt: null, lastErrorAt: null, errorCount: 0, cooldownUntil: 0,
    consecutiveFailures: 0, autoDisabled: null, credits: null, ...extra,
  };
}

/** 一份「窗口没用过」的额度数据 */
function freshCredits(remaining = 10) {
  return {
    fetchedAt: Date.now(), plan: 'test-plan',
    credits: { monthly: remaining, purchased: 0, free: 0, remaining },
    creditsRemaining: remaining,
    windows: [
      { name: '5h', used: 0, cap: 3, resetsAt: 0, pct: 0 },
      { name: 'weekly', used: 0, cap: 6, resetsAt: 0, pct: 0 },
    ],
    worstPct: 0,
  };
}

const cleanup = (dir) => { try { rmSync(dir, { recursive: true, force: true }); } catch {} };

test('Key 池：请求头里的 key 被忽略，实际用的是池内 key', async () => {
  const dir = makeCwd([mkKey('key_pool0001', 'poolA', 'user_poolAAAAAAAAAAAA0001')]);
  const mock = await startMockUpstream();
  const proxy = await startProxy({ upstreamPort: mock.port, cwd: dir });
  try {
    const r = await proxy.post('/v1/chat/completions', CHAT, { Authorization: 'Bearer user_fromClientHeader9999' });
    assert.equal(r.status, 200);
    const sent = mock.lastGenerate();
    assert.ok(sent, 'mock 上游应收到 generate 请求');
    assert.match(sent.headers.authorization, /user_poolAAAAAAAAAAAA0001/, '上游应收到池内 key，而不是客户端 header 里的 key');
  } finally { await proxy.kill(); await mock.close(); cleanup(dir); }
});

test('会话粘性：同一会话固定同一个 Key，不同会话分散到不同 Key', async () => {
  const keys = [
    mkKey('key_s1', 'A', 'user_ssssssssssssssss0001', { credits: freshCredits() }),
    mkKey('key_s2', 'B', 'user_ssssssssssssssss0002', { credits: freshCredits() }),
    mkKey('key_s3', 'C', 'user_ssssssssssssssss0003', { credits: freshCredits() }),
  ];
  const dir = makeCwd(keys);
  const mock = await startMockUpstream();
  const proxy = await startProxy({ upstreamPort: mock.port, cwd: dir });
  try {
    const sess = (id) => proxy.post('/v1/chat/completions', CHAT, { 'x-cc-session': id });
    await sess('sess-aaaaaa'); await sess('sess-bbbbbb'); await sess('sess-cccccc');
    const list = await (await proxy.get('/admin/api/sessions')).json();
    assert.equal(list.sessions.length, 3, '三个会话都应有绑定');
    const ids = list.sessions.map((s) => s.keyId);
    assert.equal(new Set(ids).size, 3, '三个会话应落在三个不同 Key 上');

    // 同一个会话再请求一次，必须还在原来的 Key
    const before = (await (await proxy.get('/admin/api/sessions')).json()).sessions.find((s) => s.id.includes('sess-aaaaaa'));
    await sess('sess-aaaaaa');
    const after = (await (await proxy.get('/admin/api/sessions')).json()).sessions.find((s) => s.id.includes('sess-aaaaaa'));
    assert.equal(after.keyId, before.keyId, '同一会话应保持同一 Key');
  } finally { await proxy.kill(); await mock.close(); cleanup(dir); }
});

test('额度用尽的 Key 自动停用，并按上游给的窗口重置时间恢复', async () => {
  const resetAt = Date.now() + 3 * 60 * 1000;
  const exhausted = {
    fetchedAt: Date.now(), plan: 'test-plan',
    credits: { monthly: 4, purchased: 0, free: 0, remaining: 4 }, creditsRemaining: 4,
    windows: [
      { name: '5h', used: 0, cap: 3, resetsAt: 0, pct: 0 },
      { name: 'weekly', used: 6, cap: 6, resetsAt: resetAt, pct: 100 },
    ],
    worstPct: 100,
  };
  const dir = makeCwd([
    mkKey('key_ex', 'exhausted', 'user_exxxxxxxxxxxxxxx0001', { credits: exhausted }),
    mkKey('key_ok', 'healthy', 'user_okokokokokokokok0001', { credits: freshCredits() }),
  ]);
  const mock = await startMockUpstream();
  const proxy = await startProxy({ upstreamPort: mock.port, cwd: dir });
  try {
    const d = await (await proxy.get('/admin/api/keys')).json();
    const ex = d.keys.find((k) => k.id === 'key_ex');
    assert.ok(ex.autoDisabled, '周额度已满的 Key 应被自动停用');
    assert.ok(Math.abs(ex.autoDisabled.until - resetAt) < 5000, '恢复时间应取上游给的窗口重置时间');
    assert.equal(ex.activeSessions, 0);

    // 请求应只落到健康的那个 Key 上
    const r = await proxy.post('/v1/chat/completions', CHAT, { 'x-cc-session': 'sess-healthy' });
    assert.equal(r.status, 200);
    const list = await (await proxy.get('/admin/api/sessions')).json();
    assert.equal(list.sessions[0].keyId, 'key_ok', '会话应落到可用 Key 上');
  } finally { await proxy.kill(); await mock.close(); cleanup(dir); }
});

test('上游报额度用尽：自动换 Key 并停用出问题的 Key', async () => {
  const FAIL = 'user_failfailfailfail01';
  const resetSec = Math.floor((Date.now() + 3600_000) / 1000);
  const dir = makeCwd([
    mkKey('key_fail', 'quota-key', FAIL),
    mkKey('key_next', 'backup-key', 'user_backupbackupback01'),
  ], {}, { maxRetries: 2 });
  const mock = await startMockUpstream({
    onRequest(req, res) {
      if (req.url === '/alpha/generate' && String(req.headers.authorization || '').includes('failfail')) {
        res.writeHead(429, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          success: false,
          error: { code: 'RATE_LIMITED', status: 429, message: 'weekly usage limit reached' },
          rateLimit: { limit: 6, remaining: 0, reset: resetSec, window: 'weekly' },
        }));
      }
    },
  });
  const proxy = await startProxy({ upstreamPort: mock.port, cwd: dir });
  try {
    const r = await proxy.post('/v1/chat/completions', CHAT, { 'x-cc-session': 'sess-transfer' });
    assert.equal(r.status, 200, '第一个 Key 报额度用尽后应自动换到备用 Key 并成功');

    const d = await (await proxy.get('/admin/api/keys')).json();
    const bad = d.keys.find((k) => k.id === 'key_fail');
    assert.ok(bad.autoDisabled, '报额度用尽的 Key 应被停用');
    assert.ok(bad.consecutiveFailures >= 1, '应记录失败次数');

    const list = await (await proxy.get('/admin/api/sessions')).json();
    assert.equal(list.sessions[0].keyId, 'key_next', '会话应重绑到备用 Key');
  } finally { await proxy.kill(); await mock.close(); cleanup(dir); }
});

test('全池不可用时明确报错（onAllExhausted=error）', async () => {
  const dir = makeCwd([mkKey('key_off', 'off', 'user_offoffoffoffoffoff01', { enabled: false })]);
  const mock = await startMockUpstream();
  const proxy = await startProxy({ upstreamPort: mock.port, cwd: dir });
  try {
    const r = await proxy.post('/v1/chat/completions', CHAT);
    assert.equal(r.status, 429, '池内没有可用 Key 时应返回 429 而不是硬跑');
    const body = await r.json();
    assert.match(body.error.message, /都不可用|池为空/);
  } finally { await proxy.kill(); await mock.close(); cleanup(dir); }
});

test('设置接口：轮询间隔可改，并会排定下次刷新', async () => {
  const dir = makeCwd([mkKey('key_set1', 'A', 'user_setsetsetsetset0001', { credits: freshCredits() })]);
  const mock = await startMockUpstream();
  const proxy = await startProxy({ upstreamPort: mock.port, cwd: dir });
  try {
    const p2 = await proxy.get('/admin/api/settings', {
      method: 'PUT', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ creditsRefreshMs: 60000, autoDisableExhausted: true }),
    });
    assert.equal(p2.status, 200);
    const d = await p2.json();
    assert.equal(d.settings.creditsRefreshMs, 60000);
    assert.ok(d.creditsRefresh.nextAt > 0, '应排定下次刷新时间');

    const got = await (await proxy.get('/admin/api/settings')).json();
    assert.equal(got.settings.creditsRefreshMs, 60000, '设置应持久化在服务端');
  } finally { await proxy.kill(); await mock.close(); cleanup(dir); }
});

test('CC_KEYS_FILE：Key 池写到指定路径（Docker 挂卷依赖这个）', async () => {
  const home = mkdtempSync(join(tmpdir(), 'ccp-keysfile-'));
  const custom = join(home, 'data', 'keys.json');
  const dir = makeCwd([mkKey('key_seed1', 'seed', 'user_seedseedseedseed01')]);
  const mock = await startMockUpstream();
  const proxy = await startProxy({ upstreamPort: mock.port, cwd: dir, env: { CC_KEYS_FILE: custom } });
  try {
    // 启动日志里应打印实际使用的路径
    assert.match(proxy.logs(), /Key pool loaded/, '应有 Key 池加载日志');
    // 通过 API 加一个 Key，应该写到 CC_KEYS_FILE 指定的位置
    const r = await proxy.post('/admin/api/keys', { key: 'user_viacustompath0001', label: 'venv', weight: 1, enabled: true });
    assert.equal(r.status, 201);
    assert.ok(existsSync(custom), `keys.json 应出现在 ${custom}`);
    const saved = JSON.parse(readFileSync(custom, 'utf8'));
    assert.ok(saved.keys.some((k) => k.key === 'user_viacustompath0001'), '写入的内容应包含新增的 Key');
  } finally { await proxy.kill(); await mock.close(); cleanup(dir); cleanup(home); }
});
