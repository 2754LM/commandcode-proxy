/**
 * console/stats.mjs — 运行状态计数器（内存态，零依赖）
 *
 * 定位：只服务形态 B 的只读 UI。**不碰协议、不碰伪装**（D4），
 * 只做三件事：计数、延迟采样、错误环形缓冲。
 *
 * 设计约束：
 *  - 模块级单例（与 proxy.mjs 的 D3 保持一致：导出即单例，不做工厂）
 *  - 定长环形缓冲，内存占用与运行时长无关（不会被长跑撑爆）
 *  - snapshot() 返回纯 JSON 可序列化对象，且**不含任何 apiKey 明文**
 */

// ── 容量常量（全部定长） ─────────────────────────────
const LATENCY_WINDOW = 256;   // 延迟采样窗口（保留最近 N 个）
const ERROR_RING = 50;        // 上游错误环形缓冲条数
const PATH_MAX = 32;          // 最多跟踪的路径数（超出后不再新建，避免无界增长）
const STATUS_MAX = 16;        // 状态码分布桶上限

// ── 计数器 ──────────────────────────────────────────
const startedAt = Date.now();
let total = 0;
let succeeded = 0;   // 2xx/3xx
let failed = 0;      // 4xx/5xx
let aborted = 0;     // 客户端断连（未 finish）
let inflight = 0;
let inflightPeak = 0;

const byStatus = new Map();  // status → count
const byPath = new Map();    // path → { total, ok, failed, latSum, latMax, lastAt }

// 延迟环形缓冲
const latencies = new Float64Array(LATENCY_WINDOW);
let latCount = 0;   // 已写入总数（可 > LATENCY_WINDOW）
let latCursor = 0;

// 错误环形缓冲
const errors = new Array(ERROR_RING).fill(null);
let errCursor = 0;
let errTotal = 0;

// ── 写入侧 ──────────────────────────────────────────

/** 请求进入（非 console 自身流量）。与 leave() 成对，幂等由调用方保证。 */
export function enter() {
  inflight++;
  if (inflight > inflightPeak) inflightPeak = inflight;
}

/** 请求退出。 */
export function leave() {
  if (inflight > 0) inflight--;
}

function pushLatency(ms) {
  latencies[latCursor] = ms;
  latCursor = (latCursor + 1) % LATENCY_WINDOW;
  latCount++;
}

function bucketPath(path) {
  let e = byPath.get(path);
  if (!e) {
    if (byPath.size >= PATH_MAX) {
      // 已满：归入 __other__，保证内存有界
      e = byPath.get('__other__');
      if (!e) { e = { total: 0, ok: 0, failed: 0, latSum: 0, latMax: 0, lastAt: 0 }; byPath.set('__other__', e); }
      return e;
    }
    e = { total: 0, ok: 0, failed: 0, latSum: 0, latMax: 0, lastAt: 0 };
    byPath.set(path, e);
  }
  return e;
}

/**
 * 记录一次已完成的代理请求。
 * @param {{path:string, status:number, latencyMs:number, finished:boolean, error?:{code?:string,type?:string,message?:string}}} rec
 */
export function record(rec) {
  const { path = '?', status = 0, latencyMs = 0, finished = true, error = null } = rec;
  total++;
  const isOk = status >= 200 && status < 400;
  if (!finished) aborted++;
  else if (isOk) succeeded++;
  else failed++;

  // 状态码分布桶有上限：新状态码在桶满之后并入 other，保证 Map 不无界增长
  if (byStatus.has(status) || byStatus.size < STATUS_MAX) {
    byStatus.set(status, (byStatus.get(status) || 0) + 1);
  } else {
    byStatus.set('other', (byStatus.get('other') || 0) + 1);
  }

  const p = bucketPath(path);
  p.total++;
  if (isOk) p.ok++; else p.failed++;
  p.latSum += latencyMs;
  if (latencyMs > p.latMax) p.latMax = latencyMs;
  p.lastAt = Date.now();

  pushLatency(latencyMs);

  if (!isOk || error) {
    errors[errCursor] = {
      at: Date.now(),
      path,
      status,
      code: error?.code || null,
      type: error?.type || null,
      message: (error?.message || '').slice(0, 300),
    };
    errCursor = (errCursor + 1) % ERROR_RING;
    errTotal++;
  }
}

// ── 读取侧 ──────────────────────────────────────────

function percentile(sorted, p) {
  if (!sorted.length) return null;
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[idx];
}

/** 延迟分位（毫秒，整数）。窗口内样本不足时返回 null。 */
export function latency() {
  const n = Math.min(latCount, LATENCY_WINDOW);
  if (n === 0) return { samples: 0, p50: null, p95: null, p99: null, max: null, last: null };
  const arr = Array.from(latencies.slice(0, latCount < LATENCY_WINDOW ? latCount : LATENCY_WINDOW));
  arr.sort((a, b) => a - b);
  return {
    samples: latCount,
    p50: percentile(arr, 50),
    p95: percentile(arr, 95),
    p99: percentile(arr, 99),
    max: arr[arr.length - 1],
    last: latencies[(latCursor - 1 + LATENCY_WINDOW) % LATENCY_WINDOW],
  };
}

/** 错误 tail：按时间倒序（最新在前）。 */
export function errorTail() {
  const out = [];
  const n = Math.min(errTotal, ERROR_RING);
  for (let i = 0; i < n; i++) {
    const e = errors[(errCursor - 1 - i + ERROR_RING * 2) % ERROR_RING];
    if (e) out.push(e);
  }
  return out;
}

/** 状态码分布：按次数倒序。 */
export function statusDistribution() {
  return [...byStatus.entries()]
    .map(([code, count]) => ({ code: code === 'other' ? 'other' : Number(code), count }))
    .sort((a, b) => b.count - a.count);
}

/** 按路径聚合：按次数倒序。 */
export function pathBreakdown() {
  return [...byPath.entries()]
    .map(([path, e]) => ({
      path,
      total: e.total,
      ok: e.ok,
      failed: e.failed,
      avgMs: e.total ? Math.round(e.latSum / e.total) : null,
      maxMs: e.latMax || null,
      lastAt: e.lastAt || null,
    }))
    .sort((a, b) => b.total - a.total);
}

/** 全量快照（纯 JSON，无密钥）。 */
export function snapshot() {
  const now = Date.now();
  return {
    startedAt,
    now,
    uptimeMs: now - startedAt,
    requests: {
      total,
      succeeded,
      failed,
      aborted,
      inflight,
      inflightPeak,
      successRate: total ? Math.round((succeeded / total) * 1000) / 10 : null,
    },
    latency: latency(),
    statuses: statusDistribution(),
    paths: pathBreakdown(),
    errors: { total: errTotal, capacity: ERROR_RING, tail: errorTail() },
  };
}
