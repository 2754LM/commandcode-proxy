# 部署指南

本文只讲**怎么把这个代理部署起来**。两种形态二选一，命令可直接复制。

- **形态 A：单文件代理**。只有 `node proxy.mjs`，客户端自带 key 直连上游，没有界面、没有多账号。
- **形态 B：带控制台 + 账号池**。入口换成 `node console/server.mjs`，**同一个端口**上额外提供 `/console` 界面与账号池（多把 key 轮换、额度卡片、准入白名单、审计）。

没有构建步骤，**零 npm 依赖**（只用 Node 内置模块），所以不要执行 `npm install`。

---

## 1. 前置条件

| 项 | 要求 |
|---|---|
| Node | 形态 A：≥ 18；形态 B：**≥ 22.5**（账号池用内置 `node:sqlite`） |
| 内存 | 请求体在转发上游前会有多份副本，实测峰值 ≈ 请求体 × 5～7；默认单请求体上限 100MB，最坏约 550MB，按此预留内存 |
| 网络 | 代理进程要能**出站直连** `CC_API_BASE`（默认 `https://api.commandcode.ai`） |
| 端口 | 默认 **3050**，默认监听 `0.0.0.0`（`config.json`） |
| 浏览器 | 只有形态 B 的界面需要，且必须从**本机**访问（见 §7） |

先自检语法：

```bash
node --version
node --check proxy.mjs
```

---

## 2. 安装与启动

### 拿到代码

```bash
git clone https://github.com/MAXeaglet/commandcode-proxy.git
cd commandcode-proxy
```

### 形态 A：单文件

```bash
node proxy.mjs
# 后台运行
nohup node proxy.mjs > cc-proxy.log 2>&1 &
```

Docker：

```bash
docker build -t commandcode-proxy:latest .
docker run -d --name cc-proxy -p 3050:3050 commandcode-proxy:latest
```

启动成功时日志里会有 `CC Proxy started`（含 url / api / models / idleTimeouts / keepAliveTimeout）。探活：`curl http://127.0.0.1:3050/health` 返回文本 `OK`。

### 形态 B：控制台 + 账号池

口令**必须 ≥ 8 位**，它就是账号密文的加密口令（见 §8）：

```bash
CC_ADMIN_PASSWORD='你的口令（≥8位）' node console/server.mjs
```

Docker（数据挂卷，容器重建不丢账号）：

```bash
docker build -f Dockerfile.console -t commandcode-proxy:console .
docker run -d --name cc-console -p 3050:3050 -v cc-data:/data \
  -e CC_ADMIN_PASSWORD='你的口令（≥8位）' commandcode-proxy:console
```

启动日志里应出现 `Account pool enabled {"accounts":0,...}`。若出现 `Failed to enable account pool`，见 §9 排查。

不设 `CC_ADMIN_PASSWORD` 时形态 B 会退化成"形态 A + 只读界面"：代理行为与单文件完全一致，账号池相关接口一律返回 501。

---

## 3. 配置

配置文件是仓库根目录的 `config.json`；**环境变量优先级更高**，覆盖对应字段。

```json
{
  "port": 3050,
  "host": "0.0.0.0",
  "apiBase": "https://api.commandcode.ai"
}
```

### 环境变量（部署相关的全部）

**端口与上游**

| 变量 | 默认 | 说明 |
|---|---|---|
| `PORT` / `HOST` | `3050` / `0.0.0.0` | 监听地址 |
| `CC_API_BASE` | `https://api.commandcode.ai` | 上游地址 |
| `CC_USE_PROVIDER_MODELS` | `true` | `false` 时不向上游拉模型目录，用内置列表 |
| `CMD_ZDR` | 关 | `1` 时对上游请求带 ZDR 头 |
| `LOG_FILE` | 空（只打 stdout） | 追加写日志文件 |

**请求与网络**

| 变量 | 默认 | 说明 |
|---|---|---|
| `CC_MAX_BODY_MB` | `100` | 请求体上限，超限返回 413 |
| `CC_MAX_INFLIGHT` | `0`（不限） | 进程内在途请求上限，超过直接 503 |
| `CC_STREAM_IDLE_MS` | `30000` | 读上游流式响应的空闲超时 |
| `CC_NONSTREAM_IDLE_MS` | `90000` | 非流式同上 |
| `CC_CLIENT_DRAIN_TIMEOUT_MS` | `0`（关） | 下游僵死保护；放在反向代理后面建议设 |
| `CC_KEEPALIVE_TIMEOUT_MS` | `65000` | 后端 keep-alive 时长，必须**大于**反代侧的值 |

**账号池与控制台（形态 B）**

| 变量 | 默认 | 说明 |
|---|---|---|
| `CC_ADMIN_PASSWORD` | 无 | **设了才启用账号池**；≥8 位；同时是账号密文的加密口令 |
| `CC_POOL_DB` | `console/pool.db` | SQLite 路径（控制台镜像里是 `/data/pool.db`） |
| `CC_POOL_VERBOSE` | 关 | `1` 每次选号都打日志（排查轮换时开） |
| `CC_POOL_WAKE` | 开 | `0` 关闭"冷却到点自动复核" |
| `CC_POOL_WAKE_TICK_MS` | `60000` | 兜底扫描间隔 |
| `CC_POOL_WAKE_GRACE_MS` | `300000` | 复核联系不上额度接口时的保守续期 |

---

## 4. 添加上游账号（形态 B）

界面：`http://127.0.0.1:3050/console` → 输入口令 → 「账号」页粘 key、点「添加账号」。优先级与权重就是页面上的两个数字输入框。

服务器上没有浏览器时用命令行（与界面共用同一套存储；`CC_ADMIN_PASSWORD` 必须与启动时一致，`CC_POOL_DB` 必须指向同一个库）：

```bash
CC_ADMIN_PASSWORD='...' node console/accounts.mjs add user_xxxxxxxx --label 主号 --priority 0 --weight 1
CC_ADMIN_PASSWORD='...' node console/accounts.mjs list
CC_ADMIN_PASSWORD='...' node console/accounts.mjs quota <keyHash|hint>
CC_ADMIN_PASSWORD='...' node console/accounts.mjs access whitelist
CC_ADMIN_PASSWORD='...' node console/accounts.mjs audit 50
```

客户端侧只需要一个 `user_` 开头的 key；准入模式（开放 / 白名单）在界面的「准入」区或上面那条 `access` 命令里切换。

---

## 5. 部署后验收清单

```bash
B=http://127.0.0.1:3050

curl -s $B/health                                      # 期望：OK
curl -s -o /dev/null -w '%{http_code}\n' $B/console    # 形态 B 期望：200；形态 A 期望：404
curl -s $B/api/console/gate                            # 形态 B 期望：{"pool":true,"authed":false,"total":N,"usable":M}
curl -s -o /dev/null -w '%{http_code}\n' $B/api/console/overview
#                                                      # 形态 B 期望：401（读接口也要会话）

# 登录（期望 200，并在 Set-Cookie 里下发 cc_admin）
curl -s -c /tmp/ck -X POST $B/api/console/auth \
  -H 'Content-Type: application/json' -d '{"password":"你的口令（≥8位）"}' >/dev/null
curl -s -b /tmp/ck $B/api/console/accounts | head -c 300      # 期望：pool 概览 + accounts 数组

# 真实发一发（用你自己的 key；必须出字）
curl -s -N $B/v1/chat/completions -H 'Authorization: Bearer user_你的key' \
  -H 'Content-Type: application/json' \
  -d '{"model":"deepseek/deepseek-v4-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}' | head -5
```

---

## 6. 放在反向代理后面

1. **SSE 必须关缓冲**，否则流式回答会被攒成一坨：

   ```nginx
   location / {
     proxy_pass http://127.0.0.1:3050;
     proxy_http_version 1.1;
     proxy_set_header Connection "";
     proxy_buffering off;
     proxy_read_timeout 300s;         # 必须大于 CC_STREAM_IDLE_MS
   }
   ```

2. 反代的 upstream `keepalive_timeout` 必须**小于**后端 `CC_KEEPALIVE_TIMEOUT_MS`（默认 65s）。反了的话，反代会复用一条后端已经 FIN 掉的连接，写 POST 请求体时吃 `EPIPE`（nginx 日志里是 `sendfile() failed (32: Broken pipe)`），而 POST 非幂等、nginx 默认不重试 → 客户端直接 502。

3. **不要把 `/console` 和 `/api/console/` 反代出去。** 控制台只接受回环来源（`127.0.0.0/8`、`::1`），但同机反代进来的请求恰好就是回环，于是这层保护会被绕过、界面直接暴露到公网（仍然要口令，但不再是"仅本机"）。要么只反代 API 路径，要么在反代层显式挡掉这两个前缀。

---

## 7. 远程访问控制台

控制台接口只对回环开放，所以远程部署时用 SSH 隧道，不要把 3050 直接暴露：

```bash
ssh -L 3050:127.0.0.1:3050 用户名@服务器
# 然后在本机浏览器打开 http://127.0.0.1:3050/console
```

`GET /` 在回环来源下会 302 跳到 `/console`；非回环来源仍返回核心的 `OK`，所以外部探活/健康检查不受影响。

---

## 8. 备份、升级与口令

- **备份**：形态 B 的全部状态都在 `CC_POOL_DB`（默认 `console/pool.db`，容器里 `/data/pool.db`）。SQLite 开了 WAL，热备请把 `pool.db`、`pool.db-wal`、`pool.db-shm` 一起拷，或先停进程/容器再拷。
- **升级**：拉新代码 → 重新 `docker build`（或直接重启进程）→ 重启。数据库有幂等的列迁移，老库直接可用。
- **口令**：`CC_ADMIN_PASSWORD` 既是登录口令，也是账号密文的加密口令（scrypt 派生 KEK）。

  > ⚠️ **换口令 = 已存账号全部解不开；忘口令 = 那些账号作废。** 口令不落盘，请单独备份。同理，用 `console/accounts.mjs` 时必须与 console 用同一个口令和同一个库。

---

## 9. 故障排查

| 症状 | 原因 | 处理 |
|---|---|---|
| `/api/console/*` 全是 401 | 全局鉴权：读和写都要会话；公开的只有 `POST /api/console/auth` 与 `GET /api/console/gate` | 先登录拿 `cc_admin` cookie（12 小时有效） |
| 控制台路径 403 `console_local_only` | 来源不是回环 | 用 §7 的 SSH 隧道 |
| 启动日志 `Failed to enable account pool` | 口令 < 8 位，或 Node < 22.5（没有 `node:sqlite`） | 换 ≥8 位口令 / 升级 Node；不影响形态 A 的代理本身 |
| 日志里一串 `fetch failed` | 代理进程出站连不上 `CC_API_BASE`（DNS / TLS / 防火墙 / 区域网络） | 在同一个容器或主机里直连验证：`docker exec -it <容器> wget -qO- $CC_API_BASE` |
| 客户端收到 503 `NO_AVAILABLE_ACCOUNT` | 池里没有可用账号（都在冷却或被停用） | 打开控制台看账号页，或补账号 |
| 端口占用 `EADDRINUSE` | 3050 已被占用 | `PORT=3060 ...` 或释放端口 |
| 反代后面间歇 502（含 `Broken pipe`） | keep-alive 时序反了 | 见 §6 第 2 条 |
| 请求返回 413 | 请求体超过 `CC_MAX_BODY_MB` | 调大该值（注意内存放大，见 §1） |
| 容器重建后账号没了 | 没有挂 `/data` 卷 | `docker run -v cc-data:/data ...` |
| 命令行改的账号在界面上看不到 | `CC_POOL_DB` 指到了不同的库 | 让两者指向同一个数据库文件 |
