# 部署手册：Master / Worker / 本机验证

本文对应控制面独立重构的 R1–R5：[重构计划](design/refactor-plan-control-plane-2026-11-16.md)、
[WS 控制面协议](design/p4-ws-control-plane.md)。目标形态是 **Master 独立进程**
（`gethen serve`）＋ **Worker 守护进程**（`gethen worker`，出站 `wss` 连 Master）。

```
Browser / 人 ──HTTP──▶ Master (gethen serve)  ◀──wss──  Worker (gethen worker) ──▶ dsh --profile sdk
                         sqlite + outbox                   WS client + SDK JSON-RPC
```

- Master 只跑控制面：工单、策略、dispatch、receipt；**不依赖 DSH**。
- Worker 出站连接 Master，**不监听公网端口**；本机 DSH 以 SDK stdio 子进程方式被执行器拉起。
- 网络要求：Worker → Master 可达（公网、VPN 或 SSH 隧道）；有 TLS 时用 `wss://`。

## 0. 前置条件

| 组件 | 要求 |
|---|---|
| Node | ≥ 22（`gethen doctor` 会检查） |
| DSH | Master 不需要；Worker 需要 `dsh --profile sdk --help` 可用 |
| 模型凭证 | Worker 侧 `OPENCODE_API_KEY` 或 `DEEPSEEK_API_KEY`（doctor 只报告是否设置，不打印值） |
| 端口 | Master 默认绑定 `127.0.0.1`；对外请走 TLS 反向代理或 SSH 隧道 |
| 目录 | 状态目录默认 `$GETHEN_STATE_DIR` 或 `~/.gethen`（`gethen init` 创建） |

本仓尚未发布 npm 包，部署使用仓库检出。把一个包装脚本放到 `PATH` 里便于 systemd 使用：

```bash
# /usr/local/bin/gethen
#!/bin/sh
# tsx 需要仓库根目录的 tsconfig paths 解析 @gethen/*，所以先切到检出目录。
cd /opt/gethen || exit 1
exec node --import tsx packages/cli/src/bin.ts "$@"
```

```bash
chmod 0755 /usr/local/bin/gethen
gethen --help
```

开发机上也可以直接用 `pnpm serve`（等价于 `tsx packages/cli/src/bin.ts serve`）。

## 1. Master 部署

### 1.1 生成 worker token

每个 Worker 一个唯一 token，绑定一个 `workerId`。推荐用 CLI 发 token：明文只打印一次，
Master 的 sqlite `workers` 表只存哈希；同时可写入 `--tokens-file` 供 serve 使用。

```bash
# 首次：先启动一次 Master 以创建 sqlite（无 tokens 时 launch=fake），然后 Ctrl-C
gethen serve --db /var/lib/gethen/master.sqlite

# 发 token（stdout 只显示一次；--tokens-file 原子合并，权限 0600）
gethen worker add \
  --db /var/lib/gethen/master.sqlite \
  --worker-id build-1 --name "build-1" \
  --tokens-file /etc/gethen/worker-tokens.json

gethen worker ls --db /var/lib/gethen/master.sqlite   # 查看已登记/已吊销
```

`/etc/gethen/worker-tokens.json`（可直接给 `gethen serve --tokens`）：

```json
[{ "workerId": "build-1", "token": "5f2c...9a" }]
```

没有 CLI 时也可手写该文件（`openssl rand -hex 32` 生成）。轮换：`gethen worker add --force`
生成新 token 并更新 `--tokens-file`，然后重启 Master；`gethen worker revoke --id build-1`
后旧 token 在下次启动时失效。

### 1.2 启动 Master

```bash
sudo -u gethen env GETHEN_STATE_DIR=/var/lib/gethen \
  gethen serve \
    --tokens /etc/gethen/worker-tokens.json \
    --host 127.0.0.1 --port 8080 \
    --workspace /srv/projects
```

| 参数 / 环境 | 说明 |
|---|---|
| `--db <file>` | master sqlite；默认 `$GETHEN_STATE_DIR/master.sqlite` |
| `--host` / `--port` | 绑定地址/端口；默认 `127.0.0.1:0`（临时端口） |
| `--tokens <file>` | worker 凭证 JSON；也可用环境 `GETHEN_WORKER_TOKENS_JSON` 直接传 JSON 字符串 |
| `--workspace <dir>` | dispatch 给 Worker 的默认工作区（envelope 用；engine 尚无 per-assignment workspace） |
| `GETHEN_STATE_DIR` | sqlite/lock 所在状态目录 |

没有 tokens 时 Master 仍可启动，但使用 **fake launch**（不发 Worker），
`GET /health` 返回 `"launch": "fake"`，`/gethen/worker` 端点不存在。

启动成功输出：

```
gethen master listening on http://127.0.0.1:8080
worker endpoint: ws://127.0.0.1:8080/gethen/worker (launch: ws)
```

HTTP 表面：

| 方法 | 路径 | 用途 |
|---|---|---|
| `GET` | `/health` | `ok`、`uptimeMs`、`tickets`、`open`、`receipts`、`escalations`、`workers`、`launch`(`ws`/`fake`)、`protocol` |
| `POST` | `/gethen/run` | 本地投递一次工单（body `{"text":"..."}`），等待一次 launch 完成 |
| `GET`(upgrade) | `/gethen/worker` | `gethen.worker/v1` WS 端点（仅此路径；其它路径 404） |

### 1.3 TLS / 反向代理

`gethen serve` 自带的是明文 HTTP/WS，生产部署有两种做法：

1. **TLS 终止在反向代理**（推荐）：用 nginx/Caddy/HAProxy 终止 TLS，转发到
   `127.0.0.1:8080`，并转发 WebSocket 升级头与 `Authorization`：

```nginx
location /gethen/worker {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Authorization $http_authorization;
    proxy_read_timeout 3600s;   # 心跳 30s；别让空闲连接被代理提前断开
}
location / {
    proxy_pass http://127.0.0.1:8080;
}
```

    Worker 配置 `masterUrl: "wss://gethen.example.com/gethen/worker"`。

2. **仅内网/VPN/SSH 隧道**：Master 继续 bind loopback，Worker 通过
   `ssh -L 8080:127.0.0.1:8080 user@master` 建隧道后连 `ws://127.0.0.1:8080/gethen/worker`。
   跨不可信网络一律不要明文。

注意：`/gethen/worker` 的鉴权是 `Authorization: Bearer <token>`；反向代理要保留该头，
且不要开 302/重写路径。

### 1.4 systemd unit（Master）

`/etc/systemd/system/gethen-master.service`：

```ini
[Unit]
Description=Gethen master control plane
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=gethen
Group=gethen
WorkingDirectory=/opt/gethen
Environment=GETHEN_STATE_DIR=/var/lib/gethen
EnvironmentFile=-/etc/gethen/master.env
ExecStart=/usr/local/bin/gethen serve \
  --tokens /etc/gethen/worker-tokens.json \
  --host 127.0.0.1 --port 8080 \
  --workspace /srv/projects
Restart=always
RestartSec=2
KillSignal=SIGTERM
TimeoutStopSec=20

# 最小权限
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=/var/lib/gethen
StateDirectory=gethen

[Install]
WantedBy=multi-user.target
```

```bash
sudo install -d -o gethen -g gethen -m 0750 /var/lib/gethen /etc/gethen
sudo systemctl daemon-reload
sudo systemctl enable --now gethen-master
systemctl status gethen-master
journalctl -u gethen-master -f
```

升级/轮换 token：改 `/etc/gethen/worker-tokens.json` 后 `systemctl restart gethen-master`。
只允许一个 Master 进程写同一个 sqlite（`master.lock`）；`gethen status` 是只读的，可随时跑。

### 1.5 可选：DSH 形态的 Master（agent 直接派活）

如果远端跑的是 DSH，可以不起独立 `gethen serve`，而是把 gethen 作为 DSH 插件挂载：
agent 用 `gethen_run` 工具派活，`/gethen/worker` 由 DSH 的 webServer 托管。

```yaml
# ~/.dsh/profiles/<profile>/cordis.patch.yml
- insert:
    - id: gethen
      name: 'file:///home/<user>/.local/share/gethen/dsh/index.js'
- id: gethen
  config:
    dir: /var/lib/gethen
    ws:
      tokensFile: /etc/gethen/worker-tokens.json
      workspace: /srv/projects
    escalation: {}            # 或 { answer: "..." } 自动答复
```

- 插件会在 `webServer` 上注册精确路径 `/gethen/worker`（`gethen.worker.v1`）；
- DSH web 默认绑回环：远端需要反向代理终止 TLS 并转发 `/gethen/worker`
  （`Upgrade`/`Authorization` 头），或用 SSH 隧道；
- agent 工具：`gethen_run` / `gethen_status` / `gethen_overview` / `gethen_escalations`；
- `gethen_status` 额外返回 `workers` 与 `launch: "ws"`；
- 没有 `ws` 配置时退回旧的 remote/fake 路径（行为不变）。

## 2. Worker 部署

### 2.1 配置文件

`/etc/gethen/worker.json`（权限 `0600`）：

```json
{
  "masterUrl": "wss://gethen.example.com/gethen/worker",
  "token": "5f2c...9a",
  "workerId": "build-1",
  "machine": "build-1",
  "provider": "deepseek",
  "model": "deepseek-v4",
  "workspaces": ["/srv/projects"],
  "quota": 1,
  "dshCommand": "/usr/local/bin/dsh",
  "dshArgs": ["--profile", "sdk"]
}
```

| 字段 | 必填 | 说明 |
|---|---|---|
| `masterUrl` | ✅ | Worker WS 地址；也接受 `https://host:port`（自动变 `wss` 并补 `/gethen/worker`） |
| `token` | ✅ | 与 Master tokens 文件中该 `workerId` 的 token 一致 |
| `workerId` | ✅ | 必须与 token 绑定的 id 一致（hello 校验） |
| `provider` / `model` | ✅ | 传给 `dsh --profile sdk` 的 initialize 参数 |
| `workspaces` | 建议 | Worker 本机允许的工作区绝对路径；缺省为启动目录 |
| `machine` | ❌ | hello 中的机器名；缺省 `os.hostname()` |
| `quota` | ❌ | 并发 assignment 上限，默认 `1` |
| `dshCommand` / `dshArgs` | ❌ | 默认 `GETHEN_DSH`/`dsh`、`["--profile","sdk"]` |

也可以用简化参数（覆盖配置文件；适合临时排障）：

```bash
gethen worker \
  --master-url wss://gethen.example.com/gethen/worker \
  --token "$GETHEN_WORKER_TOKEN" --worker-id build-1 \
  --provider deepseek --model deepseek-v4 \
  --workspace /srv/projects
```

启动输出：`worker build-1 connecting to wss://.../gethen/worker`。

### 2.2 运行要求

- `dsh --profile sdk --help` 可用（`gethen doctor` 检查项）；
- 模型凭证放在环境里：`OPENCODE_API_KEY` 或 `DEEPSEEK_API_KEY`（不要写进 worker JSON）；
- `workspaces` 里的目录对该用户可读写（SDK 会话 cwd 就是 workspace）；
- `~/.dsh` 已存在（先手工跑一次 `dsh` 让 DSH 初始化）；
- Worker 不给外部开端口；只需出站访问 Master。

> 注意：当前独立 `gethen serve` 的 WS launch 选择**第一个已连接的 Worker**
> （`bridge-ws` 的默认策略）；多机调度（fleet/router）尚未接入独立 serve。

### 2.3 systemd unit（Worker）

`/etc/systemd/system/gethen-worker.service`：

```ini
[Unit]
Description=Gethen worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=gethen
Group=gethen
WorkingDirectory=/srv/projects
Environment=GETHEN_DSH=/usr/local/bin/dsh
EnvironmentFile=-/etc/gethen/worker.env   # 例: OPENCODE_API_KEY=...
ExecStart=/usr/local/bin/gethen worker --config /etc/gethen/worker.json
Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=30

NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

`/etc/gethen/worker.env`（`0600`）示例：

```bash
OPENCODE_API_KEY=sk-...
# 或 DEEPSEEK_API_KEY=...
```

```bash
sudo systemctl enable --now gethen-worker
journalctl -u gethen-worker -f          # 日志走 stderr → journald
```

SIGTERM 时的关闭顺序：停止重连 → 关闭 WS → 关闭 `dsh --profile sdk` 子进程。
Worker 断线会自动重连（指数退避 1s→30s），进行中的 assignment 不重放，
由 Master 按 `assignmentId` 对账。

## 3. 本机验证

不接真实模型的确定性验证（WS + FakeExecutor，无需 dsh/凭证）：

```bash
pnpm verify:ws
```

真实 `dsh --profile sdk` + 模型的 e2e（opt-in，需要 dsh、模型凭证与额度）：

```bash
GETHEN_E2E=1 node scripts/e2e-sdk.mjs
# 可用 GETHEN_E2E_PROVIDER / GETHEN_E2E_MODEL / GETHEN_E2E_TIMEOUT_MS 覆盖默认值；
# 不带 GETHEN_E2E=1 时脚本只打印 skipped 并退出 0。
```

运维自查：

```bash
gethen doctor                  # 人类可读：ok / warn / info；有 warn 也返回 0
gethen doctor --json           # 结构化；凭证只报是否设置
gethen status                  # 读 master sqlite：tickets/open/receipts/escalations + 最近 10 个 ticket
gethen status --json           # 同上，含 assignment ids
gethen status --db /var/lib/gethen/master.sqlite --json
```

`gethen status` 不启动 outbox/launch、也不抢 `master.lock`，可以对着运行中的 Master 读
（sqlite WAL）；它只反映**持久化状态**，Worker 在线数看 `/health` 的 `workers`。

## 3.1 用 CLI 触发与观察

```bash
# Master 上（或任意能访问 Master 的机器）
gethen run --master http://127.0.0.1:8080 "在 /tmp/demo 下创建 ok.txt"
gethen run --master http://127.0.0.1:8080 --json "..."   # 原样输出 Master 响应

gethen status --db /var/lib/gethen/master.sqlite          # 计数 + 最近工单
gethen worker ls --db /var/lib/gethen/master.sqlite
gethen doctor --json
```

- `report` 为 `ready`/`failed` 时 `gethen run` 退出 0/非 0；`waiting` 表示升级在等人（退出 0）。
- 想要自动答复升级（仅测试用）：`gethen serve --escalation auto`（按 defaultOption/第一个选项回答）。
- Worker 默认 `park`（等人）；生产建议由 DSH agent 或面板回答。

## 4. 故障排查

| 现象 | 排查 |
|---|---|
| Worker 反复重连、`journalctl` 里 401 | token 不一致：核对 Worker JSON 的 `token` 与 Master tokens 文件里同 `workerId` 的项；Master 改过 tokens 后要重启生效 |
| 连接被 404 / 立即断开 | 升级路径不是 `/gethen/worker`；或 Master 没配 tokens（此时 `/health.launch == "fake"`，端点不存在）。先 `curl -s http://127.0.0.1:8080/health` |
| `workers: 0`，投递后 report 为 `failed` | 没有 Worker 在线：检查 Worker 进程、网络/VPN、TLS 证书与代理的 Upgrade 头 |
| 运行结束 report 为 `waiting`、ticket 停在 `person/answer` | Worker 的升级（DSH approval / user-question）被 **parked**：当前独立 serve 没有交互式 resolver，需要面板/后续 `--escalation` 才能自动应答 |
| `dsh --profile sdk --help` 失败 | `gethen doctor` 会 warn：安装/升级 DSH，或设 `GETHEN_DSH` 指向正确二进制 |
| `credentials: none of ... is set` | Worker 运行环境没有模型凭证；写入 systemd `EnvironmentFile` 后重启 Worker |
| `state-dir ... not writable` | 修权限/属主，或设 `GETHEN_STATE_DIR` 到可写目录（systemd 用 `StateDirectory=`） |
| sqlite `database is locked` / 启动即退出 | 同一个 db 只允许一个 Master 写进程；确认没有第二个 `gethen serve`，`master.lock` 属于运行用户 |
| 反向代理下 WS 频繁断 | 代理需 `proxy_http_version 1.1` + `Upgrade`/`Connection` 头，读超时 > 心跳（≥ 60s）；`wss` 证书链对 Worker 可信 |
| 版本/协议不匹配 | `GET /health` 的 `protocol` 应为 `gethen.worker.v1`；两边都从同一仓库版本部署 |

## 5. 备份与升级

- sqlite + WAL：停 Master 后直接拷贝 `$GETHEN_STATE_DIR/master.sqlite*`；或
  `sqlite3 master.sqlite ".backup '/backup/master-$(date +%F).sqlite'"`。
- 升级顺序：停 Worker → 升级检出 → `pnpm install`（依赖变更时）→ 重启 Master → 起 Worker。
- schema 由 `SqliteStore` 幂等创建；不兼容变更按版本迁移脚本处理。

## 6. 安全清单

- [ ] Worker → Master 跨网时启用 `wss`；Master 只 bind loopback 或内网地址；
- [ ] 每个 Worker 独立 token（`openssl rand`），token 文件 `0600`，轮换后重启 Master；
- [ ] Worker 不暴露入站端口；`workspaces` 白名单只放必要目录；
- [ ] 模型凭证只放 systemd EnvironmentFile / 密钥管理，不写 worker JSON、不进仓库；
- [ ] `gethen doctor --json` 的输出可安全贴进工单（凭证值不会被打印）。

### 升级注意：插件 bundle 更新后重启

DSH 的 HMR 对 **bundle 文件替换**并不总是可靠（模块缓存/加载顺序）。升级
`~/.local/share/gethen/dsh/{index.js,worker.js}` 后，如果出现"工具消失"或
`/gethen/worker` 返回 404，重启一次 DSH 即可：

```bash
pkill -f 'dsh --profile <profile>'
dsh --profile <profile> --no-open --port <port>
```

CLI 形态（`gethen serve`）无此问题，重启进程即可。
