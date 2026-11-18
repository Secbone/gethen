# 部署手册：Master / Worker / 本机验证

> 想最快跑通？先看 [启用指南 enable.md](enable.md)，本文是完整部署与运维参考。

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
    --workspace ws-collab
```

| 参数 / 环境 | 说明 |
|---|---|
| `--db <file>` | master sqlite；默认 `$GETHEN_STATE_DIR/master.sqlite` |
| `--host` / `--port` | 绑定地址/端口；默认 `127.0.0.1:0`（临时端口） |
| `--tokens <file>` | worker 凭证 JSON；也可用环境 `GETHEN_WORKER_TOKENS_JSON` 直接传 JSON 字符串 |
| `--workspace <id>` | dispatch 给 Worker 的逻辑 workstream id（默认 `default`）；不是 Master 文件系统路径 |
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
| `POST` | `/gethen/run` | 本地投递一次工单（body `{"text":"...","waitMs?":n}`）；等待 launch 结束并落 receipt，返回 `settled` |
| `GET` | `/gethen/tasks` | 当前已知 assignment 的实时状态（phase/step/lastTool/lastMessage） |
| `GET` | `/gethen/tasks/:id[?timeoutMs=n]` | 查询单个 assignment（缓存优先，否则向 worker 发 status 请求） |
| `POST` | `/gethen/tasks/:id/steer` | `{"text":"..."}` 给运行中的 session 追加输入（best-effort） |
| `POST` | `/gethen/tasks/:id/cancel` | `{"reason":"..."?}` 取消执行（worker 发 failed/cancelled report） |
| `GET` | `/gethen/tasks/:id/events?limit=&afterSeq=&type=` | 该 assignment 的事件时间线（worker 事件 + 合成 `gethen.result`，内存环形缓存） |
| `GET` | `/gethen/sessions?timeoutMs=` | Worker 上的 DSH session 目录（config/state/lastActivity/eventCount 等，用于复用决策） |
| `GET` | `/gethen/sessions/:id/history?limit=&afterSeq=&timeoutMs=` | 某个 session 的历史事件 |
| `POST` | `/gethen/tasks/:id/answer` | 回答 parked escalation：`{"kind":"answer"|"escalate"|"drop","option?":"...","text":"..."}`；worker 恢复后的 late result 会更新 Ticket/receipt |
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
  --workspace ws-collab
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
      workspace: ws-collab
    escalation: {}            # 或 { answer: "..." } 自动答复
```

- 插件会在 `webServer` 上注册精确路径 `/gethen/worker`（`gethen.worker.v1`）；
- DSH web 默认绑回环：远端需要反向代理终止 TLS 并转发 `/gethen/worker`
  （`Upgrade`/`Authorization` 头），或用 SSH 隧道；
- agent 工具：`gethen_run` / `gethen_status` / `gethen_overview` / `gethen_escalations`；
- `gethen_status` 额外返回 `workers` 与 `launch: "ws"`；
- 没有 `ws` 配置时退回旧的 remote/fake 路径（行为不变）。

### 1.6 告警、retention 与 dead letter

`GET /health`、`gethen_status`、`gethen_task action=status` 返回：

- `warnings: []`：非零退化项。默认阈值为 1，可用 `warnings` 配置项调整；当前覆盖
  `recovered_without_report`、`mailbox_dead`、`worker_offline`、`queue_timeout`。
- `settlements`：`directCompleted` / `recoveredCompleted` / `recoveredWithoutReport`。
- `scheduling`：队列/运行/复用/超时计数。

retention：

```bash
gethen serve ... --retention-ms 86400000    # 或 GETHEN_RETENTION_MS=86400000
```

该值统一覆盖 merged/completed/closed/dropped/stopped 的归档窗口；DSH 插件用
`config.retentionMs`。默认仍是 completed/closed 7 天、merged 30 天、dropped/stopped 1 天。

dead letter：

```bash
gethen mailbox list  --mailbox mailbox:master [--limit 20]
gethen mailbox replay --mailbox mailbox:master [--id <id> ...]
gethen mailbox clear  --mailbox mailbox:master [--id <id> ...]
```

`list` 只读；`replay` 把 dead 行改回 ready、attempts=0，让运行中的 bridge/worker 重新
消费；`clear` 删除。默认邮箱库 `<GETHEN_STATE_DIR>/mailbox.sqlite`，可用 `--db` 指定。

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
| `dnsOverrides` | 可选 | `{"master.example.com":"203.0.113.10"}`，DNS 分流/伪造时保留真实域名与 TLS SNI |
| `requestTimeoutMs` | 可选 | 单次 SDK JSON-RPC 超时（默认 600000；不再限制整轮时长） |
| `isolation` | 可选 | `shared`（默认）或 `process`（每任务独立 dsh，cancel 可精确终止） |
| `workspaceMap` | 可选 | `{"/srv/projects":"/home/me/project"}`，Master 下发路径映射到本机路径 |
| `workerId` | ✅ | 必须与 token 绑定的 id 一致（hello 校验） |
| `provider` / `model` | ✅ | 传给 `dsh --profile sdk` 的 initialize 参数 |
| `workspaces` | 建议 | 旧版工作区绝对路径；与 `workstreams` 二选一，缺省为启动目录 |
| `workstreams` | 推荐 | 逻辑 workstream 声明（`id/repo/root?/base?/remote?/isolation?`），见 §2.2 |
| `workspaceRoot` | 可选 | Worker 拥有的 worktree/clone 父目录；默认 `<stateDir>/workstreams/<workerId>` |
| `machine` | ❌ | hello 中的机器名；缺省 `os.hostname()` |
| `quota` | ❌ | 并发 assignment 上限，默认 `1` |
| `dshCommand` / `dshArgs` | ❌ | 默认 `GETHEN_DSH`/`dsh`、`["--profile","sdk"]` |

也可以用简化参数（覆盖配置文件；适合临时排障）：

```bash
gethen worker \
  --master-url wss://gethen.example.com/gethen/worker \
  --token "$GETHEN_WORKER_TOKEN" --worker-id build-1 \
  --provider deepseek --model deepseek-v4 \
  --workspace ws-collab
```

启动输出：`worker build-1 connecting to wss://.../gethen/worker`。

### 2.2 工作区隔离与交付（推荐）

Master 只下发逻辑 workstream id（如 `ws-collab`），Worker 在自己的目录里创建 git
worktree，绝不写 Master 的仓库：

```json
{
  "workstreams": [
    {
      "id": "ws-collab",
      "repo": "/home/gethen/Code/gethen",
      "root": "/home/gethen/workstreams/ws-collab",
      "base": "origin/refactor/superagent",
      "remote": "origin",
      "isolation": "worktree"
    }
  ],
  "workspaceRoot": "/home/gethen/workstreams"
}
```

- `repo` 是 Worker 自己的 clone；`root` 必须在 repo 之外（默认
  `<workspaceRoot>/<id>`）。`isolation` 支持 `worktree`（默认）与 `clone`。
- 分支由约定生成：`feat/<id>`（不能覆盖）。Worker 在 `ready` 时校验分支、补提交
  剩余改动并 `git push --set-upstream <remote> feat/<id>`；push 失败会把报告改为
  `failed`。
- Worker 在任务提示后追加 `[GETHEN-DELIVERY]`，要求报告写明 commit、测试命令与结果、
  风险；验证后的证据追加到 `summary` 的 `delivery:` 行。
- 配置了 `workstreams` 后，未映射的绝对路径会被拒绝（`WORKSTREAM_NOT_OWNED`）；
  旧部署仍可用 `workspaces`/`workspaceMap`，但不再具备默认隔离。

### 2.3 运行要求

- `dsh --profile sdk --help` 可用（`gethen doctor` 检查项）；
- 模型凭证放在环境里：`OPENCODE_API_KEY` 或 `DEEPSEEK_API_KEY`（不要写进 worker JSON）；
- `workstreams[].repo` 是 Worker 本机的 clone 且对该用户可读写；Materialize 出来的 worktree 位于 Worker 拥有的 `workspaceRoot` 下；
- `~/.dsh` 已存在（先手工跑一次 `dsh` 让 DSH 初始化）；
- Worker 不给外部开端口；只需出站访问 Master。

> 注意：`gethen serve` 与 DSH 插件都通过 `@gethen/master-runtime` 的 fleet 路由支持多 Worker；
> `--worker-id` 只用于把部署限制到单个 Worker。

### 2.4 systemd unit（Worker）

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
WorkingDirectory=/home/gethen
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

### 2.5 Worker 权限策略与升级

Worker 没有单独的权限配置：sandbox 与 approval 由 DSH 的 SDK profile 组合决定，
Worker 只是用 `dshArgs` 启动 `dsh --profile sdk`。DSH 0.2.2-rc.1 的 base profile
默认是 `workspace-write` + `ask`：

- 工具调用超出 workspace-write 沙箱时，DSH 发 `approval/request`；
- gethen executor 把它映射成 `permission` 升级（选项 `allow` / `reject`），经 mailbox
  上报；Master 没配 resolver 时 `park()`，`gethen run` 返回 `waiting`；
- 用 `gethen ticket answer` / `gethen task answer` / DSH `gethen_task action=answer`
  / `POST /gethen/tasks/:id/answer` 答复；`allow` 映射为 DSH `allowed-once`，
  其它值一律拒绝。

常用策略（全部在 Worker 本机配置，Master 无法远程修改）：

1. **有人值守（默认，推荐）**：保持 `workspace-write` + `ask`，靠 answer 推进；
   `gethen task logs <assignmentId>` 可看到 DSH 的 approval 记录。
2. **无人值守但仍要沙箱**：在 `/etc/gethen/sdk-profile.patch.yml` 里把审批策略改成
   `never`，需要审批的调用会确定性拒绝（fail closed），不会停在那里等人：

   ```yaml
   # /etc/gethen/sdk-profile.patch.yml
   - id: approval
     config:
       policy: never
   ```

   也可以保留 `ask`，在 Master 用 `--escalation auto`（按升级声明的 default/第一个
   选项自动答复，对 permission 就是 `allow`）；这只建议在可信环境使用。
3. **完全放开（危险）**：给 Worker 进程加环境变量
   `DSH_PERMISSION_MODE=danger-full-access`（systemd 的 `Environment=`），base profile
   会把 bash sandbox 设为 full access、approval 设为 `never`，不再产生 permission
   升级。仅限一次性/隔离机器，不要在生产或共享主机启用。

确认当前策略：Worker 日志里 `dsh --profile sdk` 的启动参数、`~/.dsh/profiles/sdk`
的 patch 层，以及任务上的 `waiting`/`permission` 记录。Master 侧的自动/人工答复见
[enable.md](enable.md) §5。

### 2.6 新增第二台 worker 接入清单

1. **签发 token**（Master 上执行，只显示一次）：

   ```bash
   gethen worker add --worker-id build-2 --tokens-file /etc/gethen/worker-tokens.json
   ```

2. **准备 Worker 自己的 repo**：`git clone <origin> /home/gethen/Code/gethen`，切到
   主线 ref（如 `refactor/superagent`）并 `git fetch`。不要指向 Master 的仓库路径。

3. **写 `/etc/gethen/worker.json`**（权限 0600）：

   ```json
   {
     "masterUrl": "wss://gethen.example.com/gethen/worker",
     "token": "<token>",
     "workerId": "build-2",
     "machine": "build-2",
     "provider": "opencode-go",
     "model": "deepseek-v4.1-flash",
     "workstreams": [
       {
         "id": "ws-collab",
         "repo": "/home/gethen/Code/gethen",
         "base": "origin/refactor/superagent",
         "remote": "origin",
         "isolation": "worktree"
       }
     ],
     "workspaceRoot": "/home/gethen/workstreams",
     "quota": 1,
     "dnsOverrides": { "gethen.example.com": "203.0.113.10" }
   }
   ```

   `workstreams` 是主推荐；旧部署可继续用 `workspaces` + `workspaceMap`。
   `workspaceMap` 只用于显式跨机路径映射，不会默认暴露 Master 仓库。`repo` 必须是
   Worker 自己的 clone，`root`/`workspaceRoot` 必须在 repo 外。

4. **权限模式**：Worker 的 DSH 沙箱仍由本机决定。默认 `workspace-write` + `ask`；
   长任务无人值守可设 `DSH_PERMISSION_MODE=allow`（或等价 profile 配置），但不要
   把 Master 目录放进可写范围；需要审批时保持 `ask`，Master 会收到 permission escalation。

5. **启动并自检**：

   ```bash
   gethen doctor --worker-config /etc/gethen/worker.json
   gethen worker --config /etc/gethen/worker.json
   curl -s http://127.0.0.1:8080/health    # Master 侧：workers 增加、warnings 为空
   curl -s http://127.0.0.1:8080/gethen/workers
   ```

   `gethen doctor` 会校验 workstream 的 id、repo 是否为 git 仓库、root 是否在 repo 外；
   Worker 首次 hello 后 `/gethen/workers` 出现新 id 且 `registered=true`。

6. **验收**：从 Master 发一个 `echo` canary 到该 workstream，确认 receipt 的 worker 是
   新 id、summary 含真实内容、`directCompleted` +1；再让两个 workstream 并发各改一个
   文件，确认各自 `feat/<id>` 分支和 worktree 不共享。

## 3. 本机验证

不接真实模型的确定性验证（WS + FakeExecutor，无需 dsh/凭证）：

```bash
pnpm verify:mailbox
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
gethen task status <assignmentId> --master http://127.0.0.1:8080
gethen task steer <assignmentId> "先跑测试再改" --master http://127.0.0.1:8080
gethen task cancel <assignmentId> --reason "停止" --master http://127.0.0.1:8080
gethen task logs <assignmentId> --master http://127.0.0.1:8080
gethen session list --master http://127.0.0.1:8080
gethen session history <sessionId> --master http://127.0.0.1:8080
gethen run --reuse-key project-x "在已有会话里继续" --master http://127.0.0.1:8080
gethen task answer <assignmentId> --option allow --text "允许执行" --master http://127.0.0.1:8080
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
| Worker 已做完但 Master 没收到最终 report | 事件流静默时 worker 会从 DSH 会话日志恢复 report，恢复不到才报 `failed`；Master 的 launch 静默超时也会发 `assignment.result`(`timedOut`) 通知，可用 `gethen_wait` 或 `gethen_status` 查看 |
| overview 里历史 ticket 一直堆积 | `ready`/`failed` 报告无人处理 7 天后会自动 close 并按 outcome retention 归档；`gethen_overview filter=all` 可查看全部 |
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