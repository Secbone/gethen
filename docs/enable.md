# 启用指南：WS 控制面（Master ↔ Worker）

本文是从零把 Gethen 控制面跑起来的最短路径。架构与设计见
[refactor-plan-control-plane-2026-11-16.md](design/refactor-plan-control-plane-2026-11-16.md)；
完整部署细节（systemd、TLS、备份、排障）见 [deploy.md](deploy.md)。

拓扑：

```
Master（DSH 插件 或 gethen serve）          Worker（本机/云主机）
  /gethen/worker  ← wss 出站连接 ←  gethen worker
  gethen_run 工具 / POST /gethen/run        dsh --profile sdk（执行）
```

## 0. 前置条件

| 角色 | 要求 |
|---|---|
| Master | Node ≥ 24、pnpm；`gethen serve` 或 DSH（含 `webServer`）；能被 Worker 出站访问（公网/VPN/SSH 隧道） |
| Worker | Node ≥ 24、DSH 0.2.2-rc.1、`~/.dsh/.env` 配好模型凭证（如 `OPENCODE_API_KEY`）、构建出 `worker.js` |
| 两者 | 同一份 Gethen 代码（`refactor/superagent` 分支）；token 由 Master 发放 |

```bash
git clone -b refactor/superagent https://github.com/Secbone/gethen.git
cd gethen && pnpm install
node -v        # 建议 >= 24（node:sqlite）
```

## 1. 构建插件 bundle

```bash
pnpm build:dsh ~/.local/share/gethen/dsh
# 产出 index.js、worker.js、dsh-client/、node_modules/ws（vendored）
```

> `worker.js` 由 Worker 的 SDK profile 加载，`index.js` 只给 DSH 形态的 Master 用。

## 2. 发 worker token（Master 上）

```bash
# 先起一次以创建 sqlite（无 tokens 时 launch=fake），Ctrl-C
gethen serve --db /var/lib/gethen/master.sqlite

# 发 token（只打印一次；--tokens-file 原子合并，serve/插件读这个文件）
gethen worker add \
  --db /var/lib/gethen/master.sqlite \
  --worker-id build-1 --name "build-1" \
  --tokens-file /etc/gethen/worker-tokens.json
```

`/etc/gethen/worker-tokens.json`（`0600`）：

```json
[{ "workerId": "build-1", "token": "…" }]
```

管理：`gethen worker ls --db …`、`gethen worker revoke --db … --id build-1`；
轮换用 `--force` 重新发并重启 Master。

## 3. 启用 Master

### 3A. DSH 形态（agent 直接派活）

```yaml
# ~/.dsh/profiles/<profile>/cordis.patch.yml
- insert:
    - id: gethen
      name: 'file:///home/<user>/.local/share/gethen/dsh/index.js'
    - id: gethen-worker
      name: 'file:///home/<user>/.local/share/gethen/dsh/worker.js'
- id: gethen
  config:
    dir: /var/lib/gethen
    ws:
      tokensFile: /etc/gethen/worker-tokens.json
      workspace: /srv/projects
    escalation: {}          # 删掉 = 升级等人；{answer:"…"} = 自动答复
```

重启 DSH：

```bash
pkill -f 'dsh --profile <profile>'
dsh --profile <profile> --no-open --port 3081
```

验证：`gethen_status` 返回 `launch:"ws"`；agent 用 `gethen_run("…")` 派活。

### 3B. 纯 CLI Master

```bash
gethen serve \
  --db /var/lib/gethen/master.sqlite \
  --tokens /etc/gethen/worker-tokens.json \
  --host 127.0.0.1 --port 8080 \
  --workspace /srv/projects
  # 测试期可加 --escalation auto（自动答复）；生产用 park（默认）

curl -s http://127.0.0.1:8080/health    # 期望 workers:0, launch:"ws"
```

远端暴露：nginx/Caddy 终止 TLS 并转发 `/gethen/worker`（保留 `Upgrade` 与
`Authorization` 头）；或者 Worker 侧 `ssh -N -L 8080:127.0.0.1:8080 <master>` 后用
`ws://127.0.0.1:8080`。

## 4. 启用 Worker

1) SDK profile 需要 provider 与 `gethen-worker` 工具。复制
[scripts/e2e/sdk-profile.patch.yml](../scripts/e2e/sdk-profile.patch.yml)，改两处路径：

```yaml
# /etc/gethen/sdk-profile.patch.yml
- id: llm-pi-ai
  config:
    providers:
      opencode-go:
        apiKeyEnv: OPENCODE_API_KEY
- insert:
    - id: gethen-worker
      name: 'file:///home/<user>/.local/share/gethen/dsh/worker.js'
```

2) Worker 配置（`/etc/gethen/worker.json`，`0600`）：

```json
{
  "masterUrl": "wss://<master>/gethen/worker",
  "token": "<第 2 步打印的 token>",
  "workerId": "build-1",
  "provider": "opencode-go",
  "model": "deepseek-v4.1-flash",
  "workspaces": ["/srv/projects"],
  "quota": 1,
  "dshArgs": ["--profile", "sdk", "--patch", "/etc/gethen/sdk-profile.patch.yml"],
  "isolation": "shared",
  // 可选：本机 DNS 不可用/分流时，直接覆盖主机解析（TLS SNI 仍是原域名）
  "dnsOverrides": { "gethen.example.com": "203.0.113.10" },
  // 可选：Master 侧路径 → 本机路径（跨机 worker）
  "workspaceMap": { "/srv/projects": "/home/me/Code/project" }
}
```

3) 启动：

```bash
GETHEN_DSH=$(command -v dsh) gethen worker --config /etc/gethen/worker.json
# 输出：worker build-1 connecting to wss://…
```

systemd unit 见 [deploy.md](deploy.md) §2.3。Worker 默认继承 DSH 的
`workspace-write` + `ask` 权限策略：超出沙箱的工具调用会变成 `permission` 升级，
无人值守时的配置方式（`DSH_PERMISSION_MODE` / approval `never`）见
[deploy.md](deploy.md) §2.4。

## 4.1 多 Worker 与路由

一个 Master 可以同时登记多个 Worker：`worker-tokens.json` 里每个 `workerId` 一项，
启动时不传 `--worker-id` 就会把所有 token 都接入候选池（传 `--worker-id` 仍只允许该
Worker，兼容原单 Worker 部署）。Worker 在 mailbox `hello` 中上报 workspace、model、
capability、quota/health，Master 形成可选池，再按 workspace/model/capability 硬过滤和
affinity/evidence/load 打分，产出：

| 决策 | 含义 |
|---|---|
| `reuse` | 该会话 key/id 已绑定到某 Worker 且仍可用，继续派给它 |
| `start` | 有合格 Worker 且有空闲配额，派给最高分者 |
| `queue` | 合格 Worker 都在满负荷，等待空位（`routing.queueTimeoutMs`，默认 30s） |
| `park` | 没有合法候选（workspace/model/capability 不匹配、离线或未注册） |

单 Worker 时仍走原来的直接派工路径，不引入额外排队或过滤。Worker 配置新增
`costClass`（low/mid/high）和可选的 `capabilities`（promptInjection/skills/mcp/
permissions/streaming/steer/fork）；这些是 Worker 自报的筛选信息，**不是** Master 能
远程强制的权限：sandbox/approval 仍由 Worker 本机 DSH profile 决定，见 §7 和
[deploy.md](deploy.md) §2.4。

观测：

```bash
curl -s https://<master>/gethen/workers   # 每个 worker 的注册/health/load
curl -s https://<master>/health           # 额外含 workerDetails
# DSH 形态：gethen_status 返回 workers 计数与 workerDetails
```

## 5. 验证

```bash
# Master /health：workers 变为 1
curl -s https://<master>/health

# CLI 触发（DSH 形态改用 agent 的 gethen_run 工具）
gethen run --master https://<master> "在 /tmp/demo 下创建 ok.txt，然后报告 ready"
gethen run --master https://<master> --json "…"     # 原样响应
gethen status --db /var/lib/gethen/master.sqlite

# Ticket 生命周期：默认 overview 只列“需要人行动”的项
# （DSH 用 gethen_overview / gethen_ticket；CLI 用 ticket 子命令）
gethen ticket ack <ticketId> --master https://<master>
gethen ticket close <ticketId> --outcome completed --master https://<master>
gethen ticket archive <ticketId> --master https://<master>

# 回答 parked 的升级：ticket 或 assignment 两种入口等价
gethen ticket answer <ticketId> "use main" --option main --master https://<master>
gethen task answer <assignmentId> --text "use main" --option main --master https://<master>
# 答复会送达仍阻塞的 Worker；恢复后的 result 仍回写该 ticket（receipt 追加）

# 运行中任务：状态查询 / 干预
gethen task status <assignmentId> --master https://<master>
gethen task steer <assignmentId> "改用最小改动" --master https://<master>
gethen task cancel <assignmentId> --reason "验证取消" --master https://<master>
gethen task logs <assignmentId> --master https://<master>

# 自动唤醒（默认）：worker 完成/需要决策会直接唤醒 Master 会话，无需轮询
# 需要时可用 gethen_wait 显式等待下一个事件（fallback；timeoutMs 接受数字或数字字符串）
gethen task status <assignmentId> --master https://<master>

# 报告必达：DSH 事件流静默时，worker 先从会话日志恢复最终 report；恢复失败才报 failed。
# 回合正常结束却没有最终 report 也按 failed 结算（不再留成无问题的 waiting）。
# Master 的 launch 静默超时先向 worker 查询状态：worker 已结束时用其保留的 report
# 结算（summary 带 recovered from worker status），仍在活动则续期；确认无活动才落
# failed 并发布 assignment.result(timedOut) 通知，不会把已完成的任务误判为超时。
# 超时 receipt 记录实际路由到的 worker/workspace 与最后活动时间，便于归属排查。
#
# Ticket 收敛：默认 overview 只列 needs-action；ready/failed 报告无人处理 7 天后自动
# close（completed/dropped），再按 outcome retention 归档；Master 每 5 分钟自动 sweep。

# 升级闭环：run 返回 waiting 后先看 escalation，再回答
gethen task answer <assignmentId> --option allow --text "允许" --master https://<master>

# 会话管理：先看目录/历史，再决定新建或复用
gethen session list --master https://<master>
gethen session history <sessionId> --master https://<master>
gethen run --session-id <sessionId> "在已有会话里继续" --master https://<master>
# 首次：新建会话并登记 key；之后用同一个 key 复用（未知 key 会失败，不会静默新建）
gethen run --session-id "" 2>/dev/null || true
# 新建并登记 key 目前通过 API/DSH 工具传 session={"mode":"new","key":"project-x"}
# run 默认 5 秒后返回 waiting/running（settled:false），用 task status 继续跟
gethen run --reuse-key project-x "复用该 key 的会话" --master https://<master>
gethen task status <assignmentId> --master https://<master>

# 本地不花钱的两级验证
pnpm verify:mailbox
GETHEN_E2E=1 node scripts/e2e-sdk.mjs               # 真实 dsh SDK + 模型
```

期望：`report:"ready"`、receipt 落库、`/health.workers=1`、Worker 侧文件创建成功。

## 6. 常见问题

| 现象 | 原因/处理 |
|---|---|
| `health.launch == "fake"` | Master 没读到 tokens：检查 `--tokens`/`tokensFile` 路径与 `gethen serve` 启动参数 |
| Upgrade 返回 404 | 反代没转发 `/gethen/worker`（需 `Upgrade`），或 DSH 插件没加载（重启一次） |
| Upgrade 返回 401 | token 不一致：重新 `gethen worker add` 或核对 worker.json |
| `workers: 0` | Worker 没连上：网络/隧道、`masterUrl` 协议（ws/wss）、token、`hello` 校验 |
| 任务 `report:"waiting"` | 升级在等人：`--escalation auto`（测试）或用 `gethen ticket answer` / `gethen task answer` 人工答复；检查 `gethen_escalations` / `gethen_status.escalations` |
| 默认 overview 为空但任务仍在 | 已 ack 或已 closed/archived：用 `gethen_overview filter=all` 或 `gethen status` 查看全部；`open` 只看未结束项 |
| Worker 侧 `dsh` 启动失败 | `dshArgs`/patch 路径错、SDK profile 缺 provider、凭证未配置 |
| 插件工具消失 / 路由失效 | bundle 更新后重启 DSH（HMR 不可靠） |

## 7. 安全要点

- 只出站：Worker 不监听端口；Master 端点必须 TLS（wss）或走 VPN/SSH 隧道；
- 每个 Worker 独立 token，只存 hash 于 Master；撤销后重启 Master 生效；
- `workspaces`/`dshArgs` 是 Worker 本机配置，不接受 Master 任意路径；
- `gethen worker add` 的明文 token 只显示一次，写入 `0600` 文件。
