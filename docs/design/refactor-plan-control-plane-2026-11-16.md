# 控制面独立重构计划（2026-11-16）

目标：把 **Gethen 控制面**（工单、调度、证据、协议、生命周期）从 DSH 宿主里独立出来，
DSH 退化为**可插拔的执行面**。Master 可以是 DSH 插件，也可以是独立进程；Worker 是
独立的 `gethen worker` 守护进程，负责管理本机 DSH 的生命周期与会话。

本计划取代 [refactor-plan-gethen.md](../refactor-plan-gethen.md) 中"P4 fleet"之后的部分，
并与 [p4-ws-control-plane.md](p4-ws-control-plane.md) 的 WS 协议合并。

## 0. 三条定论

1. **控制面必须零 DSH 依赖**：`core/master/router/fleet/evidence/pack/evolve/memory-client/
   worker-protocol` 不 import DSH；DSH 只在 `@gethen/dsh` 适配层出现。
2. **执行面可插拔**：DSH 是第一个执行器（会话/工具/技能/沙箱），以后可接 OpenCode、
   Pi、Codex；Gethen 不重写 agent loop。
3. **进程形态**：
   - Master：`gethen serve`（独立）或 DSH 插件（宿主适配），二者共用同一控制面；
   - Worker：`gethen worker` 守护进程（systemd），监督本机 DSH profile，出站连 Master；
   - DSH 内只保留会话内工具（`worker.js`：`gethen_escalate/gethen_report`）。

## 1. 现状与问题

| 现状 | 问题 |
|---|---|
| `master` 引擎已零 DSH（纯函数 + sqlite） | 好，保留 |
| `@gethen/dsh` 同时做装配 + 控制面入口 | Master 无法脱离 DSH 独立运行 |
| Worker 侧只有 `worker.js` 会话内工具 | 没有进程能启停/回收本机 DSH，fleet 的 ControlOp 无处执行 |
| 传输依赖 `ds-harness-remote`（第三方中继） | 数据/账号依赖第三方，生命周期能力弱 |
| `router`/`fleet` 未实现 | 多机调度、复用、排队都没有 |

## 2. 目标形态

```
┌ Master 进程（gethen serve）或 DSH 插件 ─────────────────────────┐
│ core/master 引擎 · router 调度 · fleet 注册表 · evidence/evolve  │
│ transport-ws：/gethen/worker 端点、心跳、租约、dispatch 队列     │
│ memory-client → gethen-memory（HTTP）                           │
└───────────────┬─────────────────────────────────────────────────┘
                │ wss（出站由 Worker 发起；Master 需可达）
┌───────────────▼──────────── Worker 机器 ────────────────────────┐
│ gethen worker（systemd 守护进程）                               │
│  · 出站 WS + token、hello/heartbeat、本地事件队列               │
│  · 管理 DSH 进程：runtime.start/stop、workspace.prepare          │
│  · slot：创建/回收执行会话（通过本地 DSH 适配器）                │
│      │ 本地 UDS / SDK                                           │
│      ▼                                                          │
│ DSH profile（每个项目一个）：会话、工具、技能、沙箱              │
│  └─ worker.js：gethen_escalate / gethen_report                  │
└─────────────────────────────────────────────────────────────────┘
```

网络要求：Worker → Master 出站可达（公网/VPN/SSH 隧道）；Worker 不监听公网端口。

## 3. 目标包结构

| 包 | 职责 | 依赖 | 含 IO/DSH |
|---|---|---|---|
| `core` | 契约/ID/Digest/错误码/信封常量 | 无 | 无 |
| `master` | 引擎、store、outbox、policy、sources、executor（LaunchPort 驱动） | core | 有（sqlite） |
| `router` | fleet 级调度、升级 gate | core, pack, fleet | 无 |
| `fleet` | Node/Runtime/Slot、ControlOp 校验、期望态对账 | core | 无 |
| `evidence` | receipts/eval/canary | core | 无 |
| `pack` / `evolve` | 能力包与演化 | core/evidence | 无 |
| `memory-client` | gethen-memory HTTP 客户端 | core | 有（HTTP） |
| **`worker-protocol`**（新） | WS 帧类型、校验、版本、ControlOp | core | 无 |
| **`transport-ws`**（新） | Master 端点（DSH 插件或独立 http+ws）、连接管理、token、lease | core, worker-protocol, master, router | 有 |
| **`worker-agent`**（新） | Worker 守护进程：WS 客户端、DSH 生命周期、slot、本地代理 | core, worker-protocol | 有 |
| `dsh` | 宿主适配：插件入口（Master 用）、`worker.js`（会话工具）、面板 | 上述 | 有 DSH 接口（仅类型/窄面） |
| `bridge-remote` | 保留的 Remote 适配器（可选，验证后下线） | worker-bridge | 有 |
| `cli` | `gethen serve/worker/doctor/fleet/pack` | 多 | 有 |
| `panel` | 产品投影（后期） | core/master/router/fleet | 无 |

依赖方向：`core` 零依赖；纯包（router/fleet/evidence/pack/evolve/worker-protocol）禁 IO；
`dsh`/`cli`/`transport-ws`/`worker-agent` 是 IO/宿主层。

## 4. 协议（沿用 p4-ws）

- 传输：`wss://<master>/gethen/worker`，`Authorization: Bearer <worker-token>`；
- 帧：`hello/hello_ok/heartbeat/dispatch/accepted/event/escalation/report/resolution/cancel/control/control_result/error`；
- 幂等：`assignmentId` / `escalationId` / `operationId`；
- 心跳 10s，30s 无心跳判 offline；断线 Assignment 记 `unknown`，重连对账；
- 背压：事件有界队列可丢（带 seq 与 dropped 计数），`report/escalation` 必达。

## 5. 进程与部署

| 组件 | 形态 | 说明 |
|---|---|---|
| Master | `gethen serve` 或 DSH 插件 | 独立进程时自带 HTTP+WS；插件时复用 `webServer.registerUpgrade` |
| Worker daemon | `gethen worker`（systemd user/ system unit） | 出站连接、生命周期、slot |
| DSH runtime | 每项目一个 profile，由 daemon 启停 | 执行器；worker.js 随会话加载 |
| 记忆 | gethen-memory（独立 HTTP） | 不变 |

systemd 示例（Worker）：

```ini
[Service]
ExecStart=/usr/local/bin/gethen worker --config /etc/gethen/worker.json
Restart=always
Environment=GETHEN_DSH=/usr/local/bin/dsh
```

## 6. 迁移阶段

### R0 边界固化（1 天）

- 在 layering 检查里加规则：`core/master/router/fleet/evidence/pack/evolve/memory-client/
  worker-protocol` 禁止 `@deepseek-ai/*` 与 `node:http/ws`；
- 把 `@gethen/dsh` 里非装配逻辑（若有）移出；
- 验收：`pnpm check` 绿，依赖方向自动化。

### R1 Master 可独立运行（3 天）

- 新增 `gethen serve`：加载配置 → `MasterService`（sqlite+outbox）→ HTTP/WS 端点骨架
  → `/health`；
- `@gethen/transport-ws` 雏形：端点注册、连接对象、无业务帧；
- DSH 插件保留，改为"宿主适配器"（复用同一 `transport-ws`）；
- 验收：不启 DSH 也能 `gethen serve` 跑 fake launch 的工单闭环；插件形态不回归。

### R2 WS 协议 MVP（4 天）

- `@gethen/worker-protocol`：帧类型 + 校验 + 契约测试（内存 duplex）；
- Master：token 发放/校验、hello/heartbeat、dispatch 队列、report 落库；
- Worker：先做 **DSH 内 agent 插件**最小客户端（hello/dispatch/report），验证协议；
- 验收：本机两个进程（Master 独立 + DSH agent）完成一次任务；确定性脚本覆盖
  dispatch→event→report 与断线重连。

### R3 Worker 守护进程（5 天）

- `@gethen/worker-agent`：systemd 常驻；WS 客户端、重连、本地事件队列；
- 管理 DSH：`runtime.start/stop`（systemd 白名单）、`workspace.prepare`；
- slot：与 DSH 会话对接（优先本地 UDS 代理到 DSH 内插件；后续可换 DSH SDK/stdio）；
- ControlOp 校验 + 审计；Master 侧 supervisor 与租约；
- 验收：Master 能起停 Worker 的 DSH、创建/回收 slot；Worker 重启自动重连并补报。

### R4 Fleet 与调度（5 天）

- `@gethen/fleet`：Node/Runtime/Slot 模型、`planReconcile()`；
- `router`：fleet 级过滤/打分、REUSE/START/QUEUE/PARK；
- Master 调度结果驱动 `transport-ws` dispatch；quota/负载来自心跳；
- 验收：两台 Worker（可用两个 profile 模拟）跑 reuse/start/queue/park 四种决策。

### R5 观察与收尾（3 天）

- 只读事件流/面板：Master 展示 Assignment 时间线；Worker DSH web 绑 loopback +
  SSH 隧道/VPN 文档；
- `gethen doctor`/`gethen fleet ls` 等运维命令；
- 决定是否下线 `bridge-remote`（保留到 R4 验证通过后）；
- 文档、CHANGELOG、ADR 更新。

总工期约 **4.5 周（21 人日）**。

## 7. 测试与门禁

- 协议契约：内存 duplex 全帧序列；
- 断线场景：dispatch 中/升级中/report 前断线，重连对账；
- 幂等：重复 dispatch/resolution/control；
- 鉴权：坏 token、错 workerId、过期 token；
- 生命周期：ControlOp 白名单、路径逃逸、未知 op；
- e2e：本机起 `gethen serve` + `gethen worker`（两个 DSH profile），跑完
  dispatch→执行→升级→报告；
- 回归：现有 455 项测试保持绿。

## 8. 关键决策（需要确认）

| # | 决策 | 建议 |
|---|---|---|
| D1 | Worker 执行驱动方式 | **修订（见调研笔记）**：daemon spawn `dsh --profile sdk`，用 SDK JSON-RPC（session/prompt、session/event、approval/request、user-question/ask）驱动会话；`worker.js` 工具保留为兼容路径 |
| D2 | Master 独立进程的 HTTP/WS 实现 | 独立形态用 `ws` 库 + `node:http`；插件形态复用 DSH webServer；两者共用 `transport-ws` 协议层 |
| D3 | 人工旁观 | Worker DSH web 绑 loopback + SSH/VPN；Master 提供只读时间线，不做远程交互 UI |
| D4 | dsh-remote 去留 | 保留为可选适配器；R4 验证通过后再决定下线 |
| D5 | 是否拆 `transport-ws` / `worker-agent` | 拆；协议层纯、宿主层薄，便于独立进程与插件复用 |

## 9. 风险

| 风险 | 对策 |
|---|---|
| Master 对 Worker 不可达（NAT） | 要求 VPN/公网/SSH 隧道；不做穿透 |
| DSH 版本变化影响 worker 适配 | 只用窄接口（agents/webServer/session），版本锁 + 契约测试 |
| 双形态（独立/插件）维护成本 | 共享 `transport-ws` 与 `MasterService`，只差宿主入口 |
| 本地 UDS 代理增加一跳 | 先做简单实现；协议不变，后续可替换为 SDK/stdio |
| 范围膨胀 | 按 R0–R5 门禁，fleet/UI 不提前 |

## 10. 参考

- [p4-ws-control-plane.md](p4-ws-control-plane.md)（协议与安全细节）
- [refactor-plan-gethen.md](../refactor-plan-gethen.md)（P0–P3 历史）
- DSH webServer `registerUpgrade`；GitHub Actions self-hosted runners；Buildkite agent；
  Temporal durable execution；multica `dsh-runtime` 协议（本地源码）

## 进展快照（2026-11-16 晚）

| 阶段 | 状态 | 产出 |
|---|---|---|
| R0 边界 | ✅ | CONTROL_PLANE 12 包禁止 DSH import（含回归测试） |
| R1 Master 独立 | ✅ | `gethen serve`（HTTP + `/gethen/worker` upgrade + token + WS LaunchPort） |
| R2 WS 协议 | ✅ | `@gethen/worker-protocol`、`@gethen/transport-ws`、`@gethen/bridge-ws` |
| R3 Worker | ✅* | `@gethen/worker-agent`、`@gethen/sdk-driver`、`@gethen/executor-sdk`、`gethen worker` |
| R4 Fleet/调度 | ✅ | `@gethen/fleet`、`router.scheduleTask`（REUSE/START/QUEUE/PARK） |
| R5 收尾 | 🚧 | 下行 error 帧、真实 SDK e2e、观察/文档、remote 去留 |

\* R3 的真实模型 e2e 正在跑（opt-in `GETHEN_E2E=1`）；approval/question 已映射到升级语义。

验证：

```bash
pnpm verify:ws        # 真实 WS + FakeExecutor：dispatch → report → receipt
GETHEN_E2E=1 node scripts/e2e-sdk.mjs   # 真实 dsh --profile sdk + 模型
```

已知缺口：engine 的 assignment workspace 由 launch 边界补齐（应回到 master）；
CLI serve 的升级默认 parked（后续加 `--escalation`）；worker-agent 重连定时器 unref，
`gethen worker` 用 ref 的 interval 保活。

### 2026-11-17 更新：删除旧通讯协议

C1/C2/C3 完成后，按"不向后兼容"删除 legacy 通讯栈（worker-protocol 帧、
transport-ws、bridge-ws、worker-agent、bridge-remote），Master/Worker/DSH/CLI
统一走 L1 transport + L2 mailbox；`ControlOp` 归入 fleet，token 哈希归入 transport。
见 [comms-two-layer.md](comms-two-layer.md) 的 C7 说明。

### 2026-11-16 收尾完成

- **DSH 形态 Master 已接 WS**：`packages/dsh` 通过 `@gethen/master-runtime` 装配，
  `webServer.registerUpgrade("/gethen/worker")`，`gethen_run` 工具走 WS dispatch；
  无 `ws` 配置时保留 remote/fake 回退。
- **master-runtime**：CLI serve 与 DSH 插件共用装配（gateway+bridge、worker 快照、
  按负载选 worker、升级回调）。
- **token 持久化**：master sqlite `workers` 表（只存 sha256），
  `gethen worker add/ls/revoke`（`--tokens-file` 原子合并、token 只打印一次）。
- **CLI**：`gethen run`、`serve --escalation auto|park`、`worker --dsh-args`。
- **真实 e2e**（`GETHEN_E2E=1`）与 `pnpm verify:ws` 均通过；31 包 / 713 tests。

通讯分层（L1 transport + L2 mailbox）见
[comms-two-layer.md](comms-two-layer.md)。

部署：见 [docs/deploy.md](../deploy.md)（§1.5 DSH 形态 Master + 反向代理）。
