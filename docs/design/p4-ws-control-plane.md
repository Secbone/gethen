# P4-WS：无中继的 Worker 控制面（方案 B）

日期：2026-11-16。目标：用自建的 **出站 WebSocket 控制面**替换 `ds-harness-remote`
作为 Master↔Worker 的传输，不依赖第三方中继，同时把 Worker 生命周期纳入 Master 管理。

状态：设计提案。落地前保留 `remote` 适配器作为可选项。进程形态与控制面独立方案见
[控制面独立重构计划](refactor-plan-control-plane-2026-11-16.md)。

## 0. 结论

- 可行，且和 P4 fleet 调度天然契合：Master 需要 Worker 的健康/负载/能力，WS 控制面
  正好承载这些信号。
- Master 端点用 DSH 自带的 `webServer.registerUpgrade(pathname)` 注册 **精确路径的
  upgrade 路由**，不需要新增 HTTP 框架（已确认 API 存在，且服务器不做认证，认证由
  路由 owner 负责）。
- Worker 不监听任何端口，只出站连接 Master；`wss` + 每 Worker 独立 token。
- 执行面仍在 Worker 的 DSH 里（`ctx.agents` 创建会话），Worker 继续挂 `worker.js`
  提供 `gethen_escalate/gethen_report`。
- Master 引擎、升级闭环、证据层、`LaunchPort` 契约都不变，只新增一个适配器。

## 1. 调研摘要

| 来源 | 结论/采用 |
|---|---|
| DSH `@deepseek-ai/dsh-host-webserver` README（本地） | `register(route)` 注册 HTTP，`registerUpgrade(route)` 注册**精确 pathname 的 upgrade**；重复路径抛错；upgrade handler 拥有握手与连接内容；服务器**不提供**认证，route owner 自行准入 |
| [multica dsh-runtime 源码](../../../../Projects/dsh-multica-runtime/src/protocol.ts)（本地） | JSONL 协议 v1：`execute/cancel` 下行，`ready/text/tool_call/usage/result` 上行；每任务一个子进程；幂等 `request_id` |
| [GitHub Actions: self-hosted runners](https://docs.github.com/en/actions/concepts/runners/self-hosted-runners) | runner 出站连接并**轮询**任务，不要求入站；适合 NAT 后的机器 |
| [Buildkite agent](https://github.com/buildkite/agent) | agent 主动 poll 平台、执行 job、回报状态与日志；控制面不需要能连 agent |
| [Temporal 持久执行](https://docs.temporal.io/evaluate/understanding-temporal) | 任务有 id、重试、心跳；断线后按事件历史恢复 |
| [Transactional outbox](../../docs/design/p1-master-research.md) | dispatch/report 走持久队列 + operationId 幂等 |

共同点：**出站、长连接、心跳、带 id 的任务、断线可恢复**；控制面不需要能反向连入。

## 2. 目标与非目标

做：

- Master 一个 WS 端点 + Worker 一个出站 agent；
- 任务派发、事件流、报告、取消、升级/答复；
- Worker 注册（能力/工作区/负载/模型）、心跳、离线判定；
- 生命周期控制（`runtime.start/stop`、`workspace.prepare`、`slot.create/close/drain`）；
- token 发放/撤销、TLS、审计；
- 断线重连与任务恢复（至少到 `report` 幂等）。

不做（本期）：

- 公网自动穿透（无中继意味着要求 Master 对 Worker 可达，或借助 VPN/SSH 隧道）；
- 像 dsh-remote 那样的内置远程 UI（人工旁观另给方案，见 §8）；
- 多 Master / 共识；跨机房调度。

## 3. 架构

```
Master（DSH，公网/VPN 可达）
  webServer.registerUpgrade("/gethen/worker")  ← wss
  WorkerSupervisor  注册表 / 心跳 / 租约 / dispatch 队列
  WsLaunchPort implements LaunchPort
       │  dispatch / resolution / cancel / control
       ▼
Worker（DSH，出站）
  worker-agent 插件：wss 连 Master
  hello: {workerId, machine, workspaces[], capabilities[], load, models[], protocol}
  执行：ctx.agents.create(sessionId=assignmentId) → 发送 assignment 信封
       订阅事件 → event 帧；检测到 REPORT/ESCALATION → report 帧
  worker.js 工具：gethen_escalate / gethen_report（已有）
```

## 4. 协议 `gethen.worker/v1`

- 传输：`wss://master/gethen/worker`，`Sec-WebSocket-Protocol: gethen.worker.v1`；
- 帧：单条 JSON 对象，必有 `type`、`v:1`；大 payload 用分帧或引用（文件走独立上传）；
- 认证：`Authorization: Bearer <worker-token>`；Master 存 token 的 hash，绑定 workerId；
- 顺序：同一连接内 FIFO；`dispatch` 的 `assignmentId` 是幂等键。

下行（Master → Worker）：

```ts
{ type:"hello_ok", workerId, serverTime, heartbeatMs }
{ type:"dispatch", assignmentId, ticketId, workspace, packDigest, envelope, model?, deadlineAt? }
{ type:"resolution", assignmentId, escalationId, resolution:{kind:"answer"|"escalate"|"drop", option?, text} }
{ type:"cancel", assignmentId, reason? }
{ type:"control", operationId, op: ControlOp }
```

上行（Worker → Master）：

```ts
{ type:"hello", workerId, protocol:1, machine, platform, runtimes[], workspaces[],
  capabilities:{...}, load:{running,quota}, models[], token? (或走 header) }
{ type:"heartbeat", at, load:{running,quota} }
{ type:"accepted", assignmentId }
{ type:"event", assignmentId, seq, at, event:{type:"message"|"tool.start"|"tool.result"|"usage", ...} }
{ type:"escalation", assignmentId, escalationId, kind, question, options[], defaultOption?, blocking:true }
{ type:"report", assignmentId, kind, summary, question?, usage?, receipt:{packDigest, events} }
{ type:"control_result", operationId, ok, state?, error? }
{ type:"error", code, message, assignmentId? }
```

规则：

- **幂等**：同 `assignmentId` 重复 `dispatch` 返回 `accepted` 并复用现有执行；
  `resolution` 按 `escalationId` 幂等；`control` 按 `operationId` 幂等。
- **心跳**：Worker 每 10s 发；Master 30s 无心跳判 `offline`，进行中的 Assignment
  记 `unknown`（不自动判失败），Worker 重连后按 `assignmentId` 对账。
- **背压**：Master 每连接有界队列（默认 512 帧）；`event` 满时丢弃最旧并置
  `droppedEvents`，`report`/`escalation` 永不丢（阻塞写入或断线重试）。
- **取消**：`cancel` 后 Worker 终止会话并回 `report{kind:"failed", summary:"cancelled"}`。
- **版本**：`hello` 带 `protocol` 与 `minMaster`；不匹配回 `hello_ok` 或 `error` 拒绝。

## 5. 状态机

Worker：`connecting → online(idle|busy) → draining → offline`；`draining` 不再接新
dispatch，现有 Assignment 完成后转 offline。

Assignment（Master 侧）：

```
queued → dispatched → accepted → running ⇄ waiting(escalation)
                                   └→ reported → ended
                        └→ unknown (断线) → 重连对账 → running/reported/failed
```

## 6. Master 侧实现

新增包：

| 包 | 职责 |
|---|---|
| `@gethen/worker-protocol` | 帧类型、校验、版本、ControlOp 类型（纯，无 IO） |
| `@gethen/master-ws` | WS 端点装配、连接/心跳/租约、dispatch 队列、token 校验、审计 |
| `@gethen/dsh`（扩展） | `applyMaster()` 里注册 `registerUpgrade("/gethen/worker")`，把 `WsLaunchPort` 接进现有 `MasterService` |

要点：

- `WorkerSupervisor`：`Map<workerId, Connection>`；每连接一个读循环 + 发送队列；
- token：CLI/面板生成（`gethen worker add`）→ 只显示一次，存 `sha256`；
  表 `workers(id, name, token_hash, capabilities_json, last_seen_at, state)`；
- dispatch：先写 `assignments`（带 operationId），再入队发送；`report` 到达后
  走现有 `engine.report` + `receipts.append`；
- `WsLaunchPort.launch()`：入队 dispatch → 等 `report`/`escalation`；升级交给
  `resolveEscalation` 回调（与 remote 适配器同一套），parked 时返回 `waiting`；
- 与 fleet 打通：`routeWorkers` 的输入从 `WorkerManifest` 改为 supervisor 快照。

## 7. Worker 侧实现

新增包 `@gethen/worker-agent`，作为 DSH 插件（可构建成 `agent.js` 条目）：

- 配置：`masterUrl`、`token`（或 `tokenFile`）、`workspaces` 允许列表、`quota`、
  `runtimes`、`models`；
- 启动：连接 → `hello` → 心跳；断线指数退避重连（1s→30s，带 jitter）；
- 执行：`dispatch` 用 `ctx.agents.create({sessionId: assignmentId, meta:{cwd}})`；
  发送信封（复用 `@gethen/master` 的 `buildAssignmentPrompt` 格式）；订阅事件转
  `event` 帧；扫到 `[GETHEN-REPORT]/[GETHEN-ESCALATION]` 发对应帧；
- 继续：收到 `resolution` 用 `agent.send` 把 resolution 文本发回同一 session；
- 控制：`runtime.*` 走 systemd 白名单，`workspace.prepare` 只允许受管根目录内，
  `slot.*` 映射到本地会话；所有 ControlOp 校验后执行；
- 本地保留 `worker.js`（`gethen_escalate/gethen_report`），会话里的模型才能用工具。

执行模式：**先做进程内 `ctx.agents`**（简单、复用现成服务）；需要更强隔离时再加
`dsh --stdio` 子进程模式（参考 multica），协议不变、只换 executor。

## 8. 人工旁观（dsh-remote 的白送能力要自己做）

- 推荐：Worker 的 DSH web 只绑 `127.0.0.1`，人通过 **SSH 隧道/VPN** 打开会话 UI；
- Master 面板展示 Assignment 的 `workerId + workspace + 会话链接模板`；
- 可选：Master 把最近的 `event` 帧缓存成只读时间线（不做交互），满足"看进度"。

## 9. 安全

- 必须 `wss`；Master 绑定非回环地址时用 TLS 证书（DSH webServer 支持 `tls`）；
- 每 Worker 独立 token，支持撤销/轮换；连接建立后校验 workerId 与 token 绑定；
- Worker 侧 `workspaces` 白名单 + ControlOp 白名单（不接受任意 shell/路径）；
- 审计：连接、dispatch、report、control 全量落 sqlite/日志；
- 限流：每连接发送队列、每 Worker 并发配额。

## 10. 配置示例

Master：

```yaml
- id: gethen
  config:
    dir: /var/lib/gethen
    ws:
      listenPath: /gethen/worker
      heartbeatTimeoutMs: 30000
      maxFrameBytes: 262144
```

Worker：

```yaml
- insert:
    - id: gethen-worker-agent
      name: 'file:///home/<user>/.local/share/gethen/dsh/agent.js'
    - id: gethen-worker
      name: 'file:///home/<user>/.local/share/gethen/dsh/worker.js'
- id: gethen-worker-agent
  config:
    masterUrl: wss://master.example.com/gethen/worker
    tokenFile: /etc/gethen/worker.token
    workspaces: ["/home/user/projects"]
    quota: 2
```

## 11. 迁移与共存

- `launch.mode: "remote" | "ws" | "dev" | "fake"`，默认 `fake`；
- 保留 `@gethen/bridge-remote`，验证后按需下线；
- 协议与 DSH 版本解耦（不依赖 ds-harness-remote 的内部 API）。

## 12. 测试策略

- **协议契约**：内存 duplex 跑 Master↔Agent 全帧序列（hello→dispatch→event→escalation→resolution→report）；
- **断线**：dispatch 中、升级中、report 前断线各一次，重连后对账；
- **幂等**：重复 dispatch/resolution/control；
- **鉴权**：坏 token、错 workerId、过期 token；
- **心跳**：超时离线、重连恢复；
- **控制面**：ControlOp 白名单/路径逃逸/未知 op；
- **e2e**：本机起两个 DSH（Master 3081 / Worker 3082），真实跑一轮；
- **回归**：现有 455 项测试保持绿。

## 13. 里程碑（约 3 周）

| 阶段 | 内容 | 工期 |
|---|---|---|
| M1 | `worker-protocol` + Master WS 端点 + token + hello/heartbeat | 3 天 |
| M2 | dispatch/执行/event/report（进程内 ctx.agents） | 4 天 |
| M3 | escalation/resolution/cancel 闭环 | 2 天 |
| M4 | ControlOp + 与 fleet/routeWorkers 打通 | 4 天 |
| M5 | 断线恢复、审计、面板只读视图、文档 | 4 天 |

## 14. 风险

| 风险 | 对策 |
|---|---|
| Master 不可达（NAT） | 要求 VPN/公网/SSH 隧道；不承诺穿透 |
| 断线期间任务状态不一致 | `assignmentId` 幂等 + 重连对账 + `unknown` 不自动失败 |
| Worker 执行隔离不足（进程内） | 先接受；需要时切 stdio 子进程模式 |
| 自建协议的兼容性负担 | 协议版本化 `v1`，帧校验严格，保持向后兼容窗口 |
| 人为操作失控（control op） | 白名单 + 受管路径 + 审计 |

## 15. 参考

- DSH `dsh-host-webserver` README（`registerUpgrade`、无内建认证）
- multica `dsh-runtime` 协议（本地源码）
- GitHub Actions self-hosted runners；Buildkite agent；Temporal durable execution
- 既有设计：[p1-master-research.md](p1-master-research.md)、[p3b-escalation-research.md](p3b-escalation-research.md)
