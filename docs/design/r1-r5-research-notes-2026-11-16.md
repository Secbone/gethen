# R1–R5 开发前调研笔记（2026-11-16）

对应 [控制面独立重构计划](refactor-plan-control-plane-2026-11-16.md)。记录动手前查到的
关键事实与据此调整的决策。

## 1. WebSocket 传输（R2/R3）

来源：[ws README](https://github.com/websockets/ws)（Autobahn 测试套件通过）。

- `new WebSocketServer({ noServer: true })` + `server.on("upgrade")` 是最适合我们的用法：
  DSH 插件形态让 `webServer` 交来 socket，独立形态自己挂 `node:http`；两种宿主共用同一
  组连接管理代码。
- 鉴权必须在 `handleUpgrade` 之前做：读取 `Authorization` 头，失败直接
  `socket.write("HTTP/1.1 401 ...")` 并销毁；
- 断线检测用 **ping/pong**：服务端每 30s `ping()`，未回 `pong` 的 `terminate()`；
  客户端监听 `ping` 并重置本地超时（与我们的心跳帧互补）；
- `maxPayload` 限制帧大小；`perMessageDeflate` 默认关闭（开销大，协议帧很小，不需要）；
- 客户端重连用指数退避 + jitter；协议层用 `assignmentId` 对账，不依赖连接状态。

**决策**：新增依赖 `ws`（+ `@types/ws`）只出现在 `transport-ws` / `worker-agent`；
协议与校验保持零依赖。

## 2. Worker 执行驱动（R3，重要修正）

来源：本地 DSH `dsh-sdk-jsonrpc-server` / `dsh-sdk-protocol` / `dsh-sdk-app` 文档。

DSH 自带 **SDK stdio JSON-RPC** 运行时（`dsh --profile sdk`）：

| 方向 | 方法 | 用途 |
|---|---|---|
| client→server | `initialize` | 选定 provider/model、cwd、`approvalRequests`、`userQuestions` 能力 |
| client→server | `session/resume`、`session/query`、`session/prompt`、`session/wait` | 恢复/创建会话、排队提示词、等待回合完成 |
| client→server | `session/working-directory/get|set`、`shutdown` | 工作目录与关闭 |
| server→client | `session/event`、`session/status` | 持久事实与状态流 |
| server→client | `approval/request`、`user-question/ask` | 审批与向人提问，客户端必须回答 |

这比"在 DSH 内跑一个插件并通过 UDS 代理"更干净：

- Worker daemon **spawn `dsh --profile sdk` 子进程**（每个项目/profile 一个），用
  JSON-RPC 建会话、流事件、应答审批/提问；
- Master 的升级语义可以直接映射到 `approval/request` / `user-question/ask` 的应答：
  Worker daemon 收到后转成 `escalation` 帧，Master 决定后把答案作为 JSON-RPC 响应回传；
- 进程隔离、崩溃可回收、DSH 版本契约稳定；不再需要本地 UDS 代理。

**决策（修订 D1）**：R3 的 Worker 执行驱动用 **DSH SDK JSON-RPC**；`worker.js` 的
`gethen_escalate/gethen_report` 保留为会话内显式工具的兼容路径（模型也可以用它），
但主通道是 SDK 的审批/提问与事件流。

## 3. 服务形态（R1/R3）

- 独立 Master：`node:http` + `ws`（`noServer`）+ 自己的 upgrade 路由；TLS 用
  `https.createServer`；健康检查 `GET /health`。
- DSH 插件 Master：复用 `webServer.registerUpgrade("/gethen/worker")`，共享同一
  `transport-ws` 连接层；不重复实现 HTTP。
- Worker：systemd 常驻（`Restart=always`），`ExecStart=gethen worker --config ...`；
  子进程 `dsh --profile sdk` 由 daemon 管理，退出/崩溃按退避重启；daemon 退出时先
  `shutdown` 子进程再退出。

## 4. 调度参考（R4）

- [Kubernetes controllers](https://kubernetes.io/docs/concepts/architecture/controller/)：
  期望态/实际态、level-triggered 对账、幂等 reconcile；
- [Temporal](https://docs.temporal.io/evaluate/understanding-temporal)：任务 id、
  心跳、重试、按事件历史恢复；
- 我们的 `fleet`：Node/Runtime/Slot + `planReconcile()`；`router`：硬过滤 + 打分 +
  REUSE/START/QUEUE/PARK（沿用既有 `routeWorkers` 思路）。

## 5. 由此产生的实现顺序

1. `@gethen/worker-protocol`（帧与校验，纯包）——R2 的基础；
2. `@gethen/fleet`（纯包，可与协议并行）——R4 的基础；
3. `@gethen/transport-ws`（连接/心跳/令牌/dispatch 队列）——R1/R2；
4. `@gethen/worker-agent`（WS 客户端 + DSH SDK 子进程驱动）——R3；
5. `gethen serve` / `gethen worker`（CLI 装配）——R1/R3；
6. 面板只读时间线与文档——R5。
