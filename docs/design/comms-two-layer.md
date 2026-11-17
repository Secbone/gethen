# 通讯层优化方案：L1 Transport + L2 Mailbox

日期：2026-11-17。目标：把"通信"拆成**两层**，L1 只管把消息可靠、可控地送到对端；
L2 用 mailbox 语义承载 agent 的任务分配与相互消息。两层都不绑定 DSH 或任何 harness。

## 0. 结论

1. **现状的问题不是"没有 WebSocket"，而是协议帧里混着传输语义和应用语义**：
   `hello/heartbeat/ping` 是 L1，`dispatch/report/escalation/resolution` 是 L2，
   它们被塞进同一个 `gethen.worker.v1` 帧集合里，导致换传输（SSH/stdio/Unix socket）
   要重写应用逻辑，换应用语义（多 agent 消息、广播、人工 mailbox）也要改传输层。
2. **目标分层**：
   - `@gethen/transport`（L1）：连接/会话、鉴权、帧、心跳、重连/断点续传、背压、
     传输层 ack、可插拔 adapter（ws / ssh-stdio / unix-socket / 未来 NATS）。
   - `@gethen/mailbox`（L2）：持久化 mailbox、消息信封、投递语义（at-least-once +
     idempotent）、lease/ack/nack、重试/DLQ、request/reply、pub/sub、topic。
   - `@gethen/agent`：L2 的消费者运行时，只依赖 `HarnessExecutor` 端口（DSH SDK 是
     第一个实现），不 import DSH。
3. **Migrate 不破坏现有远端链路**：先抽 L1、再引入 L2 双跑（feature flag），
   当前 `gethen.worker.v1` 帧作为兼容层保留，直到 L2 稳定。
4. 参考了 CloudEvents（信封字段）、A2A（agent 任务生命周期）、NATS JetStream
   pull consumer（ack/redelivery/worker pool）、RabbitMQ（durable queue/prefetch/DLQ）、
   WAMP（传输可换 + RPC/PubSub 应用协议）的做法。

## 1. 现状审计（哪些已经对了）

| 已有 | 所在 | 与目标的关系 |
|---|---|---|
| WS 连接、Bearer、hello、心跳、重连、幂等 assignment | `@gethen/transport-ws`、`@gethen/worker-agent` | **已经是 L1 的雏形**，但和 L2 帧混在一起 |
| dispatch/report/escalation/resolution 语义 | `@gethen/worker-protocol`、`@gethen/bridge-ws` | 应下沉到 L2 mailbox 消息 |
| worker 执行（DSH SDK） | `@gethen/executor-sdk` | 已经很接近 `HarnessExecutor` 端口 |
| 控制面装配 | `@gethen/master-runtime` | 应只依赖 L2 客户端，不直接碰网关/bridge |
| 持久化 | `@gethen/master` sqlite | L2 mailbox 可复用同一个 sqlite/事务 |

**问题清单**：

1. `worker-protocol` 同时定义 transport（hello/heartbeat）和 app（dispatch/report），
   换传输时无法复用 app 层；
2. `bridge-ws` 直接实现 `LaunchPort`，把"派工"和"传输"耦合；
3. `worker-agent` 内部把 dispatch 直接映射到 executor，没有 mailbox 队列/重试/多 agent 消息；
4. 没有点对点消息/广播/人工 mailbox：Router 与 Worker、Worker 与 Worker 之间无法直接通信；
5. 没有统一的投递语义（ack/lease/redelivery/DLQ），断线重连只能靠 assignment id 对账；
6. 没有 mailbox 级 observability（队列深度、未 ack 数、死信）。

## 2. 目标架构

```
┌────────────────────────────── Master 进程（CLI 或 DSH 插件）──────────────────────────┐
│ Agent Runtime（Router / 人工代理）                                                     │
│   · Mailbox consumer loops（每 agent 一个）                                            │
│   · scheduler / fleet / evidence（现有包）                                             │
│   · MailboxClient: send / request / subscribe / ack / nack                             │
├───────────────────────────────────────────────────────────────────────────────────────┤
│ L2  @gethen/mailbox   envelopes + durable mailboxes + delivery semantics               │
├───────────────────────────────────────────────────────────────────────────────────────┤
│ L1  @gethen/transport  connection/session + framing + auth + heartbeat + resume        │
│     adapter: ws（当前） | ssh-stdio | unix-socket | nats（可选）                        │
└───────────────────────────────┬───────────────────────────────────────────────────────┘
                                │ 出站连接
┌───────────────────────────────▼───────────────────────────────────────────────────────┐
│ Worker 进程（gethen worker）                                                          │
│   Agent Runtime（mailbox consumer）                                                    │
│     · 收 Assign → HarnessExecutor.start()（DSH SDK adapter）                          │
│     · 收 Answer → 恢复挂起的 approval/question                                         │
│     · 发 Progress / Ask / Result                                                       │
│   HarnessExecutor port（DSH / 未来的 pi / opencode / codex）                          │
└───────────────────────────────────────────────────────────────────────────────────────┘
```

L1 不认识 agent、ticket、report；L2 不认识 WebSocket、socket、heartbeat。

## 3. L1：Transport 层

### 3.1 接口（建议）

```ts
export interface Transport {
  connect(endpoint: TransportEndpoint, auth: TransportAuth): Promise<TransportSession>;
}

export interface TransportSession {
  readonly id: string;
  readonly state: "connecting" | "open" | "closed";
  send(message: TransportMessage): Promise<void>;          // 已序列化字节/JSON
  onMessage(handler: (message: TransportMessage) => void): () => void;
  onState(handler: (state: SessionState) => void): () => void;
  ping(): Promise<void>;                                   // liveness
  close(reason?: string): Promise<void>;
}

export interface TransportMessage {
  id: string;                 // 传输级消息 id
  payload: Uint8Array | string;
  contentType: string;
  replyTo?: string;
  headers?: Record<string, string>;
}
```

### 3.2 职责

| 能力 | 说明 |
|---|---|
| 连接与会话 | dial/listen、握手、版本协商、连接生命周期 |
| 鉴权 | Bearer / mTLS / SSH key，**只做认证**，不做业务授权 |
| 帧与复用 | 分帧、最大帧、压缩（可选）、流控窗口 |
| 心跳与故障检测 | ping/pong；超时判死；半开连接处理 |
| 重连 | 指数退避 + jitter；连接 id/会话 id；可选断点续传 |
| 背压 | 有界发送队列、`bufferedAmount` 阈值、拒绝策略 |
| 传输级 ack | 可选（at-least-once 模式下用于重传，不保证业务语义） |
| 观测 | 连接数、RTT、重连次数、发送/接收字节、错误码 |

### 3.3 Adapter

- `ws`：现有 `transport-ws` 改造（保留 v1 兼容 shim）；
- `ssh-stdio`：Master → `ssh worker gethen-agent --stdio`，复用 SSH 认证；
- `unix-socket`：同机/容器场景；
- `nats`（可选）：多 Master/大规模时用 JetStream 做 L1 的持久化投递（不自己造）。

## 4. L2：Mailbox 层

### 4.1 消息信封（CloudEvents 风格）

```ts
export interface Envelope<T = unknown> {
  specversion: "1.0";
  id: string;                 // ULID/UUID，幂等键
  source: string;             // "agent:router-1" / "worker:build-1"
  type: string;               // "gethen.task.assign" / "gethen.task.result" / ...
  subject?: string;           // ticket/assignment id
  time: string;               // RFC3339
  datacontenttype: "application/json";
  data: T;
  // Gethen 扩展
  thread?: string;            // 会话/任务线程
  correlationId?: string;     // request/reply 关联
  causationId?: string;       // 因果链
  to?: string;                // "agent:worker-1" / "mailbox:human"
  replyTo?: string;           // 回复投递到哪个 mailbox/topic
  deadline?: string;
  priority?: number;          // 0..9
  ttlMs?: number;
}
```

采用 CloudEvents 的 `id/source/type/specversion/time` 与扩展属性，**不要求 A2A 兼容**，
但消息类型命名与 A2A 的 task 生命周期对齐（见 §4.3）。

### 4.2 Mailbox 语义

| 能力 | 语义 |
|---|---|
| 寻址 | `agent:<id>` 点对点；`topic:<name>` 广播；`mailbox:human` 人工 |
| 顺序 | **单个 mailbox 内 FIFO**；多生产者按到达序（由 L1 保证） |
| 投递 | at-least-once；消费者 `ack(id)` / `nack(id, {requeue|dead})` |
| 幂等 | 信封 `id` 去重；消费者记录已处理 id（窗口 + 持久化） |
| Lease | 取件即 lease（visibility timeout），超时自动重投；心跳续租 |
| 重试 | 次数/退避策略；超限 → dead-letter mailbox + 告警 |
| 保留 | 按类型 TTL；任务结果保留到被确认；死信永久直到人工清理 |
| Request/Reply | `correlationId` + `replyTo`；超时返回错误信封 |
| 订阅 | topic 订阅 + 过滤（type/subject/source）；每订阅独立 cursor |
| 优先级 | 每 mailbox 可选 priority queue（0..9） |
| 观测 | 深度、未 ack、最老消息年龄、死信数、消费速率 |

### 4.3 任务生命周期消息（应用语义）

| 消息 type | 方向 | 对应现有帧 |
|---|---|---|
| `gethen.task.assign` | Master → Worker | `dispatch` |
| `gethen.task.accept` | Worker → Master | `accepted` |
| `gethen.task.progress` | Worker → Master | `event`（可选持久化） |
| `gethen.task.ask` | Worker → Master/Human | `escalation` |
| `gethen.task.answer` | Master/Human → Worker | `resolution` |
| `gethen.task.result` | Worker → Master | `report` |
| `gethen.task.cancel` | Master → Worker | `cancel` |
| `gethen.agent.hello` / `heartbeat` | **留在 L1**（会话层），不用 mailbox | `hello/heartbeat` |
| `gethen.control.*` | Master → Worker agent | `control` |
| `gethen.agent.message` | 任意 agent ↔ agent | 新增（点对点/线程消息） |

> Router 与 Worker 的"相互沟通"用 `gethen.agent.message` + `thread` 承载；
> 人机消息用 `mailbox:human` + Ask/Answer。

## 5. 与现有代码的映射 / 包结构

| 新包 | 来源 | 说明 |
|---|---|---|
| `@gethen/transport` | 重构 `@gethen/transport-ws` 的 L1 部分 | `Transport` 接口 + ws adapter；v1 帧作为兼容 payload |
| `@gethen/mailbox` | 新（纯逻辑 + sqlite store） | 信封、mailbox、ack/lease/retry/DLQ、subscribe |
| `@gethen/agent` | 重构 `@gethen/worker-agent` 应用部分 | mailbox consumer + `HarnessExecutor` port |
| `@gethen/bridge-mailbox` | 替代 `@gethen/bridge-ws` 的应用部分 | `LaunchPort` 用 mailbox request/reply 实现（assign→result） |
| `@gethen/executor-sdk` | 保留 | 变成 `HarnessExecutor` 的一个实现 |
| `@gethen/master-runtime` | 改造 | 只依赖 `MailboxClient` + `Transport`，不 import ws |

依赖方向：`worker-protocol`（v1 兼容）→ `transport`（L1）→ `mailbox`（L2）→
`agent`/`master-runtime`；`mailbox` 不依赖 `transport`，只依赖存储接口（可插拔）。

## 6. 投递保证与失败处理

| 场景 | 行为 |
|---|---|
| 连接断开 | L1 重连；L2 未 ack 消息在 lease 超时后重投 |
| Worker 崩溃 | lease 超时 → 重投（可换 worker）；assignment id 幂等 |
| 重复投递 | 消费者按信封 id 去重 |
| Master 重启 | mailbox 持久化；租约与重试状态恢复 |
| 任务升级（Ask） | 消息落库并 park；人工/agent 回复 Answer；超时策略可配 |
| 毒消息 | 重试超限 → dead-letter + 告警；不阻塞 mailbox |
| 背压 | 发送队列有界；消费者 prefetch 上限；满时 nack/暂停拉取 |

## 7. 迁移计划（约 2–3 周）

| 阶段 | 内容 | 验收 |
|---|---|---|
| **C1** 抽 L1（3d） | 新建 `@gethen/transport`：接口 + ws adapter（从 transport-ws 抽）；v1 帧走 `TransportMessage.payload`；现有 gateway/agent 改用它 | 现有 20+ 传输测试迁移后全绿；CLI/DSH 链路不回归 |
| **C2** Mailbox 核心（5d） | `@gethen/mailbox`：信封、mailbox、ack/lease/retry/DLQ、sqlite 表；纯逻辑 + store 测试 | 契约测试：FIFO、重复投递去重、lease 重投、死信、重启恢复 |
| **C3** 双跑（4d） | Master dispatcher 用 mailbox assign/result；worker 用 mailbox consumer 执行；`comms.mode=legacy|mailbox` 开关；v1 帧保留 | 本地 `verify:ws` + `GETHEN_E2E=1` 在两种模式都通过 |
| **C4** Harness 解耦（2d） | `HarnessExecutor` 端口正式化；DSH adapter 独立；agent 不 import DSH | 用 fake harness 跑 mailbox e2e；dsh 仍是可选实现 |
| **C5** 多 agent / 人工（3d） | `gethen.agent.message`、topic 订阅、`mailbox:human`、面板时间线；DLQ 运维命令 | Router↔Worker 消息、人工答复升级、死信查询 |
| **C6** 传输可替换（2d） | `ssh-stdio` adapter 作为第二实现；配置切换 | 同一 L2 代码在 ws 与 ssh 下通过同一组契约测试 |

**兼容策略**：C1 后 v1 帧仍可用；C3 起 mailbox 与 legacy 双跑；C5 后默认 mailbox，
legacy 保留一个版本周期。

## 8. 备选方案评估

| 方案 | 优点 | 不采用为主方案的原因 |
|---|---|---|
| **WAMP**（RPC + PubSub，传输可换） | 现成的应用协议分层范例；router 实现成熟 | 需要引入 router/协议栈；与现有轻量 TS 栈和 DSH 集成成本高；可借鉴其分层，不直接采用 |
| **NATS JetStream** | 持久化、ack/redelivery、pull consumer、worker pool 成熟 | 多一个基础设施依赖；Master 需要部署 NATS；适合作为可选 L1 adapter，不是默认 |
| **RabbitMQ / AMQP** | 队列语义完善（DLQ、TTL、priority） | 同样引入 broker；运维成本高 |
| **MQTT 5** | 轻量、QoS | 语义偏 IoT，任务/mailbox 需自己补 |
| **Akka/Pekko mailbox** | actor mailbox 语义成熟 | JVM actor 栈与现有 TS 栈不匹配；借鉴"每 actor 单 mailbox + FIFO" |
| **直接上 A2A 协议** | 标准 agent 互操作 | 目前需要 HTTP/JSON-RPC 服务端与 AgentCard；可作为未来外部互操作适配器，内部先用自己的 mailbox |

**建议**：内部用 sqlite + 自研 mailbox（零新增基础设施、可嵌入 DSH/CLI），
信封字段对齐 CloudEvents；未来加 `nats` 作为 L1 adapter 或 A2A 作为对外适配器。

## 9. 风险

| 风险 | 对策 |
|---|---|
| 双跑期两套路径不一致 | 以 mailbox 契约为准的 conformance 测试，legacy 只做透传 |
| mailbox 持久化膨胀 | TTL + 结果确认后清理 + DLQ 上限 + 定期 vacuum |
| 迁移破坏现有远端部署 | feature flag + 版本协商；v1 帧保留 |
| 过度设计（多 agent 广播用不上） | C1–C3 只做 assign/result/ask/answer；pub/sub 放 C5 |
| sqlite 单写者瓶颈 | 短期够用（几十 worker）；需要更大规模时换 NATS adapter，不改 L2 API |

## 10. 参考

- [CloudEvents 1.0 spec](https://github.com/cloudevents/spec/blob/main/cloudevents/spec.md)（信封字段、扩展属性、隐私约束）
- [A2A Protocol spec](https://a2a-protocol.org/latest/specification/)（Task/Message 生命周期、SendMessage/GetTask/Cancel/Subscribe、幂等与异步）
- [NATS JetStream pull consumers](https://docs.nats.io/learn/jetstream/pull-consumers)（fetch/consume、显式 ack、redelivery、worker pool）
- [RabbitMQ Queues](https://www.rabbitmq.com/docs/queues)（durable queue、ack/nack、prefetch、TTL、DLQ、priority）
- [WAMP](https://wamp-proto.org/)（RPC + PubSub 应用层协议，可跑在 WebSocket/TCP/UDS 上——分层参考）

## 实施进展（2026-11-17）

| 阶段 | 状态 | 产出 |
|---|---|---|
| C1 L1 transport | ✅ | `@gethen/transport`：TransportSession 接口 + ws adapter（鉴权/心跳/背压/close），19 tests |
| C2 L2 mailbox | ✅ | `@gethen/mailbox`：CloudEvents 信封、memory+sqlite store、lease/ack/nack/retry/DLQ、request/reply、cursor，43 tests |
| C4-a harness 端口 | ✅ | `@gethen/harness`：HarnessExecutor/HarnessTask/Sink/Report 纯端口，12 tests |
| C3 link/agent/bridge | 🚧 | `@gethen/mailbox-link`、`@gethen/agent`、`@gethen/bridge-mailbox` 并行开发中 |
| 集成 | ⏳ | `master-runtime` 增加 `comms.mode=legacy\|mailbox`，CLI/DSH 开关，fake-harness e2e |

### 2026-11-17 更新：C3 完成

| 阶段 | 状态 | 产出 |
|---|---|---|
| C3-a mailbox-link | ✅ | L2 mailbox over L1 transport（server/client、subscribe/ack/lease、request/reply、attach 重连），20 tests |
| C3-b agent consumer | ✅ | Worker 侧 mailbox consumer + HarnessExecutor（assign/ask/answer/result/cancel、续租、quota），18 tests |
| C3-c bridge-mailbox | ✅ | Master LaunchPort over mailbox（assign/result、ask/answer、parked、timeout、receipt），15 tests |
| C3-d master-runtime | ✅ | `comms.mode=legacy\|mailbox`：sqlite mailbox store + ws transport server + 每 session mailbox link + bridge；health/close 扩展，11 loopback tests |
| C3-e CLI | ✅ | `gethen serve --comms mailbox`、`gethen worker --comms mailbox`、`pnpm verify:mailbox` |
| C4-b harness adapter | ✅ | `executor-sdk` 提供 `createSdkHarnessExecutor`（DSH 作为 harness 实现） |

剩余：C5（agent 间消息/人工 mailbox/面板时间线/DLQ 运维）、C6（ssh-stdio 传输 adapter 验证可替换性）。

### 2026-11-17 C7：删除旧协议（不向后兼容）

- 删除 `@gethen/worker-protocol`、`@gethen/transport-ws`、`@gethen/bridge-ws`、`@gethen/worker-agent`、`@gethen/bridge-remote`；
- `master-runtime`、`@gethen/dsh`、CLI 只剩 mailbox 栈（L1 transport + L2 mailbox）；
- `ControlOp` 词汇表移入 `@gethen/fleet`；token 哈希/内存 token store 移入 `@gethen/transport`；
- 旧的 `devRemote`/`remote`/`comms` 配置、`--comms` 参数、`worker-protocol` 帧全部移除；
- 验证：`pnpm verify:mailbox`、`GETHEN_E2E=1 node scripts/e2e-sdk.mjs`（真实 dsh+模型，mailbox 路径）。

本地验证：

```bash
pnpm verify:ws        # legacy WS 栈
pnpm verify:mailbox   # L1 transport + L2 mailbox 栈（fake harness，不花模型钱）
GETHEN_E2E=1 node scripts/e2e-sdk.mjs   # legacy + 真实 dsh
```
