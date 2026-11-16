# P1 研发调研与任务拆解（自研 master）

日期：2026-11-16。对应 [重构计划](../refactor-plan-gethen.md) 的 P1。目的：先弄清
"要建什么、为什么这样建、怎么验收"，再写代码。

## 0. 方法与范围

内部输入：

- `dsh-superagent-prototype` 的规格文档（`docs/001-domain.md`、`002-authorization.md`、
  `003-execution.md`）与 `tests/core-engine.spec.ts`（113 条行为用例）。**只读参考，不复制代码。**
- 本机 DSH 0.2.2-rc.1 的插件能力：`tools`、`systemPrompt`、`sessions`、`agents`、
  `skills`、`schedule`、`subprocess`、profile/bundle 机制。
- 已在本仓的 `@gethen/router`、`@gethen/worker-bridge` 契约。

外部输入（2026-11-16 检索）：

| 来源 | 关键点 |
|---|---|
| [Anthropic: Building effective agents](https://www.anthropic.com/engineering/building-effective-agents) | 区分 workflow（代码编排）与 agent（模型自决）；orchestrator-workers、routing、evaluator-optimizer；先简单后复杂；工具/接口（ACI）要和 UI 一样认真设计 |
| [Temporal: Understanding Temporal](https://docs.temporal.io/evaluate/understanding-temporal) | durable execution：workflow 确定性重放、副作用放 activity、event history、重试/超时、信号/查询、worker 轮询 |
| [Kubernetes: Controllers](https://kubernetes.io/docs/concepts/architecture/controller/) | 期望态 vs 实际态、level-triggered 对账、幂等 reconcile |
| [Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) | 业务写与消息同事务落库；relay 至少一次；消费者必须幂等 |
| [12-factor agents](https://github.com/humanlayer/12-factor-agents) | 自有控制流、把 agent 当软件、用工具调用联系人类 |
| [Stripe: idempotency](https://stripe.com/blog/idempotency) | 客户端幂等键、同 key 同结果、未知结果先查再重试 |

结论：**不引入 Temporal/K8s 之类的外部运行时**，但采用它们的三个核心思想：

1. 引擎是确定性 workflow（纯函数 + 注入时钟/ID），所有外部副作用走 outbox；
2. 执行面用"期望态对账"（P4 fleet 用，P1 只留端口）；
3. 所有跨边界写操作带 `operationId`，幂等且可查询。

## 1. 从参考实现提炼的硬规则（P1 必须实现）

这些是 113 条用例里反复验证过的语义，直接转成我们的验收清单。

### 1.1 Ticket 生命周期与 stage

按顺序匹配，第一个命中生效（`person/*` 表示等人，`working/*` 表示在跑）：

| stage | 条件 |
|---|---|
| `person/delivery` | 最近一次投递失败，按 2s→1m 退避重试；成功/新 Assignment/结束清除 |
| `person/answer` | 存在未回答的 Attention（升级） |
| `person/review` | 最近 Report 是 `ready` |
| `person/blocked` / `person/failed` | 最近 Report 是 `blocked`/`failed`，且之后没有人发言 |
| `working/queued` | 没有 Assignment 或仍是 pending |
| `idle` | 最近 Report 是 `waiting`，或 Assignment 已结束 |
| `working` | 其余 |

规则：`blocked`/`failed` 的 Ticket 收到人的新消息后回到执行；`idle` 的 Ticket 由订阅
Thread 上的 Trigger 继续。

### 1.2 Eval

- 同一 Thread 同时只跑一个 Eval；期间到达的 Trigger 合并进下一批（debounce）。
- Eval 有 deadline；过期未决 → `unavailable`，并把人可读的原因记下来。
- 评估期间 Policy 版本变化 → 该 Eval 的结果为 `unavailable`（不是 accept）。
- 一次 pump 启动所有到期 Eval；重复 pump 不能重复启动。
- 同一 `(source, triggerId)` 只接收一次；同一个 Trigger 被接受两次只创建一张 Ticket。

### 1.3 Mandate 与授权

- Mandate 不可变：记录 policyVersion、strength、scope、命中的规则、来源身份。
- 只有 Eval 能创建 Ticket；只有人能显式 stop；Policy 变化只**投递通知**，不撤销
  Mandate；Mandate 不再满足当前 Policy 时，恢复被拒绝。
- Subscription 只提供上下文，不授予权限、不恢复 Ticket。
- 人要 resume 已结束 Ticket：必须通过显式 attach 或在该 Thread 上发言，且 Mandate
  仍通过当前 Policy。
- 我们简化：不做多源 admission 矩阵。P1 的策略是
  **source allowlist + project policy + acceptMode(off/observe/auto)**，但保留
  "默认拒绝 + 显式 mandate" 的语义。

### 1.4 Assignment 与 Report

- Assignment：`pending → running → ended`；只有最后一份 Report 能结束 Ticket。
- Report kind 决定 ticket stage（见 1.1）；`blocked`/`failed` 时**暂停自动 Input**。
- 投递失败有退避；新 Assignment 或结束清除失败记录。
- 进程重启：进行中的 Assignment 带 recovery note 重新开始；未取走的 Input 保留。
- 同一 Ticket 同时只有一个 Assignment（我们的 slot 模型天然保证）。

### 1.5 与我们的架构的差异

参考实现的 Executor 是**本机 DSH Session**；我们的 Executor 是 `WorkerBridge`
（远端会话）。因此：

- 不实现 `executor/dsh.ts` 的会话管理，只实现 `launch` effect → `WorkerBridge`。
- Linux 进程/工作区隔离交给 Worker 侧（P3/P4），master 不碰。
- "background work / worker slot / 30 分钟" 这类 DSH 会话细节**不进入 master**；
  由 Worker 侧插件在信封里报告 `status`。

## 2. P1 范围

做：

- `packages/master`：`model / engine / policy / store / outbox / sources/github /
  prompts / executor / index`
- 本地 Trigger 闭环：local source → eval → ticket → assignment（FakeBridge）→ report → receipt
- 单见证 sqlite（`node:sqlite`）与 outbox relay
- 单元/契约/集成测试；`pnpm check` 绿

不做（后续阶段）：

- fleet/调度（P4）、evidence/canary（P2）、memory（P5）、远程 bridge（P3）、panel 产品视图（P6）
- webhook 模式 GitHub（P1 只轮询）、飞书、Codex/ACP

关键决策：

1. **不复制参考实现的多源授权模型**，用单操作者 + 项目策略；但保留 mandate/stop/accept-mode。
2. **单写者**：一个进程写库（文件锁）；所有 effect 与业务写同事务入 outbox。
3. **引擎纯函数**：不读时钟、不写库、不发请求；时钟/ID/effect 由调用方注入。
4. **P1 的 Executor 是 FakeBridge**，真实 remote 在 P3 接入，master 不感知。

## 3. 数据与 schema（SQLite）

```sql
threads(id, source, created_at)
triggers(id, source, thread, actor, bot, addressed, text, at, meta_json, UNIQUE(source,id))
evals(id, thread, trigger_ids_json, policy_version, started_at, deadline_at, status) -- running|decided|expired
decisions(id, eval, outcome, strength, reasons_json, at)
tickets(id, mandate, source, stage, opened_at, ended_at, outcome, attention_json, latest_report, inputs_json, assignments_json)
mandates(id, ticket, policy_version, strength, scope, basis_json, matched_rules_json, created_at, withdrawn_at)
assignments(id, ticket, worker, workspace, pack_digest, state, started_at, ended_at, operation_id)
inputs(id, ticket, kind, text, trigger, delivered_at, read_at, from_person)
subscriptions(ticket, thread, own, context_only, PRIMARY KEY(ticket,thread))
policy_meta(version, json, updated_at)
effects(id, kind, key, payload_json, status, attempt, due_at, created_at, done_at) -- waiting|running|done|failed
operations(id PRIMARY KEY, kind, request_hash, result_json, created_at) -- 幂等收据
receipts(id, ticket, assignment, worker, pack_digest, outcome, summary, usage_json, at)
```

- 事务边界：`ticket 状态 + assignment + effect 入队 + operation 收据` 同一事务。
- outbox relay：轮询 `status='waiting' AND due_at<=now`，`UPDATE ... WHERE id=? AND status='waiting'`
  抢占，执行后写 `done`；失败按退避重试；崩溃后按 `running` 超时重置。
- 时间一律存 RFC3339 UTC；所有查询按字符串比较。

## 4. 端口（接口）

```ts
export interface Clock { now(): Date }
export interface Ids { next(prefix: string): string }

export interface EffectSink {
  /** Same transaction as the business write. */
  enqueue(effect: { kind: string; key: string; payload: unknown; dueAt: string }): void;
}

export interface PolicyPort { current(): Policy }
export interface SourcePort { id: string; poll(): Promise<Trigger[]> }
export interface ReceiptSink { append(receipt: AssignmentReceipt): void }

/** P1: FakeBridge; P3: RemoteBridge. */
export interface LaunchPort {
  launch(assignment: Assignment, ticket: Ticket, pack: PackRef): Promise<{
    status: "completed" | "failed" | "aborted";
    report: WorkerReport;
    receipt: AssignmentReceipt;
  }>;
}
```

引擎方法（纯）：

```ts
receive(trigger): "stored" | "duplicate" | "filtered"
evaluateDue(): number            // 启动到期 Eval
decide(triggerId, outcome, reasons): Decision
assignmentStarted(ticketId, assignmentId): void
report(ticketId, report): void
retryTicket(ticketId): Assignment
attach(ticketId, triggerId): void
stopTicket(ticketId, reason): void
note(ticketId, text): Input
pump(now): Effect[]             // 返回可执行 effect（由 store/relay 执行）
```

## 5. 任务拆解

| # | 任务 | 产出 | 依赖 | 预估 |
|---|---|---|---|---|
| T1 | `model.ts` + `engine.ts`（纯状态机） | 类型、状态转移、receive/evaluate/decide/report/attach/stop/retry；核心 30 条行为测试 | 无 | 2 天 |
| T2 | `store.ts` + `outbox.ts` | node:sqlite schema、事务、单写者、effect relay、崩溃恢复、幂等表；契约测试 | model 类型 | 2 天 |
| T3 | `policy.ts` | 单操作者策略、accept mode、mandate 校验、stop/resume 规则；测试 | T1 | 0.5 天 |
| T4 | `sources/github.ts` | 轮询 Issue/PR/评论/CI、读位置、去重、回写；可 mock 的 HTTP 端口 | T1/T2 | 1.5 天 |
| T5 | `prompts.ts` + `executor.ts` | 角色提示、assignment 信封、FakeBridge 执行器；集成测试 | T1/T2 | 1 天 |
| T6 | 装配 + DSH 最小挂载 | local Trigger、`gethen_run` 工具、`/gethen` 只读视图 | T1–T5 | 1 天 |

并行策略：T1 先定类型；T2 依赖类型但可与 T1 并行开工（先按契约写 store）；
T3/T4/T5 在 T1 合入后并行；T6 最后。

## 6. 测试计划

- 单元（engine）：重复 Trigger、Eval 串行/debounce、deadline 过期、policy 版本变化、
  重复 accept 只建一张 Ticket、report 映射 stage、blocked 暂停 Input、stop 后不可 resume、
  mandate 失效拒绝 resume、attach 恢复、retry 生成新 Assignment。
- 契约（store）：同一套用例跑 sqlite 实现在内存实现上；事务原子性；outbox 抢占与
  至少一次；崩溃恢复（running 超时重置）；幂等表。
- 集成：local trigger → eval → ticket → launch(Fake) → report → receipt；重启后继续。
- 性质测试（可选）：effect 严格按入队顺序执行；时间单调。

## 7. 风险与未决

| 风险 | 处理 |
|---|---|
| 简化授权是否够用 | P1 只支持单操作者；多源身份留到需要时（记录在 ADR-0008 的 open questions） |
| node:sqlite 并发与锁 | 单写者 + 文件锁；测试覆盖崩溃恢复；不追求多进程写 |
| GitHub 轮询限流/去重 | 读位置持久化 + 条件请求；P1 只做轮询，webhook 留 P4 |
| 与参考实现语义偏差 | 以 113 条用例为规格清单，先覆盖核心 30 条，其余按需补 |
| 范围膨胀 | T1–T6 之外的需求进 backlog，不进 P1 |

## 8. 参考链接

- <https://www.anthropic.com/engineering/building-effective-agents>
- <https://docs.temporal.io/evaluate/understanding-temporal>
- <https://kubernetes.io/docs/concepts/architecture/controller/>
- <https://microservices.io/patterns/data/transactional-outbox.html>
- <https://github.com/humanlayer/12-factor-agents>
- <https://stripe.com/blog/idempotency>
- 本地：`~/Code/dsh-superagent-prototype/docs/001-003`、`tests/core-engine.spec.ts`
