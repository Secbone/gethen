# Gethen 重构计划（2026-11-16）

状态：提案。约束：**不考虑向后兼容**；目标是最小表面积、单一职责、可扩展。

本计划把当前 36 个包的产品收敛为 **14 个产品包 + 1 个离线评测目录**，只保留一个
Master 引擎、一条传输契约、一层能力包、一层证据、一个外部记忆服务。每一步都有
删除清单和验收门，任何一步都可以单独回滚。

## 0. 摘要

| 现在 | 目标 |
|---|---|
| 36 个包，约 2.1 万行源码 | 14 个产品包，自有代码约 1.5 万行；无 fork、无 vendor |
| 两套控制面（superagent、极简 gethen 控制面）并存 | 自研一个 `@gethen/master` 引擎 |
| 三套执行路径（旧 CLI runtime、HarnessWorker、DSH 会话） | 只有 `WorkerBridge` 一条契约，`remote` 适配器先落地 |
| 记忆散在 memory/memory-file/memory-evolution | gethen-memory HTTP 客户端，Master/Worker 各一个 namespace |
| 调度只看单机 Worker | Fleet（Node/Runtime/Slot）+ 调度器 + 类型化 ControlOp |

工期约 **6 周**（P0–P7），每阶段结束可交付、可演示。

**不 vendor：** `dsh-superagent` 只作为规格参考（放在仓库外，MIT），不 import、
不 copy。我们按自己的形态实现，避免把单仓库 bot 的假设和 fork 债务带进来。

## 1. 目标与约束

产品目标（本计划服务的场景）：

- 一台常驻云端的 **Master**（DSH 宿主，7×24），是人的唯一入口。
- 1–2 台本地 + 2–3 台云端 **Node**，各自承载不同项目；另有临时任务。
- Master 负责资源调度：复用常驻 Worker、在正确的 Node/Runtime 新开槽位、排队或转人。
- Worker 执行中需要决策时**阻塞**：先问 Master 记忆，答不了再问人（无超时）。
- Master 有跨任务记忆；Worker 只有项目/任务记忆；提升必须经过证据校验。

工程约束：

- 不自造 agent runtime：DSH 提供 session/tool/skill/schedule/panel。
- 远程不开放端口、不支持任意 RPC：控制面只发**类型化操作**，机器侧执行。
- 一个概念只有一个包：不允许两份领域模型、两套配置、两条执行栈。

非目标：多租户、Master 多实例/共识、自动装机/配网、通用远程 shell、Windows。

## 2. 最终架构

```
人 ⇄ Web/IM ⇄ Master（DSH + @gethen/dsh，常驻云端）
                 ├─ master   引擎：Source→Trigger→Eval→Decision→Ticket→Assignment
                 ├─ router   调度：Fleet 过滤 + 打分 + REUSE/START/QUEUE/PARK
                 ├─ fleet    期望态/实际态对账 + 类型化 ControlOp
                 ├─ evidence receipts / eval / canary / archive
                 └─ evolve   Dream → CapabilityPack → channel
                        │ ①会话面（ds-harness-remote，E2E） ②控制面（ControlOp）
        ┌───────────────┴────────────────────────────────────┐
        │ Node = 机器（1–2 本地 + 2–3 云）                     │
        │  systemd: gethen-runtime@<profile>.service          │
        │    └─ DSH + @gethen/dsh（role: worker）             │
        │  Control Session（常驻）：snapshot / start-stop /   │
        │    workspace.prepare / slot.create-close / quota    │
        └─────────────────────────────────────────────────────┘
                        │ HTTP
                 gethen-memory（L1 sessions / L2 notes / L3 wiki / L4 注入）
```

四条边界，各自只有一个实现：

- **控制面**只决策、不执行命令；**执行面**只执行、不决策。
- **会话面**只做"创建/发送/等待/读取/打断"；**控制面**只做生命周期。
- **记忆面**是一个 HTTP 服务，不是库；Gethen 只是客户端。
- **证据面**只追加：receipt、eval、canary、archive。

## 3. 包结构

### 3.1 目标包清单（14）

| 包 | 职责 | 依赖 | 关键导出 |
|---|---|---|---|
| `core` | 共享契约：ID、JSON、Digest、ArtifactRef、错误码、信封版本、端口接口 | 无 | `Digest`、`JsonValue`、`ArtifactRef`、`PortError` |
| `master` | **自研控制面引擎**：Ticket/Assignment/Report/Escalation 状态机、Policy、Eval/Decision、Sources（先 GitHub）、durable outbox、prompt 装配、Store | core, pack | `Engine`、`Policy`、`SourceRegistry`、`ReceiptSink` |
| `router` | 调度与升级判定（纯函数）：Fleet 过滤、加权打分、REUSE/START/QUEUE/PARK、Resolution gate | core, pack, fleet | `schedule()`、`gateResolution()`、`nextAssignmentState()` |
| `fleet` | Node/Runtime/Workspace/Slot 模型、期望态对账、ControlOp 校验 | core | `FleetState`、`ControlOp`、`planReconcile()` |
| `worker-bridge` | 传输契约：清单、派工、会话状态、升级/答复、receipt | core | `WorkerBridge`、`Escalation`、`Resolution`、`AssignmentReceipt` |
| `bridge-remote` | 唯一 `dshRemoteClient` 适配器（含 feature-detect 与降级） | core, worker-bridge | `RemoteBridge` |
| `worker` | Worker 侧逻辑：Control Session、pack 物化、`escalate` 工具、槽位清理 | core, worker-bridge, pack, memory-client | `ControlSession`、`materializePack()`、`janitor()` |
| `pack` | CapabilityPack：内容寻址、版本、渠道（stable/canary） | core | `defineCapabilityPack()`、`PackChannel` |
| `evidence` | 追加式证据：Receipt 存储、EvalRecord、指标比较、Canary、Archive | core | `ReceiptStore`、`EvalProtocol`、`CanaryMonitor` |
| `evolve` | Dream/Evolution：候选生成、门控、归档、下发 | core, pack, evidence | `DreamService`、`EvolutionLoop` |
| `memory-client` | gethen-memory HTTP 客户端、namespace 约定、预算召回、晋升 | core | `MemoryClient`、`recall()`、`promote()` |
| `panel` | 产品投影与视图：Fleet、队列、升级、receipt、成本 | core, master, router, fleet, evidence | `buildOverview()`、`renderPanel()` |
| `dsh` | 装配层：master/worker 两个 entry、工具、路由、client shell | 以上全部 | `apply()`、`applyWorker()` |
| `cli` | 运维 CLI：`init`、`doctor`、`fleet`、`pack`、`bench` | core, fleet, evidence | 命令入口 |

### 3.2 依赖 DAG（只能是这个方向）

```
core ─┬─ pack ──────┬─ router ─┐
      │             │          │
      ├─ worker-bridge ─ bridge-remote
      ├─ fleet ──────┘          │
      ├─ evidence ───┬─ evolve  │
      ├─ memory-client           │
      └─ master ─────────────────┤
                                 └─ panel ─ dsh
                                      cli（只依赖 core/fleet/evidence，不依赖 dsh）
```

规则：`core` 零依赖；`router`/`fleet`/`worker-bridge`/`pack`/`evidence` 纯逻辑，
禁止 Node 内置模块以外的 IO（文件/网络由适配器注入）；`dsh` 是唯一装配点。

### 3.3 现有 36 包的处置

| 现有包 | 处置 |
|---|---|
| `core` | 保留，瘦身为纯契约；原事件/artifact 细节并入 master |
| `domain` | **删除**：类型并入 master，共享部分进 core |
| `dsh` / `dsh-client` / `panel` | 合并为 `dsh` + `panel`；client shell 并入 `dsh` |
| `router` / `pack` / `worker-bridge` | 保留（本轮已建） |
| `worker` | **重写**为 Worker 侧插件逻辑；旧 `WorkerAdapter` 契约删除 |
| `evolve` | 保留（Dream/loop），依赖改为 evidence |
| `observability` / `evolution` | **合并进 `evidence`**（receipt、archive、指标比较、canary） |
| `source-github` | **并入 master**：作为 sources/github 的起点，扩展多项目 |
| `effects` / `store` / `storage-file` | **删除**：master 自带 durable effects 与 sqlite store；锁/原子写并入 master/evidence 内部 |
| `assistant` / `host` / `session` / `harnesses` / `kernel` / `models` | **删除**：DSH 提供或已被 master 取代 |
| `skills` / `skills-file` / `skills-mcp` | **删除**：DSH skills 提供 |
| `memory` / `memory-file` / `memory-mcp` / `memory-evolution` | **删除**：gethen-memory 取代（评测如需旧实现，从 git 历史取） |
| `transport-pi` / `transport-acp` | **删除**：远程执行统一走 `bridge-remote`；离线对照需要时再建 |
| `evaluation` / `workspace` / `benchmark` / `benchmark-file` | **移出产品**：P7 迁到独立 `bench/` 或单独仓库，产品不依赖 |
| `cli` | 保留但瘦身：删 `run/chat/iterate/loop/memory/trace`，保留 `init/doctor/fleet/pack/bench` |

### 3.4 目录

```
packages/   core master router fleet worker-bridge bridge-remote worker
            pack evidence evolve memory-client panel dsh cli
bench/      （P7）离线评测：数据集、Python 审计、旧 harness 快照
docs/       adr/、refactor-plan-gethen.md、design/、research/
scripts/    build-dsh.mjs、e2e-remote.ts、check-layering.mjs
```

## 4. 领域模型与协议

### 4.1 Master 侧（沿用 master 引擎，不重写）

```
Source → Trigger → Thread → Eval → Decision → Ticket + Mandate
                                        └→ Assignment → Worker → Report → Receipt
```

Assignment 状态机（新增阻塞语义）：

```
running ──blocking escalate──▶ waiting ──answer──▶ running
   │                              │
   ├──ended                       └──转人──▶ parked ──人答复──▶ running
   └──failed→ended
```

规则：阻塞时 Worker **结束本轮**；`parked` 无超时，靠人推进；每次升级/答复落库。

### 4.2 调度侧（新增）

```ts
type NodeId = string;
interface Node {
  id: NodeId; labels: { locality: "local" | "cloud"; projects: string[];
    runtimes: RuntimeKind[]; gpu?: boolean; secrets?: string[] };
  resources: { cpu: number; memMb: number; diskMb: number; gpu?: number };
  health: "online" | "degraded" | "offline"; lastHeartbeatAt: string;
}
interface Runtime { node: NodeId; kind: "dsh" | "opencode" | "pi";
  profile: string; state: "stopped" | "starting" | "idle" | "busy" | "draining" | "failed";
  models: string[]; slots: { running: number; quota: number }; }
interface Slot { id: string; node: NodeId; runtime: string; workspace: string;
  kind: "resident" | "ephemeral"; state: "idle" | "busy" | "draining" | "closed";
  sessionId?: string; ttlMs?: number; }
```

调度决策（纯函数，输入 Fleet + 任务，输出决定性结果）：

```ts
type ScheduleDecision =
  | { action: "reuse"; slot: string }
  | { action: "start"; node: NodeId; runtime: string; workspace: string; kind: "resident" | "ephemeral" }
  | { action: "queue"; reason: string }
  | { action: "park"; reason: string };
```

硬过滤：节点在线、runtime 可接活、workspace 存在、模型可用、能力匹配、
**locality 策略**（`local` 项目禁止 cloud 节点）、槽位未满。
打分：reuse 热度、项目亲和、负载余量、成本档、历史成功率（receipts）。

### 4.3 控制协议

```ts
type ControlOp =
  | { op: "snapshot" }
  | { op: "runtime.start" | "runtime.stop" | "runtime.restart"; runtime: string; drain?: boolean }
  | { op: "workspace.prepare"; repo: string; branch?: string; worktree?: boolean }
  | { op: "workspace.cleanup"; path: string }
  | { op: "slot.create"; workspace: string; pack: PackRef; kind: "resident" | "ephemeral"; ttlMs?: number }
  | { op: "slot.close"; slot: string; force?: boolean }
  | { op: "slot.drain"; slot: string }
  | { op: "quota.set"; slots: number; models?: string[] };
```

- 每个 op 带 `operationId`，幂等；Worker 侧白名单校验（只接受本地注册过的
  runtime/workspace），**不接受自由文本命令**。
- 输出回执：`{ opId, ok, state?, error? }`。
- 破坏性操作（stop/cleanup/force close）默认走人确认。

### 4.4 信封协议（会话面，`gethen/v1`）

- 任务：`[GETHEN-ASSIGNMENT]` + JSON（ticket、assignment、pack digest、objective、report 契约、deadline）。
- 报告：`[GETHEN-REPORT]` + JSON（status、summary、changedFiles、tests、usage、packDigest）。
- 升级：`[GETHEN-ESCALATION]` + JSON（kind、question、options、context、blocking）。
- 答复：`[GETHEN-RESOLUTION]` + JSON（option/text、confidence、basis）。

大 payload（>4KB）走工作区文件 `.gethen/inbox|outbox/`，消息里只放引用。

### 4.5 记忆

- namespace：`gethen.master`、`gethen.worker.<node>.<project>`。
- Worker 只能写自己的 namespace；Master 只读 Worker 的 L4 注入结果。
- 晋升：Worker 提 `MemoryCandidate` → `evolve` 用证据校验 → 写 Master namespace。
- 召回预算：≤2k tokens，必须返回 confidence；服务不可达 → 转人。

## 5. 运行形态

### 5.1 部署

| 角色 | 机器 | 进程 | profile |
|---|---|---|---|
| Master | 云端 1 台 | `dsh --profile gethen-master --port 3080` | base + web + `@gethen/dsh`(master) + remote(client) |
| Worker | 本地 1–2、云 2–3 | `systemctl --user start gethen-runtime@<profile>` | base + web + `@gethen/dsh`(worker) + remote(host) |
| Memory | 任意（可同 Master） | `gethen-memory serve`（4317） | 独立服务 |

Node 上每个项目一个 profile（如 `gethen-project-a`），由
`gethen-runtime@<profile>.service` 托管，`Restart=always`。

### 5.2 数据

- Master：`~/.dsh/state/gethen/master.sqlite`（tickets/assignments/effects/registry）、
  `~/.dsh/state/gethen/receipts.sqlite`（证据）。
- Worker：`~/.dsh/state/gethen/<profile>/`（槽位会话映射、控制回执）。
- 记忆：gethen-memory 自带布局（L1–L4）。

## 6. 重构阶段

每个阶段：动作 → 删除 → 验收 → 工期。

### P0 清场（2 天）

- 冻结产品包集合；在 `scripts/check-layering.mjs` 里把目标 DAG 写死（先只保留
  core/master/router/fleet/pack/evidence/worker-bridge/panel/dsh）。
- 删除明确已死的包：`assistant`、`host`、`session`、`harnesses`、`kernel`、`models`、
  `skills*`、`memory*`、`transport-*`、`source-github`。
- 删除 `experiments/`；CLI 只保留 `init/doctor`（其余命令先删）。
- 验收：`pnpm check` 通过，包数 ≤ 16，无悬空 import。
- 风险：评测脚本失效——接受，P7 重建。

### P1 自研 master 引擎（3 周）

按最小必要面积实现 `@gethen/master`，约 2.5–3k 行：

| 模块 | 内容 | 估算 |
|---|---|---|
| `model.ts` | Trigger/Thread/Ticket/Mandate/Assignment/Report/Escalation/Resolution + 状态机 | 400 |
| `engine.ts` | `receive/evaluate/dispatch/report`，纯逻辑，副作用走端口 | 400 |
| `policy.ts` | 单操作者 + 项目策略 + accept mode；默认拒绝、mandate 校验 | 250 |
| `store.ts` | `node:sqlite`：tickets/effects/slots/receipts；单写者锁 | 350 |
| `outbox.ts` | durable effect、operationId 幂等、重启恢复 | 200 |
| `sources/github.ts` | 轮询→Trigger→回写，多项目配置，读位置持久化 | 450 |
| `prompts.ts` | 角色提示与系统提示分段 | 150 |
| `executor.ts` | Assignment → `WorkerBridge`（不含 DSH 耦合） | 150 |
| `index.ts` | 端口与装配 | 100 |

- 复用仓库已有代码的**逻辑**（重写而非搬家）：`domain` 的类型与 engine、`effects`
  的幂等、`store` 的原子写、`source-github` 的读位置。
- 删除 `@gethen/domain`、`@gethen/effects`、`@gethen/store`、`@gethen/storage-file`、
  `@gethen/source-github`、`@gethen/dsh` 的旧极简控制面。
- 规格参考：`dsh-superagent` 的 `tests/core-engine.spec.ts` 作为行为清单（只读，
  不复制代码），逐条对齐：重复 Trigger、过期 Eval、mandate 撤销、重试、并发提交。
- 验收：本地 Trigger → Ticket → Assignment（FakeBridge）→ Report → Receipt；
  重启后 outbox 恢复且不重复执行；`/gethen` 面板显示工单。
- 时间盒：3 周；若某块超期，优先砍 GitHub 源，用 local Trigger 先闭环。

### P2 证据面（2 天）

- 新建 `@gethen/evidence`：Receipt 追加存储（含 pack digest/worker/model/usage）、
  EvalRecord、指标比较、Canary、Archive（从 `evolution`/`observability` 迁入）。
- master 通过注入的 `ReceiptSink` 写证据；`evolve` 改为依赖 evidence。
- 删除 `observability`、`evolution`、`effects`、`store`、`storage-file`（能力已并入）。
- 验收：一次任务产生可查询 receipt；canary 能基于 receipt 判定回滚。

### P3 Worker 通道与 M0 闭环（4 天）

- 完工 `worker-bridge`（契约）+ `bridge-remote`（唯一 `dshRemoteClient` 适配器，
  feature-detect `remoteGateway`，降级到 `remote_*` 工具路径）。
- 新建 `@gethen/worker`：Control Session、pack 物化、`escalate` 工具、janitor。
- `dsh` 拆两个 entry：`applyMaster()`、`applyWorker()`。
- 验收（M0）：Master（远端）→ 本机 Worker：
  1. 本地句触发 → 派工 → 远端会话执行 → 报告 → receipt；
  2. Worker 发一次 `ESCALATION`，Master 用预置 L2 回答（不打扰人），会话续跑；
  3. 断线/未知回执场景按 operationId 收据恢复。
- 风险：社区插件 API 非公开——适配器 200 行内、锁版本 0.6.5。

### P4 Fleet 与调度（5 天）

- 新建 `@gethen/fleet`：模型、ControlOp 校验、`planReconcile()`；Worker 侧
  `runtime.* / workspace.* / slot.*` 落地（systemd 单元 + 白名单）。
- `router` 重写为 fleet 调度（reuse/start/queue/park），保留升级 gate。
- 验收：2 个节点、1 个 resident + 1 个 ephemeral 槽位；节点满载 → QUEUE；
  drain → 回收；重启 Master 后按期望态收敛。

### P5 记忆接入（3 天）

- `memory-client`：HTTP 客户端、namespace、预算召回、候选晋升。
- 升级流程接 Master 召回；`evolve` 用 dreaming 做晋升校验。
- 删除 `memory*` 遗留（若 P0 未删）。
- 验收：升级从 L2 回答并落 `basis: memory`；记忆服务停掉 → 升级转人。

### P6 产品面（3 天）

- `panel`：Fleet、队列、升级、receipt、成本五个视图；`dsh` 挂 client shell。
- `cli`：`gethen doctor`、`gethen fleet ls/status`、`gethen pack ls/rollback`。
- 验收：一次完整巡检（节点、槽位、排队、升级、成本）不碰终端。

### P7 收尾（1 天）

- 把 `benchmark`/`benchmark-file`/`evaluation`/`workspace` 迁到 `bench/` 或独立仓库，
  产品 `pnpm check` 不再依赖它们。
- 更新 ADR-0007/0008、layering 文档、CHANGELOG、README；删除过期 docs。
- 验收：`pnpm check` ≤ 90s；包数 = 14；文档与代码一致。

总工期约 **30 人日（6 周）**。

## 7. 门禁与测试

- **分层**：`scripts/check-layering.mjs` 只允许 3.2 的 DAG；`core` 零依赖；
  纯包禁止 `node:fs/net/child_process`（由脚本检查）。
- **测试金字塔**：
  - 纯包：纯函数单测（router/fleet/evidence/worker-bridge）。
  - 契约：每个 bridge 适配器用 fake 实现同一套契约测试。
  - 集成：`dsh` 用 FakeBridge + 内存 store 跑完整 Ticket 流程。
  - e2e：`scripts/e2e-remote.ts`（默认跳过，需 `GETHEN_E2E=1` + 已登录 Remote）。
- **CI**：`pnpm check` = gen + layering + tsc + vitest + biome；Python 审计保留在 `bench/`。

## 8. 风险与缓解

| 风险 | 缓解 |
|---|---|
| 自研 master 的持久化/幂等/并发正确性 | 单写者锁 + operationId 幂等 + 崩溃恢复测试；用 superagent 的引擎测试当规格清单 |
| 自研授权语义不完整 | 单操作者场景，策略显式且默认拒绝；mandate 过期/重试用例必须覆盖 |
| 删除旧 runtime 让评测中断 | 评测数据/脚本保留；P7 在 bench 重建，期间不阻塞产品 |
| 远程插件 API 变动 | 版本锁 0.6.5、feature-detect、适配器隔离、降级路径 |
| 调度器复杂化 | 先实现硬过滤 + 打分 + 4 种决策，不做抢占/迁移/公平性 |
| 阻塞升级导致队列堆积 | 面板可见 + 人工重排；超时策略推迟 |
| 范围膨胀 | 每阶段有验收门；不通过不进下一阶段 |

## 9. 完成定义（DoD）

- 14 个产品包，依赖图与 3.2 一致，`pnpm check` 全绿且 ≤90s。
- 只有一份领域模型（master）、一条传输契约（worker-bridge）、一个记忆实现（外部服务）。
- M0 e2e 脚本可重复跑通，含升级与断线恢复。
- 两台节点可调度：reuse / start / queue / park 四种决策都有测试与面板展示。
- ADR-0009（本重构）与 0007/0008 状态更新，README 与 layering 同步。
- 仓库内没有任何 `dsh-superagent` 代码（仅文档引用其作为规格参考）。

## 进度（2026-11-16）

- **P0 完成**（commit `81e5a3d`）：36 → 26 包，旧 CLI runtime、`experiments/`、
  探针脚本删除；CLI 收敛为 `init/doctor/help`。
- **P1 完成**（commits `7ac9195`、`b1b7156`、后续）：自研 `@gethen/master` 落地
  ——model/engine/memory（19 tests）、sqlite store + lock + outbox（18 tests）、
  policy（32）、GitHub source（12）、prompts/executor（24）、service 端到端（3）、
  integration（2）；`@gethen/dsh` 重写为最小挂载（`gethen_status/run/overview`，
  fake launch）。全仓 27 包、456 项测试通过。
- **P1 偏差**：面板与 client shell 延后到 P6；GitHub source 已实现并测试，但尚未
  接入 service 的轮询循环（随 P4 fleet/source 调度一起接）。

### P2/P3 进度（2026-11-16 晚）

- **P2 完成**：新增 `@gethen/evidence`（receipts 聚合、eval gate、canary，25 tests）；
  删除 `observability`、`source-github`、`harnesses`、`transport-pi`、`transport-acp`、
  `store` 六个旧包（77 个跟踪文件），包数 27 → 23。
- **P3 代码完成**：
  - `@gethen/bridge-remote`：DSH Remote 客户端 → `WorkerBridge` 适配器（16 tests，
    operationId 规范化、session/state 映射、transcript 归一化；参数形状已按真实
    `remote_*` 工具 schema 对准）。
  - `@gethen/worker`：重写为 Worker 侧信封/报告/升级文本 + pack 物化（20 tests）。
  - `@gethen/dsh`：新增 `remote-launch.ts`，`ctx.get("dshRemoteClient")` + `config.remote`
    时走远端派工；否则 fake。`gethen_status` 返回 `launch: "fake"|"remote"`。
- 构件冒烟（fake remote client）：create → send → wait → read → report → receipt，
  `launch:"remote"`。
- **待办**：真实远端 e2e（需要远端 DSH 已登录 Remote 并配置 `remote.hostId/workspaceId`）；
  阻塞升级（`gethen_escalate` 工具 + Master 召回）留到 P3b。

### P3b 进度（2026-11-16 晚 2）

阻塞升级闭环已打通：

- master：`Engine.escalate/resolve`（attention、Assignment `waiting`、写回 Input），
  store 第 14 张表 `escalations` + `saveEscalation/getEscalation/listEscalations`。
- dsh：`remote-launch` 支持升级感知循环（wait → read → 先解 ESCALATION → resolver →
  send RESOLUTION → 同 session 续跑；无 resolver → parked；`maxRounds` 默认 3）；
  `onEscalation/onResolution` 回调把记录写进 store；`gethen_escalations` 工具可查。
- worker：`packages/dsh/src/worker-entry.ts` 提供 `gethen_escalate` / `gethen_report`
  两个工具，构建出 `worker.js`，可作为 Worker profile 的 Cordis 入口。
- 验证：`node scripts/verify-escalation.mjs`（10 项断言，含 answered→resolved→续跑、
  parked 不自动继续、worker bundle 工具注册）；gate 23 包 / 455 tests 全绿。
- 待用户验证：真实远端 e2e（远端 DSH 登录 Remote、master 配 `remote` + 可选
  `escalation.answer`，worker profile 挂 `worker.js`）。

### 2026-11-16 修订：控制面独立

[控制面独立重构计划](design/refactor-plan-control-plane-2026-11-16.md) 取代本文件
P4 之后的部分：Master 可独立进程（`gethen serve`）或 DSH 插件；Worker 改为
`gethen worker` 守护进程管理本机 DSH 生命周期；传输改为自建 WS 控制面（无中继），
`dsh-remote` 保留为可选适配器。协议细节见
[P4-WS 设计](design/p4-ws-control-plane.md)。
