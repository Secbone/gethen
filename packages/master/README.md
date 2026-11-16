# @gethen/master

自研控制面引擎（P1）。纯领域状态机 + SQLite 持久化 + durable outbox，执行面通过
`LaunchPort` 注入，不依赖 DSH 或远程通道。

## 模块

| 文件 | 作用 |
|---|---|
| `model.ts` | Trigger/Thread/Eval/Decision/Mandate/Ticket/Assignment/Input/Report/Effect，`ticketStage()` |
| `ports.ts` | `Repositories`/`Repo`、`Clock`、`Ids`、`PolicyPort`、`SourcePort`、`LaunchPort`、`ReceiptSink`、`HttpPort`、`SourceState` |
| `engine.ts` | `receive/evaluateDue/candidate/decide/assignmentStarted/report/stopTicket/retryTicket/note/attach`；所有副作用经 `drain()` 返回 |
| `memory.ts` | 内存版 `Repositories`（测试用） |
| `store.ts` | `node:sqlite` 持久化（13 张表）、事务、effects 队列、operations 幂等、receipts |
| `outbox.ts` | 至少一次 relay：claim → handler → complete，指数退避 |
| `lock.ts` | 单写者文件锁（死 pid 接管） |
| `policy.ts` | 单操作者授权、mandate 有效性、resume 规则 |
| `sources/github.ts` | GitHub 轮询 → Trigger、水位去重、评论回写 |
| `prompts.ts` | `[GETHEN-ASSIGNMENT]` 信封、`[GETHEN-REPORT]` 解析、resolution 文本 |
| `executor.ts` | `launch` effect handler：assignmentStarted → LaunchPort → report → receipt |
| `service.ts` | 进程级装配：lock + store + engine + outbox，`submit(trigger)` 端到端 |
| `fake.ts` | P1 的确定性 `LaunchPort`，P3 换成 WorkerBridge 适配器 |

## 用法

```ts
const service = new MasterService({
  dbPath: join(dir, "master.sqlite"),
  policy: { version: 1, acceptMode: "auto", rules: [], sources: { local: { allowed: ["local-user"], bots: [] } } },
  launch: createFakeLaunch(),
});
service.start();
await service.submit(trigger); // receive → eval → accept → outbox launch → report → receipt
service.stop();
```

## 不变量

- 引擎不读时钟、不写库、不发请求；时间/ID/执行全部注入。
- 业务状态、effect 入队、operation 收据在同一事务。
- 一个 Ticket 同时只有一个 live Assignment；阻塞升级让 Assignment 进入 `waiting`。
- `ticketStage()` 是面板与引擎对"在等谁"的唯一口径。
