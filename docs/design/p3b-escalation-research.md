# P3b 调研：阻塞式升级（Escalation）怎么闭环

日期：2026-11-16。目标：Worker 不再"自己猜"或"静默停住"，而是**结构化地问**，
Master 先答、答不了转人，同一会话续跑。

## 1. 外部模式（2026-11 检索）

| 来源 | 关键做法 | 我们的采用 |
|---|---|---|
| [MCP Elicitation](https://modelcontextprotocol.io/specification/2025-06-18/client/elicitation) | 服务端向客户端/用户发起**结构化请求**（JSON Schema），响应三态：accept / decline / cancel；不得索取敏感信息 | Escalation = 结构化请求（kind/question/options/default）；Resolution 三态：answer / escalate-to-human / drop |
| [LangGraph Interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts) | 图执行到 interrupt 暂停，状态由 checkpointer 持久化，恢复时把人工输入作为节点返回值 | Assignment 进 `waiting`（持久化），答复后**同一会话**继续；不重建会话 |
| [Temporal Durable Execution](https://docs.temporal.io/evaluate/understanding-temporal) | 外部事件通过 signal 进入运行中的 workflow；workflow 可等待任意长时间 | 我们的"signal"是 `remote_send_message` 发回的 `[GETHEN-RESOLUTION]`；等待无超时（用户决定） |
| [12-factor agents](https://github.com/humanlayer/12-factor-agents) | "contact humans with tool calls"：把需要人的动作建模成工具调用，而不是自由文本 | Worker 侧 `gethen_escalate` 工具；不是让模型随便说"我需要问一下" |
| [Anthropic: building effective agents](https://www.anthropic.com/engineering/building-effective-agents) | Agent 可在 checkpoint/blocker 处暂停等人；环境反馈决定下一步 | 升级即 checkpoint；答完继续，不重启任务 |

共同点：**请求结构化、状态持久化、恢复用同一个执行上下文、人工是可插拔的兜底**。

## 2. 协议（沿用既有信封，不新造）

```text
Worker → Master  [GETHEN-ESCALATION] {id, kind, question, options[], defaultOption?, blocking:true, at}
Master → Worker  [GETHEN-RESOLUTION] {id, escalation, kind:"answer", option?, text, confidence, basis[], at}
                 [GETHEN-RESOLUTION] {id, escalation, kind:"escalate", to:"human", question, at}
                 [GETHEN-RESOLUTION] {id, escalation, kind:"drop", reason, at}
```

规则：

- `blocking:true` 时 Worker **结束本轮**；Assignment → `waiting`；Ticket → `attention`（stage `person/answer`）。
- Master 先查策略/记忆（P5 接 gethen-memory；P3b 用注入的 resolver）；答不了 → `parked` + 通知人。
- 答复通过 `remote_send_message` 回到**同一 session**；Worker 下一轮开头看到 `[GETHEN-RESOLUTION]` 继续。
- 不设超时（用户决定）：parked 一直等人；面板可见。
- 每次 escalation/resolution 落库（id、ticket、assignment、question、options、status、resolution），可审计、可学习。

## 3. 轮询式闭环（P3 的现实约束）

DSH Remote 通道只有 create/send/wait/read（没有 Worker→Master 回调），所以 Master 用轮询：

```
open(session) → send(assignment)
loop:
  wait_until_idle(poll ≤ maxRounds)
  page = read(transcript)
  if escalation(page):      # 先解析升级
      persist(escalation)
      resolution = await resolver(escalation)   # 注入；默认 undefined
      if resolution:
          persist(resolution)
          send(session, buildResolutionText(resolution))
          continue
      else:                  # 转人
          persist(parked)
          return {report: waiting+question, receipt, escalations}
  if report(page): return report
```

- `resolver` 是唯一可插拔点：P3b 默认没有（→ parked）；测试/演示注入策略答案；P5 换成记忆查询。
- 轮询有界：`maxRounds` 默认 3（一次任务最多 3 次升级往返），防止无限循环。
- 幂等：escalation.id 由 Worker 生成；resolution.id 由 Master 生成；重发同一 resolution 不重复计数。

## 4. 落地清单

| # | 工作 | 位置 |
|---|---|---|
| A | 引擎 `escalate()/resolve()`；store 增 `escalations` 表与读写 | `@gethen/master` |
| B | remote launch 升级感知循环（解析→resolver→send→resume；parked 返回） | `@gethen/dsh` |
| C | Worker 角色入口 `applyWorker()`，注册 `gethen_escalate` 工具 | `@gethen/dsh` |
| D | Lead 集成：resolver 注入、`gethen_escalations` 工具、构建与门禁、验证脚本 | repo |

## 5. 验收（用户验证时按这个跑）

1. 远端 Worker 上让模型执行一个需要提问的任务（例如"两个分支哪个合并"）；
2. Worker 调用 `gethen_escalate` → Ticket 变 `person/answer`，Assignment `waiting`；
3. Master 注入固定答案 → 自动发回 RESOLUTION → 同一 session 出最终 REPORT；
4. `gethen_escalations` 能看到 question/status/resolved；
5. 无 resolver 时 → `parked`，问题可见，不自动继续。

## 6. 参考

- MCP Elicitation spec（结构化请求/三态响应）
- LangGraph Interrupts（暂停/持久化/恢复）
- Temporal Durable Execution（signal/等待）
- 12-factor agents factor 7（contact humans with tool calls）
- Anthropic: Building effective agents（blockers/checkpoints）
