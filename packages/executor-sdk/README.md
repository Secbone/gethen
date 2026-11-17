# @gethen/executor-sdk

DSH as a `HarnessExecutor`: it implements the harness-agnostic execution port
from [`@gethen/harness`](../harness) with DSH SDK JSON-RPC sessions from
[`@gethen/sdk-driver`](../sdk-driver). `@gethen/agent` (the Worker mailbox
consumer) can run DSH through this adapter without importing DSH, the transport
or any worker protocol types.

One `dsh --profile sdk` runtime is kept per workspace; every assignment runs in
its own DSH session named after its assignment (`sessionId = assignmentId`).

## Usage

```ts
import { createMailboxAgent } from "@gethen/agent";
import { createSdkHarnessExecutor } from "@gethen/executor-sdk";

const harness = createSdkHarnessExecutor({
  provider: "deepseek",
  model: "deepseek-v4.1-flash",
  dshArgs: ["--profile", "sdk", "--patch", "/path/to/sdk-profile.patch.yml"],
});
const agent = createMailboxAgent({ workerId: "worker-1", mailbox, harness });
// ... agent.start(); on shutdown: await agent.stop(); await harness.close();
```

`createSdkHarnessExecutor(options)` returns a `HarnessExecutor` with an extra
`close()` that releases the pooled DSH runtimes. Options: `provider`, `model`,
`dshCommand?`, `dshArgs?`, `cwdFor?(workspace)`, `clientFactory?` (test/shared
pool seam), `requestTimeoutMs?`, `idleTimeoutMs?` (silence watchdog, default
300000), `interruptGraceMs?` (default 1000), `isolation?: "shared" | "process"`
(default `"shared"`), `maxLogChars?` (default 8000; `0` disables truncation),
`sessionStore?` (default `new DshSessionStore()`), `sessionListLimit?`
(default 100), `now?`.

## Flow

1. `start(task, sink)` ensures the workspace runtime exists, starts it with
   `dsh --profile sdk`, and `initialize`s it once with the configured
   `provider` / `model` / `cwd` (`cwdFor(workspace)`, defaulting to the
   workspace) plus `approvalRequests: true` and `userQuestions: true`.
2. It subscribes to `session.event` (filtered by `sessionId`), forwards each
   event to `sink.event` as `HarnessEvent` (`type`, RFC 3339 `at`, opaque
   `data`), and scans **every string value** in each event payload — assistant
   text, tool results, tool inputs, nested arrays/objects — for a complete
   `[GETHEN-REPORT]` or `[GETHEN-ESCALATION]` block. Only individual strings are
   considered (a block split across events or fields is never glued together)
   and the last complete report block wins.
3. It prompts the session with `task.prompt`.
4. Completion is **event-driven**: the session's `turn/end` event finishes the
   turn; a `session.status → idle` notification is a fallback for a harness that
   misses it. `session/wait` runs in the background only as a fallback for a
   broken event stream and has **no total-duration timeout** (only a silence
   watchdog). The last valid report block is parsed with `@gethen/master`'s
   `parseReportText` and narrowed to the port's three states: `ready → ready`,
   `failed → failed`, `waiting`/`blocked`/`progress → waiting` (question
   preserved, so blocked becomes `waiting + question`), `dropped → ready`. A
   missing report becomes `waiting`; an invalid report block or a failed wait
   becomes `failed`.
5. A **silence watchdog** (`idleTimeoutMs`, default 5 min) fails the execution
   with `no session events for <n>ms` when the stream goes quiet; it resets on
   every `session.event`. `task.deadlineAt` also fails the execution when it
   passes. Both hard-stop the runtime.
6. Errors never escape `start`: runtime initialize/prompt/wait, resolution
   delivery and cancellation failures are reported as a single
   `HarnessReport{kind:"failed"}`.

## DSH channels → harness escalations

| DSH SDK channel | Harness escalation | Resolution mapping |
|---|---|---|
| `approval/request` (`allow <tool>?`) | `permission`, options `allow` / `reject`, `blocking: true` | `option:"allow"` → DSH `allowed-once`; anything else (`reject`, `escalate`, `drop`, cancel) → `rejected` |
| `user-question/ask` | one `decision` escalation per question, asked **sequentially** (one live escalation at a time) | `answer` → DSH `answers:[{id, selected:[option label], custom?}]`; `escalate` / `drop` → `{cancelled:true}` |
| tool-result `[GETHEN-ESCALATION]` (compat `gethen_escalate` tool) | the escalation it carries (kind/question/options, `blocking: true`) | the answer is written back into the same session as a `[GETHEN-RESOLUTION]` block, then the executor waits again (at most 5 tool-result escalations per execution) |

## Log events (stderr + lifecycle)

The runtime's stderr and execution lifecycle are forwarded as extra
`HarnessEvent`s with `type: "log"`, so a Master can see the whole worker process
even when the model stays silent:

| `data` | When |
|---|---|
| `{stream: "stderr", text}` | every stderr chunk from the shared/dedicated `dsh` process |
| `{phase: "start", text}` | the assignment session was prompted |
| `{phase: "end", text}` | the execution settled (`text` is `"<report.kind>: <summary>"`) |
| `{phase: "watchdog", text}` | the silence watchdog fired |
| `{phase: "deadline", text}` | the assignment deadline passed |
| `{phase: "cancel", text}` | a cancel/interrupt started |

Each `text` is bounded by `maxLogChars` (default 8000) and ends with
`…[truncated]` when shortened, so a noisy runtime cannot explode mailbox
messages. Log events **do not** touch the live `HarnessTaskState` or its `seq`
(the session-event sequence stays a session-event count). The per-execution
stderr listener is unsubscribed when the execution settles or the client closes;
on a shared client every execution has its own listener and unsubscribes
independently.

## Session selection

`HarnessTask.session` chooses the DSH session (the executor keeps an in-memory
session registry; `executor.sessions()` returns its snapshot):

| `session` | Behavior |
|---|---|
| omitted, or `{mode: "new"}` | a fresh session named after the assignment (`sessionId = assignmentId`); an optional `key` is registered for later reuse |
| `{mode: "reuse", sessionId}` | `resume(sessionId)` on the workspace runtime, then prompt that same session; a failed resume reports `failed` (no silent fallback) |
| `{mode: "reuse", key}` | registry lookup: the first use creates a session named after that assignment and records `key → sessionId`; later uses resume the recorded session |

`state().sessionId` names the attached session and `sessions()` lists
`{sessionId, key?, workspace, lastUsedAt, activeAssignmentId?}` entries (copies;
mutating the snapshot does not change the registry).

**Concurrency policy:** a session (or a registry key) that already has a running
assignment rejects the second start with a `failed` report
(`session "<id>" is already running assignment "<assignment>"`); tasks are not
queued. `interruptGraceMs`/`cancel` still apply per execution.

**Process isolation:** `isolation: "process"` spawns a dedicated runtime per
assignment; reuse still works because `resume` runs against the newly spawned
client (the DSH store is shared), and the dedicated runtime is terminated after
the execution.

**Session directory (`sessions()`):** merges the executor registry (key,
activeAssignmentId, lastUsedAt) with `@gethen/dsh-sessions`'
`DshSessionStore.list()` summaries (config, persisted state, lastActivityAt,
eventCount, tokenUsage, hasLog). Sessions with a running assignment report
`state: "running"`; entries are sorted by `lastActivityAt ?? lastUsedAt`
descending and capped at `sessionListLimit`. Store failures degrade to
registry-only (no throw). The cache is awaited once per `start` and refreshed in
the background after `finish`.

**History (`sessionHistory(sessionId, {limit?, afterSeq?}))`:** passes through
to `DshSessionStore.history`; unknown sessions and read failures return `[]`
(never throw), so the Master/agent can render logs safely. This gives the Master
the config/state/history needed to decide new-vs-reuse for the next assignment.

## Live state, steering and isolation

`HarnessExecution.state()` returns a `HarnessTaskState` snapshot (`phase`,
`step`, `lastTool`, `lastEventAt`, truncated `lastMessage`, `seq`) updated from
the event stream: `step/start → thinking + step`, `tool/call → tool + lastTool`,
`tool/result → thinking`, `assistant/message → lastMessage`, `turn/end → done`,
and `failed` on failure. The snapshot is a copy; mutating it does not affect the
execution.

`HarnessExecution.steer(text)` queues another `session/prompt` into the running
session (best effort; failures are ignored). `HarnessExecution.interrupt()` is
the hard-stop sequence: reject a pending escalation, steer `STOP_STEER_TEXT`,
wait `interruptGraceMs`, then terminate/close the runtime.

`isolation`:

| Mode | Runtime | Completion cleanup | Cancel/interrupt cleanup |
|---|---|---|---|
| `"shared"` (default) | one pooled `DshSdkClient` per workspace, reused by assignments | session binding removed; runtime kept for the next assignment | the shared client is closed (all its sessions end) |
| `"process"` | one dedicated `DshSdkClient` per assignment | dedicated runtime is `terminate()`d (SIGKILL) | dedicated runtime is `terminate()`d |

## Deadline and cancellation

- An assignment whose `deadlineAt` has already passed is reported `failed`
  before any runtime is spawned; a future deadline arms a timer that fails and
  hard-stops the execution.
- The DSH SDK protocol has **no per-turn cancel/interrupt method** — only
  process `shutdown`/EOF. `HarnessExecution.cancel(reason?)` therefore runs the
  interrupt sequence (reject pending escalation → steer a stop → grace period →
  terminate/close) and reports `{kind:"failed", summary:"cancelled"}` (or
  `"cancelled: <reason>"`) exactly once. In `"shared"` isolation the whole
  workspace runtime closes, so other assignments on it fail too; use
  `isolation: "process"` when per-assignment isolation matters.

## Migration note

The legacy `SessionExecutor`/worker-agent implementation was removed in C7.
Wiring that used `createSdkExecutor(...)` with `@gethen/worker-agent` should now
build `createSdkHarnessExecutor(...)` and hand it to `@gethen/agent`'s
`createMailboxAgent(...)`; the old WS agent (`worker-agent`) no longer has a
DSH executor in this package.

## Overriding the SDK profile

The overlay (provider route, worker tool entry) is deployment configuration; see
`scripts/e2e-sdk.mjs` and `scripts/e2e/sdk-profile.patch.yml` for the opt-in
end-to-end profile. Nothing in the profile is modified by this package: pass
`dshArgs: ["--profile", "sdk", "--patch", ...]` or use `GETHEN_DSH`.
