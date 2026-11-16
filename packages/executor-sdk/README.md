# @gethen/executor-sdk

The real Worker execution driver: it implements the `SessionExecutor` port from
[`@gethen/worker-agent`](../worker-agent) with DSH SDK JSON-RPC sessions from
[`@gethen/sdk-driver`](../sdk-driver). One `dsh --profile sdk` runtime is kept
per workspace; each dispatch runs in its own DSH session named after its
assignment (`sessionId = assignmentId`).

It also ships a `HarnessExecutor` adapter (`src/harness-adapter.ts`) over the
same implementation, so the newer `@gethen/agent` mailbox consumer can run DSH
behind the harness-agnostic port without importing DSH. See
[HarnessExecutor adapter](#harnessexecutor-adapter).

## Flow

1. `start(task, sink)` ensures the workspace runtime exists, starts it with
   `dsh --profile sdk`, and `initialize`s it once with the configured
   `provider` / `model` / `cwd` (`cwdFor(workspace)`, defaulting to the
   workspace) plus `approvalRequests: true` and `userQuestions: true`.
2. It subscribes to `session.event` (filtered by `sessionId`), forwards each
   event to `sink.event`, and scans **every string value** in each event payload
   — assistant text, tool results, tool inputs, nested arrays/objects — for a
   complete `[GETHEN-REPORT]` or `[GETHEN-ESCALATION]` block. Only individual
   strings are considered (a block split across events or fields is never glued
   together) and the last complete report block wins.
3. It prompts the session with `task.envelope.prompt`; when the envelope has no
   `prompt` string, the whole envelope is sent as JSON.
4. In the background it `session/wait`s, then reports what the worker said:
   the parsed report, or `{kind:"waiting", summary:"worker produced no report"}`.
   Transport errors become `{kind:"failed"}` and an invalid report block becomes
   a `failed` report too.
5. A `[GETHEN-ESCALATION]` block found in an event (the worker-side
   `gethen_escalate` compat tool returns one in its tool result) is surfaced as a
   blocking `sink.escalation`; after the Master answers, the executor prompts the
   same session with a `[GETHEN-RESOLUTION]` block and waits again. A report in
   the same batch wins over an escalation; at most 5 tool-result escalations are
   answered per execution.

## DSH channels → escalations

| DSH SDK channel | Master escalation | Resolution mapping |
|---|---|---|
| `approval/request` (`allow <tool>?`) | `permission`, options `allow` / `reject` | `option:"allow"` → DSH `allowed-once`; anything else (`reject`, `escalate`, `drop`, cancel) → `rejected` |
| `user-question/ask` | one blocking `decision` per question, asked **sequentially** (the worker protocol carries one live escalation at a time) | `answer` → DSH `answers:[{id, selected:[option label], custom?}]`; `escalate` / `drop` → `{cancelled:true}` |

Approval and question handlers are routed to the execution by `sessionId`.
Questions with options map the option id (`q0`, `q1`, …) back to its label; a
question without options (or an answer whose `option` matches nothing) is
returned as `custom` text. A multi-question batch is labelled `(1/N)`, `(2/N)`,
… so a person sees the progress.

## Cancellation

The DSH SDK protocol has **no per-turn cancel/interrupt method** — only process
`shutdown` and stdio EOF. Therefore `ExecutionHandle.cancel(reason?)`:

- rejects any pending escalation (DSH answers `rejected` / `cancelled`),
- closes the workspace's SDK client, which kills **every session hosted by that
  runtime**, not just this assignment,
- reports `{kind:"failed", summary:"cancelled"}` (or `"cancelled: <reason>"`)
  exactly once.

Because one runtime is shared per workspace, cancelling one assignment also
ends other assignments running on the same workspace; their waits reject and
they report `failed`. If a deployment needs per-assignment isolation for
cancellation, configure a distinct workspace string per assignment so each gets
its own runtime, or wait for a per-session cancel in the DSH SDK protocol.

`SdkExecutor.close()` shuts down every pooled runtime and stops accepting work;
the daemon calls it when the agent stops.

## HarnessExecutor adapter

`createSdkHarnessExecutor(options)` wraps `createSdkExecutor` and exposes it as
the `HarnessExecutor` port from [`@gethen/harness`](../harness). The options are
the same (`provider`, `model`, `dshCommand`, `dshArgs`, `cwdFor`,
`clientFactory`, `requestTimeoutMs`, `now`); the per-workspace DSH client pool,
JSON-RPC sessions, transcript scanning and approval/question answering are
unchanged.

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

Shape mapping:

| worker-protocol / DSH | harness port |
|---|---|
| `ExecutorTask` | `HarnessTask` — the rendered `prompt` is folded into the envelope (`{...envelope, prompt}`), `metadata.packDigest` becomes `packDigest` |
| `ExecutorEvent` (session event) | `HarnessEvent` — `type`, RFC 3339 `at` from the event `time` (or `now()`), opaque `data` |
| `ExecutorEscalation` | `HarnessEscalation` — `escalationId` → `id`, options/defaultOption pass through, `blocking: true` |
| `ResolutionBody` | `HarnessResolution` — missing `text` becomes `""` |
| `ExecutorReport` | `HarnessReport` — `ready`/`dropped → ready`, `failed → failed`, `waiting`/`progress`/`blocked → waiting` (question preserved, so a blocked report becomes `waiting` + `question`) |

Errors (runtime initialize, prompt, a later resolution/cancel delivery) become a
single `HarnessReport{kind:"failed"}`; `start` never rejects at the consumer.
`sendResolution`/`cancel` delegate to the underlying execution handle.
`cancel` keeps the existing DSH limitation: the SDK protocol has no per-turn
cancel, so it closes the workspace runtime (all sessions on it) and reports
`failed`. `SdkHarnessExecutor.close()` releases every pooled runtime.

When migrating a Worker from `@gethen/worker-agent` (transport-owned execute
loop) to `@gethen/agent` (mailbox consumer), replace `createSdkExecutor(...)` in
the wiring with `createSdkHarnessExecutor(...)`; the old `SessionExecutor`
exports stay for the legacy WS agent.

## Options

| Option | Meaning |
|---|---|
| `dshCommand` / `dshArgs` | Runtime command; defaults to `GETHEN_DSH`/`dsh` with `--profile sdk` |
| `provider` / `model` | Route passed to `initialize` |
| `cwdFor(workspace)` | Session cwd per workspace (defaults to the workspace string) |
| `clientFactory()` | Injection seam for tests or a shared client pool; returns a started client, or an unstarted one the executor starts |
| `requestTimeoutMs` | JSON-RPC timeout handed to the client (also bounds `session/wait` unless the client is configured otherwise) |
| `now()` | Injectable clock, used for the assignment-deadline guard |

An assignment whose `deadlineAt` has already passed is reported `failed` before
any runtime is spawned.

## Real e2e (opt-in)

`scripts/e2e-sdk.mjs` drives the whole path — `startServe` with an in-memory
worker token → worker-agent over WS → this executor → a real
`dsh --profile sdk` child — and asserts a `ready` report, a persisted receipt,
`/health.workers === 1` and the file the worker was asked to create:

```sh
GETHEN_E2E=1 node scripts/e2e-sdk.mjs
# optional: GETHEN_E2E_TRACE=1 for the live worker session trace
```

It spawns the runtime with the overlay `scripts/e2e/sdk-profile.patch.yml`
(pi-ai `opencode-go` provider, `OPENCODE_API_KEY` credential reference, and the
Gethen worker tool entry) and never edits the user's `sdk` profile.
