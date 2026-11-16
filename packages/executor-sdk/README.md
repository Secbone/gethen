# @gethen/executor-sdk

The real Worker execution driver: it implements the `SessionExecutor` port from
[`@gethen/worker-agent`](../worker-agent) with DSH SDK JSON-RPC sessions from
[`@gethen/sdk-driver`](../sdk-driver). One `dsh --profile sdk` runtime is kept
per workspace; each dispatch runs in its own DSH session named after its
assignment (`sessionId = assignmentId`).

## Flow

1. `start(task, sink)` ensures the workspace runtime exists, starts it with
   `dsh --profile sdk`, and `initialize`s it once with the configured
   `provider` / `model` / `cwd` (`cwdFor(workspace)`, defaulting to the
   workspace) plus `approvalRequests: true` and `userQuestions: true`.
2. It subscribes to `session.event` (filtered by `sessionId`), forwards each
   event to `sink.event`, and remembers the last `assistant/message` text that
   carries a `[GETHEN-REPORT]` block.
3. It prompts the session with `task.envelope.prompt`; when the envelope has no
   `prompt` string, the whole envelope is sent as JSON.
4. In the background it `session/wait`s, then reports what the worker said:
   the parsed report, or `{kind:"waiting", summary:"worker produced no report"}`.
   Transport errors become `{kind:"failed"}` and an invalid report block becomes
   a `failed` report too.

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
