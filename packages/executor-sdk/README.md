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
pool seam), `requestTimeoutMs?`, `now?`.

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
4. In the background it `session/wait`s, then parses the last report block with
   `@gethen/master`'s `parseReportText` and narrows it to the port's three
   states: `ready → ready`, `failed → failed`, `waiting`/`blocked`/`progress →
   waiting` (question preserved, so blocked becomes `waiting + question`),
   `dropped → ready`. A missing report becomes `waiting`; an invalid report
   block, a failed wait or a failed prompt becomes `failed`.
5. Errors never escape `start`: runtime initialize/prompt/wait, resolution
   delivery and cancellation failures are reported as a single
   `HarnessReport{kind:"failed"}`.

## DSH channels → harness escalations

| DSH SDK channel | Harness escalation | Resolution mapping |
|---|---|---|
| `approval/request` (`allow <tool>?`) | `permission`, options `allow` / `reject`, `blocking: true` | `option:"allow"` → DSH `allowed-once`; anything else (`reject`, `escalate`, `drop`, cancel) → `rejected` |
| `user-question/ask` | one `decision` escalation per question, asked **sequentially** (one live escalation at a time) | `answer` → DSH `answers:[{id, selected:[option label], custom?}]`; `escalate` / `drop` → `{cancelled:true}` |
| tool-result `[GETHEN-ESCALATION]` (compat `gethen_escalate` tool) | the escalation it carries (kind/question/options, `blocking: true`) | the answer is written back into the same session as a `[GETHEN-RESOLUTION]` block, then the executor waits again (at most 5 tool-result escalations per execution) |

## Deadline and cancellation

- An assignment whose `deadlineAt` has already passed is reported `failed`
  before any runtime is spawned.
- The DSH SDK protocol has **no per-turn cancel/interrupt method** — only
  process `shutdown`/EOF. `HarnessExecution.cancel(reason?)` therefore rejects
  any pending escalation, closes the workspace's SDK client (killing **every**
  session it hosts), and reports `{kind:"failed", summary:"cancelled"}` (or
  `"cancelled: <reason>"`) exactly once. Other assignments on the same workspace
  fail too; use a distinct workspace per assignment if isolation matters.

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
