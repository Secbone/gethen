# @gethen/harness

Harness-agnostic execution port for Gethen workers. A mailbox consumer starts one
`HarnessTask` on a `HarnessExecutor`; the executor streams `HarnessEvent`s, asks
blocking `HarnessEscalation`s and finishes with one `HarnessReport`. DSH
(`@gethen/executor-sdk`) is only the first implementation — a pi, opencode or
codex adapter can implement the same port without touching the mailbox,
transport or master.

## Boundary

- **Zero dependencies** and a pure module: no IO, no clock, no Node builtins,
  no imports from DSH/`@deepseek-ai`, `@gethen/master`, `@gethen/worker-agent`
  or any other Gethen package. JSON types are declared locally.
- The port never parses harness transcripts and never formats prompts: an
  adapter owns its harness vocabulary and maps it onto these five core shapes
  (`task`, `event`, `escalation`, `resolution`, `report`).
- Validation helpers are pure: they accept untrusted data from a harness or a
  transcript and return `{ ok: true, value }` or `{ ok: false, code, message }`.

## Port

| Type | Meaning |
|---|---|
| `HarnessTask` | one assignment: ids, workspace, rendered prompt, optional envelope/model/deadline/metadata |
| `HarnessEvent` | one observation: open `type`, RFC 3339 `at`, opaque JSON `data` (message/tool/usage/…) |
| `HarnessEscalation` | one blocking question: `id`, `kind`, `question`, optional `options`/`defaultOption`, `blocking: true` |
| `HarnessResolution` | the answer: `kind` (`answer`/`escalate`/`drop`), optional `option`/`text` |
| `HarnessReport` | the result: `kind` (`ready`/`failed`/`waiting`), `summary`, optional `question`/`usage` |
| `HarnessSink` | `event()`, `escalation()`, `report()` |
| `HarnessExecution` | `sendResolution(resolution)`, `cancel(reason?)` |
| `HarnessExecutor` | `start(task, sink): Promise<HarnessExecution>` |
| `HarnessCapabilities` | optional `models`, `tools`, `workspaceRoots` for placement |

Lifecycle rules:

1. `start` resolves with the handle before/while the execution runs;
2. events may arrive at any time until the report;
3. at most one escalation is waiting for a resolution at a time (answer it with
   `sendResolution`; the consumer dedupes by `escalation.id`);
4. exactly one report ends the run: `ready`/`failed` are terminal
   (`hasTerminalReport`), `waiting` leaves the execution open for a person;
5. `cancel` must cause exactly one `failed` report (never a normal report too).

Pure helpers: `validateHarnessReport`, `validateHarnessEscalation`,
`validateHarnessResolution`, `hasTerminalReport`, `isHarnessReportKind`,
`isHarnessEscalationKind`, `isHarnessResolutionKind`, `parseHarnessDeadline`
(undefined = no deadline; an invalid RFC 3339 string throws).

## How `@gethen/executor-sdk` will adapt (C4 migration)

The DSH SDK executor already has a structurally identical port; C4 becomes a
thin adapter that maps the worker-agent types to these:

| worker-agent / DSH | harness port |
|---|---|
| `ExecutorTask` | `HarnessTask` (`prompt` = envelope `prompt`, else `JSON.stringify(envelope)`; `packDigest` → `metadata.packDigest`) |
| `session.event` notification | `HarnessEvent` (`type`, `at` from `Date`, `data` = event payload) |
| `ExecutorEscalation` (`approval/request`, `user-question/ask`, tool-result `[GETHEN-ESCALATION]`) | `HarnessEscalation` |
| `ResolutionBody` | `HarnessResolution` |
| `ExecutorReport` | `HarnessReport` |

Suggested report mapping while worker-protocol still has six kinds:
`ready → ready`, `waiting → waiting`, `blocked → waiting` (with `question`),
`failed → failed`, `dropped → ready`, `progress → event` (not a report).
`executor-sdk`'s DSH-specific pieces (JSON-RPC client, transcript scanning,
`approval/request` answering, per-workspace pooling) stay in that package: the
port only sees the mapped shapes, which is what lets a future non-DSH harness
plug into the same mailbox consumer.
