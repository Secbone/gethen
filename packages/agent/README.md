# @gethen/agent

Worker-side L2 mailbox consumer. It consumes `gethen.task.*` envelopes from its
own mailbox (`mailbox:<workerId>`) and drives each assignment through the
harness-agnostic `HarnessExecutor` port (`@gethen/harness`). It knows no
transport, no DSH and no `worker-protocol` types: the mailbox client/link is
injected and any harness implementing the port can run behind it.

## Message contract (application types are plain strings)

| Direction | Envelope `type` | `data` |
|---|---|---|
| in | `gethen.task.assign` | `{assignmentId, ticketId, workspace, prompt, envelope?, model?, deadlineAt?, metadata?}` |
| out | `gethen.task.progress` | `{assignmentId, event: {type, at, data}}` (at-most-once, optional throttle) |
| out | `gethen.task.ask` | `{assignmentId, escalationId, kind, question, options?, defaultOption?}` |
| in | `gethen.task.answer` | `{assignmentId?, escalationId?, resolution: {kind, option?, text?}}` |
| in | `gethen.task.cancel` | `{assignmentId?, reason?}` |
| out | `gethen.task.result` | `{assignmentId, kind, summary, question?, usage?}` |

Outgoing envelopes are addressed to the assign's `replyTo`, else
`options.masterMailbox` (default `mailbox:master`); every one carries
`subject = assignmentId` and `correlationId = causationId = <assign envelope id>`.
`asks` set `replyTo = mailbox:<workerId>` so answers can come straight back.
Unknown inbound types (including `gethen.task.progress`/`result` on a worker
inbox) are acked and ignored.

## Delivery policy

- One `HarnessTask` per assign; the harness streams events, may block on one
  escalation at a time, and finishes with exactly one report.
- The assign is **acked only after its result was sent**. A failed send keeps
  the result in an in-memory outbox and the assign message leased; the next
  `runOnce()`/tick/`flush()` retries. `gethen.task.answer` is acked after
  `sendResolution` succeeded, and nacked (requeue) when it did not.
- A harness `start` failure nacks the assign for a limited number of attempts
  (`maxAttempts`); when attempts are exhausted the consumer reports `failed` and
  acks. Invalid assigns are dead-lettered with an `onError` code.
- While an execution runs — including while it waits for an answer — its assign
  lease is renewed every `leaseRenewIntervalMs`.
- At most `quota` executions run at once. Extra assigns are requeued (with
  `retryDelayMs`); control messages (answer/cancel) are still consumed at
  capacity.
- Duplicate assigns (same `assignmentId`) are acked without re-running, for the
  lifetime of the execution and a `dedupeWindowMs` window afterwards.
- A report of any kind (`ready`/`failed`/`waiting`) is delivered as the result
  and ends the assignment. A harness that needs to keep working should emit
  events/escalations and report only when it stops.
- `stop()` stops intake, waits `stopTimeoutMs` for running executions, cancels
  what is left (`execution.cancel("agent stopped")`), waits again, flushes the
  outbox and resolves. `start()` polls by default; an injected `listen()`
  source (e.g. a mailbox link) can push messages instead.

## Injection points

`createMailboxAgent({workerId, mailbox, harness, ...})` needs a mailbox and a
`HarnessExecutor`. `mailbox` may be:

- a `MailboxClient` from `@gethen/mailbox` (its sync `receive`/`ack`/`nack`/
  `renewLeases`/`send` satisfy `MailboxLike`; every call may also return a
  promise);
- a bound remote client such as `MailboxClientLink` from
  `@gethen/mailbox-link`: it structurally satisfies `BoundMailboxLike`
  (`readonly mailbox`, calls without a mailbox argument) and is adapted by
  `toMailboxLike`. No import of `mailbox-link`/`transport` is needed, so this
  package stays on the L2 side of the two-layer split;
- any custom `MailboxLike` (tests use an in-memory fake).

`now`, `listen` and `onError` keep tests and alternate delivery loops hermetic.

## Differences from the old `worker-agent` state machine

- No transport concern: no hello/heartbeat/reconnect, no frames, no
  `worker-protocol` imports, and no `mailbox-link`/`transport` dependency. The
  agent only speaks mailbox envelopes; a remote mailbox is just another
  `MailboxLike`/`BoundMailboxLike` implementation.
- No `accepted`/`result` frame pairing and no per-assignment event numbering:
  progress is best-effort and deduped/retried by the mailbox, not replayed after
  reconnect.
- A blocking escalation is mailbox `ask` + `answer` with a correlation id; the
  lease stays alive while waiting, instead of an in-memory `entry.escalation`.
- Disconnect/restart recovery belongs to the mailbox lease + redelivery (and to
  the L1 transport/link), not to an in-process execution map. Finished results
  are retried from the outbox before the assign is acked.
- Quota, dedupe and stop are policy knobs here; the old agent hard-coded most of
  them around a single WS connection.
