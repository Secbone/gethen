# @gethen/worker

Worker-side Gethen logic: the session-facing envelopes and the capability-pack
materialization a Worker applies before it starts working. Pure module — no IO,
no clock, no DSH imports. The Master side of the same protocol lives in
`packages/master/src/prompts.ts`; the marker strings must stay identical.

## Envelope protocol

| Direction | Markers | Body |
|---|---|---|
| Master → Worker (task) | `[GETHEN-ASSIGNMENT]` … `[/GETHEN-ASSIGNMENT]` | `gethen/assignment/v1`: ticket/assignment ids, workspace, pack digest, objective, report contract |
| Worker → Master (result) | `[GETHEN-REPORT]` … `[/GETHEN-REPORT]` | `kind`, `summary`, optional `question`, `at` |
| Worker → Master (question) | `[GETHEN-ESCALATION]` … `[/GETHEN-ESCALATION]` | escalation id/kind/question/options/context/blocking |
| Master → Worker (answer) | `[GETHEN-RESOLUTION]` … `[/GETHEN-RESOLUTION]` | `id`, optional `option`, `text`, `basis` |

Parsers return `undefined` when the marker is absent and throw when a block is
present but malformed, so a truncated or invented block never silently becomes
a valid envelope.

## Flow

1. The first message of an Assignment is the Master's prompt. Call
   `parseAssignmentPrompt(firstMessage)`; it returns the envelope or `undefined`
   when the message is not an assignment.
2. Call `materializePack(dispatch.pack)` and apply the returned `systemPrompt`,
   `role`, `model`, `skills` and `mcp` to the Worker session. A malformed pack
   throws instead of starting a Worker with half its instructions.
3. Do the work. Send `buildReportText({ kind: "progress", … })` when useful.
4. When a person must decide, send `buildEscalationText(escalation)` and end the
   turn. The Master replies with a resolution block, read by
   `parseResolutionText`, or escalates the question to a person.
5. End the Assignment with exactly one final report (`ready`, `failed`,
   `blocked`, `waiting` or `dropped`).
