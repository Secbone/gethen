/**
 * Worker-side Gethen logic: the session-facing envelopes and the
 * capability-pack materialization the Worker needs before it starts.
 *
 * Typical DSH Worker wiring:
 *
 * 1. The first message of an Assignment is the Master's prompt. Parse it:
 *    `const envelope = parseAssignmentPrompt(firstMessage)` — it carries the
 *    ticket/assignment ids, workspace, pack digest, objective and the report
 *    contract (`envelope.report`).
 * 2. Materialize the capability pack the dispatch carried:
 *    `const pack = materializePack(dispatch.pack)` — apply `systemPrompt`,
 *    `role`, `model`, `skills` and `mcp` to the new session.
 * 3. Do the work; stream progress reports with `buildReportText` when useful.
 * 4. When a person must decide, send `buildEscalationText(escalation)` and end
 *    the turn. The Master answers with a resolution block, read with
 *    `parseResolutionText`; otherwise it escalates the question to a person.
 * 5. Finish the Assignment with exactly one final `buildReportText` report
 *    (`ready`, `failed`, `blocked`, `waiting` or `dropped`).
 *
 * All functions are pure: no IO, no clock, no DSH imports.
 */
export * from "./envelope";
export * from "./pack";
