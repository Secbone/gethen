# ADR-0008: Master/Worker control plane: one engine, blocking escalations, layered memory

Status: proposed (2026-11-16).

## Context

ADR-0007 chose a DSH-hosted Gethen with heterogeneous Workers, a portable core
and self-evolution in the control plane. The operator decisions behind this ADR
are narrower:

- **Gethen is the product.** Internal implementation may vendor or rework the
  `dsh-superagent-prototype` (MIT) and multica's runtime pattern; users must only
  see Gethen.
- **The Master runs on one remote DSH host, always on.** A local machine is one
  of several Workers; it is not the product entry point.
- **Escalations always block.** A Worker that needs a decision ends its turn and
  waits; the Master answers from its own memory, and only an unresolved question
  reaches a person. There is no timeout in the first version: a parked
  Assignment waits, visible in the panel, until a person acts.
- **No product API boundary yet.** We build what the end-to-end workflow needs
  and settle the boundary afterwards.

Two assets already cover most of the ground. The superagent prototype implements
the human-facing control plane (Engine, Trigger/Thread/Eval/Decision,
Ticket/Mandate, Policy/admission, durable effects, panel, one DSH session per
Ticket) but is scoped to one repository and has no cross-worker routing, no
Master-side escalation resolution and no layered memory. Gethen implements the
evidence plane (receipts, capability packs, channels, canary, archive,
worker-scoped memory) but its control plane is minimal.

## Decision

1. **One Master engine, owned by Gethen.** Vendor the prototype's `src/core` and
   `src/policy.ts` as `@gethen/router-core` (MIT, upstream commit recorded,
   attribution kept). `@gethen/domain` becomes a compatibility facade and is
   removed after migration; `@gethen/dsh`'s minimal ticket engine is retired so
   only one engine holds ticket state.

2. **Packages.** `router-core` (vendored domain/state machine), `router` (worker
   registry, routing, escalation brokering, pack selection), `worker-bridge`
   (the only transport contract), `memory-client` (HTTP client for the
   `gethen-memory` service, namespace conventions and bounded recall). `pack`,
   `evolve`, `observability`, `benchmark` and `panel` stay where they are and
   become the capability/evidence plane.

3. **One Remote channel, two consumers.** `@gethen/worker-bridge` never imports
   DSH. Its first adapter uses the DSH Remote client service so the Master can
   dispatch deterministically, while people observe or take over the same
   session through the Remote UI and Agent tools. A `stdio` adapter
   (multica-style `execute`/`cancel` JSONL) is added later for batch work.

4. **Assignment lifecycle.** `running → waiting → running → ended`, plus a
   durable `parked` state for questions that reached a person. A blocking
   escalation ends the Worker's turn instead of holding a model turn open. Every
   escalation and resolution is persisted, so a Master restart loses nothing.

5. **Escalation protocol.** `Escalation` (kind, question, options, context) and
   `Resolution` (answer with confidence and basis, reroute, escalate to human,
   drop) are typed records, not free text. Policy may force a human even for a
   confident answer; permission and scope questions always do in the first
   version.

6. **Routing.** Hard filters first (health, workspace, model, capability), then a
   weighted score from repository affinity, receipt evidence, load headroom and
   operator preference; an LLM tiebreak is allowed only for the residual and
   must record reason codes. No Worker is chosen without a capability match.

7. **Memory is an external service, consumed as a client.** Gethen does not
   store memory itself. `gethen-memory` runs as its own HTTP service (L1 raw
   sessions, L2 notes, L3 wiki, L4 capped injected context) with one namespace
   per Worker for project/task knowledge and one namespace for the Master for
   cross-task preferences. Promotion from a Worker namespace into the Master
   namespace uses gethen-memory's evidence-gated dream pass rather than a
   direct write. Recall for an escalation is bounded and must carry a
   confidence. The legacy `memory`, `memory-file` and `memory-evolution`
   packages stay out of the online path.

## Alternatives considered

- **Ship the prototype as the product.** Rejected: it is a single-repository
  bot without the evidence/evolution layer, and Gethen would become a branch of
  a research prototype.
- **Extend Gethen's existing control plane.** Rejected: its policy, panel,
  durable effects and Ticket lifecycle would all have to be rebuilt and kept
  compatible with the prototype's already-proven model.
- **Non-blocking escalations with defaults or timeouts.** Rejected by the
  operator for v1: irreversible actions must never be decided by a missing
  answer.
- **Stdio-only transport.** Rejected: it gives no shared session for human
  observation and requires a separate daemon.
- **Let Workers write L2 directly.** Rejected: unbounded memory pollution
  without an audit trail.

## Consequences

- We maintain a vendored fork. Keep the diff small, pin the upstream commit and
  add a sync checklist; `router-core` must stay free of DSH imports.
- The Master is a single point of progress. If it is down, escalations queue and
  Workers wait. Mitigation is visibility and fast restart, not silent defaults.
- "No timeout" allows a parked queue to grow. Mitigation is panel visibility and
  manual reprioritization; adding timeouts later is a policy change, not a
  protocol change.
- The Remote adapter depends on a community plugin's service shape. Keep it
  thin, feature-detect, and fall back to driving a Router Agent with the
  `remote_*` tools rather than coupling the engine to plugin internals.
- Evidence quality is only as good as receipt discipline: every Assignment must
  record its pack digest, Worker, model and usage.
- Master answers depend on the gethen-memory service. An unreachable memory
  service degrades to asking a person; it never licenses an invented answer.

## Implementation plan

- **M0** — Master plugin on the remote DSH, this machine as Worker #1 over the
  Remote channel: one local Trigger → Ticket → Assignment → remote session →
  one escalation answered from a seeded L2 memory → Report + receipt.
- **M1** — Worker registry, rule-based routing, pack selection, blocking
  escalation with park, human takeover through the Remote panel.
- **M2** — L2 promotion pipeline, receipt-derived routing scores, worker pool
  with several workspaces, interrupt/takeover.
- **M3** — per-Worker pack canary, replay evaluation of routing decisions, a
  second Worker machine.

## Open questions

- The external API boundary (CLI/HTTP) and its authentication/tenant model.
- Whether the Master ever becomes multi-instance; today it is one host.
- When timeouts or automatic reroute are safe enough to add.
