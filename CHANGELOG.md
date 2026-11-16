# Changelog

Notable changes. Loosely follows [Keep a Changelog](https://keepachangelog.com/).
Authoritative architecture decisions live in [`docs/adr/`](docs/adr/); the
milestone list in [`docs/plan.md`](docs/plan.md) is historical background.

## Unreleased

### Changed
- Rename the product from Mentat to Gethen: workspace scope `@gethen/*`, CLI and
  plugin ids, `GETHEN_*` environment variables, `gethen_*` tools, `.gethen`
  state directories and the `gethen` launcher. Data under the old `.mentat/`
  state directory is reused through a compatibility symlink.

### Docs
- Add ADR-0007 and `docs/research/gethen-on-dsh-2026-11-05.md`: Gethen as a
  DSH-hosted product with a Router/Worker control plane, heterogeneous workers
  and a versioned Capability Pack for self-evolution.
- Add ADR-0008: one Master engine, blocking escalations, layered memory and a
  single Remote transport with two consumers (Master engine, human panel).

### Fixed
- DSH plugin tests clear `GETHEN_*` from the environment before `apply`, so a
  developer shell or DSH profile exporting `GETHEN_WORKER` cannot spawn a real
  engine inside the unit tests.
- The Gethen panel now calls DSH `connection.requestRejection` on `/gethen`,
  `/gethen/overview` and `/gethen/poll`, so the panel uses the same Host/Origin
  fence and signed browser cookie as the rest of the Web UI. Unauthenticated
  requests return 401 instead of exposing the overview.

### Added
- `@gethen/worker-bridge`: transport contract between the Master and Workers
  (worker manifest, dispatch, session state, escalation/resolution, receipt),
  with boundary validation and no DSH or network imports.
- `@gethen/router`: Master routing (`routeWorkers` hard filters + weighted
  affinity/evidence/load scoring), blocking escalation gating and the
  Assignment lifecycle (`running | waiting | parked | ended`).
- `CanaryMonitor` + `CanaryScheduler`: compare canary receipts with the stable
  baseline and automatically roll back above an absolute failure ceiling or a
  regression delta. `gethen_channel` reports the assessment.
- `MatrixPackEvaluator` + `GETHEN_BENCH_WORKERS`: evaluate one pack across
  several workers, namespace signals by worker, and accept only when no worker
  regresses.
- `GETHEN_WORKER=pi`: dispatch Assignments through `HarnessWorker` + the Pi
  RPC adapter, with `GETHEN_MODEL` selecting the pack model. Verified in a real
  DSH model-driven run (`worker: "pi"`, report `pong`).
- `pnpm build:dsh [out]`: bundle the DSH plugin entries and copy the Pi bridge
  plus the client-shell package into one deployable directory.
- `PackChannel`: deterministic stable/canary selection, promotion and rollback
  with durable `pack-channel.json` state. Assignments and receipts record the
  selected channel; `gethen_channel` exposes status/canary/promote/rollback,
  and the panel overview shows the active versions.
- `GethenEvolver` + `EvolutionScheduler`: read failed Assignment receipts,
  deduplicate and skip already-learned observations, then run Dream through the
  configured pack evaluator. Exposed as `gethen_evolve` and an optional
  `GETHEN_EVOLVE_INTERVAL_MS` periodic pass.
- `BenchmarkPackEvaluator`: run a `@gethen/benchmark` episode plan against each
  CapabilityPack through a `WorkerAdapter`, expose macro-averaged
  `<arm>.<metric>` signals, and wire it into `gethen_dream` with
  `GETHEN_BENCH_PLAN`/`GETHEN_BENCH_DATASET`. Verified in a real DSH
  model-driven run: passphrase benchmark 0 → 1, accepted, archived.
- `gethen_dream`: operator-configured `CommandPackEvaluator` wired through
  `@gethen/evolve`, with a real DSH model-driven E2E (quality 1 → 2, accepted,
  archived).
- `GETHEN_WORKER=opencode`: dispatch Assignments through `HarnessWorker` and
  the OpenCode ACP adapter. Verified end-to-end with the OpenCode API:
  Router session → `gethen_run` → Worker report → receipt.
- `@gethen/dsh-client`: DSH client-shell Settings → Gethen page reading the
  `/gethen/overview` projection; verified in a real `dsh web` run.
- `scripts/github-source-smoke.ts`: real `gh` auth/network smoke for the
  GitHub Source (manual; not run in `pnpm check`).
- `@gethen/dsh/panel`: server-rendered `/gethen` panel with overview and
  immediate Source poll, mounted only in web profiles.
- `@gethen/panel`: read-only overview projection (`needsPerson`, open/ended
  Tickets, assignments, decisions) and a terminal renderer; exposed through
  `gethen_overview`.
- `@gethen/dsh`: mount `GitHubSource` with a `SourcePoller` driven by
  `GETHEN_GITHUB_REPO`/`GETHEN_GITHUB_AGENT`, plus a `gethen_poll` tool.
- `@gethen/source-github`: `GitHubSource` for Issue/PR/review/comment/CI
  Triggers, persisted read positions, and an `OutboxTransport` Channel over
  `gh api`.
- `@gethen/evolve`: Dream/Evolution loop with versioned pack candidates,
  injected evaluation, gate chain, Pareto acceptance, JSON pack archive and a
  durable `dream` effect on `@gethen/effects`.
- `@gethen/pack`: versioned `CapabilityPack` contract with content addressing.
  `GethenRuntime` attaches its digest to Assignments and receipts;
  `HarnessWorker` injects prompt/model/MCP/executionPolicy and rejects
  unsupported skills/memory/tool allowlists.
- Artifact identity/adapters refactor (ADR-0006): `ArtifactRef`, `Artifact`,
  `ArtifactAdapter`, `ArtifactRegistry`, canonical-JSON `Digest`, and
  content-addressed data candidates. See
  `docs/research/artifact-architecture-2026-10-22.md`.
- `@gethen/workspace`: `ArtifactStore` CAS; `JsonStoreWorkspace` persists refs
  only and rehydrates payloads by digest.
- `@gethen/evolution`: external `EvalProtocol`, `IterationReceipt`, per-iteration
  `outcome`, and niche-aware archive selection.
- `@gethen/observability`: append-only `ReceiptStore`; `evolve` and `iterate`
  now persist a protocol digest and per-iteration receipts.
- `@gethen/workspace`: `GitWorkspace` attaches a `form: "code"` artifact
  (`digest: git:<commit>`) to every candidate.
- CI: `.github/workflows/check.yml` runs the full `pnpm check` gate on push/PR.
- `docs/adr/` decision records and this changelog.
- `@gethen/observability`: `memoryAttribution()` aggregates a trajectory's
  memory operations (injections, reads, writes, reflections, history retrieval)
  for substrate-level attribution.
- `@gethen/assistant`: `reflectionTrigger()` — evidence-gated reflection policy.
- CLI `run --reflect-auto` (reflect only on a correction or reusable failures).
- `@gethen/core`: `correctiveInstruction()`; `@gethen/session`:
  `correctiveRetries` bounded corrective re-ask for invalid final answers.

### Changed
- PAST-Bench bridge emits a `memory` attribution block; adapter logs it.

### Fixed
- `@gethen/transport-pi`: a failed/aborted model call (e.g. expired API key)
  now surfaces as `run.settled` error instead of a silent empty completion.
- `@gethen/host`: states explicitly when no historical evidence matched, so the
  model abstains instead of fabricating.

### Removed
- `IterableArtifact`, `EvalSpec`, `EvalResult`, `ArtifactSnapshot` and
  `ArtifactRebuilder`. Artifacts are now immutable values with adapters.
- Dead `buildMemoryContext` (superseded by `buildTurnContext`).
- Empty `packages/runtime` leftover directory.

### Refactor
- `memory-mcp`: `server.mjs` → `loader.mjs` (it is a loader shim).
- Extracted `@gethen/harnesses` (engine registry); `@gethen/host` is now
  engine-agnostic (ADR-0004). `compat.ts` moved to the CLI.
- Split `host.ts` (585→~515 lines): `config.ts`, `policy.ts`, `readiness.ts`.
- `assistant/history.ts` reuses `memory`'s tokenizer instead of duplicating it.
- Root `tsconfig` now type-checks `scripts/**/*.ts`; dropped the redundant
  `benchmarks/tsconfig.json`.
- Layering checker verifies the allowed-deps table against real packages.

### Added (continued)
- `@gethen/benchmark`: `BridgeRegistry` + bridge-contract ADR (ADR-0005).
- In-package `@gethen/host` contract tests.
