# RSI-Exam ↔ Gethen (preflight + minimal bridge)

RSI-Exam runs its 88 executable research tasks through
[Harbor](https://www.harborframework.com/). This directory holds the preflight
artifacts and the minimal Gethen-as-Harbor-agent bridge. The 35 public tasks are
downloadable; 53 are held back.

## Preflight (done)

1. **Data.** `benchmarks/public/download.py` (pinned HF revision) pulls
   `RSI-Exam/RSI-Exam` into `.gethen/benchmark-data/rsi-exam/` — 571 MB / 2694
   files. `benchmarks/public/discover.py` records the revision in
   `benchmarks/public/dataset-metadata.json`.
2. **Harbor.** `uv tool install harbor` (provides `harbor`/`hb`/`hr`).
3. **Inventory / resource fit.** Per-task `task.toml` gives storage, CPU, memory
   and timeouts. The lightest task is `finscope_dcf_valuation`
   (2 GB storage, 2 CPU, 512 MB); some tasks declare 8 CPU / 32 GB and 100–200 GB
   storage with a 12 h agent timeout — not all fit this host.

Note: the HF download is slow and a handful of `legal_matter_caseload_regulatory/`
`world_corpus` files intermittently fail with TLS EOF; the downloader is
resumable (re-run the same command). That task also needs a task-side API key.

## Pipeline smoke (passed) — `nop` agent, 2026-10-17

```bash
export PATH="$HOME/.local/bin:$PATH"
harbor run -p .gethen/benchmark-data/rsi-exam/finscope_dcf_valuation \
  -a nop -y -n 1 --jobs-dir /tmp/rsi-smoke
```

Result: 1 trial, 0 exceptions, mean reward ≈ 2e-6, 7 m 12 s total — the
environment image builds, the task runs, and the sealed-split verifier scores.
Artifacts: `results/2026-10-17-nop-finscope/`.

## Minimal Gethen bridge — `gethen_agent.py`

Harbor resolves agents by name **or by a local import path**
(`module.path:ClassName`), so no Harbor plugin/publish step is needed.
`GethenAgent` is a `BaseInstalledAgent` that installs the Gethen checkout and
runs `scripts/past-bench-agent.ts` inside the task container.

Validate it loads (introspectable):

```bash
export PYTHONPATH="$PWD/benchmarks/rsi-exam:$PYTHONPATH"
harbor agent schema "gethen_agent:GethenAgent"     # prints the agent options
harbor run --print-config -p .gethen/benchmark-data/rsi-exam/finscope_dcf_valuation \
  -a "gethen_agent:GethenAgent" -m opencode-go/deepseek-v4.1-flash -y -n 1 \
  --jobs-dir /tmp/rsi-smoke2
```

## Blocker (2026-10-18)

Running the Gethen agent against the lightest task fails at **agent setup**:

```
Trial finscope_dcf_valuation__YS6jxBM failed: Agent setup timed out after 360.0 seconds
Running command: apt-get update && apt-get install -y curl git ca-certificates
```

RSI-Exam task agents run with `[agent] network_mode = "no-network"` and the base
images ship no Node/curl/git, so a Gethen agent **cannot be installed at setup**.
Evidence: `results/2026-10-18-gethen-agent-blocked/`.

The bridge is real (Harbor imports it and accepts the job plan); what remains is
provisioning, not glue. To actually run Gethen on RSI-Exam, one of:

1. **Prebake a `gethen-agent` image** (Node + the Gethen checkout, no runtime
   install) and point Harbor at it — the same way Harbor's built-in agents ship
   their own images/base with a model bridge.
2. **Use Harbor's model-connection bridge** so Gethen's inner harness reaches the
   model endpoint from inside the sandbox, and pass that endpoint through
   `GETHEN_*` env (the `GETHEN_MODEL`/`GETHEN_HARNESS` knobs are already wired).
3. **Wrap an existing Harbor agent**: Gethen's substrate *is* `pi`/`opencode`; a
   thin Gethen layer over Harbor's `pi` agent would inherit its install + model
   bridge, adding only Gethen's memory/assistant layer.

Until then, treat `GethenAgent` as scaffolding: it loads and plans, but is not a
leaderboard-grade subject.
