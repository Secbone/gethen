"""PAST-Bench runtime adapter that runs Gethen as the subject agent.

This is the upstream side of the minimal Gethen <-> PAST-Bench bridge. It is a
one-shot CLI adapter (like the built-in codex/claude adapters) that shells out
to ``scripts/past-bench-agent.ts`` in the Gethen checkout, passing the task
prompt on stdin and reading a single JSON result back.

It deliberately:
  * keeps the real ``HOME`` (so Gethen's Pi/OpenCode credential discovery works),
    unlike the stock CLI adapters which isolate HOME;
  * ignores PAST-Bench's provider model/key wiring and lets Gethen resolve the
    model from ``GETHEN_MODEL`` / GETHEN's own registry.

Required environment:
  GETHEN_ROOT   path to the Gethen checkout (contains scripts/past-bench-agent.ts)
Optional:
  GETHEN_NODE       node binary (default: "node")
  GETHEN_HARNESS    Gethen harness id: pi | acp | fake (default: "pi")
  GETHEN_MODEL      Gethen model id (default: this adapter entry's model_id)
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from past_bench.models.content import TextBlock
from past_bench.models.message import Message
from past_bench.models.trace import TokenUsage
from past_bench.runtime.adapters.cli_agent import _OneShotCliAdapter
from past_bench.runtime.protocol import StepResponse


class GethenCliAdapter(_OneShotCliAdapter):
    """Drive one Gethen host turn per PAST-Bench task."""

    cli_name = "gethen"

    def _run_cli(self, prompt: str) -> StepResponse:
        root = os.environ.get("GETHEN_ROOT")
        if not root:
            return StepResponse(status="error", error="GETHEN_ROOT is required for the Gethen adapter")
        script = Path(root) / "scripts" / "past-bench-agent.ts"
        if not script.exists():
            return StepResponse(status="error", error=f"Gethen bridge script not found: {script}")

        node = os.environ.get("GETHEN_NODE", "node")
        harness = os.environ.get("GETHEN_HARNESS", "pi")
        model = os.environ.get("GETHEN_MODEL") or self.request.model.model_id or ""

        command = [
            node,
            "--import",
            "tsx",
            str(script),
            "--cwd",
            str(self._work_dir),
            "--harness",
            harness,
            "--timeout-ms",
            str(self.timeout_seconds * 1000),
        ]
        if model:
            command += ["--model", model]

        # Real HOME so Gethen finds its own credentials; run node from the Gethen
        # root so the bare `tsx` loader resolves from its node_modules, while the
        # host work dir is passed explicitly via --cwd above.
        env = os.environ.copy()
        proc = subprocess.run(
            command,
            input=prompt,
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds + 30,
            env=env,
            cwd=str(root),
        )

        payload = None
        for line in reversed(proc.stdout.splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
                break
            except json.JSONDecodeError:
                continue
        if not isinstance(payload, dict):
            detail = (proc.stderr or proc.stdout or "").strip()[-1000:]
            return StepResponse(status="error", error=f"gethen returned no JSON result: {detail}")

        usage_payload = payload.get("usage") or {}
        usage = TokenUsage(
            input_tokens=int(usage_payload.get("input_tokens") or 0),
            output_tokens=int(usage_payload.get("output_tokens") or 0),
        )
        if payload.get("error"):
            return StepResponse(status="error", error=str(payload["error"]), usage=usage)

        memory = payload.get("memory")
        if isinstance(memory, dict):
            # Substrate attribution: surface Gethen's memory operations in the
            # trial log. PAST-Bench's own dispatch metrics parse agent tool calls,
            # so this is exposed for audit rather than folded into its counters.
            print(f"[gethen-memory] {json.dumps(memory, sort_keys=True)}", file=sys.stderr, flush=True)

        text = str(payload.get("final_output") or "").strip()
        return StepResponse(
            status="finished",
            assistant_message=Message(role="assistant", content=[TextBlock(text=text)]),
            usage=usage,
            final_output=text,
        )
