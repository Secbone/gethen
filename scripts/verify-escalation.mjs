#!/usr/bin/env node
// Deterministic P3b verification: no network, no model, no real DSH.
// Exercises the built plugin bundle with an injected fake Remote client and
// asserts both escalation paths (answered -> resolved + resumed, unanswered -> parked).
//
// Usage: node scripts/verify-escalation.mjs [bundleIndexJs]
// Default bundle: ~/.local/share/gethen/dsh/index.js (built by `pnpm build:dsh`).

import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";

const bundle = process.argv[2] ?? join(homedir(), ".local/share/gethen/dsh/index.js");
const workerBundle = join(dirname(bundle), "worker.js");

if (!existsSync(bundle)) {
  console.error(`bundle not found: ${bundle}\nrun: pnpm build:dsh ~/.local/share/gethen/dsh`);
  process.exit(2);
}

const { apply } = await import(bundle);

function fakeRemote() {
  let reads = 0;
  const calls = [];
  return {
    calls,
    async devices() {
      return [{ deviceId: "dev-a", name: "worker-1", online: true }];
    },
    async listRemoteWorkspaces() {
      return [{ workspaceId: "ws-1", path: "/srv", title: "project" }];
    },
    async runSessionTool(operation, args) {
      calls.push({ operation, args });
      if (operation === "remote_create_session") return { sessionId: "sess-1" };
      if (operation === "remote_wait_sessions") {
        return { sessions: [{ sessionId: "sess-1", state: "idle" }] };
      }
      if (operation === "remote_read_session") {
        reads += 1;
        if (reads === 1) {
          return {
            events: [
              {
                seq: 1,
                at: "2026-11-16T00:00:00.000Z",
                type: "message",
                text: '[GETHEN-ESCALATION]{"id":"escalation-1","kind":"decision","question":"merge or rebase?","options":[{"id":"merge","label":"merge"},{"id":"rebase","label":"rebase"}],"blocking":true,"at":"2026-11-16T00:00:00.000Z"}[/GETHEN-ESCALATION]',
              },
            ],
            truncated: false,
          };
        }
        return {
          events: [
            {
              seq: 2,
              at: "2026-11-16T00:01:00.000Z",
              type: "message",
              text: '[GETHEN-REPORT]{"kind":"ready","summary":"done","at":"2026-11-16T00:01:00.000Z"}[/GETHEN-REPORT]',
            },
          ],
          truncated: false,
        };
      }
      return { ok: true };
    },
  };
}

function context(client) {
  const tools = [];
  const services = new Map();
  return {
    tools,
    ctx: {
      provide: (name, value) => {
        services.set(name, value);
        return () => {};
      },
      tools: {
        register: (definition) => {
          tools.push(definition);
          return () => {};
        },
      },
      get: (name) => (name === "dshRemoteClient" ? client : undefined),
    },
  };
}

async function scenario(config) {
  const client = fakeRemote();
  const { ctx, tools } = context(client);
  apply(ctx, {
    dir: join(
      process.env.TMPDIR ?? "/tmp",
      `gethen-verify-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    ),
    remote: { hostId: "dev-a", workspaceId: "ws-1", worker: "worker-1" },
    ...config,
  });
  const byName = Object.fromEntries(tools.map((tool) => [tool.name, tool]));
  const run = await byName.gethen_run.execute({ text: "organize the branch" }, {});
  const status = await byName.gethen_status.execute({}, {});
  const escalations = (await byName.gethen_escalations.execute({}, {})).escalations;
  const resolutionSent = client.calls.some(
    (call) =>
      call.operation === "remote_send_message" &&
      String(call.args?.message ?? "").includes("GETHEN-RESOLUTION"),
  );
  return { run, status, escalations, resolutionSent };
}

function check(label, condition, detail) {
  if (condition) {
    console.log(`ok   ${label}`);
  } else {
    console.error(`FAIL ${label}${detail === undefined ? "" : `: ${JSON.stringify(detail)}`}`);
    process.exitCode = 1;
  }
}

const answered = await scenario({ escalation: { answer: "rebase onto main" } });
check("answered: final report is ready", answered.run.report === "ready", answered.run);
check("answered: one escalation persisted", answered.status.escalations === 1, answered.status);
check(
  "answered: escalation is resolved with the injected answer",
  answered.escalations[0]?.status === "resolved" &&
    answered.escalations[0]?.resolution?.text === "rebase onto main",
  answered.escalations[0],
);
check("answered: resolution sent back to the same session", answered.resolutionSent);

const parked = await scenario({});
check("parked: report is waiting", parked.run.report === "waiting", parked.run);
check(
  "parked: escalation stays parked for a person",
  parked.escalations[0]?.status === "parked",
  parked.escalations[0],
);
check("parked: no resolution is sent automatically", parked.resolutionSent === false);

if (existsSync(workerBundle)) {
  const worker = await import(workerBundle);
  check("worker bundle exports a plugin apply", typeof worker.apply === "function");
  check("worker bundle injects tools", Array.isArray(worker.inject));
  const { ctx, tools } = context(undefined);
  worker.apply(ctx, {});
  check(
    "worker bundle registers gethen_escalate and gethen_report",
    ["gethen_escalate", "gethen_report"].every((name) => tools.some((tool) => tool.name === name)),
    tools.map((tool) => tool.name),
  );
} else {
  console.log(`skip worker bundle checks (missing ${workerBundle})`);
}

if (process.exitCode === 1) {
  console.error("\nP3b verification FAILED");
} else {
  console.log("\nP3b verification passed");
}
