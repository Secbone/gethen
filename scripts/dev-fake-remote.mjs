#!/usr/bin/env node
// Dev-only fake Remote client (service `gethenDevRemote`) for local self-loop verification.
//
// It implements the structural RemoteClientLike surface with a scripted Worker:
// first transcript read returns one [GETHEN-ESCALATION], the Master answers it
// (config.escalation.answer), the second read returns a ready [GETHEN-REPORT].
// Mount it in a dev profile BEFORE the gethen entry, e.g.
//
//   - insert:
//       - id: gethen-dev-remote
//         name: 'file:///home/<user>/Code/gethen/scripts/dev-fake-remote.mjs'
//       - id: gethen
//         name: 'file:///home/<user>/.local/share/gethen/dsh/index.js'
//   - id: gethen
//     config:
//       remote: { hostId: "dev-local", workspaceId: "ws-dev", clientService: "gethenDevRemote" }
//       escalation: { answer: "rebase onto main" }
//
// The plugin provides the `gethenDevRemote` service; the Master reads it via
// `remote.clientService`. This is a test double for the transport only; the Master engine, launch loop,
// escalation parsing, resolution send and persistence all run for real.
import { appendFileSync } from "node:fs";

export const name = "gethen-dev-remote";

const LOG = process.env.GETHEN_DEV_REMOTE_LOG ?? "/tmp/gethen-dev-remote.log";
const ESCALATION = {
  id: "escalation-dev-1",
  kind: "decision",
  question: "merge or rebase?",
  options: [
    { id: "merge", label: "merge" },
    { id: "rebase", label: "rebase" },
  ],
  blocking: true,
  at: new Date().toISOString(),
};

export function apply(ctx) {
  let reads = 0;
  const calls = [];
  const client = {
    async devices() {
      return [{ deviceId: "dev-local", name: "dev-local", online: true }];
    },
    async listRemoteWorkspaces() {
      return [{ workspaceId: "ws-dev", path: "/tmp", title: "dev" }];
    },
    async runSessionTool(operation, args) {
      calls.push({ operation, args });
      appendFileSync(LOG, `${new Date().toISOString()} ${operation}\n`);
      if (operation === "remote_create_session") return { sessionId: "sess-dev" };
      if (operation === "remote_wait_sessions") {
        return { sessions: [{ sessionId: "sess-dev", state: "idle" }] };
      }
      if (operation === "remote_read_session") {
        reads += 1;
        const at = new Date().toISOString();
        if (reads === 1) {
          return {
            events: [
              {
                seq: 1,
                at,
                type: "message",
                text: `[GETHEN-ESCALATION]\n${JSON.stringify(ESCALATION)}\n[/GETHEN-ESCALATION]`,
              },
            ],
            truncated: false,
          };
        }
        return {
          events: [
            {
              seq: 2,
              at,
              type: "message",
              text: `[GETHEN-REPORT]\n${JSON.stringify({ kind: "ready", summary: "dev loop fixed", at })}\n[/GETHEN-REPORT]`,
            },
          ],
          truncated: false,
        };
      }
      return { ok: true };
    },
  };
  ctx.provide("gethenDevRemote", client);
  appendFileSync(LOG, `${new Date().toISOString()} dev fake remote provided\n`);
}
