import { appendFileSync } from "node:fs";
import type { RemoteClientLike, RemoteDevice, RemoteWorkspace } from "@gethen/bridge-remote";

/**
 * Dev-only scripted Remote client: each session replays
 * escalation -> resolution -> report, and every call is appended to
 * `GETHEN_DEV_REMOTE_LOG` (default /tmp/gethen-dev-remote.log) for inspection.
 *
 * It exists so the Master's launch loop can be exercised inside a live DSH
 * process without a logged-in Remote account or a second machine.
 */
export function createScriptedRemoteClient(): RemoteClientLike {
  const logPath = process.env.GETHEN_DEV_REMOTE_LOG ?? "/tmp/gethen-dev-remote.log";
  const log = (line: string) => {
    try {
      appendFileSync(logPath, `${new Date().toISOString()} ${line}\n`);
    } catch {
      // logging is best-effort
    }
  };
  const runs = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const reads = new Map<string, number>();
  log("scripted client created");

  return {
    async devices(): Promise<RemoteDevice[]> {
      return [{ deviceId: "dev-local", name: "dev-local", online: true }];
    },
    async listRemoteWorkspaces(): Promise<RemoteWorkspace[]> {
      return [{ workspaceId: "ws-dev", path: "/tmp", title: "dev" }];
    },
    async runSessionTool(operation: string, args: unknown): Promise<unknown> {
      log(operation);
      if (operation === "remote_create_session") {
        const sessionId = `sess-dev-${runs}-${reads.size + 1}`;
        reads.set(sessionId, 0);
        return { sessionId };
      }
      if (operation === "remote_wait_sessions") {
        const targets = (args as { targets?: Array<{ sessionId?: string }> } | undefined)?.targets;
        const sessionId = targets?.[0]?.sessionId ?? "";
        return { sessions: [{ sessionId, state: "idle" }] };
      }
      if (operation === "remote_read_session") {
        const sessionId = String((args as { sessionId?: string } | undefined)?.sessionId ?? "");
        const round = (reads.get(sessionId) ?? 0) + 1;
        reads.set(sessionId, round);
        const at = new Date().toISOString();
        if (round === 1) {
          const escalation = {
            id: `escalation-${sessionId}`,
            kind: "decision",
            question: "merge or rebase?",
            options: [
              { id: "merge", label: "merge" },
              { id: "rebase", label: "rebase" },
            ],
            blocking: true,
            at,
          };
          return {
            events: [
              {
                seq: 1,
                at,
                type: "message",
                text: `[GETHEN-ESCALATION]\n${JSON.stringify(escalation)}\n[/GETHEN-ESCALATION]`,
              },
            ],
            truncated: false,
          };
        }
        const report = { kind: "ready", summary: "dev loop fixed", at };
        return {
          events: [
            {
              seq: 2,
              at,
              type: "message",
              text: `[GETHEN-REPORT]\n${JSON.stringify(report)}\n[/GETHEN-REPORT]`,
            },
          ],
          truncated: false,
        };
      }
      return { ok: true };
    },
  };
}
