import type { RemoteClientLike } from "@gethen/bridge-remote";
import type { Assignment, LaunchRequest, Ticket } from "@gethen/master";
import { describe, expect, it } from "vitest";
import { createRemoteLaunchPort } from "./remote-launch";

const ticket: Ticket = {
  id: "t1",
  mandate: "m1",
  source: "local",
  threads: ["th1"],
  subscriptions: ["th1"],
  channels: [],
  inputs: [],
  assignments: ["a1"],
  openedAt: "2026-11-16T00:00:00.000Z",
};

const assignment: Assignment = {
  id: "a1",
  ticket: "t1",
  worker: "device-a",
  workspace: "ws",
  packDigest: "sha256:pack",
  state: "pending",
  operationId: "launch-0123456789abcdef",
};

const request: LaunchRequest = {
  assignment,
  ticket,
  packDigest: "sha256:pack",
  prompt: "[GETHEN-ASSIGNMENT]{...}[/GETHEN-ASSIGNMENT]",
};

interface Call {
  operation: string;
  args: unknown;
}

function fakeClient(replies: {
  state?: unknown;
  transcript?: unknown;
}): RemoteClientLike & { calls: Call[] } {
  const calls: Call[] = [];
  const client = {
    calls,
    async devices() {
      return [];
    },
    async listRemoteWorkspaces() {
      return [];
    },
    async runSessionTool(operation: string, args: unknown) {
      calls.push({ operation, args });
      if (operation === "remote_create_session") return { sessionId: "sess-1" };
      if (operation === "remote_wait_sessions") {
        return replies.state ?? { sessions: [{ sessionId: "sess-1", state: "idle" }] };
      }
      if (operation === "remote_read_session") {
        return (
          replies.transcript ?? {
            events: [
              {
                seq: 1,
                at: "2026-11-16T00:05:00.000Z",
                type: "message",
                text: '[GETHEN-REPORT]{"kind":"ready","summary":"done","at":"2026-11-16T00:05:00.000Z"}[/GETHEN-REPORT]',
              },
            ],
            truncated: false,
          }
        );
      }
      return { ok: true };
    },
  };
  return client as RemoteClientLike & { calls: Call[] };
}

describe("createRemoteLaunchPort", () => {
  it("creates a session, sends the prompt, waits and parses the report", async () => {
    const client = fakeClient({});
    const port = createRemoteLaunchPort(
      { hostId: "device-a", workspaceId: "ws-1", sleep: async () => {} },
      client,
    );
    const result = await port.launch(request);

    expect(client.calls.map((call) => call.operation)).toEqual([
      "remote_create_session",
      "remote_send_message",
      "remote_wait_sessions",
      "remote_read_session",
    ]);
    expect(result.report.kind).toBe("ready");
    expect(result.receipt).toMatchObject({
      assignment: "a1",
      ticket: "t1",
      workspace: "ws-1",
      packDigest: "sha256:pack",
      outcome: "completed",
      events: 1,
    });
  });

  it("waits repeatedly while the session is running", async () => {
    const client = fakeClient({ state: { sessions: [{ sessionId: "sess-1", state: "idle" }] } });
    let waits = 0;
    const original = client.runSessionTool.bind(client);
    client.runSessionTool = async (operation: string, args: unknown) => {
      if (operation === "remote_wait_sessions") {
        waits += 1;
        return waits === 1
          ? { sessions: [{ sessionId: "sess-1", state: "running" }] }
          : { sessions: [{ sessionId: "sess-1", state: "idle" }] };
      }
      return original(operation, args);
    };
    const port = createRemoteLaunchPort(
      { hostId: "device-a", workspaceId: "ws-1", sleep: async () => {} },
      client,
    );
    const result = await port.launch(request);
    expect(waits).toBe(2);
    expect(result.report.kind).toBe("ready");
  });

  it("falls back to a waiting report when the worker never reports", async () => {
    const client = fakeClient({ transcript: { events: [], truncated: false } });
    const port = createRemoteLaunchPort(
      { hostId: "device-a", workspaceId: "ws-1", sleep: async () => {}, maxPolls: 0 },
      client,
    );
    const result = await port.launch(request);
    expect(result.report).toMatchObject({ kind: "waiting" });
    expect(result.receipt.outcome).toBe("completed");
  });

  it("records a failed receipt when the session ends in an unknown state", async () => {
    const client = fakeClient({
      state: { sessions: [{ sessionId: "sess-1", state: "unknown" }] },
      transcript: { events: [], truncated: false },
    });
    const port = createRemoteLaunchPort(
      { hostId: "device-a", workspaceId: "ws-1", sleep: async () => {}, maxPolls: 0 },
      client,
    );
    const result = await port.launch(request);
    expect(result.report.kind).toBe("failed");
    expect(result.receipt.outcome).toBe("failed");
  });
});
