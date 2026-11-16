import type { RemoteClientLike } from "@gethen/bridge-remote";
import {
  type Assignment,
  type EscalationRecord,
  type LaunchRequest,
  RESOLUTION_CLOSE,
  RESOLUTION_OPEN,
  RESOLUTION_PROTO,
  type Ticket,
} from "@gethen/master";
import { describe, expect, it, vi } from "vitest";
import { createRemoteLaunchPort } from "./remote-launch";

const REPORT_AT = "2026-11-16T00:05:00.000Z";

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

interface FakeOptions {
  /** Scripts `remote_wait_sessions`; defaults to one idle answer. */
  state?: (waitCall: number) => unknown;
  /** Scripts `remote_read_session`; defaults to a ready Report page. */
  read?: (readCall: number) => unknown;
}

interface FakeRemote extends RemoteClientLike {
  calls: Call[];
  messages: string[];
}

function page(texts: string[]): unknown {
  return {
    events: texts.map((text, index) => ({
      seq: index + 1,
      at: REPORT_AT,
      type: "message",
      text,
    })),
    truncated: false,
  };
}

function sessionState(state: string): unknown {
  return { sessions: [{ sessionId: "sess-1", state }] };
}

function reportText(kind = "ready", summary = "done"): string {
  return `[GETHEN-REPORT]${JSON.stringify({ kind, summary, at: REPORT_AT })}[/GETHEN-REPORT]`;
}

function escalationText(overrides: Record<string, unknown> = {}): string {
  const body = {
    id: "esc-1",
    ticket: "t1",
    assignment: "a1",
    session: "sess-1",
    kind: "decision",
    question: "Which branch should I merge?",
    options: [
      { id: "merge", label: "Merge to main" },
      { id: "rebase", label: "Rebase on main" },
    ],
    defaultOption: "merge",
    blocking: true,
    at: REPORT_AT,
    ...overrides,
  };
  return `${ESCALATION_OPEN}\n${JSON.stringify(body, null, 2)}\n${ESCALATION_CLOSE}`;
}

const ESCALATION_OPEN = "[GETHEN-ESCALATION]";
const ESCALATION_CLOSE = "[/GETHEN-ESCALATION]";

function fakeRemote(options: FakeOptions = {}): FakeRemote {
  const calls: Call[] = [];
  const messages: string[] = [];
  let waits = 0;
  let reads = 0;
  const client = {
    calls,
    messages,
    async devices() {
      return [];
    },
    async listRemoteWorkspaces() {
      return [];
    },
    async runSessionTool(operation: string, args: unknown) {
      calls.push({ operation, args });
      if (operation === "remote_create_session") return { sessionId: "sess-1" };
      if (operation === "remote_send_message") {
        const message = (args as { message?: unknown }).message;
        if (typeof message === "string") messages.push(message);
        return { ok: true };
      }
      if (operation === "remote_wait_sessions") {
        waits += 1;
        return options.state?.(waits) ?? sessionState("idle");
      }
      if (operation === "remote_read_session") {
        reads += 1;
        return options.read?.(reads) ?? page([reportText()]);
      }
      return { ok: true };
    },
  };
  return client as FakeRemote;
}

function port(
  client: RemoteClientLike,
  overrides: Partial<Parameters<typeof createRemoteLaunchPort>[0]> = {},
) {
  return createRemoteLaunchPort(
    { hostId: "device-a", workspaceId: "ws-1", sleep: async () => {}, ...overrides },
    client,
  );
}

function resolutionBody(message: string): Record<string, unknown> {
  const open = message.indexOf(RESOLUTION_OPEN);
  const close = message.indexOf(RESOLUTION_CLOSE);
  expect(open).toBeGreaterThanOrEqual(0);
  expect(close).toBeGreaterThan(open);
  return JSON.parse(message.slice(open + RESOLUTION_OPEN.length, close)) as Record<string, unknown>;
}

const answered = async () => ({
  kind: "answer" as const,
  text: "Take the merge.",
  option: "merge",
});

describe("createRemoteLaunchPort", () => {
  it("creates a session, sends the prompt, waits and parses the report", async () => {
    const client = fakeRemote();
    const result = await port(client).launch(request);

    expect(client.calls.map((call) => call.operation)).toEqual([
      "remote_create_session",
      "remote_send_message",
      "remote_wait_sessions",
      "remote_read_session",
    ]);
    expect(client.messages).toEqual([request.prompt]);
    expect(result.report.kind).toBe("ready");
    expect(result.receipt).toMatchObject({
      assignment: "a1",
      ticket: "t1",
      workspace: "ws-1",
      packDigest: "sha256:pack",
      outcome: "completed",
      events: 1,
    });
    expect(result.escalations).toBeUndefined();
  });

  it("waits repeatedly while the session is running", async () => {
    const client = fakeRemote({
      state: (waitCall) => sessionState(waitCall === 1 ? "running" : "idle"),
    });
    const result = await port(client).launch(request);
    expect(client.calls.filter((call) => call.operation === "remote_wait_sessions")).toHaveLength(
      2,
    );
    expect(result.report.kind).toBe("ready");
  });

  it("falls back to a waiting report when the worker never reports", async () => {
    const client = fakeRemote({ read: () => page([]) });
    const result = await port(client, { maxPolls: 0 }).launch(request);
    expect(result.report).toMatchObject({ kind: "waiting" });
    expect(result.receipt.outcome).toBe("completed");
  });

  it("records a failed receipt when the session ends in an unknown state", async () => {
    const client = fakeRemote({ state: () => sessionState("unknown"), read: () => page([]) });
    const result = await port(client, { maxPolls: 0 }).launch(request);
    expect(result.report.kind).toBe("failed");
    expect(result.receipt.outcome).toBe("failed");
  });

  it("closes the loop: escalation, answer, resolution and a final report", async () => {
    const statuses: string[] = [];
    const onEscalation = vi.fn((record: EscalationRecord) => {
      statuses.push(record.status);
    });
    const onResolution = vi.fn((record: EscalationRecord) => {
      statuses.push(`after:${record.status}`);
    });
    const client = fakeRemote({
      read: (readCall) =>
        readCall === 1 ? page([escalationText()]) : page([reportText("ready", "merged")]),
    });
    const result = await port(client, {
      resolveEscalation: answered,
      onEscalation,
      onResolution,
    }).launch(request);

    expect(onEscalation).toHaveBeenCalledTimes(1);
    expect(onResolution).toHaveBeenCalledTimes(1);
    expect(statuses).toEqual(["waiting", "after:resolved"]);

    expect(client.messages).toHaveLength(2);
    expect(client.messages.at(1)).toContain(RESOLUTION_OPEN);

    const record = result.escalations?.at(0);
    expect(record).toMatchObject({
      id: "esc-1",
      ticket: "t1",
      assignment: "a1",
      session: "sess-1",
      kind: "decision",
      question: "Which branch should I merge?",
      defaultOption: "merge",
      status: "resolved",
    });
    expect(record?.resolution).toEqual({
      kind: "answer",
      text: "Take the merge.",
      option: "merge",
      at: expect.any(String),
    });

    expect(result.report).toMatchObject({ kind: "ready", summary: "merged" });
    expect(result.receipt).toMatchObject({ outcome: "completed", events: 1 });
  });

  it("sends a well-formed resolution block back into the same session", async () => {
    const client = fakeRemote({
      read: (readCall) =>
        readCall === 1 ? page([escalationText()]) : page([reportText("ready", "merged")]),
    });
    await port(client, { resolveEscalation: answered }).launch(request);

    const resolutionMessage = client.messages.at(1);
    expect(resolutionMessage).toBeDefined();
    const message = resolutionMessage ?? "";
    expect(message.startsWith(RESOLUTION_OPEN)).toBe(true);
    expect(message.trimEnd().endsWith(RESOLUTION_CLOSE)).toBe(true);

    const body = resolutionBody(message);
    expect(body).toMatchObject({
      proto: RESOLUTION_PROTO,
      escalation: "esc-1",
      kind: "answer",
      option: "merge",
      text: "Take the merge.",
      confidence: 1,
      basis: ["inference"],
    });
    expect(body.id).toBe("resolution-esc-1");
    expect(typeof body.at).toBe("string");

    const sends = client.calls.filter((call) => call.operation === "remote_send_message");
    expect(sends).toHaveLength(2);
    expect(sends.at(1)?.args).toMatchObject({
      sessionId: "sess-1",
      operationId: "resolution-round-0",
    });
  });

  it("parks for a person when the resolver returns undefined", async () => {
    const onResolution = vi.fn();
    const client = fakeRemote({ read: () => page([escalationText()]) });
    const result = await port(client, {
      resolveEscalation: async () => undefined,
      onResolution,
    }).launch(request);

    expect(client.messages).toHaveLength(1);
    expect(onResolution).toHaveBeenCalledTimes(1);
    expect(result.report).toEqual({
      kind: "waiting",
      summary: "waiting for a person: Which branch should I merge?",
      question: "Which branch should I merge?",
      at: expect.any(String),
    });
    expect(result.escalations?.at(0)).toMatchObject({ status: "parked" });
    expect(result.escalations?.at(0)?.resolution).toBeUndefined();
    expect(result.receipt).toMatchObject({
      outcome: "completed",
      summary: "waiting for a person: Which branch should I merge?",
      events: 1,
    });
  });

  it("parks by default when no resolver is configured", async () => {
    const client = fakeRemote({ read: () => page([escalationText()]) });
    const result = await port(client).launch(request);
    expect(result.report.kind).toBe("waiting");
    expect(result.escalations?.at(0)?.status).toBe("parked");
    expect(client.messages).toHaveLength(1);
  });

  it("falls back to the request ids when the escalation omits them", async () => {
    const client = fakeRemote({
      read: () =>
        page([
          escalationText({
            ticket: undefined,
            assignment: undefined,
            session: undefined,
            defaultOption: undefined,
          }),
        ]),
    });
    const result = await port(client).launch(request);
    const record = result.escalations?.at(0);
    expect(record).toMatchObject({ ticket: "t1", assignment: "a1", session: "sess-1" });
    expect(record?.defaultOption).toBeUndefined();
  });

  it("stops at the escalation round limit", async () => {
    const resolveEscalation = vi.fn(answered);
    let read = 0;
    const client = fakeRemote({
      read: () => page([escalationText({ id: `esc-${++read}` })]),
    });
    const result = await port(client, { resolveEscalation, maxRounds: 1 }).launch(request);

    expect(resolveEscalation).toHaveBeenCalledTimes(1);
    expect(client.messages).toHaveLength(2);
    expect(result.report).toEqual({
      kind: "waiting",
      summary: "escalation round limit reached",
      at: expect.any(String),
    });
    expect(result.receipt.outcome).toBe("completed");
    expect(result.escalations).toHaveLength(1);
  });

  it("allows three escalation rounds by default", async () => {
    const resolveEscalation = vi.fn(answered);
    let read = 0;
    const client = fakeRemote({
      read: () => page([escalationText({ id: `esc-${++read}` })]),
    });
    const result = await port(client, { resolveEscalation }).launch(request);

    expect(resolveEscalation).toHaveBeenCalledTimes(3);
    expect(client.messages).toHaveLength(4);
    expect(result.report).toMatchObject({
      kind: "waiting",
      summary: "escalation round limit reached",
    });
    expect(result.escalations).toHaveLength(3);
  });

  it("parks (and still resumes) on an escalate resolution", async () => {
    const client = fakeRemote({
      read: (readCall) =>
        readCall === 1 ? page([escalationText()]) : page([reportText("ready", "merged")]),
    });
    const result = await port(client, {
      resolveEscalation: async () => ({ kind: "escalate", text: "A person must decide." }),
    }).launch(request);

    expect(client.messages).toHaveLength(2);
    expect(resolutionBody(client.messages.at(1) ?? "")).toMatchObject({ kind: "escalate" });
    expect(result.escalations?.at(0)).toMatchObject({
      status: "parked",
      resolution: { kind: "escalate", text: "A person must decide." },
    });
    expect(result.report.kind).toBe("ready");
  });
});
