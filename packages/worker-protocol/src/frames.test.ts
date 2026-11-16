import { describe, expect, it } from "vitest";
import {
  encodeFrame,
  type HelloFrame,
  MAX_FRAME_BYTES,
  PROTOCOL_NAME,
  PROTOCOL_VERSION,
  type ProtocolFailure,
  parseDownstreamFrame,
  parseUpstreamFrame,
  type ReportFrame,
  type Result,
} from "./frames";

const V = PROTOCOL_VERSION;

function expectOk<T>(result: Result<T>): T {
  if (!result.ok) throw new Error(`expected ok, got ${result.code}: ${result.message}`);
  return result.value;
}

function expectFail(result: Result<unknown>, code?: string): ProtocolFailure {
  if (result.ok) throw new Error("expected a failure result");
  if (code !== undefined) expect(result.code).toBe(code);
  return result;
}

function without(value: Record<string, unknown>, field: string): Record<string, unknown> {
  const copy = { ...value };
  delete copy[field];
  return copy;
}

// Ready-to-parse fixtures. They are plain objects on purpose: the parsers take
// `unknown`, mirroring a value that just came out of `JSON.parse`.
function helloOk(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    v: V,
    type: "hello_ok",
    workerId: "worker-1",
    serverTime: "2026-11-16T00:00:00.000Z",
    heartbeatMs: 10_000,
    ...overrides,
  };
}

function dispatch(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    v: V,
    type: "dispatch",
    assignmentId: "a1",
    ticketId: "t1",
    workspace: "/home/u/projects/repo",
    packDigest: "sha256:pack",
    envelope: { proto: "gethen/assignment/v1", objective: "fix it" },
    ...overrides,
  };
}

function resolution(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    v: V,
    type: "resolution",
    assignmentId: "a1",
    escalationId: "e1",
    resolution: { kind: "answer", option: "merge", text: "merge it" },
    ...overrides,
  };
}

function cancel(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return { v: V, type: "cancel", assignmentId: "a1", ...overrides };
}

function control(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return { v: V, type: "control", operationId: "op-1", op: { op: "snapshot" }, ...overrides };
}

function hello(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    v: V,
    type: "hello",
    workerId: "worker-1",
    protocol: V,
    machine: "laptop",
    platform: "linux",
    runtimes: ["dsh"],
    workspaces: ["/home/u/projects"],
    capabilities: { promptInjection: "system", mcp: false },
    load: { running: 0, quota: 2 },
    models: ["deepseek"],
    ...overrides,
  };
}

function heartbeat(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    v: V,
    type: "heartbeat",
    at: "2026-11-16T00:00:10.000Z",
    load: { running: 1, quota: 2 },
    ...overrides,
  };
}

function event(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    v: V,
    type: "event",
    assignmentId: "a1",
    seq: 3,
    at: "2026-11-16T00:01:00.000Z",
    event: { type: "message", text: "hello" },
    ...overrides,
  };
}

function escalation(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    v: V,
    type: "escalation",
    assignmentId: "a1",
    escalationId: "e1",
    kind: "decision",
    question: "Which branch?",
    options: [
      { id: "merge", label: "Merge" },
      { id: "rebase", label: "Rebase" },
    ],
    defaultOption: "merge",
    blocking: true,
    ...overrides,
  };
}

function report(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    v: V,
    type: "report",
    assignmentId: "a1",
    kind: "ready",
    summary: "all tests pass",
    usage: { inputTokens: 10, outputTokens: 20 },
    receipt: { packDigest: "sha256:pack", events: 4 },
    ...overrides,
  };
}

function controlResult(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    v: V,
    type: "control_result",
    operationId: "op-1",
    ok: true,
    state: { state: "online" },
    ...overrides,
  };
}

function errorFrame(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    v: V,
    type: "error",
    code: "BAD_FRAME",
    message: "something went wrong",
    ...overrides,
  };
}

describe("protocol constants", () => {
  it("names gethen.worker/v1 and pins the version and frame limit", () => {
    expect(PROTOCOL_NAME).toBe("gethen.worker.v1");
    expect(PROTOCOL_VERSION).toBe(1);
    expect(MAX_FRAME_BYTES).toBe(262_144);
  });
});

describe("parseDownstreamFrame", () => {
  it("parses hello_ok", () => {
    expect(expectOk(parseDownstreamFrame(helloOk()))).toEqual({
      v: V,
      type: "hello_ok",
      workerId: "worker-1",
      serverTime: "2026-11-16T00:00:00.000Z",
      heartbeatMs: 10_000,
    });
  });

  it("parses dispatch with optional model and deadline", () => {
    const frame = expectOk(
      parseDownstreamFrame(dispatch({ model: "deepseek", deadlineAt: "2026-11-16T01:00:00.000Z" })),
    );
    expect(frame).toMatchObject({
      type: "dispatch",
      assignmentId: "a1",
      ticketId: "t1",
      workspace: "/home/u/projects/repo",
      packDigest: "sha256:pack",
      model: "deepseek",
      deadlineAt: "2026-11-16T01:00:00.000Z",
    });
    expect(frame).toHaveProperty("envelope.objective", "fix it");
  });

  it("parses dispatch without optional fields", () => {
    const frame = expectOk(parseDownstreamFrame(dispatch()));
    expect(frame).not.toHaveProperty("model");
    expect(frame).not.toHaveProperty("deadlineAt");
  });

  it("parses resolution frames", () => {
    expect(expectOk(parseDownstreamFrame(resolution()))).toMatchObject({
      type: "resolution",
      assignmentId: "a1",
      escalationId: "e1",
      resolution: { kind: "answer", option: "merge", text: "merge it" },
    });
    expect(
      expectOk(parseDownstreamFrame(resolution({ resolution: { kind: "drop", text: "stop" } }))),
    ).toHaveProperty("resolution.kind", "drop");
  });

  it("parses cancel with and without a reason", () => {
    expect(expectOk(parseDownstreamFrame(cancel({ reason: "user stop" })))).toMatchObject({
      type: "cancel",
      assignmentId: "a1",
      reason: "user stop",
    });
    expect(expectOk(parseDownstreamFrame(cancel()))).not.toHaveProperty("reason");
  });

  it("parses control frames structurally (policy is applied by the Worker)", () => {
    expect(expectOk(parseDownstreamFrame(control()))).toMatchObject({
      type: "control",
      operationId: "op-1",
      op: { op: "snapshot" },
    });
    expect(
      expectOk(
        parseDownstreamFrame(control({ op: { op: "runtime.start", name: "dsh", drain: false } })),
      ),
    ).toMatchObject({ op: { op: "runtime.start", name: "dsh", drain: false } });
  });
});

describe("parseUpstreamFrame", () => {
  it("parses hello with capabilities, load and token", () => {
    const frame = expectOk(parseUpstreamFrame(hello({ token: "secret" })));
    expect(frame).toMatchObject({
      type: "hello",
      workerId: "worker-1",
      protocol: V,
      machine: "laptop",
      platform: "linux",
      runtimes: ["dsh"],
      workspaces: ["/home/u/projects"],
      models: ["deepseek"],
      load: { running: 0, quota: 2 },
      token: "secret",
    });
    expect(frame).toHaveProperty("capabilities.promptInjection", "system");
  });

  it("parses heartbeat", () => {
    expect(expectOk(parseUpstreamFrame(heartbeat()))).toMatchObject({
      type: "heartbeat",
      at: "2026-11-16T00:00:10.000Z",
      load: { running: 1, quota: 2 },
    });
  });

  it("parses accepted", () => {
    expect(expectOk(parseUpstreamFrame({ v: V, type: "accepted", assignmentId: "a1" }))).toEqual({
      v: V,
      type: "accepted",
      assignmentId: "a1",
    });
  });

  it("parses event with an opaque payload", () => {
    const frame = expectOk(parseUpstreamFrame(event()));
    expect(frame).toMatchObject({ type: "event", assignmentId: "a1", seq: 3 });
    expect(frame).toHaveProperty("event.type", "message");
    expect(frame).toHaveProperty("event.text", "hello");
  });

  it("parses escalation with options and defaultOption", () => {
    expect(expectOk(parseUpstreamFrame(escalation()))).toMatchObject({
      type: "escalation",
      assignmentId: "a1",
      escalationId: "e1",
      kind: "decision",
      question: "Which branch?",
      options: [
        { id: "merge", label: "Merge" },
        { id: "rebase", label: "Rebase" },
      ],
      defaultOption: "merge",
      blocking: true,
    });
  });

  it("parses report with usage and receipt", () => {
    expect(expectOk(parseUpstreamFrame(report()))).toMatchObject({
      type: "report",
      assignmentId: "a1",
      kind: "ready",
      summary: "all tests pass",
      usage: { inputTokens: 10, outputTokens: 20 },
      receipt: { packDigest: "sha256:pack", events: 4 },
    });
  });

  it("parses control_result and error", () => {
    expect(expectOk(parseUpstreamFrame(controlResult()))).toMatchObject({
      type: "control_result",
      operationId: "op-1",
      ok: true,
      state: { state: "online" },
    });
    expect(expectOk(parseUpstreamFrame(errorFrame({ assignmentId: "a1" })))).toMatchObject({
      type: "error",
      code: "BAD_FRAME",
      message: "something went wrong",
      assignmentId: "a1",
    });
  });
});

describe("frame validation", () => {
  it("rejects values that are not JSON frames", () => {
    for (const value of [null, [], 42, true]) {
      expect(expectFail(parseUpstreamFrame(value), "invalid_frame").message).toContain(
        "JSON object",
      );
      expect(expectFail(parseDownstreamFrame(value), "invalid_frame").message).toContain(
        "JSON object",
      );
    }
    expect(expectFail(parseUpstreamFrame("frame"), "invalid_frame").message).toContain(
      "valid JSON",
    );
    expect(expectFail(parseDownstreamFrame("{oops"), "invalid_frame").message).toContain(
      "valid JSON",
    );
  });

  it("rejects missing or unsupported versions", () => {
    for (const value of [without(hello(), "v"), { ...hello(), v: 2 }, { ...hello(), v: "1" }]) {
      expectFail(parseUpstreamFrame(value), "unsupported_version");
    }
    expectFail(parseDownstreamFrame({ ...dispatch(), v: 3 }), "unsupported_version");
    expect(
      expectFail(parseUpstreamFrame(hello({ protocol: 2 })), "unsupported_version").message,
    ).toBe('hello "protocol" must be 1');
  });

  it("rejects unknown frame types in both directions", () => {
    expect(
      expectFail(parseUpstreamFrame({ v: V, type: "nope" }), "unknown_type").message,
    ).toContain("type");
    expect(
      expectFail(parseDownstreamFrame({ v: V, type: "report" }), "unknown_type").message,
    ).toContain("type");
  });

  it("rejects missing required fields", () => {
    const downstreamCases: Array<[string, Record<string, unknown>]> = [
      ["dispatch.assignmentId", without(dispatch(), "assignmentId")],
      ["dispatch.envelope", without(dispatch(), "envelope")],
      ["hello_ok.workerId", without(helloOk(), "workerId")],
      ["control.operationId", without(control(), "operationId")],
    ];
    for (const [label, value] of downstreamCases) {
      expectFail(parseDownstreamFrame(value), "invalid_field");
      expect(parseDownstreamFrame(value).ok, label).toBe(false);
    }

    const upstreamCases: Array<[string, Record<string, unknown>]> = [
      ["hello.machine", without(hello(), "machine")],
      ["hello.load", without(hello(), "load")],
      ["heartbeat.at", without(heartbeat(), "at")],
      ["event.at", without(event(), "at")],
      ["escalation.question", without(escalation(), "question")],
      ["escalation.options", without(escalation(), "options")],
      ["report.receipt", without(report(), "receipt")],
      ["control_result.ok", without(controlResult(), "ok")],
      ["error.message", without(errorFrame(), "message")],
    ];
    for (const [label, value] of upstreamCases) {
      expectFail(parseUpstreamFrame(value), "invalid_field");
      expect(parseUpstreamFrame(value).ok, label).toBe(false);
    }
  });

  it("rejects wrong field types and unknown enum values", () => {
    const downstreamCases: Array<[Record<string, unknown>, string]> = [
      [dispatch({ envelope: "not-an-object" }), "invalid_field"],
      [dispatch({ assignmentId: "" }), "invalid_field"],
      [resolution({ resolution: { kind: "maybe", text: "x" } }), "invalid_field"],
      [resolution({ resolution: { kind: "answer" } }), "invalid_field"],
      [helloOk({ heartbeatMs: 0 }), "invalid_field"],
      [helloOk({ heartbeatMs: "10000" }), "invalid_field"],
      [control({ op: { op: "rm-rf" } }), "unknown_control_op"],
      [control({ op: { op: "runtime.start" } }), "invalid_field"],
      [control({ op: "snapshot" }), "invalid_field"],
    ];
    for (const [value, code] of downstreamCases) expectFail(parseDownstreamFrame(value), code);

    const upstreamCases = [
      hello({ runtimes: ["dsh", 3] }),
      hello({ capabilities: [] }),
      hello({ load: { running: -1, quota: 2 } }),
      heartbeat({ load: { running: 0.5, quota: 1 } }),
      event({ seq: 1.5 }),
      event({ event: { text: "no type" } }),
      escalation({ kind: "urgent" }),
      escalation({ options: "merge" }),
      escalation({ blocking: false }),
      report({ kind: "done" }),
      report({ receipt: { packDigest: "sha256:pack", events: -1 } }),
      controlResult({ ok: "yes" }),
    ];
    for (const value of upstreamCases) expectFail(parseUpstreamFrame(value), "invalid_field");
  });
});

describe("frame size, unknown fields and encoding", () => {
  it("rejects frames over MAX_FRAME_BYTES on parse", () => {
    const oversized = hello({ workspaces: ["/".repeat(MAX_FRAME_BYTES + 1)] });
    expect(expectFail(parseUpstreamFrame(oversized), "frame_too_large").message).toContain(
      String(MAX_FRAME_BYTES),
    );
  });

  it("tolerates and drops unknown extra fields", () => {
    const frame = expectOk(parseUpstreamFrame(hello({ extra: { nested: true }, another: 1 })));
    expect(frame).not.toHaveProperty("extra");
    expect(frame).not.toHaveProperty("another");
    const reportFrame = expectOk(parseUpstreamFrame(report({ extra: "ignored" })));
    expect(reportFrame).not.toHaveProperty("extra");
  });

  it("rejects values that are not JSON-serializable", () => {
    const value = { v: V, type: "hello", big: 1n };
    expect(expectFail(parseUpstreamFrame(value), "invalid_frame").message).toContain(
      "JSON-serializable",
    );
  });

  it("round-trips typed frames through encodeFrame and parse", () => {
    const outgoing: HelloFrame = {
      v: V,
      type: "hello",
      workerId: "worker-1",
      protocol: V,
      machine: "laptop",
      platform: "linux",
      runtimes: ["dsh"],
      workspaces: ["/home/u/projects"],
      capabilities: { promptInjection: "system" },
      load: { running: 0, quota: 2 },
      models: ["deepseek"],
    };
    const helloJson = encodeFrame(outgoing);
    expect(typeof helloJson).toBe("string");
    // The parsers accept both a parsed object and the raw wire string.
    expect(expectOk(parseUpstreamFrame(helloJson))).toEqual(outgoing);
    expect(expectOk(parseUpstreamFrame(JSON.parse(helloJson) as unknown))).toEqual(outgoing);

    const inbound: ReportFrame = {
      v: V,
      type: "report",
      assignmentId: "a1",
      kind: "ready",
      summary: "done",
      receipt: { packDigest: "sha256:pack", events: 2 },
    };
    const reportJson = encodeFrame(inbound);
    expect(expectOk(parseUpstreamFrame(JSON.parse(reportJson) as unknown))).toEqual(inbound);
  });

  it("refuses to encode a frame over MAX_FRAME_BYTES", () => {
    const tooBig: HelloFrame = {
      v: V,
      type: "hello",
      workerId: "worker-1",
      protocol: V,
      machine: "laptop",
      platform: "linux",
      runtimes: [],
      workspaces: ["/".repeat(MAX_FRAME_BYTES + 1)],
      capabilities: {},
      load: { running: 0, quota: 1 },
      models: [],
    };
    expect(() => encodeFrame(tooBig)).toThrow(/over the/);
  });
});
