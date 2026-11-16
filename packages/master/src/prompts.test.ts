import { describe, expect, it } from "vitest";
import type { Assignment, ReportKind, Ticket } from "./model";
import {
  ASSIGNMENT_CLOSE,
  ASSIGNMENT_OPEN,
  ASSIGNMENT_PROTO,
  type AssignmentEnvelope,
  buildAssignmentPrompt,
  buildResolutionText,
  parseReportText,
  REPORT_CLOSE,
  REPORT_KINDS,
  REPORT_OPEN,
  REPORT_PROTO,
  RESOLUTION_CLOSE,
  RESOLUTION_OPEN,
  RESOLUTION_PROTO,
  type ResolutionEnvelope,
} from "./prompts";

const ticket: Ticket = {
  id: "ticket-1",
  mandate: "mandate-1",
  source: "local",
  threads: ["thread-1"],
  subscriptions: ["thread-1"],
  channels: ["#dev"],
  inputs: ["input-1"],
  assignments: ["assignment-1"],
  openedAt: "2026-11-16T00:00:00.000Z",
};

const assignment: Assignment = {
  id: "assignment-1",
  ticket: "ticket-1",
  worker: "worker-a",
  workspace: "/tmp/gethen/ws",
  packDigest: "sha256:abc123",
  state: "pending",
  operationId: "launch:assignment-1",
};

function assignmentPrompt(reportFormat?: string): string {
  return buildAssignmentPrompt({
    ticket,
    assignment,
    packDigest: "sha256:abc123",
    objective: "Make the failing test pass.",
    ...(reportFormat === undefined ? {} : { reportFormat }),
  });
}

/** Slice and parse the JSON body between the assignment markers. */
function envelopeOf(prompt: string): AssignmentEnvelope {
  const open = prompt.indexOf(ASSIGNMENT_OPEN);
  const close = prompt.indexOf(ASSIGNMENT_CLOSE);
  expect(open).toBe(0);
  expect(close).toBeGreaterThan(open);
  const body = prompt.slice(open + ASSIGNMENT_OPEN.length, close);
  return JSON.parse(body) as AssignmentEnvelope;
}

function reportBlock(body: unknown): string {
  return `some prose before\n${REPORT_OPEN}\n${JSON.stringify(body)}\n${REPORT_CLOSE}\nafter`;
}

describe("buildAssignmentPrompt", () => {
  it("renders a parseable gethen/assignment/v1 envelope between the markers", () => {
    const prompt = assignmentPrompt();
    expect(prompt.startsWith(ASSIGNMENT_OPEN)).toBe(true);
    expect(prompt).toContain(`${ASSIGNMENT_CLOSE}\n`);

    const envelope = envelopeOf(prompt);
    expect(envelope.proto).toBe(ASSIGNMENT_PROTO);
    expect(envelope.ticket.id).toBe("ticket-1");
    expect(envelope.ticket.threads).toEqual(["thread-1"]);
    expect(envelope.ticket.subscriptions).toEqual(["thread-1"]);
    expect(envelope.ticket.channels).toEqual(["#dev"]);
    expect(envelope.assignment.id).toBe("assignment-1");
    expect(envelope.assignment.worker).toBe("worker-a");
    expect(envelope.workspace).toBe("/tmp/gethen/ws");
    expect(envelope.packDigest).toBe("sha256:abc123");
    expect(envelope.objective).toBe("Make the failing test pass.");
  });

  it("states the report contract and the Worker instructions", () => {
    const prompt = assignmentPrompt();
    const envelope = envelopeOf(prompt);
    expect(envelope.report.proto).toBe(REPORT_PROTO);
    expect(envelope.report.marker).toBe(REPORT_OPEN);
    expect(envelope.report.closeMarker).toBe(REPORT_CLOSE);
    expect(envelope.report.kinds).toEqual(REPORT_KINDS);
    expect(envelope.report.fields.at).toContain("RFC 3339");
    expect(envelope.report.format).toContain(REPORT_OPEN);

    const instructions = prompt.slice(prompt.indexOf(ASSIGNMENT_CLOSE));
    expect(instructions).toContain("Worker");
    expect(instructions).toContain("objective");
    expect(instructions).toContain("report");
  });

  it("honours a caller-supplied report format", () => {
    const prompt = assignmentPrompt("Answer in one JSON line.");
    expect(envelopeOf(prompt).report.format).toBe("Answer in one JSON line.");
  });
});

describe("parseReportText", () => {
  const at = "2026-11-16T01:02:03.000Z";

  it.each([...REPORT_KINDS])("parses a %s report with a question", (kind: ReportKind) => {
    const report = parseReportText(
      reportBlock({
        kind,
        summary: "did the work",
        question: "Which branch?",
        at,
      }),
    );
    expect(report).toEqual({
      kind,
      summary: "did the work",
      question: "Which branch?",
      at,
    });
  });

  it("parses a report without a question and tolerates formatting", () => {
    const report = parseReportText(
      `${REPORT_OPEN}
        {
          "kind": "ready",
          "summary": "done",
          "at": "${at}"
        }
      ${REPORT_CLOSE}`,
    );
    expect(report).toEqual({ kind: "ready", summary: "done", at });
    expect(report).not.toHaveProperty("question");
  });

  it("omits an empty question", () => {
    const report = parseReportText(
      reportBlock({ kind: "ready", summary: "done", question: "", at }),
    );
    expect(report?.question).toBeUndefined();
  });

  it("returns undefined when there is no report block", () => {
    expect(parseReportText("no report here")).toBeUndefined();
    expect(parseReportText("")).toBeUndefined();
  });

  it("throws on an unknown kind", () => {
    expect(() => parseReportText(reportBlock({ kind: "finished", summary: "x", at }))).toThrow(
      /kind/,
    );
  });

  it("throws when at is missing", () => {
    expect(() => parseReportText(reportBlock({ kind: "ready", summary: "done" }))).toThrow(/"at"/);
    expect(() => parseReportText(reportBlock({ kind: "ready", summary: "done", at: "" }))).toThrow(
      /"at"/,
    );
  });

  it("throws when summary is missing", () => {
    expect(() => parseReportText(reportBlock({ kind: "ready", at }))).toThrow(/summary/);
  });

  it("throws on malformed JSON and unclosed blocks", () => {
    expect(() => parseReportText(`${REPORT_OPEN}{not json${REPORT_CLOSE}`)).toThrow(/JSON/);
    expect(() => parseReportText(`${REPORT_OPEN}{"kind":"ready"}`)).toThrow(/closing marker/);
  });

  it("rejects a non-object body", () => {
    expect(() => parseReportText(reportBlock([1, 2, 3]))).toThrow(/JSON object/);
  });
});

describe("buildResolutionText", () => {
  it("renders a gethen/resolution/v1 envelope with the answer fields", () => {
    const text = buildResolutionText({
      id: "decision-1",
      option: "rebase",
      text: "Rebase on main and retry.",
      basis: ["policy#1", "receipt#2"],
    });
    expect(text.startsWith(RESOLUTION_OPEN)).toBe(true);
    expect(text.endsWith(RESOLUTION_CLOSE)).toBe(true);

    const body = text.slice(RESOLUTION_OPEN.length, text.lastIndexOf(RESOLUTION_CLOSE));
    const envelope = JSON.parse(body) as ResolutionEnvelope;
    expect(envelope.proto).toBe(RESOLUTION_PROTO);
    expect(envelope.id).toBe("decision-1");
    expect(envelope.option).toBe("rebase");
    expect(envelope.text).toBe("Rebase on main and retry.");
    expect(envelope.basis).toEqual(["policy#1", "receipt#2"]);
  });

  it("omits an absent option", () => {
    const text = buildResolutionText({ id: "decision-2", text: "Use the default.", basis: [] });
    const body = text.slice(RESOLUTION_OPEN.length, text.lastIndexOf(RESOLUTION_CLOSE));
    const envelope = JSON.parse(body) as ResolutionEnvelope;
    expect(envelope).not.toHaveProperty("option");
    expect(envelope.basis).toEqual([]);
  });
});
