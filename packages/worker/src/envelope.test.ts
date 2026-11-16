import { describe, expect, it } from "vitest";
import {
  ASSIGNMENT_CLOSE,
  ASSIGNMENT_OPEN,
  ASSIGNMENT_PROTO,
  buildEscalationText,
  buildReportText,
  ESCALATION_CLOSE,
  ESCALATION_OPEN,
  type EscalationLike,
  parseAssignmentPrompt,
  parseResolutionText,
  REPORT_CLOSE,
  REPORT_KINDS,
  REPORT_OPEN,
  REPORT_PROTO,
  RESOLUTION_CLOSE,
  RESOLUTION_OPEN,
  RESOLUTION_PROTO,
} from "./envelope";

const AT = "2026-11-16T08:30:00.000Z";

/** The JSON body the Master's `buildAssignmentPrompt` puts between the markers. */
function masterEnvelope(): Record<string, unknown> {
  return {
    proto: ASSIGNMENT_PROTO,
    ticket: {
      id: "ticket-1",
      mandate: "mandate-1",
      source: "local",
      threads: ["thread-1"],
      subscriptions: ["thread-1"],
      channels: ["#dev"],
      inputs: ["input-1"],
      assignments: ["assignment-1"],
      openedAt: AT,
    },
    assignment: {
      id: "assignment-1",
      ticket: "ticket-1",
      worker: "worker-a",
      workspace: "/tmp/gethen/ws",
      packDigest: "sha256:abc123",
      state: "pending",
      operationId: "launch:assignment-1",
    },
    workspace: "/tmp/gethen/ws",
    packDigest: "sha256:abc123",
    objective: "Make the failing test pass.",
    report: {
      proto: REPORT_PROTO,
      marker: REPORT_OPEN,
      closeMarker: REPORT_CLOSE,
      kinds: [...REPORT_KINDS],
      fields: {
        kind: "one of report.kinds",
        summary: "string",
        question: "string (optional; one concrete question for a person)",
        at: "RFC 3339 UTC timestamp",
      },
      format: `Reply with one ${REPORT_OPEN} ... ${REPORT_CLOSE} block.`,
    },
  };
}

/** Render a prompt the way the Master does: block, blank line, instructions. */
function assignmentPrompt(body: unknown): string {
  return [
    ASSIGNMENT_OPEN,
    JSON.stringify(body, null, 2),
    ASSIGNMENT_CLOSE,
    "",
    "You are the Gethen Worker for this Assignment.",
  ].join("\n");
}

function bodyOf(text: string, open: string, close: string): unknown {
  const start = text.indexOf(open);
  expect(start).toBeGreaterThanOrEqual(0);
  const bodyStart = start + open.length;
  const end = text.indexOf(close, bodyStart);
  expect(end).toBeGreaterThan(bodyStart);
  return JSON.parse(text.slice(bodyStart, end));
}

describe("envelope markers", () => {
  it("mirrors the Master's marker and proto strings exactly", () => {
    expect([ASSIGNMENT_OPEN, ASSIGNMENT_CLOSE]).toEqual([
      "[GETHEN-ASSIGNMENT]",
      "[/GETHEN-ASSIGNMENT]",
    ]);
    expect([REPORT_OPEN, REPORT_CLOSE]).toEqual(["[GETHEN-REPORT]", "[/GETHEN-REPORT]"]);
    expect([ESCALATION_OPEN, ESCALATION_CLOSE]).toEqual([
      "[GETHEN-ESCALATION]",
      "[/GETHEN-ESCALATION]",
    ]);
    expect([RESOLUTION_OPEN, RESOLUTION_CLOSE]).toEqual([
      "[GETHEN-RESOLUTION]",
      "[/GETHEN-RESOLUTION]",
    ]);
    expect(ASSIGNMENT_PROTO).toBe("gethen/assignment/v1");
    expect(REPORT_PROTO).toBe("gethen/report/v1");
    expect(RESOLUTION_PROTO).toBe("gethen/resolution/v1");
    expect(REPORT_KINDS).toEqual(["progress", "waiting", "ready", "blocked", "failed", "dropped"]);
  });
});

describe("parseAssignmentPrompt", () => {
  it("parses the envelope a Master prompt carries", () => {
    const text = `narrator: dispatching\n${assignmentPrompt(masterEnvelope())}\nmore prose`;
    const envelope = parseAssignmentPrompt(text);

    expect(envelope).toEqual({
      proto: ASSIGNMENT_PROTO,
      ticket: { id: "ticket-1" },
      assignment: {
        id: "assignment-1",
        workspace: "/tmp/gethen/ws",
        worker: "worker-a",
      },
      workspace: "/tmp/gethen/ws",
      packDigest: "sha256:abc123",
      objective: "Make the failing test pass.",
      report: {
        proto: REPORT_PROTO,
        marker: REPORT_OPEN,
        closeMarker: REPORT_CLOSE,
        kinds: [...REPORT_KINDS],
      },
    });
  });

  it("omits the report contract when the envelope carries none", () => {
    const body = masterEnvelope();
    delete body.report;
    expect(parseAssignmentPrompt(assignmentPrompt(body))?.report).toBeUndefined();
  });

  it("returns undefined when there is no assignment marker", () => {
    expect(parseAssignmentPrompt("")).toBeUndefined();
    expect(parseAssignmentPrompt("plain prose only")).toBeUndefined();
    expect(
      parseAssignmentPrompt(`prose with ${REPORT_OPEN}${REPORT_CLOSE} but no assignment`),
    ).toBeUndefined();
  });

  it("throws on an unclosed assignment block", () => {
    expect(() => parseAssignmentPrompt(`${ASSIGNMENT_OPEN}{"proto":"x"}`)).toThrow(
      /closing marker/,
    );
  });

  it("throws on malformed JSON and non-object bodies", () => {
    expect(() => parseAssignmentPrompt(`${ASSIGNMENT_OPEN}{not json${ASSIGNMENT_CLOSE}`)).toThrow(
      /JSON/,
    );
    expect(() => parseAssignmentPrompt(`${ASSIGNMENT_OPEN}[1,2,3]${ASSIGNMENT_CLOSE}`)).toThrow(
      /JSON object/,
    );
  });

  it("throws on a wrong proto", () => {
    const body = { ...masterEnvelope(), proto: "gethen/assignment/v2" };
    expect(() => parseAssignmentPrompt(assignmentPrompt(body))).toThrow(/proto/);
  });

  it("throws when a required field is missing or mistyped", () => {
    const base = masterEnvelope();
    const parse = (overrides: Record<string, unknown>) => () =>
      parseAssignmentPrompt(assignmentPrompt({ ...base, ...overrides }));

    expect(parse({ ticket: {} })).toThrow(/ticket\.id/);
    expect(parse({ assignment: { id: "assignment-1" } })).toThrow(/assignment\.workspace/);
    expect(parse({ workspace: 7 })).toThrow(/workspace/);
    expect(parse({ packDigest: 7 })).toThrow(/packDigest/);
    expect(parse({ objective: "" })).toThrow(/objective/);
    expect(parse({ report: "gethen/report/v1" })).toThrow(/report/);
    expect(
      parse({
        report: { ...(base.report as Record<string, unknown>), kinds: "ready" },
      }),
    ).toThrow(/kinds/);
  });
});

describe("buildReportText", () => {
  it("renders the report block the Master parses", () => {
    const text = buildReportText({
      kind: "blocked",
      summary: "Need a decision on the branch.",
      question: "Merge or rebase?",
      at: AT,
    });

    expect(text.startsWith(REPORT_OPEN)).toBe(true);
    expect(text.endsWith(REPORT_CLOSE)).toBe(true);
    expect(bodyOf(text, REPORT_OPEN, REPORT_CLOSE)).toEqual({
      kind: "blocked",
      summary: "Need a decision on the branch.",
      question: "Merge or rebase?",
      at: AT,
    });

    for (const kind of REPORT_KINDS) {
      const each = buildReportText({ kind, summary: `did ${kind}`, question: "", at: AT });
      expect(bodyOf(each, REPORT_OPEN, REPORT_CLOSE)).toEqual({
        kind,
        summary: `did ${kind}`,
        at: AT,
      });
    }
  });

  it("rejects reports the Master's parser would reject", () => {
    expect(() => buildReportText({ kind: "finished" as never, summary: "x", at: AT })).toThrow(
      /kind/,
    );
    expect(() => buildReportText({ kind: "ready", summary: "", at: AT })).toThrow(/summary/);
    expect(() => buildReportText({ kind: "ready", summary: "done", at: "" })).toThrow(/"at"/);
  });
});

describe("buildEscalationText", () => {
  const escalation: EscalationLike = {
    id: "escalation-1",
    ticket: "ticket-1",
    assignment: "assignment-1",
    kind: "decision",
    question: "Merge or rebase?",
    options: [
      { id: "merge", label: "Merge" },
      { id: "rebase", label: "Rebase", tradeoff: "Linear history" },
    ],
    context: {
      summary: "The branch diverged from main.",
      tried: ["fetch", "rebase --dry-run"],
      evidence: ["ci/1234"],
    },
    blocking: true,
    at: AT,
  };

  it("renders a parseable escalation block", () => {
    const text = buildEscalationText(escalation);
    expect(text.startsWith(ESCALATION_OPEN)).toBe(true);
    expect(text.endsWith(ESCALATION_CLOSE)).toBe(true);
    expect(bodyOf(text, ESCALATION_OPEN, ESCALATION_CLOSE)).toEqual(escalation);

    const full = buildEscalationText({
      ...escalation,
      session: "session-1",
      defaultOption: "rebase",
    });
    expect(bodyOf(full, ESCALATION_OPEN, ESCALATION_CLOSE)).toMatchObject({
      session: "session-1",
      defaultOption: "rebase",
    });
  });

  it("rejects an escalation without a question", () => {
    expect(() => buildEscalationText({ ...escalation, question: "" })).toThrow(/question/);
  });
});

describe("parseResolutionText", () => {
  it("parses the Master's resolution envelope", () => {
    const text = [
      "prose before",
      RESOLUTION_OPEN,
      JSON.stringify(
        {
          proto: RESOLUTION_PROTO,
          id: "decision-1",
          option: "rebase",
          text: "Rebase on main and retry.",
          basis: ["policy#1", "receipt#2"],
        },
        null,
        2,
      ),
      RESOLUTION_CLOSE,
      "prose after",
    ].join("\n");

    expect(parseResolutionText(text)).toEqual({
      id: "decision-1",
      option: "rebase",
      text: "Rebase on main and retry.",
      basis: ["policy#1", "receipt#2"],
    });
  });

  it("parses a bridge-shaped answer without a proto", () => {
    const text = `${RESOLUTION_OPEN}
      {
        "id": "resolution-1",
        "escalation": "escalation-1",
        "kind": "answer",
        "option": "merge",
        "text": "Take the merge.",
        "confidence": 0.82,
        "basis": ["inference"],
        "at": "${AT}"
      }
      ${RESOLUTION_CLOSE}`;

    expect(parseResolutionText(text)).toEqual({
      id: "resolution-1",
      text: "Take the merge.",
      option: "merge",
      basis: ["inference"],
      escalation: "escalation-1",
      confidence: 0.82,
      at: AT,
    });
  });

  it("returns undefined without a resolution marker", () => {
    expect(parseResolutionText("")).toBeUndefined();
    expect(parseResolutionText(`prose with ${REPORT_OPEN} block`)).toBeUndefined();
  });

  it("throws on malformed resolution blocks", () => {
    expect(() => parseResolutionText(`${RESOLUTION_OPEN}{"id":"r"}`)).toThrow(/closing marker/);
    expect(() => parseResolutionText(`${RESOLUTION_OPEN}{not json${RESOLUTION_CLOSE}`)).toThrow(
      /JSON/,
    );
    expect(() => parseResolutionText(`${RESOLUTION_OPEN}[1]${RESOLUTION_CLOSE}`)).toThrow(
      /JSON object/,
    );
    expect(() => parseResolutionText(`${RESOLUTION_OPEN}{"id":"r"}${RESOLUTION_CLOSE}`)).toThrow(
      /text/,
    );
    expect(() =>
      parseResolutionText(
        `${RESOLUTION_OPEN}{"id":"r","text":"x","proto":"gethen/resolution/v2"}${RESOLUTION_CLOSE}`,
      ),
    ).toThrow(/proto/);
    expect(() =>
      parseResolutionText(
        `${RESOLUTION_OPEN}{"id":"r","text":"x","basis":"policy"}${RESOLUTION_CLOSE}`,
      ),
    ).toThrow(/basis/);
  });
});
