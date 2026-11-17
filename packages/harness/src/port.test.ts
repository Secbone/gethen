import { describe, expect, expectTypeOf, it } from "vitest";
import {
  HARNESS_ESCALATION_KINDS,
  HARNESS_REPORT_KINDS,
  HARNESS_RESOLUTION_KINDS,
  HARNESS_TASK_PHASES,
  type HarnessCapabilities,
  type HarnessEscalation,
  type HarnessEvent,
  type HarnessExecution,
  type HarnessExecution as HarnessExecutionType,
  type HarnessExecutor,
  type HarnessReport,
  type HarnessReportKind,
  type HarnessResolution,
  type HarnessSink,
  type HarnessTask,
  type HarnessTaskPhase,
  type HarnessTaskState,
  HarnessValidationError,
  hasTerminalReport,
  isHarnessEscalationKind,
  isHarnessReportKind,
  isHarnessResolutionKind,
  isHarnessTaskPhase,
  isTerminalPhase,
  parseHarnessDeadline,
  validateHarnessEscalation,
  validateHarnessReport,
  validateHarnessResolution,
} from "./port";

const AT = "2026-11-17T00:00:00.000Z";

describe("validateHarnessReport", () => {
  it.each([...HARNESS_REPORT_KINDS])("accepts and normalizes a %s report", (kind) => {
    const result = validateHarnessReport({ kind, summary: `did ${kind}`, at: "ignored" });
    expect(result).toEqual({ ok: true, value: { kind, summary: `did ${kind}` } });
  });

  it("keeps an optional question and opaque usage, and does not mutate its input", () => {
    const usage = { inputTokens: 3, outputTokens: 5 };
    const input = { kind: "waiting", summary: "need a person", question: "Which env?", usage };
    const result = validateHarnessReport(input);

    expect(result).toEqual({
      ok: true,
      value: { kind: "waiting", summary: "need a person", question: "Which env?", usage },
    });
    expect(input).toEqual({
      kind: "waiting",
      summary: "need a person",
      question: "Which env?",
      usage,
    });

    const emptyQuestion = validateHarnessReport({ kind: "ready", summary: "done", question: "" });
    expect(emptyQuestion).toMatchObject({ ok: true, value: { kind: "ready" } });
    if (emptyQuestion.ok) expect(emptyQuestion.value).not.toHaveProperty("question");
  });

  it("rejects non-objects and unknown kinds or missing summaries with a code", () => {
    for (const value of [undefined, null, "ready", 42, []]) {
      const result = validateHarnessReport(value);
      expect(result).toMatchObject({ ok: false, code: "invalid_report" });
    }

    expect(validateHarnessReport({ kind: "completed", summary: "x" })).toMatchObject({
      ok: false,
      code: "invalid_report_kind",
    });
    expect(validateHarnessReport({ kind: "ready" })).toMatchObject({
      ok: false,
      code: "missing_summary",
    });
    expect(validateHarnessReport({ kind: "ready", summary: "   " })).toMatchObject({
      ok: false,
      code: "missing_summary",
    });
    expect(validateHarnessReport({ kind: "ready", summary: "x", question: 7 })).toMatchObject({
      ok: false,
      code: "invalid_question",
    });
    expect(validateHarnessReport({ kind: "ready", summary: "x", usage: "many" })).toMatchObject({
      ok: false,
      code: "invalid_usage",
    });
  });
});

describe("report kind helpers", () => {
  it("knows which reports end an execution", () => {
    expect(hasTerminalReport("ready")).toBe(true);
    expect(hasTerminalReport("failed")).toBe(true);
    expect(hasTerminalReport("waiting")).toBe(false);
    expect(HARNESS_REPORT_KINDS.filter((kind) => hasTerminalReport(kind))).toEqual([
      "ready",
      "failed",
    ]);
  });

  it("exposes type guards for report, escalation and resolution kinds", () => {
    expect(isHarnessReportKind("failed")).toBe(true);
    expect(isHarnessReportKind("completed")).toBe(false);
    expect(isHarnessEscalationKind("permission")).toBe(true);
    expect(isHarnessEscalationKind("approval")).toBe(false);
    expect(isHarnessResolutionKind("answer")).toBe(true);
    expect(isHarnessResolutionKind("accepted")).toBe(false);
    expect(HARNESS_ESCALATION_KINDS).toContain("blocked");
    expect(HARNESS_RESOLUTION_KINDS).toContain("drop");
  });
});

describe("validateHarnessEscalation", () => {
  it("accepts a blocking escalation with unique options and a default", () => {
    const result = validateHarnessEscalation({
      id: "esc-1",
      kind: "permission",
      question: "allow shell?",
      options: [
        { id: "allow", label: "Allow once" },
        { id: "reject", label: "Reject", extra: "dropped" },
      ],
      defaultOption: "reject",
      blocking: true,
    });
    expect(result).toEqual({
      ok: true,
      value: {
        id: "esc-1",
        kind: "permission",
        question: "allow shell?",
        options: [
          { id: "allow", label: "Allow once" },
          { id: "reject", label: "Reject" },
        ],
        defaultOption: "reject",
        blocking: true,
      },
    });
  });

  it("rejects bad shapes, malformed options and defaults outside the option list", () => {
    const base = { id: "esc-1", kind: "decision", question: "go?", blocking: true };
    expect(validateHarnessEscalation(null)).toMatchObject({
      ok: false,
      code: "invalid_escalation",
    });
    expect(validateHarnessEscalation({ ...base, id: "" })).toMatchObject({
      ok: false,
      code: "missing_escalation_id",
    });
    expect(validateHarnessEscalation({ ...base, kind: "approval" })).toMatchObject({
      ok: false,
      code: "invalid_escalation_kind",
    });
    expect(validateHarnessEscalation({ ...base, question: " " })).toMatchObject({
      ok: false,
      code: "missing_question",
    });
    expect(validateHarnessEscalation({ ...base, blocking: false })).toMatchObject({
      ok: false,
      code: "not_blocking",
    });
    expect(validateHarnessEscalation({ ...base, options: "yes" })).toMatchObject({
      ok: false,
      code: "invalid_options",
    });
    expect(validateHarnessEscalation({ ...base, options: [{ id: "a" }] })).toMatchObject({
      ok: false,
      code: "invalid_options",
    });
    expect(
      validateHarnessEscalation({
        ...base,
        options: [
          { id: "a", label: "A" },
          { id: "a", label: "Again" },
        ],
      }),
    ).toMatchObject({ ok: false, code: "invalid_options" });
    expect(
      validateHarnessEscalation({ ...base, defaultOption: "missing", options: [] }),
    ).toMatchObject({ ok: false, code: "invalid_default_option" });
  });
});

describe("validateHarnessResolution", () => {
  it("accepts answers and drops, and rejects unknown kinds or mistyped fields", () => {
    expect(validateHarnessResolution({ kind: "answer", option: "allow", text: "go" })).toEqual({
      ok: true,
      value: { kind: "answer", option: "allow", text: "go" },
    });
    expect(validateHarnessResolution({ kind: "drop" })).toEqual({
      ok: true,
      value: { kind: "drop" },
    });
    expect(validateHarnessResolution({ kind: "maybe" })).toMatchObject({
      ok: false,
      code: "invalid_resolution_kind",
    });
    expect(validateHarnessResolution("answer")).toMatchObject({
      ok: false,
      code: "invalid_resolution",
    });
    expect(validateHarnessResolution({ kind: "answer", option: "" })).toMatchObject({
      ok: false,
      code: "invalid_option",
    });
    expect(validateHarnessResolution({ kind: "answer", text: 1 })).toMatchObject({
      ok: false,
      code: "invalid_text",
    });
  });
});

describe("parseHarnessDeadline", () => {
  it("parses RFC 3339 deadlines and rejects garbage", () => {
    expect(parseHarnessDeadline(undefined)).toBeUndefined();
    expect(parseHarnessDeadline(AT)).toBe(Date.parse(AT));
    expect(parseHarnessDeadline("2026-11-17T08:00:00+08:00")).toBe(
      Date.parse("2026-11-17T00:00:00.000Z"),
    );

    for (const invalid of ["soon", "", "2026-13-40T99:99:99Z"]) {
      expect(() => parseHarnessDeadline(invalid)).toThrow(HarnessValidationError);
      try {
        parseHarnessDeadline(invalid);
      } catch (error) {
        expect(error).toMatchObject({ code: "invalid_deadline" });
      }
    }
  });
});

describe("HarnessExecutor port", () => {
  it("exposes the harness-agnostic task, sink, execution and capability shapes", async () => {
    expectTypeOf<HarnessExecutor>().toMatchTypeOf<{
      start(task: HarnessTask, sink: HarnessSink): Promise<HarnessExecution>;
    }>();
    expectTypeOf<HarnessEscalation["blocking"]>().toEqualTypeOf<true>();
    expectTypeOf<HarnessReportKind>().toEqualTypeOf<"ready" | "failed" | "waiting">();
    expectTypeOf<HarnessResolution["kind"]>().toEqualTypeOf<"answer" | "escalate" | "drop">();
    expectTypeOf<HarnessCapabilities>().toMatchTypeOf<{
      models?: string[];
      tools?: string[];
      workspaceRoots?: string[];
    }>();

    const reports: HarnessReport[] = [];
    const events: HarnessEvent[] = [];
    const sink: HarnessSink = {
      event: (event) => events.push(event),
      escalation: () => undefined,
      report: (report) => reports.push(report),
    };

    const executor: HarnessExecutor = {
      async start(task, taskSink): Promise<HarnessExecution> {
        expectTypeOf(task).toEqualTypeOf<HarnessTask>();
        expect(hasTerminalReport("waiting")).toBe(false);
        taskSink.event({ type: "message", at: AT, data: { text: `started ${task.assignmentId}` } });
        taskSink.report({ kind: "ready", summary: "done" });
        return {
          sendResolution: async (resolution) => {
            expectTypeOf(resolution).toEqualTypeOf<HarnessResolution>();
          },
          cancel: async () => undefined,
        };
      },
    };

    const execution = await executor.start(
      {
        assignmentId: "assignment-1",
        ticketId: "ticket-1",
        workspace: "/work/ws",
        prompt: "do the work",
        metadata: { packDigest: "sha256:pack" },
      },
      sink,
    );
    await execution.cancel("test cleanup");

    expect(events).toEqual([{ type: "message", at: AT, data: { text: "started assignment-1" } }]);
    expect(reports).toEqual([{ kind: "ready", summary: "done" }]);
  });
});

describe("live task state and control surface", () => {
  it("knows terminal phases and guards phase values", () => {
    expect(HARNESS_TASK_PHASES).toEqual([
      "starting",
      "thinking",
      "tool",
      "waiting",
      "done",
      "failed",
    ]);
    expect(isTerminalPhase("done")).toBe(true);
    expect(isTerminalPhase("failed")).toBe(true);
    for (const phase of ["starting", "thinking", "tool", "waiting"] as const) {
      expect(isTerminalPhase(phase)).toBe(false);
    }
    expect(isHarnessTaskPhase("tool")).toBe(true);
    expect(isHarnessTaskPhase("finished")).toBe(false);
  });

  it("exposes optional state/steer/interrupt methods on a harness execution", async () => {
    expectTypeOf<HarnessExecutionType["state"]>().toEqualTypeOf<
      (() => HarnessTaskState) | undefined
    >();
    expectTypeOf<HarnessExecutionType["steer"]>().toEqualTypeOf<
      ((text: string) => Promise<void>) | undefined
    >();
    expectTypeOf<HarnessExecutionType["interrupt"]>().toEqualTypeOf<
      (() => Promise<void>) | undefined
    >();

    const phases: HarnessTaskPhase[] = ["starting", "thinking"];
    expect(phases).toHaveLength(2);

    const state: HarnessTaskState = {
      assignmentId: "assignment-1",
      phase: "tool",
      step: 2,
      lastTool: { name: "bash", startedAt: AT },
      lastEventAt: AT,
      lastMessage: "running the tests",
      seq: 7,
    };
    expect(state.lastTool?.name).toBe("bash");
    expect(isTerminalPhase(state.phase)).toBe(false);
  });
});
