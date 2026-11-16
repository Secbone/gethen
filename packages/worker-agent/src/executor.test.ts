import { describe, expect, it } from "vitest";
import {
  type ExecutorEscalation,
  type ExecutorEvent,
  type ExecutorReport,
  type ExecutorSink,
  FakeExecutor,
} from "./executor";

const TASK = {
  assignmentId: "assignment-1",
  ticketId: "ticket-1",
  workspace: "/tmp/gethen-ws",
  packDigest: "sha256:pack-1",
  envelope: { proto: "gethen/assignment/v1", objective: "fix the failing test" },
};

interface Collected {
  events: ExecutorEvent[];
  escalations: ExecutorEscalation[];
  reports: ExecutorReport[];
  sink: ExecutorSink;
}

function collector(): Collected {
  const events: ExecutorEvent[] = [];
  const escalations: ExecutorEscalation[] = [];
  const reports: ExecutorReport[] = [];
  return {
    events,
    escalations,
    reports,
    sink: {
      event: (event) => events.push(event),
      escalation: (escalation) => escalations.push(escalation),
      report: (report) => reports.push(report),
    },
  };
}

function task(overrides: Partial<typeof TASK> = {}): typeof TASK {
  return { ...TASK, ...overrides };
}

describe("FakeExecutor", () => {
  it("emits an event and a blocking escalation before waiting for a resolution", async () => {
    const executor = new FakeExecutor();
    const seen = collector();
    const handle = await executor.start(task(), seen.sink);

    expect(executor.tasks).toEqual([TASK]);
    expect(seen.events).toEqual([{ type: "message", text: "fake executor started" }]);
    expect(seen.escalations).toEqual([
      {
        escalationId: "escalation-assignment-1",
        kind: "decision",
        question: "Proceed with the fake plan?",
        options: [
          { id: "yes", label: "Yes" },
          { id: "no", label: "No" },
        ],
      },
    ]);
    expect(seen.reports).toHaveLength(0);

    await handle.sendResolution({ kind: "answer", option: "yes", text: "go ahead" });
    expect(executor.resolutions).toEqual([{ kind: "answer", option: "yes", text: "go ahead" }]);
    expect(seen.reports).toEqual([
      { kind: "ready", summary: "fake executor finished assignment-1" },
    ]);
  });

  it("reports failure on cancel and ignores a late resolution", async () => {
    const executor = new FakeExecutor();
    const seen = collector();
    const handle = await executor.start(task(), seen.sink);

    await handle.cancel("no longer needed");
    expect(executor.cancels).toEqual(["no longer needed"]);
    expect(seen.reports).toEqual([{ kind: "failed", summary: "cancelled: no longer needed" }]);

    await handle.sendResolution({ kind: "drop", text: "late answer" });
    expect(seen.reports).toHaveLength(1);
  });

  it("uses the scripted event, escalation and report", async () => {
    const executor = new FakeExecutor({
      event: { type: "tool.start", name: "bash" },
      escalation: {
        escalationId: "escalation-custom",
        kind: "permission",
        question: "Allow network access?",
        options: [{ id: "allow", label: "Allow" }],
        defaultOption: "allow",
      },
      report: (resolution) => ({
        kind: "waiting",
        summary: `resolved ${resolution.option ?? resolution.kind}`,
      }),
    });
    const seen = collector();
    const handle = await executor.start(task(), seen.sink);

    expect(seen.events).toEqual([{ type: "tool.start", name: "bash" }]);
    expect(seen.escalations[0]?.escalationId).toBe("escalation-custom");
    expect(seen.escalations[0]?.defaultOption).toBe("allow");

    await handle.sendResolution({ kind: "answer", option: "allow", text: "allowed" });
    expect(seen.reports).toEqual([{ kind: "waiting", summary: "resolved allow" }]);
  });
});
