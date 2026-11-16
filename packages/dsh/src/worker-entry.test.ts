import { REPORT_KINDS } from "@gethen/worker";
import { describe, expect, it } from "vitest";
import {
  applyWorker,
  type WorkerDshContext,
  type WorkerToolDefinition,
  workerName,
} from "./worker-entry";

const ESCALATION_OPEN = "[GETHEN-ESCALATION]";
const ESCALATION_CLOSE = "[/GETHEN-ESCALATION]";
const REPORT_OPEN = "[GETHEN-REPORT]";
const REPORT_CLOSE = "[/GETHEN-REPORT]";

interface EscalateResult {
  text: string;
  instruction: string;
}

interface ReportResult {
  text: string;
}

function context() {
  const tools: WorkerToolDefinition[] = [];
  const ctx: WorkerDshContext = {
    tools: {
      register(definition) {
        tools.push(definition);
        return () => {};
      },
    },
  };
  return { ctx, tools };
}

function tool(tools: WorkerToolDefinition[], name: string): WorkerToolDefinition {
  const found = tools.find((candidate) => candidate.name === name);
  if (!found) throw new Error(`missing tool ${name}`);
  return found;
}

function param(definition: WorkerToolDefinition, name: string): Record<string, unknown> {
  const value = definition.parameters[name];
  if (typeof value !== "object" || value === null) {
    throw new Error(`missing parameter ${name}`);
  }
  return value as Record<string, unknown>;
}

/** Extract and parse the JSON body between two marker lines. */
function bodyOf(text: string, open: string, close: string): Record<string, unknown> {
  const start = text.indexOf(open);
  const end = text.indexOf(close, start + open.length);
  if (start === -1 || end === -1) throw new Error(`missing marker block ${open}`);
  return JSON.parse(text.slice(start + open.length, end).trim()) as Record<string, unknown>;
}

async function execute(
  tools: WorkerToolDefinition[],
  name: string,
  args: Record<string, unknown>,
): Promise<unknown> {
  return tool(tools, name).execute(args, {});
}

describe("gethen worker entry", () => {
  it("registers the worker tools", () => {
    const { ctx, tools } = context();
    applyWorker(ctx);
    expect(workerName).toBe("gethen-worker");
    expect(tools.map((entry) => entry.name)).toEqual(["gethen_escalate", "gethen_report"]);
  });

  it("exposes the tool parameter schemas", () => {
    const { ctx, tools } = context();
    applyWorker(ctx);
    const escalate = tool(tools, "gethen_escalate");
    expect(param(escalate, "question")).toMatchObject({ type: "string", required: true });
    expect(param(escalate, "kind")).toMatchObject({
      type: "string",
      required: false,
      enum: ["decision", "blocked", "permission", "scope", "conflict"],
    });
    expect(param(escalate, "options")).toMatchObject({ type: "array", required: false });
    expect(param(escalate, "defaultOption")).toMatchObject({ type: "string", required: false });

    const report = tool(tools, "gethen_report");
    expect(param(report, "kind")).toMatchObject({
      type: "string",
      required: true,
      enum: [...REPORT_KINDS],
    });
    expect(param(report, "summary")).toMatchObject({ type: "string", required: true });
    expect(param(report, "question")).toMatchObject({ type: "string", required: false });
  });

  it("renders an escalation block with the default decision kind", async () => {
    const { ctx, tools } = context();
    applyWorker(ctx);
    const result = (await execute(tools, "gethen_escalate", {
      question: "Merge or rebase?",
      options: [
        { id: "merge", label: "Merge" },
        { id: "rebase", label: "Rebase" },
      ],
    })) as EscalateResult;
    expect(result.text).toContain(ESCALATION_OPEN);
    expect(result.text).toContain(ESCALATION_CLOSE);
    expect(result.instruction).toBe(
      "Reply with this block verbatim as your final message and end the turn.",
    );
    const body = bodyOf(result.text, ESCALATION_OPEN, ESCALATION_CLOSE);
    expect(body.kind).toBe("decision");
    expect(body.question).toBe("Merge or rebase?");
    expect(body.options).toEqual([
      { id: "merge", label: "Merge" },
      { id: "rebase", label: "Rebase" },
    ]);
    expect(body.blocking).toBe(true);
    expect(String(body.id)).toMatch(/^escalation-/);
    expect(body.defaultOption).toBeUndefined();
    expect(new Date(String(body.at)).toISOString()).toBe(body.at);
  });

  it("passes escalation kind, options and defaultOption through", async () => {
    const { ctx, tools } = context();
    applyWorker(ctx);
    const result = (await execute(tools, "gethen_escalate", {
      question: "Allow network access for the build?",
      kind: "permission",
      options: [
        { id: "allow", label: "Allow" },
        { id: "deny", label: "Deny" },
      ],
      defaultOption: "deny",
    })) as EscalateResult;
    const body = bodyOf(result.text, ESCALATION_OPEN, ESCALATION_CLOSE);
    expect(body.kind).toBe("permission");
    expect(body.defaultOption).toBe("deny");
    expect(body.options).toHaveLength(2);
  });

  it("rejects malformed escalation input", async () => {
    const { ctx, tools } = context();
    applyWorker(ctx);
    await expect(execute(tools, "gethen_escalate", {})).rejects.toThrow(/question/);
    await expect(
      execute(tools, "gethen_escalate", { question: "Which?", kind: "unknown" }),
    ).rejects.toThrow(/kind/);
    await expect(
      execute(tools, "gethen_escalate", { question: "Which?", defaultOption: "b" }),
    ).rejects.toThrow(/defaultOption/);
    await expect(
      execute(tools, "gethen_escalate", {
        question: "Which?",
        options: [
          { id: "a", label: "A" },
          { id: "a", label: "B" },
        ],
      }),
    ).rejects.toThrow(/unique/);
  });

  it("renders a report block without a question", async () => {
    const { ctx, tools } = context();
    applyWorker(ctx);
    const result = (await execute(tools, "gethen_report", {
      kind: "ready",
      summary: "Implemented the fix.",
    })) as ReportResult;
    expect(result.text).toContain(REPORT_OPEN);
    expect(result.text).toContain(REPORT_CLOSE);
    const body = bodyOf(result.text, REPORT_OPEN, REPORT_CLOSE);
    expect(body).toMatchObject({ kind: "ready", summary: "Implemented the fix." });
    expect(body.question).toBeUndefined();
    expect(new Date(String(body.at)).toISOString()).toBe(body.at);
  });

  it("passes every report kind and the optional question through", async () => {
    const { ctx, tools } = context();
    applyWorker(ctx);
    for (const kind of REPORT_KINDS) {
      const result = (await execute(tools, "gethen_report", {
        kind,
        summary: `report ${kind}`,
      })) as ReportResult;
      const body = bodyOf(result.text, REPORT_OPEN, REPORT_CLOSE);
      expect(body.kind).toBe(kind);
    }
    const blocked = (await execute(tools, "gethen_report", {
      kind: "blocked",
      summary: "Blocked on a decision.",
      question: "Which branch should I merge?",
    })) as ReportResult;
    expect(bodyOf(blocked.text, REPORT_OPEN, REPORT_CLOSE).question).toBe(
      "Which branch should I merge?",
    );
    await expect(
      execute(tools, "gethen_report", { kind: "finished", summary: "x" }),
    ).rejects.toThrow(/kind/);
  });
});
