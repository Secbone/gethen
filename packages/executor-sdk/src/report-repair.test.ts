import { REPORT_CLOSE, REPORT_OPEN } from "@gethen/master";
import { describe, expect, it } from "vitest";
import { repairReportBlock } from "./harness-adapter";

describe("repairReportBlock", () => {
  it("escapes raw control characters inside JSON strings", () => {
    const broken = `${REPORT_OPEN}\n{"kind":"ready","summary":"line one\nline two\tend"}\n${REPORT_CLOSE}`;
    const repaired = repairReportBlock(broken);
    const body = repaired.slice(
      repaired.indexOf(REPORT_OPEN) + REPORT_OPEN.length,
      repaired.lastIndexOf(REPORT_CLOSE),
    );
    expect(() => JSON.parse(body)).not.toThrow();
    expect(JSON.parse(body).summary).toBe("line one\nline two\tend");
  });

  it("leaves valid blocks and prose untouched", () => {
    const valid = `${REPORT_OPEN}{"kind":"ready","summary":"ok"}${REPORT_CLOSE}`;
    expect(repairReportBlock(valid)).toBe(valid);
    expect(repairReportBlock("no marker here")).toBe("no marker here");
  });
});
