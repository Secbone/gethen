import { assistantPromptOptions } from "@gethen/assistant";
import { createFakeAdapter } from "@gethen/core";
import { createPiAdapter } from "@gethen/transport-pi";
import { expect, it } from "vitest";

it("requests replacement only from a supporting harness", () => {
  expect(assistantPromptOptions(createPiAdapter()).systemPromptMode).toBe("replace");
  expect(assistantPromptOptions(createFakeAdapter()).systemPromptMode).toBe("append");
});
