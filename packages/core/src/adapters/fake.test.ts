import { describeHarnessContract } from "@gethen/core/testing";
import { createFakeAdapter } from "./fake";

describeHarnessContract("fake", () => createFakeAdapter());
