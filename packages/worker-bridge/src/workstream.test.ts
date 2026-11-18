import { describe, expect, it } from "vitest";
import {
  isFilesystemPath,
  isWorkstreamId,
  WORKSTREAM_BRANCH_PREFIX,
  workstreamBranch,
  workstreamContract,
} from "./workstream";

describe("workstream contract", () => {
  it("derives the delivery branch from the logical id", () => {
    expect(WORKSTREAM_BRANCH_PREFIX).toBe("feat/");
    expect(workstreamBranch("ws-collab")).toBe("feat/ws-collab");
    expect(workstreamContract("ws-collab")).toEqual({
      id: "ws-collab",
      branch: "feat/ws-collab",
      delivery: "push",
    });
  });

  it("rejects ids that could escape a Worker root or form a bad branch", () => {
    for (const id of ["", "../escape", "a/b", "-leading", ".hidden", "a..b"]) {
      expect(isWorkstreamId(id), id).toBe(false);
      expect(() => workstreamContract(id)).toThrow(/workstream id/);
    }
    expect(isWorkstreamId("ws-collab_2")).toBe(true);
  });

  it("recognizes filesystem paths so a Master path is never mistaken for an id", () => {
    for (const path of ["/srv/projects", "./repo", "../repo", "C:\\repo"]) {
      expect(isFilesystemPath(path), path).toBe(true);
    }
    for (const id of ["ws-collab", "default", "repo-2"]) {
      expect(isFilesystemPath(id), id).toBe(false);
    }
  });
});
