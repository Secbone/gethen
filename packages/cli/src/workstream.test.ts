import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AssignmentReceipt, LaunchPort, LaunchRequest, Ticket } from "@gethen/master";
import { createRoutedLaunchPort, WorkerDirectory } from "@gethen/master-runtime";
import type { WorkerManifest } from "@gethen/worker-bridge";
import { describe, expect, it } from "vitest";
import {
  assertOwnedWorkspace,
  finalizeDelivery,
  parseWorkstreams,
  prepareWorkstream,
  type WorkstreamConfig,
  WorkstreamError,
} from "./workstream";

const AT = "2026-11-18T00:00:00.000Z";

function git(cwd: string, args: string[]): string {
  return execFileSync("git", args, {
    cwd,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
}

interface Fixture {
  dir: string;
  repo: string;
  origin: string;
}

/** A Worker-owned source clone plus a local bare remote to push deliveries to. */
function makeWorkerRepo(): Fixture {
  const dir = mkdtempSync(join(tmpdir(), "gethen-workstream-"));
  const origin = join(dir, "origin.git");
  execFileSync("git", ["init", "--bare", origin], { cwd: dir, stdio: "ignore" });
  const repo = join(dir, "source");
  mkdirSync(repo);
  git(repo, ["init", "-b", "main"]);
  git(repo, ["config", "user.email", "worker@test"]);
  git(repo, ["config", "user.name", "worker"]);
  writeFileSync(join(repo, "README.md"), "base\n");
  git(repo, ["add", "-A"]);
  git(repo, ["commit", "-m", "init"]);
  git(repo, ["remote", "add", "origin", origin]);
  git(repo, ["push", "-u", "origin", "main"]);
  return { dir, repo, origin };
}

function workspace(id: string, fixture: Fixture, root?: string): WorkstreamConfig {
  return {
    id,
    repo: fixture.repo,
    branch: `feat/${id}`,
    isolation: "worktree",
    ...(root === undefined ? {} : { root }),
  };
}

function manifest(worker: string, workspaceId: string): WorkerManifest {
  return {
    id: worker,
    machine: worker,
    runtime: "dsh",
    workspaces: [{ id: workspaceId, repo: "/worker/source", branch: `feat/${workspaceId}` }],
    capabilities: {
      promptInjection: "system",
      skills: "filesystem",
      mcp: false,
      permissions: "approval",
      streaming: true,
      steer: true,
      fork: false,
    },
    models: ["deepseek-v4.1-flash"],
    costClass: "mid",
    load: { running: 0, quota: 1 },
    health: { state: "online", lastHeartbeatAt: AT },
  };
}

function request(assignment: string, workspaceId: string): LaunchRequest {
  const ticket: Ticket = {
    id: `ticket-${assignment}`,
    mandate: `mandate-${assignment}`,
    source: "local",
    threads: ["local"],
    subscriptions: ["local"],
    channels: [],
    inputs: [],
    assignments: [assignment],
    openedAt: AT,
  };
  return {
    assignment: {
      id: assignment,
      ticket: ticket.id,
      worker: "",
      workspace: workspaceId,
      packDigest: "sha256:pack",
      state: "running",
      operationId: `launch-${assignment}`,
    },
    ticket,
    packDigest: "sha256:pack",
    prompt: "do the work",
  };
}

describe("Worker-owned workstream workspaces", () => {
  it("gives two concurrent workstreams isolated branches and leaves the source clean", async () => {
    const fixture = makeWorkerRepo();
    const runtime = { workerId: "worker-1", workspaceRoot: join(fixture.dir, "worktrees") };
    const alpha = prepareWorkstream(workspace("alpha", fixture), runtime);
    const beta = prepareWorkstream(workspace("beta", fixture), runtime);

    expect(alpha.path).not.toBe(beta.path);
    expect(alpha.branch).toBe("feat/alpha");
    expect(beta.branch).toBe("feat/beta");
    expect(alpha.created).toBe(true);
    expect(beta.created).toBe(true);

    // Start both writes together: the two workspaces must never share a tree.
    let release: () => void = () => {};
    const barrier = new Promise<void>((resolve) => {
      release = resolve;
    });
    const run = async (prepared: typeof alpha, file: string, text: string) => {
      await barrier;
      writeFileSync(join(prepared.path, file), text);
      return finalizeDelivery(prepared);
    };
    const both = Promise.all([run(alpha, "alpha.txt", "alpha\n"), run(beta, "beta.txt", "beta\n")]);
    release();
    const [alphaDelivery, betaDelivery] = await both;

    expect(alphaDelivery).toMatchObject({ branch: "feat/alpha", pushed: true });
    expect(betaDelivery).toMatchObject({ branch: "feat/beta", pushed: true });
    expect(alphaDelivery.commit).not.toBe(betaDelivery.commit);

    // Each worktree contains only its own change.
    expect(readFileSync(join(alpha.path, "alpha.txt"), "utf8")).toBe("alpha\n");
    expect(existsSync(join(alpha.path, "beta.txt"))).toBe(false);
    expect(readFileSync(join(beta.path, "beta.txt"), "utf8")).toBe("beta\n");
    expect(existsSync(join(beta.path, "alpha.txt"))).toBe(false);

    // The Worker-owned source repo (the only shared checkout) is untouched.
    expect(git(fixture.repo, ["rev-parse", "--abbrev-ref", "HEAD"])).toBe("main");
    expect(git(fixture.repo, ["status", "--porcelain"])).toBe("");
    expect(existsSync(join(fixture.repo, "alpha.txt"))).toBe(false);
    expect(existsSync(join(fixture.repo, "beta.txt"))).toBe(false);

    // Both delivery branches reached the remote with their own file only.
    expect(git(fixture.origin, ["show", "refs/heads/feat/alpha:alpha.txt"])).toBe("alpha");
    expect(git(fixture.origin, ["show", "refs/heads/feat/beta:beta.txt"])).toBe("beta");
  });

  it("routes two concurrent assignments to their own workspace at the fake launch port", async () => {
    const fixture = makeWorkerRepo();
    const runtime = { workerId: "worker-1", workspaceRoot: join(fixture.dir, "worktrees") };
    const directory = new WorkerDirectory(["worker-alpha", "worker-beta"]);
    directory.register(manifest("worker-alpha", "alpha"), AT);
    directory.register(manifest("worker-beta", "beta"), AT);

    const portFor = (config: WorkstreamConfig): LaunchPort => ({
      async launch(input: LaunchRequest) {
        const prepared = prepareWorkstream(config, runtime);
        writeFileSync(join(prepared.path, `${config.id}.txt`), `${config.id}\n`);
        const delivery = finalizeDelivery(prepared, { message: `test ${config.id}` });
        const receipt: AssignmentReceipt = {
          assignment: input.assignment.id,
          ticket: input.ticket.id,
          worker: input.assignment.worker,
          workspace: input.assignment.workspace,
          packDigest: input.packDigest,
          at: AT,
          outcome: "completed",
          summary: `${config.id}@${delivery.commit}`,
          events: 0,
        };
        return {
          report: { kind: "ready", summary: receipt.summary, at: AT },
          receipt,
        };
      },
    });

    const port = createRoutedLaunchPort({
      directory,
      ports: new Map([
        ["worker-alpha", portFor(workspace("alpha", fixture))],
        ["worker-beta", portFor(workspace("beta", fixture))],
      ]),
    });
    const [one, two] = await Promise.all([
      port.launch(request("assignment-alpha", "alpha")),
      port.launch(request("assignment-beta", "beta")),
    ]);

    expect(one.report.kind).toBe("ready");
    expect(two.report.kind).toBe("ready");
    expect(one.receipt.workspace).toBe("alpha");
    expect(two.receipt.workspace).toBe("beta");
    expect(one.receipt.worker).toBe("worker-alpha");
    expect(two.receipt.worker).toBe("worker-beta");
    expect(one.report.summary).not.toBe(two.report.summary);

    // Neither worker wrote the shared source checkout.
    expect(git(fixture.repo, ["status", "--porcelain"])).toBe("");
    expect(existsSync(join(fixture.repo, "alpha.txt"))).toBe(false);
    expect(existsSync(join(fixture.repo, "beta.txt"))).toBe(false);
    expect(port.metrics()).toMatchObject({ queued: 0, running: 0, dispatched: 2 });
  });

  it("refuses workstream roots inside the source repo and unowned Master paths", () => {
    const fixture = makeWorkerRepo();
    expect(() =>
      prepareWorkstream(workspace("escape", fixture, join(fixture.repo, "worktrees", "escape")), {
        workerId: "worker-1",
        workspaceRoot: fixture.dir,
      }),
    ).toThrow(/inside the source repo/);
    expect(() => assertOwnedWorkspace("/srv/master/repo", undefined)).toThrow(WorkstreamError);
    expect(() => parseWorkstreams([{ id: "x", repo: fixture.repo, branch: "main" }])).toThrow(
      /branch must be "feat\/x"/,
    );
  });
});
