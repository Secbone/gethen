import { describe, expect, it, vi } from "vitest";
import {
  type ApprovalRequestParams,
  DshSdkClient,
  type DshSdkSpawnOptions,
  type SessionEventNotification,
  type SessionStatusNotification,
  type StartDshSdkOptions,
  type UserQuestionRequest,
} from "./client";
import { FakeChild } from "./test-doubles";

function start(options: StartDshSdkOptions = {}) {
  const child = new FakeChild();
  const client = new DshSdkClient();
  client.start({ spawnImpl: () => child, ...options });
  return { child, client };
}

function frameOf(child: FakeChild, method: string): Record<string, unknown> {
  const frame = child.frame(method);
  if (frame === undefined) throw new Error(`the client did not send a "${method}" frame`);
  return frame;
}

function paramsOf(child: FakeChild, method: string): Record<string, unknown> {
  const params = frameOf(child, method).params;
  if (typeof params !== "object" || params === null) {
    throw new Error(`the "${method}" frame has no params object`);
  }
  return params as Record<string, unknown>;
}

async function tick(): Promise<void> {
  await new Promise((resolve) => setImmediate(resolve));
}

describe("DshSdkClient process", () => {
  it("spawns dsh --profile sdk with the configured cwd and merged env", () => {
    const calls: Array<{ command: string; args: string[]; options: DshSdkSpawnOptions }> = [];
    const child = new FakeChild();
    const client = new DshSdkClient();
    client.start({
      spawnImpl: (command, args, options) => {
        calls.push({ command, args, options });
        return child;
      },
      cwd: "/work/ws",
      env: { GETHEN_TEST: "1" },
    });

    expect(calls).toHaveLength(1);
    const call = calls[0];
    if (call === undefined) throw new Error("spawn was not called");
    expect(call.command).toBe("dsh");
    expect(call.args).toEqual(["--profile", "sdk"]);
    expect(call.options.cwd).toBe("/work/ws");
    expect(call.options.env.GETHEN_TEST).toBe("1");
    expect(call.options.env.PATH).toBe(process.env.PATH);
    expect(call.options.stdio).toEqual(["pipe", "pipe", "pipe"]);
    expect(client.running).toBe(true);
    client.close();
  });

  it("prefers an explicit command, then GETHEN_DSH, then dsh", () => {
    const previous = process.env.GETHEN_DSH;
    try {
      process.env.GETHEN_DSH = "/opt/dsh";
      const fromEnv: string[] = [];
      new DshSdkClient()
        .start({
          spawnImpl: (command) => {
            fromEnv.push(command);
            return new FakeChild();
          },
        })
        .close();
      expect(fromEnv).toEqual(["/opt/dsh"]);
    } finally {
      if (previous === undefined) delete process.env.GETHEN_DSH;
      else process.env.GETHEN_DSH = previous;
    }

    const explicit: string[] = [];
    new DshSdkClient()
      .start({
        command: "/custom/dsh",
        args: ["--profile", "sdk-test"],
        spawnImpl: (command, args) => {
          explicit.push(`${command} ${args.join(" ")}`);
          return new FakeChild();
        },
      })
      .close();
    expect(explicit).toEqual(["/custom/dsh --profile sdk-test"]);
  });

  it("completes the initialize handshake", async () => {
    const { child, client } = start();
    const pending = client.initialize({
      provider: "deepseek",
      model: "deepseek-v4",
      cwd: "/work/ws",
      maxTokens: 2_000,
      reasoningEffort: "high",
      approvalRequests: true,
      userQuestions: true,
    });

    const frame = frameOf(child, "initialize");
    expect(frame.params).toEqual({
      cwd: "/work/ws",
      provider: "deepseek",
      model: "deepseek-v4",
      maxTokens: 2_000,
      reasoningEffort: "high",
      approvalRequests: true,
      capabilities: { userQuestions: true },
    });

    child.respond(frame.id, {
      serverInfo: { name: "deepseek-harness-sdk-runtime", version: "0.2.2-rc.1" },
    });
    await expect(pending).resolves.toEqual({
      serverInfo: { name: "deepseek-harness-sdk-runtime", version: "0.2.2-rc.1" },
    });
    client.close();
  });

  it("declares approval and question capabilities only when a handler exists", async () => {
    const approvals = start();
    approvals.client.onApprovalRequest(() => "cancelled");
    const first = approvals.client.initialize({ provider: "p", model: "m" });
    const firstParams = paramsOf(approvals.child, "initialize");
    expect(firstParams.approvalRequests).toBe(true);
    expect(firstParams).not.toHaveProperty("capabilities");
    approvals.child.respond(frameOf(approvals.child, "initialize").id, {
      serverInfo: { name: "sdk", version: "1" },
    });
    await first;
    approvals.client.close();

    const questions = start();
    questions.client.onUserQuestion(() => ({ cancelled: true }));
    const second = questions.client.initialize({ provider: "p", model: "m" });
    const secondParams = paramsOf(questions.child, "initialize");
    expect(secondParams).toHaveProperty("capabilities", { userQuestions: true });
    expect(secondParams).not.toHaveProperty("approvalRequests");
    questions.child.respond(frameOf(questions.child, "initialize").id, {
      serverInfo: { name: "sdk", version: "1" },
    });
    await second;
    questions.client.close();
  });

  it("prompts and queries with text content blocks", async () => {
    const { child, client } = start();

    const prompt = client.prompt("s1", "fix the failing test");
    const promptFrame = frameOf(child, "session/prompt");
    expect(promptFrame.params).toEqual({
      sessionId: "s1",
      contentBlocks: [{ type: "text", text: "fix the failing test" }],
    });
    child.respond(promptFrame.id, { messageId: "msg-1" });
    await expect(prompt).resolves.toEqual({ messageId: "msg-1" });

    const query = client.query("s1", "/compact");
    const queryFrame = frameOf(child, "session/query");
    expect(queryFrame.params).toEqual({
      sessionId: "s1",
      contentBlocks: [{ type: "text", text: "/compact" }],
    });
    child.respond(queryFrame.id, { kind: "command", execution: { commandId: "c1" } });
    await expect(query).resolves.toEqual({ kind: "command", execution: { commandId: "c1" } });
    client.close();
  });

  it("waits, resumes and reads or changes the working directory", async () => {
    const { child, client } = start();

    const waiting = client.wait("s1", 1_000);
    expect(paramsOf(child, "session/wait")).toEqual({ sessionId: "s1" });
    child.respond(frameOf(child, "session/wait").id, {});
    await expect(waiting).resolves.toBeUndefined();

    const resuming = client.resume("s1");
    expect(paramsOf(child, "session/resume")).toEqual({ sessionId: "s1" });
    child.respond(frameOf(child, "session/resume").id, {});
    await expect(resuming).resolves.toBeUndefined();

    const read = client.getWorkingDirectory("s1");
    expect(paramsOf(child, "session/working-directory/get")).toEqual({ sessionId: "s1" });
    child.respond(frameOf(child, "session/working-directory/get").id, { cwd: "/work/ws" });
    await expect(read).resolves.toBe("/work/ws");

    const changed = client.setWorkingDirectory("s1", "sub");
    expect(paramsOf(child, "session/working-directory/set")).toEqual({
      sessionId: "s1",
      path: "sub",
    });
    child.respond(frameOf(child, "session/working-directory/set").id, { cwd: "/work/ws/sub" });
    await expect(changed).resolves.toBe("/work/ws/sub");
    client.close();
  });

  it("dispatches session events and status notifications until unsubscribed", () => {
    const { child, client } = start();
    const events: SessionEventNotification[] = [];
    const statuses: SessionStatusNotification[] = [];
    const stderr: string[] = [];
    const offEvent = client.onEvent((notification) => events.push(notification));
    const offStatus = client.onStatus((notification) => statuses.push(notification));
    const offStderr = client.onStderr((text) => stderr.push(text));

    child.notify("session.event", { sessionId: "s1", event: { type: "message", text: "hi" } });
    child.notify("session.status", { sessionId: "s1", status: "running" });
    child.stderr.emit("data", "warning: heads up\n");

    expect(events).toEqual([{ sessionId: "s1", event: { type: "message", text: "hi" } }]);
    expect(statuses).toEqual([{ sessionId: "s1", status: "running" }]);
    expect(stderr).toEqual(["warning: heads up\n"]);

    offEvent();
    offStatus();
    offStderr();
    child.notify("session.event", { sessionId: "s1", event: { type: "message" } });
    child.notify("session.status", { sessionId: "s1", status: "idle" });
    child.stderr.emit("data", "late\n");
    expect(events).toHaveLength(1);
    expect(statuses).toHaveLength(1);
    expect(stderr).toHaveLength(1);
    client.close();
  });

  it("answers approval requests and cancels without a working handler", async () => {
    const { child, client } = start();
    const seen: ApprovalRequestParams[] = [];
    client.onApprovalRequest((request) => {
      seen.push(request);
      return request.toolName === "shell" ? "allowed-once" : "rejected";
    });

    child.request(41, "approval/request", { sessionId: "s1", toolName: "shell" });
    await tick();
    expect(seen).toEqual([{ sessionId: "s1", toolName: "shell" }]);
    expect(child.frameById(41)).toEqual({
      jsonrpc: "2.0",
      id: 41,
      result: { outcome: "allowed-once" },
    });

    child.request(42, "approval/request", { sessionId: "s1", toolName: "write" });
    await tick();
    expect(child.frameById(42)?.result).toEqual({ outcome: "rejected" });
    client.close();

    const bare = start();
    bare.child.request(43, "approval/request", { sessionId: "s1", toolName: "shell" });
    await tick();
    expect(bare.child.frameById(43)?.result).toEqual({ outcome: "cancelled" });

    bare.client.onApprovalRequest(() => {
      throw new Error("no answerer");
    });
    bare.child.request(44, "approval/request", { sessionId: "s1", toolName: "shell" });
    await tick();
    expect(bare.child.frameById(44)?.result).toEqual({ outcome: "cancelled" });
    bare.client.close();
  });

  it("answers user questions and cancels without a working handler", async () => {
    const { child, client } = start();
    const seen: UserQuestionRequest[] = [];
    client.onUserQuestion((request) => {
      seen.push(request);
      return { answers: [{ id: "q1", selected: ["Merge"] }] };
    });

    child.request(51, "user-question/ask", {
      requestId: "q-1",
      sessionId: "s1",
      questions: [{ id: "q1", question: "Merge or rebase?", options: [{ label: "Merge" }] }],
    });
    await tick();
    expect(seen).toHaveLength(1);
    expect(child.frameById(51)?.result).toEqual({
      answers: [{ id: "q1", selected: ["Merge"] }],
    });
    client.close();

    const bare = start();
    bare.child.request(52, "user-question/ask", {
      requestId: "q-2",
      sessionId: "s1",
      questions: [],
    });
    await tick();
    expect(bare.child.frameById(52)?.result).toEqual({ cancelled: true });

    bare.client.onUserQuestion(() => {
      throw new Error("no answerer");
    });
    bare.child.request(53, "user-question/ask", {
      requestId: "q-3",
      sessionId: "s1",
      questions: [],
    });
    await tick();
    expect(bare.child.frameById(53)?.result).toEqual({ cancelled: true });
    bare.client.close();
  });

  it("rejects calls before start and after close, and kills a live child on close", async () => {
    const unstarted = new DshSdkClient();
    await expect(unstarted.prompt("s1", "hi")).rejects.toThrow(/not started/);

    const { child, client } = start();
    client.close();
    await expect(client.prompt("s1", "hi")).rejects.toThrow(/not started/);
    expect(client.running).toBe(false);
    expect(child.signals).toEqual(["SIGKILL"]);
    expect(() => client.start({ spawnImpl: () => child })).toThrow(/already been started/);
  });

  it("rejects pending calls when the child exits or fails to spawn", async () => {
    const exited = start();
    const waiting = exited.client.prompt("s1", "hi");
    const failed = expect(waiting).rejects.toThrow(/exited/);
    exited.child.exit(1, null);
    await failed;
    expect(exited.client.running).toBe(false);
    exited.client.close();

    const broken = start();
    const requested = broken.client.prompt("s1", "hi");
    const refused = expect(requested).rejects.toThrow(/failed to spawn|failed:/);
    broken.child.emit("error", new Error("spawn ENOENT"));
    await refused;
    expect(broken.client.running).toBe(false);
    broken.client.close();
  });

  it("shuts down gracefully, and SIGTERMs a runtime that does not exit", async () => {
    const graceful = start();
    const done = graceful.client.shutdown();
    const frame = frameOf(graceful.child, "shutdown");
    graceful.child.respond(frame.id, {});
    graceful.child.exit(0, null);
    await done;

    expect(graceful.client.running).toBe(false);
    expect(graceful.child.signals).toEqual([]);
    expect(() => graceful.client.close()).not.toThrow();

    const stubborn = start({ shutdownTimeoutMs: 10 });
    const timeout = stubborn.client.shutdown();
    stubborn.child.respond(frameOf(stubborn.child, "shutdown").id, {});
    await timeout;

    expect(stubborn.child.signals).toEqual(["SIGTERM"]);
    expect(stubborn.client.running).toBe(false);
  });

  it("honours the per-wait timeout override", async () => {
    vi.useFakeTimers();
    try {
      const { child, client } = start();
      const waiting = client.wait("s1", 50);
      frameOf(child, "session/wait");
      const rejection = expect(waiting).rejects.toThrow(/timed out after 50ms/);
      vi.advanceTimersByTime(50);
      await rejection;
      client.close();
    } finally {
      vi.useRealTimers();
    }
  });
});
