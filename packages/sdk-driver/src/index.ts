/**
 * Zero-dependency DSH SDK driver.
 *
 * The Worker daemon spawns one `dsh --profile sdk` child per project/profile
 * and drives it over the SDK's newline-delimited JSON-RPC stdio protocol:
 *
 * ```ts
 * const driver = new DshSdkClient().start({ cwd: "/work/project" });
 * driver.onEvent(({ sessionId, event }) => forward(sessionId, event));
 * driver.onStatus(({ sessionId, status }) => track(sessionId, status));
 * driver.onApprovalRequest(async (request) => decideApproval(request));
 * driver.onUserQuestion(async (request) => askMaster(request));
 *
 * const { serverInfo } = await driver.initialize({ provider: "deepseek", model: "deepseek-v4" });
 * await driver.prompt("session-1", "fix the failing test");
 * await driver.wait("session-1", 10 * 60_000);
 * await driver.shutdown();
 * ```
 *
 * Everything process-shaped is injectable (`spawnImpl`, streams, timeouts), so
 * tests use an in-memory child and never launch the real runtime.
 */
export * from "./client";
export * from "./rpc";
