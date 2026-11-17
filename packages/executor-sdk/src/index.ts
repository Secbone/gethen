/**
 * `@gethen/executor-sdk`: DSH as a `HarnessExecutor`.
 *
 * `createSdkHarnessExecutor()` implements the harness-agnostic port from
 * `@gethen/harness` with DSH SDK JSON-RPC sessions (`dsh --profile sdk`): one
 * runtime per workspace, one session per assignment, streamed `session.event`s,
 * and DSH's native `approval/request` / `user-question/ask` channels mapped onto
 * blocking Harness escalations. See README.md for the mapping and cancellation
 * semantics.
 */
export * from "./harness-adapter";
