import { homedir } from "node:os";
import { join } from "node:path";

/**
 * The default state directory: `$GETHEN_STATE_DIR`, else `~/.gethen`.
 * Shared by `serve` (where the master database lives), `status` (where to read
 * it from) and `doctor` (which checks it is writable).
 */
export function defaultStateDir(
  env: NodeJS.ProcessEnv = process.env,
  home: string = homedir(),
): string {
  return env.GETHEN_STATE_DIR ?? join(home, ".gethen");
}

/** The default master sqlite file under the state directory. */
export function defaultMasterDbPath(
  env: NodeJS.ProcessEnv = process.env,
  home: string = homedir(),
): string {
  return join(defaultStateDir(env, home), "master.sqlite");
}
