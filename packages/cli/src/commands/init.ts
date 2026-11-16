import { existsSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export interface InitOptions {
  /** Override the home directory (tests). Defaults to os.homedir(). */
  readonly home?: string;
  /** Sink for report lines (tests). Defaults to stdout. */
  readonly write?: (line: string) => void;
}

/** Idempotently create ~/.gethen and print next steps. No secrets are written. */
export function initCommand(options: InitOptions = {}): number {
  const home = options.home ?? homedir();
  const write =
    options.write ??
    ((line: string) => {
      process.stdout.write(`${line}\n`);
    });

  const dir = join(home, ".gethen");
  const existed = existsSync(dir);
  mkdirSync(dir, { recursive: true });

  write(existed ? `${dir} already exists` : `created ${dir}`);
  write("next steps:");
  write("  1. run `gethen doctor` to check Node, dsh and ~/.dsh");
  write("  2. set GETHEN_WORKER / GETHEN_MODEL when you use a worker");
  write("  3. run `gethen help` for usage");

  return 0;
}
