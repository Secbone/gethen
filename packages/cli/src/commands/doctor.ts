import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const MIN_NODE_MAJOR = 22;
const DSH_TIMEOUT_MS = 5_000;

export interface DoctorOptions {
  /** Override the home directory (tests). Defaults to os.homedir(). */
  readonly home?: string;
  /** Sink for report lines (tests). Defaults to stdout. */
  readonly write?: (line: string) => void;
}

type Level = "ok" | "warn" | "info";

/**
 * Local prerequisite checks. Warnings never make the command fail: this only
 * reports state. gethen-memory reachability is intentionally not checked so
 * the CLI stays dependency-free (and offline-safe).
 */
export function doctorCommand(options: DoctorOptions = {}): number {
  const home = options.home ?? homedir();
  const write =
    options.write ??
    ((line: string) => {
      process.stdout.write(`${line}\n`);
    });
  const report = (level: Level, message: string): void => {
    write(`${level.padEnd(4)} ${message}`);
  };

  const major = Number.parseInt(process.version.replace(/^v/, ""), 10);
  if (Number.isNaN(major)) {
    report("warn", `could not parse node version ${process.version}`);
  } else if (major >= MIN_NODE_MAJOR) {
    report("ok", `node ${process.version} (>= ${MIN_NODE_MAJOR})`);
  } else {
    report("warn", `node ${process.version} is older than ${MIN_NODE_MAJOR}; upgrade Node`);
  }

  const dsh = dshVersion();
  if (dsh === undefined) {
    report("warn", "dsh not found on PATH; install DSH or ignore if unused");
  } else {
    report("ok", `dsh ${dsh}`);
  }

  const dshHome = join(home, ".dsh");
  if (existsSync(dshHome)) {
    report("ok", `${dshHome} exists`);
  } else {
    report("warn", `${dshHome} not found; run dsh once to create it`);
  }

  const gethenEnv = Object.keys(process.env)
    .filter((key) => key.startsWith("GETHEN_") && process.env[key] !== "")
    .sort();
  if (gethenEnv.length === 0) {
    report("info", "GETHEN_* environment: none set (optional)");
  } else {
    report("info", `GETHEN_* environment: ${gethenEnv.length} set (${gethenEnv.join(", ")})`);
  }

  return 0;
}

function dshVersion(): string | undefined {
  try {
    const result = spawnSync("dsh", ["--version"], {
      timeout: DSH_TIMEOUT_MS,
      encoding: "utf8",
    });
    if (result.error || result.status !== 0) return undefined;
    const firstLine = result.stdout.trim().split("\n")[0] ?? "";
    return firstLine.trim() === "" ? "available" : firstLine.trim();
  } catch {
    return undefined;
  }
}
