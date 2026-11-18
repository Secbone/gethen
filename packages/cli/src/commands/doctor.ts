import { spawnSync } from "node:child_process";
import { accessSync, constants, existsSync, readFileSync, statSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { defaultStateDir } from "../paths";
import { parseWorkstreams, validateWorkstreams } from "../workstream";

const MIN_NODE_MAJOR = 22;
// `dsh --profile sdk --help` starts the SDK runtime before printing usage and
// can take several seconds on a cold start; keep enough headroom to avoid
// reporting a false warning on a healthy install.
const PROBE_TIMEOUT_MS = 10_000;

/** Env vars that may carry a model-provider credential (presence is reported, never the value). */
const CREDENTIAL_VARS = ["OPENCODE_API_KEY", "DEEPSEEK_API_KEY"] as const;

export interface DoctorOptions {
  /** Override the home directory (tests). Defaults to os.homedir(). */
  readonly home?: string;
  /** Override the state directory (tests). Defaults to `$GETHEN_STATE_DIR` / `~/.gethen`. */
  readonly stateDir?: string;
  /** Environment to inspect (tests). Defaults to `process.env`. */
  readonly env?: NodeJS.ProcessEnv;
  /** Print one structured JSON document instead of one line per check. */
  readonly json?: boolean;
  /** Sink for report lines (tests). Defaults to stdout. */
  readonly write?: (line: string) => void;
  /** Worker config file to validate; defaults to `$GETHEN_WORKER_CONFIG`. */
  readonly workerConfig?: string;
}

type Level = "ok" | "warn" | "info";

/** One diagnostic check; `name` is stable for `--json` consumers. */
export interface DoctorCheck {
  name: string;
  status: Level;
  message: string;
}

/** The full diagnostic result; warnings never make the command fail. */
export interface DoctorReport {
  /** True when no check warned. */
  ok: boolean;
  warnings: number;
  stateDir: string;
  checks: DoctorCheck[];
}

/**
 * Local prerequisite checks. Warnings never make the command fail: this only
 * reports state (exit code is always 0, matching earlier `gethen doctor`
 * behavior). Credentials are only probed for presence, never printed.
 */
export function doctorCommand(options: DoctorOptions = {}): number {
  const write =
    options.write ??
    ((line: string) => {
      process.stdout.write(`${line}\n`);
    });
  const report = runDoctor(options);
  if (options.json === true) {
    write(JSON.stringify(report, null, 2));
  } else {
    for (const check of report.checks) write(`${check.status.padEnd(4)} ${check.message}`);
  }
  return 0;
}

/** Collect the checks without printing; used by `doctorCommand` and tests. */
export function runDoctor(options: DoctorOptions = {}): DoctorReport {
  const env = options.env ?? process.env;
  const home = options.home ?? homedir();
  const stateDir = options.stateDir ?? defaultStateDir(env, home);
  const checks: DoctorCheck[] = [];

  checks.push(nodeCheck());
  const dsh = dshCheck(env);
  checks.push(dsh.check);
  checks.push(dshSdkCheck(env, dsh.runnable));
  checks.push(dshHomeCheck(home));
  checks.push(credentialsCheck(env));
  checks.push(stateDirCheck(stateDir));
  checks.push(gethenEnvCheck(env));
  checks.push(workerConfigCheck(options.workerConfig ?? env.GETHEN_WORKER_CONFIG));

  const warnings = checks.filter((check) => check.status === "warn").length;
  return { ok: warnings === 0, warnings, stateDir, checks };
}

function nodeCheck(): DoctorCheck {
  const major = Number.parseInt(process.version.replace(/^v/, ""), 10);
  if (Number.isNaN(major)) {
    return {
      name: "node",
      status: "warn",
      message: `could not parse node version ${process.version}`,
    };
  }
  if (major >= MIN_NODE_MAJOR) {
    return {
      name: "node",
      status: "ok",
      message: `node ${process.version} (>= ${MIN_NODE_MAJOR})`,
    };
  }
  return {
    name: "node",
    status: "warn",
    message: `node ${process.version} is older than ${MIN_NODE_MAJOR}; upgrade Node`,
  };
}

function dshCheck(env: NodeJS.ProcessEnv): { check: DoctorCheck; runnable: boolean } {
  const command = env.GETHEN_DSH ?? "dsh";
  const version = probe(command, ["--version"], env);
  if (version === undefined) {
    return {
      runnable: false,
      check: {
        name: "dsh",
        status: "warn",
        message:
          command === "dsh"
            ? "dsh not found on PATH; install DSH or ignore if unused"
            : `dsh not runnable (${command}); install DSH or set GETHEN_DSH`,
      },
    };
  }
  return {
    runnable: true,
    check: {
      name: "dsh",
      status: "ok",
      message: command === "dsh" ? `dsh ${version}` : `dsh ${version} (${command})`,
    },
  };
}

function dshSdkCheck(env: NodeJS.ProcessEnv, dshRunnable: boolean): DoctorCheck {
  if (!dshRunnable) {
    return {
      name: "dsh-sdk",
      status: "info",
      message: "dsh --profile sdk --help skipped (dsh not runnable)",
    };
  }
  const command = env.GETHEN_DSH ?? "dsh";
  if (probe(command, ["--profile", "sdk", "--help"], env) === undefined) {
    return {
      name: "dsh-sdk",
      status: "warn",
      message: "dsh --profile sdk --help failed; install or upgrade DSH (gethen worker needs it)",
    };
  }
  return { name: "dsh-sdk", status: "ok", message: "dsh --profile sdk --help works" };
}

function dshHomeCheck(home: string): DoctorCheck {
  const dshHome = join(home, ".dsh");
  if (existsSync(dshHome)) {
    return { name: "dsh-home", status: "ok", message: `${dshHome} exists` };
  }
  return {
    name: "dsh-home",
    status: "warn",
    message: `${dshHome} not found; run dsh once to create it`,
  };
}

function credentialsCheck(env: NodeJS.ProcessEnv): DoctorCheck {
  const present = CREDENTIAL_VARS.filter((name) => (env[name] ?? "").length > 0);
  if (present.length === 0) {
    return {
      name: "credentials",
      status: "warn",
      message: `credentials: none of ${CREDENTIAL_VARS.join(", ")} is set (model runs may fail)`,
    };
  }
  return { name: "credentials", status: "ok", message: `credentials: ${present.join(", ")} set` };
}

function stateDirCheck(stateDir: string): DoctorCheck {
  if (!existsSync(stateDir)) {
    return {
      name: "state-dir",
      status: "warn",
      message: `${stateDir} does not exist; run \`gethen init\` (or let \`gethen serve\` create it)`,
    };
  }
  let directory = false;
  try {
    directory = statSync(stateDir).isDirectory();
  } catch {
    directory = false;
  }
  if (!directory) {
    return { name: "state-dir", status: "warn", message: `${stateDir} is not a directory` };
  }
  try {
    accessSync(stateDir, constants.W_OK);
  } catch {
    return { name: "state-dir", status: "warn", message: `${stateDir} is not writable` };
  }
  return { name: "state-dir", status: "ok", message: `${stateDir} is writable` };
}

function workerConfigCheck(path: string | undefined): DoctorCheck {
  if (path === undefined || path.trim().length === 0) {
    return {
      name: "workstreams",
      status: "info",
      message: "worker config not set (optional); set GETHEN_WORKER_CONFIG to validate workstreams",
    };
  }
  if (!existsSync(path)) {
    return {
      name: "workstreams",
      status: "warn",
      message: `worker config not found: ${path}`,
    };
  }
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8")) as Record<string, unknown>;
    const workstreams = parseWorkstreams(parsed.workstreams);
    if (workstreams === undefined || workstreams.length === 0) {
      return {
        name: "workstreams",
        status: "info",
        message: `${path}: no workstreams configured (legacy workspaces only)`,
      };
    }
    const issues = validateWorkstreams(workstreams);
    if (issues.length > 0) {
      return {
        name: "workstreams",
        status: "warn",
        message: `${path}: ${issues.join("; ")}`,
      };
    }
    return {
      name: "workstreams",
      status: "ok",
      message: `${path}: ${workstreams.length} workstream(s) valid`,
    };
  } catch (failure) {
    return {
      name: "workstreams",
      status: "warn",
      message: `${path}: ${failure instanceof Error ? failure.message : String(failure)}`,
    };
  }
}

function gethenEnvCheck(env: NodeJS.ProcessEnv): DoctorCheck {
  const names = Object.keys(env)
    .filter((key) => key.startsWith("GETHEN_") && (env[key] ?? "") !== "")
    .sort();
  return {
    name: "gethen-env",
    status: "info",
    message:
      names.length === 0
        ? "GETHEN_* environment: none set (optional)"
        : `GETHEN_* environment: ${names.length} set (${names.join(", ")})`,
  };
}

function probe(command: string, args: string[], env: NodeJS.ProcessEnv): string | undefined {
  try {
    const result = spawnSync(command, args, {
      timeout: PROBE_TIMEOUT_MS,
      encoding: "utf8",
      // Keep the caller's PATH (tests inject a fake runtime command) while
      // honouring any explicit environment overrides.
      env: { ...process.env, ...env },
    });
    if (result.error || result.status !== 0) return undefined;
    const firstLine = result.stdout.trim().split("\n")[0] ?? "";
    return firstLine.trim() === "" ? "available" : firstLine.trim();
  } catch {
    return undefined;
  }
}
