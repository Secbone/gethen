import { randomBytes, randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname } from "node:path";
import { SqliteStore, type WorkerRecord } from "@gethen/master";
import { hashWorkerToken, type WorkerTokenRecord } from "@gethen/transport-ws";
import { defaultMasterDbPath } from "../paths";
import { formatTable } from "../table";
import { parseWorkerTokens } from "./serve";

/** Token entropy: 32 random bytes, hex encoded (64 characters). */
const TOKEN_BYTES = 32;

export interface WorkerTokenOptions {
  /** Environment for the default db path (tests). Defaults to `process.env`. */
  readonly env?: NodeJS.ProcessEnv;
  /** Override the home directory used for the default db path (tests). */
  readonly home?: string;
  /** Sink for stdout lines (tests). Defaults to stdout. */
  readonly write?: (line: string) => void;
  /** Sink for error lines (tests). Defaults to stderr. */
  readonly error?: (line: string) => void;
  /** Injectable clock (tests). Defaults to `new Date()`. */
  readonly now?: () => Date;
  /** Injectable token source (tests). Defaults to 32 random bytes as hex. */
  readonly randomToken?: () => string;
}

/** One worker identity as reported by `gethen worker ls --json`. */
export interface WorkerListEntry {
  id: string;
  name: string;
  createdAt: string;
  lastSeenAt: string | null;
  revoked: boolean;
  revokedAt: string | null;
}

/** Dispatch `gethen worker add|ls|revoke`; returns the CLI exit code. */
export function workerTokenCommand(argv: string[], options: WorkerTokenOptions = {}): number {
  switch (argv[0]) {
    case "add":
      return workerAdd(argv.slice(1), options);
    case "ls":
      return workerList(argv.slice(1), options);
    case "revoke":
      return workerRevoke(argv.slice(1), options);
    default:
      return 1;
  }
}

/**
 * Create one worker identity and print its token exactly once. Only the
 * SHA-256 hash is persisted (in the master sqlite `workers` table); the
 * plaintext token is never written unless `--tokens-file` explicitly asks for
 * the serve-compatible credentials file.
 */
export function workerAdd(argv: string[], options: WorkerTokenOptions = {}): number {
  const write = writer(options);
  const writeError = errorWriter(options);
  if (argv.includes("-h") || argv.includes("--help")) {
    write(WORKER_ADD_USAGE);
    return 0;
  }

  const workerId = flagValue(argv, "--worker-id");
  if (workerId === undefined || workerId.trim().length === 0) {
    writeError("gethen worker add: --worker-id <id> is required");
    return 1;
  }
  const id = workerId.trim();
  const name = optionalFlag(argv, "--name") ?? id;
  const tokensFile = optionalFlag(argv, "--tokens-file");
  const force = argv.includes("--force");

  const dbPath = resolveDbPath(argv, options);
  if (!existsSync(dbPath)) {
    writeError(missingDb(dbPath));
    return 1;
  }

  const token = (options.randomToken ?? (() => randomBytes(TOKEN_BYTES).toString("hex")))();
  const createdAt = (options.now ?? (() => new Date()))().toISOString();

  let replaced = false;
  try {
    const store = new SqliteStore(dbPath);
    try {
      const existing = store.getWorker(id);
      if (existing !== undefined && !force) {
        const suffix = existing.revokedAt === undefined ? "" : " (revoked)";
        writeError(
          `gethen worker add: worker "${id}" already exists${suffix}; pass --force to replace it`,
        );
        return 1;
      }
      replaced = existing !== undefined;
      store.saveWorker({ id, name, tokenHash: hashWorkerToken(token), createdAt });
    } finally {
      store.close();
    }
  } catch (error) {
    writeError(`gethen worker add: ${messageOf(error)}`);
    return 1;
  }

  write(`worker ${id} ${replaced ? "replaced" : "created"} (name: ${name})`);
  write(`token: ${token}`);
  write("This token is shown only once; store it in the worker config now.");

  if (tokensFile !== undefined) {
    try {
      const records = readTokensFile(tokensFile);
      const merged = mergeWorkerToken(records, { workerId: id, token });
      writeTokensFile(tokensFile, merged);
      write(`tokens file updated: ${tokensFile} (${merged.length} worker(s))`);
    } catch (error) {
      writeError(
        `gethen worker add: worker record saved, but cannot update --tokens-file ` +
          `${tokensFile}: ${messageOf(error)}`,
      );
      return 1;
    }
  }
  return 0;
}

/** List persisted worker identities: text table, or JSON with `--json`. */
export function workerList(argv: string[], options: WorkerTokenOptions = {}): number {
  const write = writer(options);
  const writeError = errorWriter(options);
  if (argv.includes("-h") || argv.includes("--help")) {
    write(WORKER_LS_USAGE);
    return 0;
  }

  const dbPath = resolveDbPath(argv, options);
  if (!existsSync(dbPath)) {
    writeError(missingDb(dbPath));
    return 1;
  }

  let workers: WorkerRecord[];
  try {
    const store = new SqliteStore(dbPath);
    try {
      workers = store.listWorkers();
    } finally {
      store.close();
    }
  } catch (error) {
    writeError(`gethen worker ls: ${messageOf(error)}`);
    return 1;
  }

  if (argv.includes("--json")) {
    write(JSON.stringify(workers.map(listEntry), null, 2));
    return 0;
  }

  if (workers.length === 0) {
    write("(no workers registered)");
    return 0;
  }
  const rows = workers.map((worker) => [
    worker.id,
    worker.name,
    worker.createdAt,
    worker.lastSeenAt ?? "-",
    worker.revokedAt === undefined ? "active" : "revoked",
  ]);
  for (const line of formatTable(["ID", "NAME", "CREATED", "LAST-SEEN", "STATUS"], rows)) {
    write(line);
  }
  return 0;
}

/** Revoke one worker identity; idempotent for already-revoked records. */
export function workerRevoke(argv: string[], options: WorkerTokenOptions = {}): number {
  const write = writer(options);
  const writeError = errorWriter(options);
  if (argv.includes("-h") || argv.includes("--help")) {
    write(WORKER_REVOKE_USAGE);
    return 0;
  }

  const idFlag = flagValue(argv, "--id") ?? flagValue(argv, "--worker-id");
  if (idFlag === undefined || idFlag.trim().length === 0) {
    writeError("gethen worker revoke: --id <id> is required");
    return 1;
  }
  const id = idFlag.trim();

  const dbPath = resolveDbPath(argv, options);
  if (!existsSync(dbPath)) {
    writeError(missingDb(dbPath));
    return 1;
  }

  try {
    const store = new SqliteStore(dbPath);
    try {
      const worker = store.getWorker(id);
      if (worker === undefined) {
        writeError(`gethen worker revoke: worker "${id}" not found`);
        return 1;
      }
      if (worker.revokedAt !== undefined) {
        write(`worker ${id} already revoked at ${worker.revokedAt}`);
        return 0;
      }
      store.revokeWorker(id, (options.now ?? (() => new Date()))().toISOString());
    } finally {
      store.close();
    }
  } catch (error) {
    writeError(`gethen worker revoke: ${messageOf(error)}`);
    return 1;
  }

  write(`worker ${id} revoked`);
  return 0;
}

function listEntry(worker: WorkerRecord): WorkerListEntry {
  return {
    id: worker.id,
    name: worker.name,
    createdAt: worker.createdAt,
    lastSeenAt: worker.lastSeenAt ?? null,
    revoked: worker.revokedAt !== undefined,
    revokedAt: worker.revokedAt ?? null,
  };
}

function readTokensFile(path: string): WorkerTokenRecord[] {
  if (!existsSync(path)) return [];
  const raw = readFileSync(path, "utf8");
  if (raw.trim().length === 0) return [];
  // `parseWorkerTokens` validates the array of {workerId, token} objects.
  return parseWorkerTokens(raw);
}

/** Replace the entry for one worker and keep every other credential. */
function mergeWorkerToken(
  records: readonly WorkerTokenRecord[],
  next: WorkerTokenRecord,
): WorkerTokenRecord[] {
  return [...records.filter((record) => record.workerId !== next.workerId), next];
}

/** Atomic write (same-directory temp file + rename), mode 0600. */
function writeTokensFile(path: string, records: readonly WorkerTokenRecord[]): void {
  mkdirSync(dirname(path), { recursive: true });
  const temporary = `${path}.tmp-${process.pid}-${randomUUID()}`;
  try {
    writeFileSync(temporary, `${JSON.stringify(records, null, 2)}\n`, { mode: 0o600 });
    renameSync(temporary, path);
  } catch (error) {
    try {
      rmSync(temporary, { force: true });
    } catch {
      // keep the original error
    }
    throw error;
  }
}

function resolveDbPath(argv: string[], options: WorkerTokenOptions): string {
  return (
    flagValue(argv, "--db") ??
    defaultMasterDbPath(options.env ?? process.env, options.home ?? homedir())
  );
}

function missingDb(dbPath: string): string {
  return `gethen worker: no master database at ${dbPath}; start \`gethen serve\` first or pass --db <file>`;
}

function optionalFlag(argv: string[], name: string): string | undefined {
  const value = flagValue(argv, name);
  return value === undefined || value.trim().length === 0 ? undefined : value.trim();
}

function flagValue(argv: string[], name: string): string | undefined {
  const index = argv.indexOf(name);
  return index >= 0 ? argv[index + 1] : undefined;
}

function writer(options: WorkerTokenOptions): (line: string) => void {
  return options.write ?? ((line: string) => process.stdout.write(`${line}\n`));
}

function errorWriter(options: WorkerTokenOptions): (line: string) => void {
  return options.error ?? ((line: string) => process.stderr.write(`${line}\n`));
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

const WORKER_ADD_USAGE = `Usage: gethen worker add --worker-id <id> [--name <name>]
                        [--tokens-file <file>] [--db <file>] [--force]

Creates a worker identity in the master sqlite (only the SHA-256 token hash is
persisted) and prints the plaintext token exactly once. --tokens-file also
merges {"workerId": ..., "token": ...} into that JSON array atomically for the
\`gethen serve --tokens\` endpoint. --force replaces an existing record and
invalidates its previous token. The default database is
$GETHEN_STATE_DIR/master.sqlite or ~/.gethen/master.sqlite.
`;

const WORKER_LS_USAGE = `Usage: gethen worker ls [--db <file>] [--json]

Lists the worker identities persisted by the master (id, name, createdAt,
lastSeenAt, revoked status). The default database is
$GETHEN_STATE_DIR/master.sqlite or ~/.gethen/master.sqlite.
`;

const WORKER_REVOKE_USAGE = `Usage: gethen worker revoke --id <id> [--db <file>]

Marks a worker revoked in the master sqlite; its tokens stop authorizing.
Already-revoked workers are reported without further change. The default
database is $GETHEN_STATE_DIR/master.sqlite or ~/.gethen/master.sqlite.
`;
