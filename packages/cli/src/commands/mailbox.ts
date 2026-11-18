/**
 * Operator surface for dead-lettered mailbox messages: list, replay (requeue
 * with a fresh attempt budget) and clear (delete). Dead letters are durable
 * rows in the mailbox sqlite; replay lets the bridge/worker consume them again.
 */
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { SqliteMailboxStore } from "@gethen/master-runtime";
import { defaultStateDir } from "../paths";

export interface MailboxOptions {
  readonly home?: string;
  readonly env?: NodeJS.ProcessEnv;
  readonly write?: (line: string) => void;
  readonly error?: (line: string) => void;
}

export function mailboxCommand(argv: string[], options: MailboxOptions = {}): number {
  const write = options.write ?? ((line: string) => process.stdout.write(`${line}\n`));
  const error = options.error ?? ((line: string) => process.stderr.write(`${line}\n`));
  if (argv.includes("-h") || argv.includes("--help") || argv.length === 0) {
    write(MAILBOX_USAGE);
    return argv.length === 0 ? 1 : 0;
  }
  const action = argv[0];
  if (action !== "list" && action !== "replay" && action !== "clear") {
    error(`gethen mailbox: unknown action "${action}"`);
    return 1;
  }
  let dbPath: string;
  let mailbox: string;
  let ids: string[];
  let json: boolean;
  try {
    const env = options.env ?? process.env;
    dbPath =
      flagValue(argv, "--db") ??
      join(defaultStateDir(env, options.home ?? homedir()), "mailbox.sqlite");
    mailbox = requireFlag(argv, "--mailbox");
    ids = flagValues(argv, "--id");
    json = argv.includes("--json");
  } catch (failure) {
    error(`gethen mailbox: ${messageOf(failure)}`);
    return 1;
  }
  if (!existsSync(dbPath)) {
    error(`gethen mailbox: no mailbox database at ${dbPath}`);
    return 1;
  }

  const store = new SqliteMailboxStore(dbPath);
  try {
    if (action === "list") {
      const limit = optionalPositive(flagValue(argv, "--limit"));
      const dead = store.deadLetters(mailbox, limit === undefined ? {} : { limit });
      if (json) {
        write(JSON.stringify({ mailbox, count: dead.length, messages: dead }, null, 2));
      } else if (dead.length === 0) {
        write(`mailbox ${mailbox}: no dead letters`);
      } else {
        for (const item of dead) {
          write(
            `${item.envelope.id}  ${item.envelope.type}  attempts=${item.attempts}  ` +
              `created=${item.createdAt}  subject=${item.envelope.subject ?? "-"}`,
          );
        }
        write(`(${dead.length} dead letter(s) in ${mailbox})`);
      }
      return 0;
    }
    const moved =
      action === "replay" ? store.replayDead(mailbox, ids) : store.clearDead(mailbox, ids);
    const scope = ids.length === 0 ? "all" : `${ids.length} selected`;
    write(
      action === "replay"
        ? `replayed ${moved} dead letter(s) in ${mailbox} (${scope})`
        : `cleared ${moved} dead letter(s) in ${mailbox} (${scope})`,
    );
    return 0;
  } catch (failure) {
    error(`gethen mailbox: ${messageOf(failure)}`);
    return 1;
  } finally {
    store.close();
  }
}

const MAILBOX_USAGE = `Usage:
  gethen mailbox list  --mailbox <name> [--db <file>] [--limit <n>] [--json]
  gethen mailbox replay --mailbox <name> [--id <id> ...] [--db <file>]
  gethen mailbox clear  --mailbox <name> [--id <id> ...] [--db <file>]

Dead letters are messages a consumer dead-lettered (explicit nack or attempts
exhausted). replay moves them back to ready with attempts=0 so a running
bridge/worker can consume them; clear deletes them. Default db:
<GETHEN_STATE_DIR>/mailbox.sqlite.`;

function requireFlag(argv: string[], name: string): string {
  const value = flagValue(argv, name);
  if (value === undefined || value.trim().length === 0) {
    throw new Error(`${name} is required`);
  }
  return value.trim();
}

function flagValue(argv: string[], name: string): string | undefined {
  const values = flagValues(argv, name);
  return values.at(-1);
}

function flagValues(argv: string[], name: string): string[] {
  const values: string[] = [];
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] !== name) continue;
    const value = argv[index + 1];
    if (value !== undefined && !value.startsWith("--")) values.push(value);
  }
  return values;
}

function optionalPositive(value: string | undefined): number | undefined {
  if (value === undefined) return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error("--limit must be a positive integer");
  }
  return parsed;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
