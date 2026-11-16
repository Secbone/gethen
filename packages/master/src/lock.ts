/**
 * Single-writer lock for the master SQLite database.
 *
 * The owner record is published with an atomic hard link, so a reader never
 * observes a half-written lock file. When the recorded process is gone the lock
 * is taken over; recovery itself is serialized through a sidecar lock so two
 * live processes cannot both recover the same dead lock.
 *
 * Not reentrant: one `FileLock` instance per process.
 */
import { randomUUID } from "node:crypto";
import { linkSync, mkdirSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

export interface LockOwner {
  pid: number;
  nonce: string;
}

/** True while the process exists. `EPERM` means it exists but is not ours. */
export function processAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (isErrno(error, "ESRCH")) return false;
    if (isErrno(error, "EPERM")) return true;
    throw error;
  }
}

export function readLockOwner(bytes: string): LockOwner {
  let parsed: unknown;
  try {
    parsed = JSON.parse(bytes);
  } catch {
    throw new Error("INVALID_LOCK: lock file is not JSON; manual recovery required");
  }
  if (typeof parsed !== "object" || parsed === null) {
    throw new Error("INVALID_LOCK: lock file has no owner; manual recovery required");
  }
  const record = parsed as { pid?: unknown; nonce?: unknown };
  if (
    typeof record.pid !== "number" ||
    !Number.isSafeInteger(record.pid) ||
    record.pid <= 0 ||
    typeof record.nonce !== "string"
  ) {
    throw new Error("INVALID_LOCK: lock file has no valid owner; manual recovery required");
  }
  return { pid: record.pid, nonce: record.nonce };
}

export class FileLock {
  readonly path: string;
  #release: (() => void) | undefined;

  constructor(path: string) {
    this.path = path;
  }

  get locked(): boolean {
    return this.#release !== undefined;
  }

  /** Throws `FILE_LOCKED` while another live process owns the lock. */
  acquire(): void {
    if (this.#release !== undefined) throw new Error(`FILE_LOCKED: ${this.path}`);
    this.#release = publishOwner(this.path);
  }

  release(): void {
    const release = this.#release;
    if (release === undefined) return;
    release();
    this.#release = undefined;
  }
}

/** Runs `fn` while holding the lock; releases even when `fn` throws. */
export function withFileLock<T>(path: string, fn: () => T): T {
  const lock = new FileLock(path);
  lock.acquire();
  try {
    return fn();
  } finally {
    lock.release();
  }
}

function publishOwner(path: string): () => void {
  mkdirSync(dirname(path), { recursive: true });
  const owner: LockOwner = { pid: process.pid, nonce: randomUUID() };
  const bytes = JSON.stringify(owner);
  const temporary = `${path}.${randomUUID()}.owner`;
  writeFileSync(temporary, bytes, { flag: "wx", mode: 0o600 });
  try {
    try {
      linkSync(temporary, path);
    } catch (error) {
      if (!isErrno(error, "EEXIST")) throw error;
      let previous: string;
      try {
        previous = readFileSync(path, "utf8");
      } catch (readError) {
        if (isErrno(readError, "ENOENT")) throw new Error(`FILE_LOCKED: ${path}`);
        throw readError;
      }
      if (processAlive(readLockOwner(previous).pid)) {
        throw new Error(`FILE_LOCKED: ${path}`);
      }
      // The recovery lock is itself a FileLock path, so recovery after a crash
      // while recovering is bounded and repeatable.
      const releaseRecovery = publishOwner(`${path}.recovery`);
      try {
        let current: string | undefined;
        try {
          current = readFileSync(path, "utf8");
        } catch (readError) {
          if (!isErrno(readError, "ENOENT")) throw readError;
        }
        if (current !== undefined) {
          if (processAlive(readLockOwner(current).pid)) {
            throw new Error(`FILE_LOCKED: ${path}`);
          }
          unlinkSync(path);
        }
        linkSync(temporary, path);
      } finally {
        releaseRecovery();
      }
    }
  } finally {
    unlinkSync(temporary);
  }
  return () => {
    let current: string;
    try {
      current = readFileSync(path, "utf8");
    } catch (error) {
      if (isErrno(error, "ENOENT")) throw new Error("LOCK_LOST: lock file disappeared");
      throw error;
    }
    if (current !== bytes) throw new Error("LOCK_LOST: lock ownership changed");
    unlinkSync(path);
  };
}

function isErrno(error: unknown, code: string): boolean {
  return typeof error === "object" && error !== null && (error as { code?: unknown }).code === code;
}
