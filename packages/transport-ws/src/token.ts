/**
 * Worker tokens for the WS control plane.
 *
 * A token is bound to exactly one `workerId`: the connection upgrade resolves
 * the bearer token to that identity, and the first `hello` frame must agree
 * with it. The in-memory store keeps only SHA-256 digests (never the plaintext
 * token after construction) and compares digests with `timingSafeEqual`.
 *
 * A durable store (sqlite) can implement the same `WorkerTokenStore` interface;
 * `hashWorkerToken` produces the value such a store persists.
 */
import { createHash, timingSafeEqual } from "node:crypto";

/** One configured worker credential. */
export interface WorkerTokenRecord {
  workerId: string;
  token: string;
}

/** The identity a bearer token resolves to. */
export interface WorkerTokenIdentity {
  workerId: string;
}

/** Durable lookup interface used by the gateway's `verifyToken` hook. */
export interface WorkerTokenStore {
  lookup(token: string): Promise<WorkerTokenIdentity | undefined>;
}

/** SHA-256 of a worker token, hex encoded (what a durable store persists). */
export function hashWorkerToken(token: string): string {
  return createHash("sha256").update(token, "utf8").digest("hex");
}

interface TokenEntry {
  workerId: string;
  digest: Buffer;
}

/**
 * Build an in-memory token store from the configured credentials.
 *
 * Empty tokens/worker ids and duplicate tokens are configuration errors and
 * throw eagerly, so a bad worker entry fails startup rather than silently
 * authorizing the wrong identity.
 */
export function createMemoryTokenStore(records: readonly WorkerTokenRecord[]): WorkerTokenStore {
  const entries: TokenEntry[] = [];
  const seen = new Set<string>();

  for (const record of records) {
    if (record.workerId.length === 0) {
      throw new Error("worker token record needs a non-empty workerId");
    }
    if (record.token.length === 0) {
      throw new Error(`worker "${record.workerId}" needs a non-empty token`);
    }
    const digest = createHash("sha256").update(record.token, "utf8").digest();
    const digestHex = digest.toString("hex");
    if (seen.has(digestHex)) {
      throw new Error(`duplicate worker token for worker "${record.workerId}"`);
    }
    seen.add(digestHex);
    entries.push({ workerId: record.workerId, digest });
  }

  return {
    async lookup(token: string): Promise<WorkerTokenIdentity | undefined> {
      // Hash the candidate first so both operands are 32 bytes; equal-length
      // buffers are what `timingSafeEqual` requires.
      const candidate = createHash("sha256").update(token, "utf8").digest();
      let match: WorkerTokenIdentity | undefined;
      for (const entry of entries) {
        // Compare every entry (no early exit) so timing does not reveal which
        // entry matched or that a match happened at all.
        if (timingSafeEqual(entry.digest, candidate) && match === undefined) {
          match = { workerId: entry.workerId };
        }
      }
      return match;
    },
  };
}
