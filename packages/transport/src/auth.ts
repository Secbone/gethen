import { createHash, timingSafeEqual } from "node:crypto";

/** One worker credential: the identity a token authenticates as. */
export interface TokenRecord {
  workerId: string;
  token: string;
}

export interface TokenStore {
  /** Resolve a presented token to its identity, or undefined when invalid. */
  lookup(token: string): Promise<{ workerId: string } | undefined>;
}

/** sha256 hex digest of a bearer token (the only form a host should persist). */
export function hashToken(token: string): string {
  return createHash("sha256").update(token, "utf8").digest("hex");
}

/**
 * In-memory token store that compares digests in constant time. Hosts that
 * persist worker credentials store `hashToken(token)` and compare the digest.
 */
export function createMemoryTokenStore(records: readonly TokenRecord[]): TokenStore {
  const entries = records.map((record) => ({
    workerId: record.workerId,
    digest: Buffer.from(hashToken(record.token), "hex"),
  }));
  return {
    async lookup(token: string): Promise<{ workerId: string } | undefined> {
      const candidate = Buffer.from(hashToken(token), "hex");
      let found: { workerId: string } | undefined;
      for (const entry of entries) {
        if (entry.digest.length === candidate.length && timingSafeEqual(entry.digest, candidate)) {
          found = { workerId: entry.workerId };
        }
      }
      return found;
    },
  };
}
