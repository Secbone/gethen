import { existsSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";

/** The most recent `gethen iterate` run directory under cwd, if any. */
export function latestRunDir(cwd = process.cwd()): string | undefined {
  const base = join(cwd, ".gethen", "runs");
  if (!existsSync(base)) return undefined;
  const entries = readdirSync(base)
    .filter((name) => statSync(join(base, name)).isDirectory())
    .sort((a, b) => statSync(join(base, a)).mtimeMs - statSync(join(base, b)).mtimeMs);
  const last = entries[entries.length - 1];
  return last ? join(base, last) : undefined;
}

export function defaultMemoryPath(cwd: string): string {
  return join(cwd, ".gethen", "memory.json");
}

export function defaultExperienceDir(cwd: string): string {
  return join(cwd, ".gethen", "experience");
}

/** Versioned data-artifact store used by `gethen evolve` (distinct from memory.json). */
export function defaultMemoryStoreDir(cwd: string): string {
  return join(cwd, ".gethen", "memory");
}
