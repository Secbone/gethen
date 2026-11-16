import type { ReflectedCandidate } from "@gethen/assistant";
import { saveReflections as apply, taskContext as context } from "@gethen/assistant";
import { createMemoryAccess } from "@gethen/memory";
import { FileMemoryRepository, JsonFileMemoryStore } from "@gethen/memory-file";

/** File binding for historical experiment callers; domain behavior stays in assistant. */
export function taskContext(path: string, task: string): string {
  return context(new JsonFileMemoryStore(path), task);
}
export function saveReflections(path: string, candidates: ReflectedCandidate[]): Promise<number> {
  return apply(createMemoryAccess(new FileMemoryRepository(path)), candidates);
}
