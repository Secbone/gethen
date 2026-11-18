#!/usr/bin/env node
// Post-build smoke: bundle the DSH entries, import the master bundle, and apply
// it against a minimal tool host. This catches registration-time failures that
// unit tests with an unvalidated fake context miss (e.g. an output schema whose
// `required` names a property missing from `properties`, which DSH rejects at
// `tools.register` and otherwise silently prevents the whole plugin mounting).
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const root = resolve(import.meta.dirname, "..");
const out = mkdtempSync(join(tmpdir(), "gethen-dsh-smoke-"));
const state = join(out, "state");

function fail(message) {
  throw new Error(`dsh bundle smoke: ${message}`);
}

/** Recursively verify that every `required` name exists in `properties`. */
function validateSchema(schema, path) {
  if (schema === null || typeof schema !== "object" || Array.isArray(schema)) return;
  const record = schema;
  if (Array.isArray(record.required)) {
    if (record.properties === undefined || typeof record.properties !== "object") {
      fail(`${path}: required present without properties`);
    }
    for (const name of record.required) {
      if (!(name in record.properties)) {
        fail(`${path}: required "${name}" is not in properties`);
      }
    }
  }
  for (const [key, value] of Object.entries(record.properties ?? {})) {
    validateSchema(value, `${path}.properties.${key}`);
  }
  if (record.items !== undefined) validateSchema(record.items, `${path}.items`);
}

const tools = [];
const cleanups = [];
const ctx = {
  tools: {
    register(definition) {
      if (typeof definition?.name !== "string") fail("tool without a name");
      tools.push(definition);
      return () => {};
    },
  },
  provide() {},
  effect(fn) {
    const dispose = fn();
    if (typeof dispose === "function") cleanups.push(dispose);
  },
  inject(_deps, fn) {
    // The real host invokes the injected callback once services exist; the
    // smoke target intentionally has no webServer/agents service.
    void fn;
  },
  get() {
    return undefined;
  },
  logger: { error: (message) => process.stderr.write(`[smoke] ${message}\n`) },
};

try {
  execFileSync(process.execPath, [join(root, "scripts", "build-dsh.mjs"), out], {
    cwd: root,
    stdio: "inherit",
  });
  const entry = pathToFileURL(join(out, "index.js")).href;
  const mod = await import(entry);
  if (typeof mod.apply !== "function") fail("bundle has no apply()");
  mod.apply(ctx, { dir: join(state, "master") });
  const names = tools.map((tool) => tool.name);
  for (const required of ["gethen_run", "gethen_status", "gethen_task"]) {
    if (!names.includes(required)) fail(`missing tool ${required}`);
  }
  for (const tool of tools) validateSchema(tool.output?.schema, `${tool.name}.output.schema`);
  process.stdout.write(`dsh bundle smoke passed (${tools.length} tools)\n`);
} finally {
  for (const cleanup of cleanups.reverse()) {
    try {
      await cleanup();
    } catch {
      // Best effort shutdown; the assertion result is what matters.
    }
  }
  rmSync(out, { recursive: true, force: true });
}
