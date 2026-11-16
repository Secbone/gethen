#!/usr/bin/env node
// Validate public package boundaries, runtime/test dependencies, and the DAG.
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { isBuiltin } from "node:module";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

export const ALLOWED_WORKSPACE_DEPS = {
  "@gethen/core": [],
  "@gethen/storage-file": [],
  "@gethen/domain": [],
  "@gethen/evidence": [],
  "@gethen/master": [],
  "@gethen/worker-protocol": [],
  "@gethen/fleet": ["@gethen/worker-protocol"],
  "@gethen/transport-ws": ["@gethen/worker-protocol"],
  "@gethen/worker-agent": ["@gethen/worker-protocol"],
  "@gethen/sdk-driver": [],
  "@gethen/router": ["@gethen/fleet", "@gethen/worker-bridge"],
  "@gethen/worker-bridge": [],
  "@gethen/bridge-remote": ["@gethen/worker-bridge"],
  "@gethen/worker": ["@gethen/worker-bridge"],
  "@gethen/dsh-client": [],
  "@gethen/dsh": [
    "@gethen/master",
    "@gethen/bridge-remote",
    "@gethen/worker",
    "@gethen/worker-bridge",
  ],
  "@gethen/effects": ["@gethen/core", "@gethen/storage-file"],
  "@gethen/pack": ["@gethen/core"],
  "@gethen/panel": ["@gethen/domain"],
  "@gethen/evolve": ["@gethen/core", "@gethen/effects", "@gethen/pack", "@gethen/storage-file"],
  "@gethen/benchmark": [],
  "@gethen/benchmark-file": ["@gethen/benchmark", "@gethen/evaluation", "@gethen/storage-file"],
  "@gethen/session": ["@gethen/core"],
  "@gethen/kernel": ["@gethen/core"],
  "@gethen/evolution": ["@gethen/core", "@gethen/kernel"],
  "@gethen/evaluation": ["@gethen/core", "@gethen/evolution"],
  "@gethen/workspace": ["@gethen/core", "@gethen/evolution", "@gethen/storage-file"],
  "@gethen/cli": ["@gethen/master"],
};

const TEST_ONLY_DEPS = {};
/** Packages that must not depend on DSH (or other hosts) at all. */
export const CONTROL_PLANE_PACKAGES = new Set([
  "@gethen/core",
  "@gethen/master",
  "@gethen/router",
  "@gethen/fleet",
  "@gethen/evidence",
  "@gethen/pack",
  "@gethen/evolve",
  "@gethen/memory-client",
  "@gethen/worker-protocol",
  "@gethen/worker-bridge",
  "@gethen/transport-ws",
  "@gethen/worker-agent",
  "@gethen/sdk-driver",
  "@gethen/panel",
]);

const PURE_PACKAGES = new Set([
  "@gethen/evidence",
  "@gethen/worker-protocol",
  "@gethen/fleet",
  "@gethen/benchmark",
  "@gethen/session",
  "@gethen/core",
  "@gethen/domain",
  "@gethen/pack",
  "@gethen/panel",
  "@gethen/kernel",
  "@gethen/evolution",
]);

function* walk(dir) {
  if (!existsSync(dir)) return;
  for (const name of readdirSync(dir).sort()) {
    if (["node_modules", "dist"].includes(name)) continue;
    const path = join(dir, name);
    if (statSync(path).isDirectory()) yield* walk(path);
    else if (/\.(?:ts|mts|mjs|js)$/.test(name)) yield path;
  }
}

/** Read actual syntax, including re-exports, dynamic imports and import types. */
function moduleSpecifiers(path) {
  const source = ts.createSourceFile(
    path,
    readFileSync(path, "utf8"),
    ts.ScriptTarget.Latest,
    true,
  );
  const specifiers = [];
  function visit(node) {
    let spec;
    if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) spec = node.moduleSpecifier;
    else if (
      ts.isCallExpression(node) &&
      (node.expression.kind === ts.SyntaxKind.ImportKeyword ||
        (ts.isIdentifier(node.expression) && node.expression.text === "require"))
    )
      spec = node.arguments[0];
    else if (ts.isImportTypeNode(node) && ts.isLiteralTypeNode(node.argument))
      spec = node.argument.literal;
    if (spec && ts.isStringLiteralLike(spec)) specifiers.push(spec.text);
    ts.forEachChild(node, visit);
  }
  visit(source);
  return specifiers;
}

export function checkLayering(root, options = {}) {
  const violations = [];
  const packagesDir = join(root, "packages");
  const packages = new Map();
  for (const dir of readdirSync(packagesDir).sort()) {
    const pkgDir = join(packagesDir, dir);
    const manifestPath = join(pkgDir, "package.json");
    if (!existsSync(manifestPath)) continue;
    const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
    if (!manifest.name?.startsWith("@gethen/")) continue;
    if (
      options.verifyAllowedTable &&
      (!manifest.scripts?.typecheck || !existsSync(join(pkgDir, "tsconfig.json")))
    )
      violations.push(
        `${manifest.name}: every package requires a runnable typecheck and tsconfig.json`,
      );
    packages.set(manifest.name, { pkgDir, manifest });
  }
  const names = [...packages.keys()].sort((a, b) => b.length - a.length);
  // Keep the hand-written allowed-deps table honest against the real repo: it
  // must name real packages and no phantom dependencies. Skipped for partial
  // fixtures, which only define the packages a given test needs.
  if (options.verifyAllowedTable) {
    for (const [name, allowed] of Object.entries(ALLOWED_WORKSPACE_DEPS)) {
      if (!packages.has(name)) {
        violations.push(`allowed-deps table names unknown package ${name}`);
        continue;
      }
      for (const dependency of allowed)
        if (!packages.has(dependency))
          violations.push(`${name}: allowed dependency ${dependency} is not a package`);
    }
  }
  const graph = new Map();
  for (const [name, { pkgDir, manifest }] of packages) {
    const allowed = ALLOWED_WORKSPACE_DEPS[name];
    if (!allowed) {
      violations.push(
        `unknown package ${name}: define its responsibility and allowed dependencies`,
      );
      continue;
    }
    const declared = new Set(
      Object.keys(manifest.dependencies ?? {}).filter((dep) => dep.startsWith("@gethen/")),
    );
    const dev = new Set(
      Object.keys(manifest.devDependencies ?? {}).filter((dep) => dep.startsWith("@gethen/")),
    );
    const used = new Set();
    const production = new Set();
    for (const [subpath, target] of Object.entries(manifest.exports ?? {})) {
      if (typeof target !== "string" || !existsSync(join(pkgDir, target))) {
        violations.push(`${name}: invalid export ${subpath}`);
      }
    }
    for (const file of walk(join(pkgDir, "src"))) {
      const test = /\.test\.[^.]+$/.test(file) || file.includes(`${sep}testing${sep}`);
      for (const alias of moduleSpecifiers(file)) {
        const label = relative(root, file);
        if (alias.startsWith(".")) {
          const target = resolve(dirname(file), alias);
          if (relative(pkgDir, target).startsWith(`..${sep}`) || target === dirname(pkgDir)) {
            violations.push(
              `${label}: cross-package relative import ${alias}; use a public export`,
            );
          }
          continue;
        }
        if (PURE_PACKAGES.has(name) && !test && isBuiltin(alias)) {
          violations.push(`${label}: ${name} must stay pure; cannot import ${alias}`);
        }
        if (CONTROL_PLANE_PACKAGES.has(name) && alias.startsWith("@deepseek-ai/")) {
          violations.push(`${label}: ${name} must not import DSH (${alias})`);
        }
        if (!alias.startsWith("@gethen/")) continue;
        const dependency = names.find(
          (candidate) => alias === candidate || alias.startsWith(`${candidate}/`),
        );
        if (!dependency) {
          violations.push(`${label}: unknown workspace import ${alias}`);
          continue;
        }
        const subpath = alias === dependency ? "." : `.${alias.slice(dependency.length)}`;
        if (!Object.hasOwn(packages.get(dependency).manifest.exports ?? {}, subpath)) {
          violations.push(`${label}: non-public import ${alias}`);
        }
        if (dependency === name) continue;
        used.add(dependency);
        if (!test) production.add(dependency);
        if (!declared.has(dependency) && !(test && dev.has(dependency))) {
          violations.push(
            `${label}: ${dependency} is not declared for ${test ? "tests" : "production"}`,
          );
        }
      }
    }
    for (const dependency of declared) {
      if (!allowed.includes(dependency))
        violations.push(`${name}: dependency ${dependency} is not allowed`);
      if (!used.has(dependency)) violations.push(`${name}: stale dependency ${dependency}`);
    }
    for (const dependency of dev) {
      if (![...allowed, ...(TEST_ONLY_DEPS[name] ?? [])].includes(dependency)) {
        violations.push(`${name}: test dependency ${dependency} is not allowed`);
      }
      if (!used.has(dependency)) violations.push(`${name}: stale test dependency ${dependency}`);
      if (production.has(dependency) && !declared.has(dependency)) {
        violations.push(`${name}: production dependency ${dependency} is declared only for tests`);
      }
    }
    graph.set(name, used);
  }
  const done = new Set();
  const active = [];
  function visit(name) {
    if (active.includes(name)) {
      violations.push(
        `workspace cycle: ${[...active.slice(active.indexOf(name)), name].join(" -> ")}`,
      );
      return;
    }
    if (done.has(name)) return;
    active.push(name);
    for (const dep of graph.get(name) ?? []) visit(dep);
    active.pop();
    done.add(name);
  }
  for (const name of graph.keys()) visit(name);
  return { violations, packages: packages.size };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const result = checkLayering(root, { verifyAllowedTable: true });
  if (result.violations.length) {
    console.error(
      `layering check failed:\n${result.violations.map((item) => `  ✗ ${item}`).join("\n")}\nSee docs/dev-rules/layering.md`,
    );
    process.exitCode = 1;
  } else console.log(`layering check passed (${result.packages} packages)`);
}
