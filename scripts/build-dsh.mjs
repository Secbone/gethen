#!/usr/bin/env node
// Bundle the DSH plugin entries (master + worker) into a file-URL profile dir.
// `--watch` keeps esbuild watching and rebuilding on save; combined with
// `hmr.root` pointing at the output dir, DSH hot-replaces the plugins.
import { cpSync, mkdirSync, realpathSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { build, context } from "esbuild";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const watch = process.argv.includes("--watch");
const positional = process.argv.slice(2).filter((arg) => arg !== "--watch");
const out = resolve(positional[0] ?? join(root, "dist", "dsh"));
mkdirSync(out, { recursive: true });
mkdirSync(join(out, "dsh-client"), { recursive: true });

const common = {
  bundle: true,
  platform: "node",
  format: "esm",
  target: "node22",
  logLevel: "warning",
  sourcemap: false,
  // `ws` ships CommonJS with dynamic `require("events")`, which breaks when
  // inlined into an ESM bundle; keep it external and vendor the package next
  // to the plugin entries (the profile has no node_modules of its own).
  external: ["ws"],
};

const entries = [
  { entry: "packages/dsh/src/index.ts", outfile: "index.js" },
  { entry: "packages/dsh/src/worker-entry.ts", outfile: "worker.js" },
];

const contexts = [];
for (const { entry, outfile } of entries) {
  const options = {
    ...common,
    entryPoints: [join(root, entry)],
    outfile: join(out, outfile),
  };
  if (watch) contexts.push(await context(options));
  else await build(options);
}

for (const file of ["client.js", "index.js", "package.json"])
  cpSync(join(root, "packages/dsh-client", file), join(out, "dsh-client", file));

// Vendor the externalized runtime dependency so the file-URL profile resolves it.
const wsDest = join(out, "node_modules/ws");
rmSync(wsDest, { recursive: true, force: true });
cpSync(realpathSync(join(root, "node_modules/ws")), wsDest, {
  recursive: true,
  dereference: true,
});

if (watch) {
  for (const ctx of contexts) await ctx.watch();
  process.stdout.write(`watching ${out}\n`);
} else {
  process.stdout.write(`${out}\n`);
}
