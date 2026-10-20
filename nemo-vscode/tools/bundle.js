/**
 * Bundles the browser entry: the client, for the extension host, and
 * the server worker, with the WebAssembly module inlined — a web
 * extension loads single files and has no `node_modules` to resolve.
 *
 * Needs the module built first (`npm run wasm`).
 *
 * Usage: node tools/bundle.js
 */

const path = require("path");
const esbuild = require("esbuild");

const ROOT = path.join(__dirname, "..");

/** What both bundles share. */
const common = {
  bundle: true,
  platform: "browser",
  target: "es2022",
  logLevel: "warning",
};

Promise.all([
  esbuild.build({
    ...common,
    entryPoints: [path.join(ROOT, "src", "browser", "client.ts")],
    outfile: path.join(ROOT, "out", "browser", "client.js"),
    // The extension host loads the entry as a CommonJS module and
    // provides `vscode` itself.
    format: "cjs",
    external: ["vscode"],
    sourcemap: true,
  }),
  esbuild.build({
    ...common,
    entryPoints: [path.join(ROOT, "src", "browser", "server.ts")],
    outfile: path.join(ROOT, "out", "browser", "server.js"),
    format: "iife",
    loader: { ".wasm": "binary" },
    // The module's own asynchronous loader reads `import.meta`; the
    // worker never calls it, initializing from the inlined bytes.
    logOverride: { "empty-import-meta": "silent" },
  }),
]).catch(() => process.exit(1));
