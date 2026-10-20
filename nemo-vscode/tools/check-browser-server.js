/**
 * Runs the bundled server worker in Node and holds a short
 * conversation with it: initialize, open a document with an error, and
 * expect its diagnostics back.
 *
 * The worker's global scope is replaced by an object with the members
 * of a message port: `onmessage` for what the client sends, and
 * `postMessage` for what the worker sends back.
 *
 * Usage: node tools/check-browser-server.js   (after `npm run compile`)
 */

const fs = require("fs");
const path = require("path");
const vm = require("vm");

const BUNDLE = path.join(__dirname, "..", "out", "browser", "server.js");

/** What the worker posted, in order. */
const posted = [];
/** Whoever waits for the next posted message. */
let waiting = null;

const port = {
  onmessage: null,
  addEventListener() {},
  postMessage(message) {
    posted.push(message);
    if (waiting) {
      waiting();
    }
  },
};

/** Resolves with the first posted message the predicate accepts. */
function receive(accept) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("no response within ten seconds")), 10000);
    const look = () => {
      const index = posted.findIndex(accept);
      if (index >= 0) {
        clearTimeout(timer);
        waiting = null;
        resolve(posted.splice(index, 1)[0]);
      }
    };
    waiting = look;
    look();
  });
}

/** Sends a message to the worker, as the client does. */
function send(message) {
  port.onmessage({ data: message });
}

async function main() {
  globalThis.self = port;
  vm.runInThisContext(fs.readFileSync(BUNDLE, "utf8"), { filename: BUNDLE });

  send({ jsonrpc: "2.0", id: 1, method: "initialize", params: { processId: null, rootUri: null, capabilities: {} } });
  const initialized = await receive((message) => message.id === 1);
  if (!initialized.result || !initialized.result.capabilities) {
    throw new Error(`no capabilities: ${JSON.stringify(initialized)}`);
  }
  send({ jsonrpc: "2.0", method: "initialized", params: {} });

  send({
    jsonrpc: "2.0",
    method: "textDocument/didOpen",
    params: { textDocument: { uri: "untitled:one", languageId: "nemo", version: 1, text: "p(?x :- q(?x) ." } },
  });
  const published = await receive((message) => message.method === "textDocument/publishDiagnostics");
  const codes = published.params.diagnostics.map((diagnostic) => diagnostic.code);
  if (!codes.includes("N0202")) {
    throw new Error(`unexpected diagnostics: ${JSON.stringify(published)}`);
  }
  console.log(`the worker responded: capabilities, and diagnostics ${codes.join(", ")}`);
  process.exit(0);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
