/**
 * The Nemo extension in a browser: the language client, around the
 * language server compiled to WebAssembly.
 *
 * VS Code for the web — and an editor built on its API, as the Nemo web
 * interface is — loads this entry instead of the native one. There is
 * no process to start: the server is `nemo-wasm`'s, in a worker beside
 * the extension host (see `server.ts`).
 */

import * as vscode from "vscode";
import { LanguageClient, LanguageClientOptions } from "vscode-languageclient/browser";

/** The running client and its worker, while there is one. */
let running: { client: LanguageClient; worker: Worker } | undefined;

/** Where the extension says what it is doing. */
let log: vscode.OutputChannel;

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  log = vscode.window.createOutputChannel("Nemo");
  context.subscriptions.push(log);

  context.subscriptions.push(
    vscode.commands.registerCommand("nemo.restartServer", async () => {
      log.appendLine("restarting on request");
      await stop();
      await start(context);
    }),
    vscode.commands.registerCommand("nemo.showOutput", () => log.show(true)),
  );

  await start(context);
}

export async function deactivate(): Promise<void> {
  await stop();
}

/** Starts the server's worker and the client talking to it. */
async function start(context: vscode.ExtensionContext): Promise<void> {
  const script = vscode.Uri.joinPath(context.extensionUri, "out", "browser", "server.js");
  const worker = new Worker(script.toString(true));
  // Documents live wherever the editor keeps them: in memory, in a
  // virtual file system — not only on disk.
  const options: LanguageClientOptions = {
    documentSelector: [{ language: "nemo" }],
    outputChannel: log,
  };
  const client = new LanguageClient("nemo", "Nemo Language Server", options, worker);
  try {
    await client.start();
    running = { client, worker };
    log.appendLine("the server is running");
    context.subscriptions.push({ dispose: () => void stop() });
  } catch (error) {
    worker.terminate();
    log.appendLine(`the server did not start: ${error}`);
    void vscode.window.showErrorMessage(
      `The Nemo language server did not start: ${error}. See the Nemo output for details.`,
    );
  }
}

/** Stops the client and its worker, if they are running. */
async function stop(): Promise<void> {
  const stopping = running;
  running = undefined;
  if (stopping) {
    await stopping.client.stop();
    stopping.worker.terminate();
  }
}
