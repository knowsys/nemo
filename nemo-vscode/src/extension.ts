/**
 * The Nemo extension: a language client around `nemo-language-server`.
 *
 * Everything the editor shows about a program's *meaning* — diagnostics,
 * hover, navigation, completion, renaming, formatting — comes from the
 * server. What lives here is what an editor needs before a server is
 * even running: which files are Nemo, how they are commented and
 * bracketed, and how to find and start the server.
 */

import * as fs from "fs";
import * as path from "path";
import * as vscode from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions,
  TransportKind,
} from "vscode-languageclient/node";

/** The running client, while there is one. */
let client: LanguageClient | undefined;

/** Where the extension says what it is doing. */
let log: vscode.OutputChannel;

/** The executable's name on this platform. */
const EXECUTABLE =
  process.platform === "win32" ? "nemo-language-server.exe" : "nemo-language-server";

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

  // A setting that names a different server means the running one is
  // the wrong one.
  context.subscriptions.push(
    vscode.workspace.onDidChangeConfiguration(async (change) => {
      if (change.affectsConfiguration("nemo.server")) {
        log.appendLine("the server setting changed; restarting");
        await stop();
        await start(context);
      }
    }),
  );

  await start(context);
}

export async function deactivate(): Promise<void> {
  await stop();
}

/** Starts the client, or explains why it cannot. */
async function start(context: vscode.ExtensionContext): Promise<void> {
  const settings = vscode.workspace.getConfiguration("nemo");
  const command = locate(settings.get<string>("server.path", ""));
  if (!command) {
    // Nothing is broken — the server is simply not built yet, which is
    // the usual state of a checkout.
    const build = "How do I get it?";
    const choice = await vscode.window.showWarningMessage(
      "The Nemo language server was not found, so diagnostics and navigation are off. " +
        "Highlighting still works.",
      build,
    );
    if (choice === build) {
      log.appendLine(
        "Build it with `cargo build --release -p nemo-language-server`, or set " +
          "`nemo.server.path` to an executable you already have.",
      );
      log.show(true);
    }
    return;
  }

  log.appendLine(`starting ${command}`);
  const server: ServerOptions = {
    run: {
      command,
      args: settings.get<string[]>("server.arguments", []),
      transport: TransportKind.stdio,
    },
    debug: {
      command,
      args: settings.get<string[]>("server.arguments", []),
      transport: TransportKind.stdio,
    },
  };
  const options: LanguageClientOptions = {
    documentSelector: [{ scheme: "file", language: "nemo" }],
    outputChannel: log,
    synchronize: {
      fileEvents: vscode.workspace.createFileSystemWatcher("**/*.rls"),
    },
  };

  client = new LanguageClient("nemo", "Nemo Language Server", server, options);
  try {
    await client.start();
    log.appendLine("the server is running");
    context.subscriptions.push({ dispose: () => void stop() });
  } catch (error) {
    client = undefined;
    log.appendLine(`the server did not start: ${error}`);
    void vscode.window.showErrorMessage(
      `The Nemo language server did not start: ${error}. See the Nemo output for details.`,
    );
  }
}

/** Stops the client, if one is running. */
async function stop(): Promise<void> {
  const running = client;
  client = undefined;
  if (running) {
    await running.stop();
  }
}

/**
 * The server executable to run: the one configured, else one built in
 * the workspace, else whatever the `PATH` holds.
 *
 * A checkout of the engine has its own build, and finding that first is
 * what makes the extension work without being configured.
 */
function locate(configured: string): string | undefined {
  if (configured) {
    // A configured path is taken as given: if it is wrong, saying so is
    // more useful than quietly running something else.
    return configured;
  }
  for (const folder of vscode.workspace.workspaceFolders ?? []) {
    for (const profile of ["release", "debug"]) {
      const candidate = path.join(folder.uri.fsPath, "target", profile, EXECUTABLE);
      if (fs.existsSync(candidate)) {
        return candidate;
      }
    }
  }
  return onPath();
}

/** The executable on the `PATH`, if it is there. */
function onPath(): string | undefined {
  const directories = (process.env.PATH ?? "").split(path.delimiter);
  for (const directory of directories) {
    if (!directory) {
      continue;
    }
    const candidate = path.join(directory, EXECUTABLE);
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return undefined;
}
