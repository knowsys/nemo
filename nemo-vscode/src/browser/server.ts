/**
 * The language server in a worker: `nemo-wasm`'s server, its messages
 * carried over the worker's message port.
 *
 * The client's requests and notifications go to the server's channel,
 * and the responses to requests come back. What the server sends on its
 * own (diagnostics, log messages, requests of the client) is pumped
 * from its request stream. The client's responses to those go into the
 * server's response sink, which takes one at a time.
 */

import {
  BrowserMessageReader,
  BrowserMessageWriter,
  Message,
  RequestMessage,
  ResponseMessage,
} from "vscode-languageserver/browser";

import module from "../../wasm/nemo_wasm_bg.wasm";
import { createNemoLanguageServer, initSync, setPanicHook } from "../../wasm/nemo_wasm.js";

declare const self: DedicatedWorkerGlobalScope;

initSync({ module });
setPanicHook();

const [channel, requests, responses] = createNemoLanguageServer();
const reader = new BrowserMessageReader(self);
const writer = new BrowserMessageWriter(self);

/** Reports what went wrong carrying a message, where the client sees it. */
function report(error: unknown): void {
  console.error("[nemo language server]", error);
}

/** The server's own messages, forwarded until it exits. */
async function pump(): Promise<void> {
  for (;;) {
    const message: Message | null = await requests.getNextRequest();
    if (message === null) {
      return;
    }
    await writer.write(message);
  }
}

void pump().catch(report);

/** The client's responses to the server's requests, sent in arrival order. */
let responseChain: Promise<void> = Promise.resolve();

reader.listen((message: Message) => {
  if ("method" in message) {
    // A request or a notification of the client.
    channel
      .sendRequest(message as RequestMessage)
      .then((response: ResponseMessage | null) =>
        response === null ? undefined : writer.write(response),
      )
      .catch(report);
  } else {
    // The client's response to a request of the server.
    responseChain = responseChain
      .then(() => responses.sendResponse(message as ResponseMessage))
      .catch(report);
  }
});
