# nemo-wasm — WebAssembly bindings for the Nemo rule engine

```js
import { NemoProgram, NemoEngine } from "nemo-wasm";

const program = NemoProgram.parse(`
    @import p from csv { resource = "data.csv" } .
    q(?x) :- p(?x, ?y) .
`);
console.log(program.ok, program.predicates);   // inspection fetches nothing

const engine = new NemoEngine();
engine.addResource("data.csv", bytesOrString); // a browser has no filesystem
engine.addModule("lib::tc", text);             // read by `@use lib::tc`
engine.importRows("p", [["a", 1], ["b", 2]]);  // rows from JS, before load
await engine.loadAsync(program);               // remote imports fetched
await engine.reasonAsync();                    // bound imports fetched

engine.rows("q");                              // Array of Arrays of values
const cursor = engine.cursor("q");              // bounded paging
while (!cursor.done) engine.readChunk(cursor);     // [{entryId, values, terms}, …]
engine.exportTables();                         // { "out.csv": Uint8Array, … }
engine.exportPredicate("q");                   // CSV bytes of any predicate
```

The engine is synchronous: a call returns when it is done, so run it
in a web worker and the main thread stays live. Results are plain
structured-cloneable data, so `postMessage` carries them. Only fetching
remote data is asynchronous.

## Remote imports

The engine never fetches. A method that needs data from the network
(`load`, `extend` or `reload` with remote imports, `reason` with a
bound SPARQL import) suspends: it throws code N0491 and keeps its state.

The async methods `loadAsync`, `extendAsync`, `reloadAsync` and
`reasonAsync` do the rest: they send the requests through the global
`fetch` (a window's, a worker's, or Node's), at most five at a time,
send a request again if the server asks to wait (429, 503) or is
overloaded (502, 504), up to five times, pass the responses to the
engine, and resume the method until it completes. They resolve when it
does, and reject with what the method throws otherwise. While requests
are out, the engine still serves reads (`count`, `rows`, cursors), and
the methods that would abandon the suspended one throw.

To fetch another way, through a proxy or with credentials, handle the
suspension directly: `pendingRequests()` lists the requests (`id`,
`url`, `method`, `headers`, `accept`, `parameters`, `form`). Respond to
each with `respond(id, bytes)` or `respondFailure(id, status, message)`
and call `resume()`, which may suspend again. A failure is final, so
retry before responding. A 413, or a response the endpoint cut off
partway, makes the engine split a bound import's request if it can. A resource registered with `addResource` is read
without suspending.

## The explanation viewer

The queries of the explanation viewer of nemo-web (nev) use that
viewer's protocol: plain camelCase objects, terms in term syntax, rules
by statement id.

- `treeForTable({predicate, tableEntries?})` — the selected facts, their
  derivation expanded by the engine: a node per fact set, connected by
  the rule deriving it (`childInformation: {rule, children}`).
- `tableEntriesForTreeNodes({predicate, tableEntries?, childInformation?})`
  — the tree the query shapes, one entry per node (`addressInTree`)
  with the facts consistent with all of its constraints.

A selection (`tableEntries.queries`) holds entry ids — the `entryId` of
a cursor row — or term lists in term syntax, as `"<a>, 3"`;
`tableEntries.pagination` slices a node. An expanded node's children
are its rule's positive premises, then one per negated literal listing
the negated predicate's facts.

`rule(id)` returns the rule a statement id names — `{rule, text, name,
source: {file, start, end, line, column}}` — so an editor can jump to
it. `trace(facts)` and `traceAscii(facts)` explain single facts as
proof trees.

An imported predicate the rules read only in part is stored only in
part, and a query over the rest throws. Call `retainImports()` before
`load` to keep imports whole.

## The language server in the browser

`createNemoLanguageServer()` returns three handles — `[channel,
requests, responses]` — carrying the Language Server Protocol over
plain objects instead of standard streams, so an editor in a web worker
gets diagnostics, completion, hover, rename, and formatting with no
native process:

```js
const [channel, requests, responses] = createNemoLanguageServer();
const reply = await channel.sendRequest({ jsonrpc: "2.0", id: 1, method: "initialize", params: { capabilities: {} } });
for (;;) {                                     // the server's own messages
    const message = await requests.getNextRequest();
    if (message === null) break;               // the server exited
    if (message.id !== undefined) await responses.sendResponse({ jsonrpc: "2.0", id: message.id, result: null });
    else handleNotification(message);          // textDocument/publishDiagnostics, ...
}
```

A document is what the editor sends in `textDocument/didOpen`; a
program's `@use` of another file resolves only if that file was opened
too, since the browser has no filesystem to read it from. The VS Code
extension in `nemo-vscode` runs it as its browser entry.

## Values

Lossless primitives cross bare: `string`, `boolean`, doubles as
`number`, integers as `number` within ±2⁵³ and `BigInt` beyond,
unlabeled tuples as `Array`s. Everything else is a plain tagged object:
`{kind: "iri", value}`, `{kind: "lang", text, language}`,
`{kind: "float", value}`, `{kind: "decimal", lexical}`,
`{kind: "datetime", lexical}`, `{kind: "date", lexical}`,
`{kind: "time", lexical}`, `{kind: "duration", lexical}` (any of the
three duration types), `{kind: "literal", lexical, datatype}`,
`{kind: "tuple", label, elements}`, `{kind: "map", label?, pairs}`,
`{kind: "null", id}`, its id the null's number as `_:0` writes it.
The same forms feed back in, except a null, which no term denotes,
with one JS-forced rule: a bare `number` with an integral value is an
*integer* term. An
integral double is written `{kind: "double", value}`, since JS cannot
tell `5` from `5.0`.

A cursor row is `{entryId, values, terms}`: its entry id, its values in
these forms, and the same values in term syntax, the text a results
table shows.

## Errors

`load`/`reason`/reads throw an `Error` whose message is the full
rendering (source snippets included) and whose `diagnostics` property
carries structured entries: `code`, `severity`, `message`, resolved
`labels` (`source`, `line`, `column`), `notes`, `hints`.
`NemoProgram.parse` never throws — it is total, reporting through
`program.diagnostics`; `engine.check(program)` collects everything
without loading data.

Running out of memory traps: the call throws a `RuntimeError`
(`unreachable`), or the worker's `error` event fires where nothing
catches it, and the module must not be used again. Stable Rust has no
hook to report it more precisely. `setPanicHook()` routes panics to the
console with their message.

## Building and testing

```
wasm-pack build --target web nemo-wasm     # or --target bundler / nodejs
wasm-pack test --node nemo-wasm            # the real test suite
```
