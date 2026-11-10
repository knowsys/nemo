// Remote imports: the async methods fetch them, or the page fetches them itself.
//
// Usage: `wasm-pack build --target nodejs nemo-wasm`,
// then `node nemo-wasm/examples/fetch.mjs`.
// The example reaches no network: it replaces the global `fetch` by one that serves from memory.

import { NemoEngine, NemoProgram } from "../pkg/nemo_wasm.js";

const program = NemoProgram.parse(
    `
    @import city from csv { resource = "https://example.org/cities.csv" } .
    large(?name) :- city(?name, ?people), ?people > 1000000 .
    `,
    "cities.rls",
);

const served = new Map([
    ["https://example.org/cities.csv", "Dresden,563000\nPrague,1357000\nVienna,1973000\n"],
]);
globalThis.fetch = async (url) => {
    console.log("fetching", url);
    const body = served.get(url);
    return body === undefined ? new Response("", { status: 404 }) : new Response(body);
};

// The synchronous `load` does not fetch. It throws, and changes nothing.
try {
    new NemoEngine().load(program);
} catch (error) {
    console.log("load:", error.diagnostics[0].code);
}

// `loadAsync` sends the requests through the global `fetch` and resolves once the
// program is loaded.
const fetching = new NemoEngine();
await fetching.loadAsync(program);
fetching.reason();
console.log("loadAsync:", fetching.count("large"), "large cities");

// `startLoad` returns the requests instead, for a page that fetches in its own way,
// such as through a proxy or with credentials. The load is suspended, with nothing stored,
// until `resume` has a response to each request.
const engine = new NemoEngine();
let pending = engine.startLoad(program);
while (pending !== null) {
    const responses = pending.map(({ id, url }) => {
        const body = served.get(url);
        return body === undefined ? { id, status: 404, message: "not served" } : { id, body };
    });
    pending = engine.resume(responses);
}
engine.reason();
console.log("startLoad and resume:", engine.count("large"), "large cities");
