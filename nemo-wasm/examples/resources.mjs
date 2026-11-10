// Data in and out without a file system: resources and rows from memory, exports as bytes.
//
// Usage: `wasm-pack build --target nodejs nemo-wasm`,
// then `node nemo-wasm/examples/resources.mjs`.

import { NemoEngine, NemoProgram } from "../pkg/nemo_wasm.js";

const program = NemoProgram.parse(
    `
    @import city from csv { resource = "cities.csv" } .
    @export large to csv .
    large(?name, ?people) :- city(?name, ?people), ?people > 1000000 .
    visited(?name) :- trip(?name, ?day), city(?name, ?people) .
    `,
    "cities.rls",
);

const engine = new NemoEngine();

// An `@import` of a resource with a registered name reads the registered content,
// a string or a `Uint8Array`, such as a file that the user picked.
engine.addResource("cities.csv", "Dresden,563000\nPrague,1357000\nVienna,1973000\n");

// Rows from JavaScript go in as arrays of values, before the load or after it.
// A bare word in a CSV file is a name, so the rows name the cities as names too.
// A JavaScript string is a string value, which no name equals, and the join below
// would find nothing for it.
const city = (name) => ({ kind: "iri", value: name });
engine.importRows("trip", [
    [city("Prague"), 1],
    [city("Vienna"), 2],
]);

engine.load(program);
engine.reason();
console.log(engine.rows("visited"));

// The program's exports are serialized into memory, one `Uint8Array` per file name.
const decoder = new TextDecoder();
for (const [name, bytes] of Object.entries(engine.exportTables())) {
    console.log(`${name}:\n${decoder.decode(bytes)}`);
}

// Any predicate can be serialized as CSV, exported or not.
console.log(decoder.decode(engine.exportPredicate("visited")));

// Rows given after the load are new facts, and the next run continues from them.
engine.importRows("trip", [[city("Dresden"), 3]]);
engine.reason();
console.log(engine.count("visited"), "cities visited");
