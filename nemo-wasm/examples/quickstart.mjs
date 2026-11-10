// The shortest path through the bindings: a program text in, rows out.
//
// Usage: `wasm-pack build --target nodejs nemo-wasm`,
// then `node nemo-wasm/examples/quickstart.mjs`.

import { NemoEngine, NemoProgram } from "../pkg/nemo_wasm.js";

// Parsing never throws. What did not parse is in `program.diagnostics`.
const program = NemoProgram.parse(
    `
    age(alice, 34) . age(bob, 17) . age(carol, 52) .
    adult(?person, ?age) :- age(?person, ?age), ?age >= 18 .
    `,
    "people.rls",
);
if (!program.ok) {
    throw new Error(program.diagnostics.map((diagnostic) => diagnostic.rendered).join("\n"));
}
console.log(program.predicates);

// The engine is synchronous. In a page, it belongs in a worker.
const engine = new NemoEngine();
engine.load(program);
engine.reason();

// A row is an array of values. A string, a boolean and a number are themselves,
// and every other value is a plain object with a `kind`: a name such as `alice` is
// `{kind: "iri", value: "alice"}`.
for (const [person, age] of engine.rows("adult")) {
    console.log(`${person.value} is an adult, aged ${age}`);
}

// A cursor reads the rows in pages. Each row has its values, the same values in term
// syntax, and the entry id by which the queries of the explanation view select it.
const cursor = engine.cursor("adult");
while (!cursor.done) {
    for (const row of engine.readChunk(cursor, 1)) {
        console.log(row.entryId, `adult(${row.terms.join(", ")}) .`);
    }
}

console.log(engine.predicates(), engine.outputPredicates(), engine.columnTypes("adult"));

// A failure throws an `Error`. Its message is the rendering that a terminal shows,
// and its `diagnostics` holds the same as data.
try {
    engine.load(program);
} catch (error) {
    const [diagnostic] = error.diagnostics;
    console.log(diagnostic.code, diagnostic.message, diagnostic.hints);
}
