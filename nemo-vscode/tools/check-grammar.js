/**
 * Runs the grammar over Nemo source and checks what it made of it.
 *
 * A syntax highlighter that is subtly wrong is worse than none, and the
 * places it goes wrong are the places the language is unusual: a `%`
 * inside a name, a `<` that is an IRI only sometimes, a number suffix
 * that is only a suffix before a non-name character. Each of those is a
 * case here.
 *
 * Usage: node tools/check-grammar.js
 */

const fs = require("fs");
const path = require("path");
const oniguruma = require("vscode-oniguruma");
const textmate = require("vscode-textmate");

const HERE = __dirname;
const GRAMMAR = path.join(HERE, "..", "syntaxes", "nemo.tmLanguage.json");

/** The cases: source text, and what must be scoped how. */
const CASES = [
  {
    name: "a percent inside a name is part of it, and the next one is a comment",
    source: 'inProg2(Hello%20World). % comments still work',
    expect: [
      { text: "Hello%20World", not: "comment" },
      { text: "% comments still work", scope: "comment.line" },
    ],
  },
  {
    name: "a name may end in a percent without opening a comment",
    source: "foo% bar .",
    expect: [{ text: "foo%", not: "comment" }],
  },
  {
    name: "a percent after a number does open a comment",
    source: "p(1) . 1%foo",
    expect: [{ text: "%foo", scope: "comment.line" }],
  },
  {
    name: "an angle-bracketed run without whitespace is an IRI",
    source: "p(<http://example.org/a>) .",
    expect: [
      { text: "<http://example.org/a>", scope: "constant.other.iri" },
      { text: "<http://example.org/a>", scope: "variable.other.constant" },
    ],
  },
  {
    name: "a less-than with a space is an operator",
    source: "q(?x) :- p(?x), ?x < 1.0E0 .",
    expect: [
      { text: "<", scope: "keyword.operator.comparison" },
      { text: "1.0E0", scope: "constant.numeric.decimal" },
    ],
  },
  {
    name: "the value comparisons tokenize whole",
    source: "q(?x) :- p(?x), ?x == 1, ?x !== 2 .",
    expect: [
      { text: "==", scope: "keyword.operator.comparison" },
      { text: "!==", scope: "keyword.operator.comparison" },
    ],
  },
  {
    name: "number suffixes attach only before a non-name character",
    source: "p(2.5f, 9d, 1fx, 0x2f, 0b1011) .",
    expect: [
      { text: "2.5f", scope: "constant.numeric.decimal" },
      { text: "9d", scope: "constant.numeric.decimal" },
      { text: "1", scope: "constant.numeric.decimal" },
      { text: "0x2f", scope: "constant.numeric.hex" },
      { text: "0b1011", scope: "constant.numeric.binary" },
    ],
  },
  {
    name: "directives, variables and rules",
    source: "@import edge from csv { resource = \"e.csv\" } .\nreach(?x) :- edge(?x) .",
    expect: [
      { text: "@import", scope: "keyword.control.directive" },
      { text: "csv", scope: "support.type", not: "property-name" },
      { text: "resource", scope: "support.type.property-name" },
      { text: ":-", scope: "keyword.operator.rule" },
    ],
  },
  {
    name: "every variable marker",
    source: "h(!n, ?x) :- p(?x, _), ?x > $limit .",
    expect: [
      { text: "!n", scope: "variable.other.existential" },
      { text: "?x", scope: "variable.other" },
      { text: "_", scope: "variable.language.anonymous" },
      { text: "$limit", scope: "variable.parameter" },
    ],
  },
  {
    name: "builtins are told from predicates",
    source: "q(?r) :- p(?a, ?b), ?r = CONCAT(?a, ?b) .",
    expect: [
      { text: "CONCAT", scope: "keyword.other.builtin" },
      { text: "q", scope: "entity.name.function" },
    ],
  },
  {
    name: "a builtin is only a builtin when it is called",
    source: "concat(1) . p(?x) :- concat(?x) .",
    expect: [{ text: "concat", scope: "keyword.other.builtin" }],
  },
  {
    name: "strings keep their escapes, and an unknown escape is not an error",
    source: 'p("say \\"hi\\"\\n", "(\\w+) (\\w+)") .',
    expect: [
      { text: '\\"', scope: "constant.character.escape" },
      { text: "\\n", scope: "constant.character.escape" },
      // `\w` is not one of the five escapes, and not an error either:
      // it stays ordinary string content for the regex engine to read.
      { text: "(\\w+) (\\w+)", scope: "string.quoted.double" },
      { text: "(\\w+) (\\w+)", not: "constant.character.escape" },
      { text: "(\\w+) (\\w+)", not: "invalid" },
    ],
  },
  {
    name: "a format string interpolates",
    source: 'r(?R) :- s(?A), ?R = f"len={STRLEN(?A)} of {{?A}}" .',
    expect: [
      { text: "{{", scope: "constant.character.escape" },
      { text: "STRLEN", scope: "keyword.other.builtin" },
      { text: "len=", scope: "string.quoted.double" },
    ],
  },
  {
    name: "a language tag is not a directive",
    source: 'p("hello"@en) .',
    expect: [{ text: "en", scope: "entity.name.tag.language" }],
  },
  {
    name: "module names and prefixed names",
    source: "@prefix ex: <http://e.org/> .\nanswer(?x) :- t::reach(?x), ex:p(?x) .",
    expect: [
      { text: "t", scope: "entity.name.namespace" },
      { text: "::", scope: "punctuation.separator.namespace" },
      { text: "ex", scope: "entity.name.namespace" },
    ],
  },
  {
    name: "aggregates and attributes",
    source: '#[name("total")]\nt(#sum(?v)) :- p(?v) .',
    expect: [
      { text: "#sum", scope: "keyword.other.aggregate" },
      { text: "name", scope: "entity.other.attribute-name" },
    ],
  },
  {
    name: "booleans are constants",
    source: "p(true, false) .",
    expect: [{ text: "true", scope: "constant.language.boolean" }],
  },
  {
    name: "a boolean's name applied to arguments is a predicate",
    source: "true(1) .",
    expect: [{ text: "true", scope: "entity.name.function", not: "constant.language" }],
  },
  {
    name: "type, format, and option words are names outside directives",
    source: "p(date, int, csv, resource, format) :- q(string) .",
    expect: [
      { text: "date", scope: "constant.other", not: "support.type" },
      { text: "int", not: "support.type" },
      { text: "csv", not: "support.type" },
      { text: "resource", not: "support.type" },
      { text: "format", not: "support.type" },
      { text: "string", not: "support.type" },
    ],
  },
  {
    name: "the connectives are names outside their directives",
    source: "edge(from, to) :- link(where, as) .",
    expect: [
      { text: "from", not: "keyword" },
      { text: "to", not: "keyword" },
      { text: "where", not: "keyword" },
      { text: "as", not: "keyword" },
    ],
  },
  {
    name: "a directive's predicate may be named like a type",
    source: "@output date .",
    expect: [{ text: "date", scope: "entity.name.function", not: "support.type" }],
  },
  {
    name: "an import names its predicate, then its format",
    source: '@import date from csv { resource = "d.csv" } .',
    expect: [
      { text: "date", scope: "entity.name.function", not: "support.type" },
      { text: "csv", scope: "support.type" },
    ],
  },
  {
    name: "an option block's keys are options and format's value is types",
    source: '@export p to dsv { delimiter = ";", format = (int, term), limit = 10 } .',
    expect: [
      { text: "to", scope: "keyword.control.connective" },
      { text: "dsv", scope: "support.type", not: "property-name" },
      { text: "delimiter", scope: "support.type.property-name" },
      { text: "format", scope: "support.type.property-name" },
      { text: "limit", scope: "support.type.property-name" },
      { text: "int", scope: "support.type", not: "property-name" },
      { text: "term", scope: "support.type", not: "property-name" },
    ],
  },
  {
    name: "a single column format is a type",
    source: '@import p from csv { format = date } .',
    expect: [{ text: "date", scope: "support.type", not: "property-name" }],
  },
  {
    name: "a multi-line option block keeps its keys, and a nested map holds terms",
    source:
      '@import v from turtle {\n    http_headers = { "X-A" = "b", date = 1 },\n    resource = "v.ttl"\n} .',
    expect: [
      { text: "turtle", scope: "support.type" },
      { text: "http_headers", scope: "support.type.property-name" },
      { text: "resource", scope: "support.type.property-name" },
      { text: "date", not: "support.type" },
    ],
  },
  {
    name: "an import's where clause holds atoms, with names as terms",
    source: "@import q(?x) from sparql { endpoint = <http://e.org/> } where b(?x, to) .",
    expect: [
      { text: "sparql", scope: "support.type" },
      { text: "where", scope: "keyword.control.connective" },
      { text: "b", scope: "entity.name.function" },
      { text: "to", not: "keyword" },
    ],
  },
  {
    name: "declarations read types, declared types, and column names",
    source: "@declare event(when: date, (int, string), point) .",
    expect: [
      { text: "event", scope: "entity.name.function" },
      { text: "when", scope: "variable.parameter" },
      { text: "date", scope: "support.type" },
      { text: "int", scope: "support.type" },
      { text: "point", scope: "entity.name.type" },
    ],
  },
  {
    name: "a type declaration names its type",
    source: "@type point(x: double, y: double) .",
    expect: [
      { text: "point", scope: "entity.name.type" },
      { text: "double", scope: "support.type" },
    ],
  },
  {
    name: "an extern reads types on both sides of its arrow",
    source: "@extern distance(point, point) -> double .",
    expect: [
      { text: "distance", scope: "entity.name.function" },
      { text: "->", scope: "keyword.operator.arrow" },
      { text: "double", scope: "support.type" },
    ],
  },
  {
    name: "a parameter's type is a type and its value a term",
    source: "@parameter $day: date = date .",
    expect: [
      { text: "$day", scope: "variable.parameter" },
      { text: "date", scope: "support.type" },
      { text: "date", scope: "constant.other" },
    ],
  },
  {
    name: "a definition holds terms",
    source: "@define stamp(?x) = pair(?x, date) .",
    expect: [
      { text: "stamp", scope: "entity.name.function" },
      { text: "date", scope: "constant.other", not: "support.type" },
    ],
  },
  {
    name: "a triple-quoted string may hold quotes",
    source: 'q("""say "hi" twice""") .',
    expect: [{ text: 'say "hi" twice', scope: "string.quoted.triple" }],
  },
  {
    name: "bare, prefixed, and qualified constants are coloured",
    source: "constants(alice, foaf:Person, t::area) .",
    expect: [
      { text: "constants", scope: "entity.name.function" },
      { text: "alice", scope: "constant.other" },
      { text: "alice", scope: "variable.other.constant" },
      { text: "Person", scope: "variable.other.constant" },
      { text: "area", scope: "variable.other.constant" },
    ],
  },
  {
    name: "directive predicates are predicates",
    source:
      '@output result .\n@input edge(source: any, any) .\n@import person from csv { resource = "p.csv" } .',
    expect: [
      { text: "@output", scope: "keyword.control.directive" },
      { text: "@input", scope: "keyword.control.directive" },
      { text: "result", scope: "entity.name.function" },
      { text: "edge", scope: "entity.name.function" },
      { text: "source", scope: "variable.parameter" },
      { text: "person", scope: "entity.name.function" },
      { text: "from", scope: "keyword.control.connective" },
    ],
  },
  {
    name: "a use binds predicates and names its alias as a namespace",
    source: "@use lib::tc { edge = link, $limit = 3 } as t .",
    expect: [
      { text: "@use", scope: "keyword.control.directive" },
      { text: "lib", scope: "entity.name.namespace" },
      { text: "tc", scope: "entity.name.namespace" },
      { text: "edge", scope: "entity.name.function" },
      { text: "link", scope: "entity.name.function" },
      { text: "$limit", scope: "variable.parameter" },
      { text: "as", scope: "keyword.control.connective" },
      { text: "t", scope: "entity.name.namespace" },
    ],
  },
  {
    name: "a module name climbs with super, and its parts may hold hyphens",
    source: "@use super::graph-tools::my-closure {} as my-c .\nout(?x) :- my-c::reach(?x) .",
    expect: [
      { text: "super", scope: "keyword.other.super" },
      { text: "graph-tools", scope: "entity.name.namespace" },
      { text: "my-closure", scope: "entity.name.namespace" },
      { text: "my-c", scope: "entity.name.namespace" },
      { text: "reach", scope: "entity.name.function" },
    ],
  },
  {
    name: "a hyphen joins the parts of a name, but not of a variable",
    source: "stock-item(item-1, ex:spare-part) :- p(?count), ?left = ?count-1 .",
    expect: [
      { text: "stock-item", scope: "entity.name.function" },
      { text: "item-1", scope: "constant.other" },
      { text: "spare-part", scope: "constant.other" },
      { text: "?count", scope: "variable.other" },
      { text: "1", scope: "constant.numeric" },
    ],
  },
  {
    name: "a hyphen before a percent is a minus, and the percent a comment",
    source: "p(a) . a-% note",
    expect: [{ text: "% note", scope: "comment.line" }],
  },
  {
    name: "named arguments wear their labels",
    source: 'person(name: "alice", age: 34) .',
    expect: [
      { text: "name", scope: "variable.parameter" },
      { text: "age", scope: "variable.parameter" },
    ],
  },
  {
    name: "a declared prefix is a namespace",
    source: "@prefix ex: <http://e.org/> .",
    expect: [
      { text: "@prefix", scope: "keyword.control.directive" },
      { text: "ex", scope: "entity.name.namespace" },
    ],
  },
  {
    name: "the aggregate scope word is a constant where no variable follows",
    source: "t(?g, #sum(?v)) :- d(?g, ?v), p(per) .",
    expect: [{ text: "per", not: "keyword" }],
  },
  {
    name: "per before its scope variable is a keyword",
    source: "t(?g, #sum(?v per ?w)) :- d(?g, ?v, ?w) .",
    expect: [{ text: "per", scope: "keyword.control.connective" }],
  },
];

const wasm = fs.readFileSync(
  path.join(HERE, "..", "node_modules", "vscode-oniguruma", "release", "onig.wasm")
);

/** Every token of a source text, with the scopes it carries. */
async function tokenize(source) {
  await oniguruma.loadWASM(wasm.buffer);
  const registry = new textmate.Registry({
    onigLib: Promise.resolve({
      createOnigScanner: (patterns) => new oniguruma.OnigScanner(patterns),
      createOnigString: (text) => new oniguruma.OnigString(text),
    }),
    loadGrammar: async (scope) =>
      scope === "source.nemo"
        ? textmate.parseRawGrammar(fs.readFileSync(GRAMMAR).toString(), GRAMMAR)
        : null,
  });
  const grammar = await registry.loadGrammar("source.nemo");
  if (!grammar) {
    throw new Error("the grammar did not load");
  }

  const tokens = [];
  let stack = textmate.INITIAL;
  for (const line of source.split("\n")) {
    const parsed = grammar.tokenizeLine(line, stack);
    for (const token of parsed.tokens) {
      tokens.push({
        text: line.substring(token.startIndex, token.endIndex),
        scopes: token.scopes,
      });
    }
    stack = parsed.ruleStack;
  }
  return tokens;
}

async function main() {
  let failures = 0;
  for (const test of CASES) {
    const tokens = await tokenize(test.source);
    for (const wanted of test.expect) {
      // A token may be split by the engine; join the run that forms it.
      const matching = tokens.filter((token) => wanted.text.includes(token.text.trim()) && token.text.trim());
      const exact = tokens.filter((token) => token.text === wanted.text);
      const found = exact.length ? exact : matching;
      if (!found.length) {
        console.error(`FAIL ${test.name}: no token matches ${JSON.stringify(wanted.text)}`);
        console.error(`  tokens: ${tokens.map((t) => JSON.stringify(t.text)).join(" ")}`);
        failures += 1;
        continue;
      }
      const scopes = found.flatMap((token) => token.scopes).join(" ");
      if (wanted.scope && !scopes.includes(wanted.scope)) {
        console.error(
          `FAIL ${test.name}: ${JSON.stringify(wanted.text)} is ${scopes}, wanted ${wanted.scope}`
        );
        failures += 1;
      }
      if (wanted.not && scopes.includes(wanted.not)) {
        console.error(
          `FAIL ${test.name}: ${JSON.stringify(wanted.text)} is ${scopes}, which must not be ${wanted.not}`
        );
        failures += 1;
      }
    }
  }
  if (failures) {
    console.error(`\n${failures} grammar checks failed`);
    process.exit(1);
  }
  console.log(`all ${CASES.length} grammar cases pass`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
