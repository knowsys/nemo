# Nemo for VS Code

Editing support for the Nemo rule language (`.rls`): syntax highlighting,
diagnostics as you type, and navigation over predicates.

## What it does

**Without a language server** — highlighting, comment toggling, bracket
matching and closing, indentation, and snippets. These come from the
grammar and the language configuration and need nothing built.

**With the language server** — everything else:

| | |
|---|---|
| Diagnostics | Parse and check errors and warnings as you type, with their codes (`N0202`, …) and the label the engine attaches |
| Hover | A predicate's columns, how many places derive it, how many read it, and the atom as written |
| Go to definition | Where a predicate gets its rows: the facts, the rule heads, and the `@import` that reads it |
| Find references | Everywhere a predicate is named, including directives |
| Highlight occurrences | The predicate under the cursor, written places distinguished from read ones |
| Outline | The file's statements, each named by what it is about |
| Rename | A predicate, everywhere it occurs, or an error, never a partial rename |
| Completion | Predicates in the file with their columns, the builtin catalogue, directives after `@`, the rule's own variables after `?`, aggregates after `#` |
| Format | Statements normalized to canonical spacing, leaving comments and blank lines exactly as written |

## Getting it running

The extension looks for the server in three places, in order:

1. `nemo.server.path`, if set.
2. A build in the open workspace: `target/release/nemo-language-server`,
   then `target/debug/`.
3. The `PATH`.

So in a checkout of the engine, building it is enough:

```
cargo build --release -p nemo-language-server
```

If it is not found, the extension says so once and highlighting keeps
working.

## Installing it

Package it into a `.vsix` and hand that to VS Code:

```
npm install
npx @vscode/vsce package
code --install-extension nemo-0.2.0.vsix --force
```

Packaging builds everything first (`vscode:prepublish`): the language
server compiled to WebAssembly (`npm run wasm`, which needs
[wasm-pack](https://rustwasm.github.io/wasm-pack/) and the
`wasm32-unknown-unknown` target), then both entries (`npm run compile`).

Then reload VS Code (**Developer: Reload Window**). Or install through the
UI instead: Extensions → the `…` menu → *Install from VSIX…*.

The `.vsix` carries the client's runtime dependency, so `node_modules`
must not be excluded from it — `.vscodeignore` prunes only what a
packaged extension does not run.

> An older `undefined_publisher.nemo` may already be installed from
> before. Two extensions claiming the `nemo` language will both try to
> handle `.rls`; uninstall the old one with
> `code --uninstall-extension undefined_publisher.nemo`.

## In a browser

VS Code for the web, and editors built on its API such as the Nemo web
interface, load the `browser` entry instead (`src/browser/`). There is
no process to start there: the client runs the language server of
`nemo-wasm` in a worker beside the extension host, and carries the
protocol over the worker's messages. Everything the server reports
comes the same; what it cannot do is read files the editor has not
opened, so a module's `@use` resolves only against open documents.

## Developing the extension

```
npm install
npm run wasm        # once, and after changing the engine
npm run compile
```

Then press <kbd>F5</kbd> to open a window with the extension loaded.
`npm run check-browser` runs the bundled server worker under Node and
checks that it responds to an editor.

### The grammar is generated

`syntaxes/nemo.tmLanguage.json` is written by `tools/grammar.py` from the
engine's own definitions — the builtin names come out of `Operator::NAMES`
rather than being copied. After changing that catalogue, re-run:

```
python tools/grammar.py
node tools/check-grammar.js
```

`cargo test -p nemo --test integration grammar` fails if the two disagree, so a
builtin cannot be added to the engine and quietly go unhighlighted.

`tools/check-grammar.js` runs the grammar over real Nemo source and
checks what it made of it. Its cases are the places the language is
unusual, and each of them is somewhere a highlighter would go wrong by
default:

- **`%` continues a name.** `inProg2(Hello%20World). % a comment` is one
  name and *then* a comment. A grammar that treats `%` as a comment
  opener unconditionally mis-colours the rest of the line.
- **`<` opens an IRI only sometimes** — when a `>` follows before any
  whitespace. `?x < 1` is a comparison; `<http://e.org/a>` is an IRI.
- **A number's `f`/`d` suffix is a suffix only before a non-name
  character.** `2.5f` is one number; `1fx` is the number `1` and the name
  `fx`.
- **Only five string escapes exist**, and an unknown one is *not* an
  error — `"(\w+)"` is ordinary content the regex engine will read.
- **`@` after a closing quote is a language tag**, not a directive.
- **A directive's words are keywords only where it reads them.** `date`
  is a type in `@declare p(date)`, a predicate in `@output date`, and a
  constant in `p(date)`; formats after `from`/`to`, option keys, and the
  connectives work the same way.

## Known limits

- Navigation is per file. A predicate derived in another file is not
  found from this one, and a module's `@use` is not followed.
- Formatting leaves a statement alone if its own text contains a `%`,
  because a comment inside it would not survive being reprinted, and a
  `%` in a string cannot be told from one that opens a comment without
  lexing. Comments between statements are always preserved.
