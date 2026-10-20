# nmo — the Nemo command line

`nmo` runs rule programs: it reads them, loads what they import, reasons
to the fixpoint, writes what they export, and reports what happened.
Everything it says about a program — parse errors, unsafe rules, dropped
rows — is a diagnostic with a code, rendered against the source it came
from; `nmo --explain N0204` tells what a code means.

```
nmo program.rls                  # run; exports land in results/
nmo program.rls -P               # print the outputs as facts instead
nmo program.rls -e all -D out/   # write every predicate as CSV into out/
nmo program.rls --report timing  # where the time went, every predicate's size
nmo program.rls --trace "reach(a, c)"
```

## Usage

```
nmo [OPTIONS] <PROGRAM>...
```

Several programs are read as one, in order. Imports resolve against the
first program's directory unless `-I` says otherwise. An import whose
`resource` is `""` reads standard input; an export whose `resource` is
`""` writes standard output (pair it with `-q` to keep the terminal to
the data).

### Results

| option | meaning |
|---|---|
| `-e, --export <MODE>` | which predicates to write: `keep` (the program's `@export` directives, the default), `idb` (every derived predicate), `edb` (every given one), `all`, `none`. A predicate the program declares no export for is written as CSV. |
| `-P, --print[=<MODE>]` | print the results to standard output as facts, one per line in term syntax, and write no files. Bare, `--export` picks the predicates and `keep` prints the declared `@output` predicates too; `idb`, `edb`, or `all` — written `-P=idb` — pick by what feeds the predicates. |
| `-D, --export-dir <DIR>` | where exports are written (`results`). |
| `-o, --overwrite` | replace export files, and the HTML report, that already exist (alias `--overwrite-results`). |
| `-g, --gzip` | compress every export with gzip. |
| `-I, --import-dir <DIR>` | the directory relative imports resolve against, by default the directory of the program file each import is written in. |
| `-p, --param <NAME=VALUE>` | give a declared `@parameter` its value; repeatable. |
| `--module-root <NAME=DIR>` | declare a module root: `@use NAME::tc` names the module `tc.rls` in `DIR`; repeatable. |
| `--threads <N>` | worker threads for the parallel phases. |

### Reporting

Everything a run says about itself goes to the error stream; standard
output is the data's.

| option | meaning |
|---|---|
| `--report <LEVEL>` | `none`; `summary` (the default; also `short`); `auto` (the summary unless the results were printed); `timing` (stage times, where the reasoning time went, every predicate's size — measured without recording, what benchmarks read; also `time`, `mem`); `full` (`timing` plus the rules by time; also `all`); `json` (the whole run as one document on standard output, which nothing else may then write to: not `--print`, not a trace without `--trace-output`, not an export to `""`; diagnostics still go to the error stream); `html` (the summary, plus a visual `report.html` in the export directory, which no export may write to). |
| `-q, --quiet` | warnings and errors only, and log errors only. |
| `--no-warnings` | show no warnings, and exit 0 where a warned run exits 3. |
| `-v, --verbose` | log more: `-v` what the run does, `-vv` the engine's decisions, `-vvv` everything including the telemetry stream. |
| `--log <FILTER>` | log by filter directives, as `debug` or `nemo::io=debug,warn`; `NMO_LOG` holds the default. |
| `--events <FILE>` | write the telemetry stream as JSON lines. |
| `--dashboard[=<PORT>]` | serve a live dashboard of the run (port 8280, or `--dashboard=0` for a free one) until interrupted. |
| `--explain <CODE>` | print what a diagnostic code means, and do nothing else. |

### Tracing

A trace explains why a fact holds after reasoning: a proof tree down to
the asserted and imported facts.

| option | meaning |
|---|---|
| `--trace <FACTS>` | the facts to trace, `;`-separated, as `--trace "reach(a, b);reach(a, c)"`. |
| `--trace-input <FILE>` | read facts to trace from a file (`;`-separated); repeatable. |
| `--trace-all` | trace every fact of every derived predicate, streaming. |
| `--trace-tree <FILE\|JSON>` | answer the explanation view's `treeForTable` query — the facts it selects, their derivation expanded by the engine — read from the file, or given inline as JSON text. It cannot be combined with the other trace flags, which would write the same output. |
| `--trace-node <FILE\|JSON>` | answer the explanation view's `tableEntriesForTreeNodes` query — the facts of every node of the tree it shapes — given and answered as `--trace-tree`. |
| `--trace-output <FILE>` | write JSON instead of printing: the deduplicated inference list for `--trace`, one inference per line for `--trace-all`, the response for `--trace-tree` and `--trace-node`. |

Any trace keeps every import whole (see below).

## Exit codes

| code | meaning |
|---|---|
| 0 | the run completed cleanly |
| 1 | the run failed; the error was reported |
| 2 | the command line could not be parsed |
| 3 | the run completed, with warnings (`--no-warnings` makes this 0) |
| 101 | the run hit a bug in Nemo; the internal error was reported |

## What the loader leaves out

By default the compiler pushes what the rules read into the imports:
columns no rule reads are skipped, rows no rule can use are excluded.
The program observes no difference, but a run that looks at tables
directly — `-e all`, `-e edb`, `--print=all`, `--print=edb`, and every
trace flag — keeps every import whole.
