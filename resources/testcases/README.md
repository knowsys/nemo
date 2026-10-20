# Testcases

End-to-end programs with their expected results, run by
`nemo/tests/testcases.rs` — one test per program:

    cargo test -p nemo --test testcases
    cargo test -p nemo --test testcases -- aggregates::

## Layout

One directory per topic: `rules` (joins, recursion, heads), `conditions`
(filters, comparisons and assignments), `arithmetic` (how the operators
compute: promotion, precision, overflow, the operators on dates and
durations), `negation`, `existential`, `aggregates`, `builtins`, `values`
(literals and their identity), `syntax`, `io` (`dsv`, `rdf`, `json`),
`programs` (larger programs), `structures`, `types` and `modules`.

## A case

A case is a program `name.rls` with its expected results in `name.expect`:
the facts in term syntax, one per line. They are compared as values, not as
text, so any spelling of a value fits. Checked are

- the predicates the program declares as `@output`, or every predicate a
  rule derives into where it declares none,
- the predicates it exports,
- and every predicate the expectation names — a predicate that only reads
  facts or an import is checked by listing it there.

A checked predicate the expectation does not list must be empty. Nulls
(`_:0`, `_:1`, …) match up to one renaming across the whole case.

A program must load and reason without an error; warnings are allowed. A
case that tests exports has a directory `name/` holding the files the
exports must write; these are compared line by line in any order.

Data files and modules sit in the `sources/` directory of the topic; it is
not searched for cases.

## Writing a case

Write the program, then let the harness write its expectation:

    NEMO_BLESS=1 cargo test -p nemo --test testcases -- <name>

and check every line of the written file by hand — the expectation is the
specification, not a record of what the engine happens to do. To have a
predicate checked that only reads facts or an import, list one fact of it
in `name.expect` before blessing.
