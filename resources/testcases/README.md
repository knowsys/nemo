# Testcases

End-to-end programs with their expected results, run by
`nemo/tests/testcases.rs`, one test per program:

    cargo test -p nemo --test testcases

A case is a program `name.rls` with its expected facts in `name.expect`.
A directory `name/` holds the files its exports must write. Data files sit
in the `sources/` directory of a topic.

The cases in `unchanged/` must pass. The cases in `changed/` and `new/`
describe behaviour this engine does not have yet and are ignored; run them
with `-- --include-ignored`.
