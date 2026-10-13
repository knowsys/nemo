# Testcases

End-to-end programs with their expected results, run by
`nemo/tests/testcases.rs`, one test per program:

    cargo test -p nemo --test testcases

A test case is a program `name.rls` with its expected facts in `name.expect`.
A directory `name/` holds the files its exports must write. Data files sit
in the `sources/` directory of a topic.