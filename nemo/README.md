# nemo

The logical layer of the Nemo rule engine, and the crate to depend on to use Nemo as a library.
It implements the Nemo language (parser, rule model, program transformations), reasoning
(normalization, planning, semi-naive execution), and tracing, on top of the database layer
[nemo-physical](../nemo-physical/README.md) and the format adapters [nemo-io](../nemo-io/README.md).

A method that needs remote data sends its requests through the engine's client: the HTTP client
of the `http` feature by default, or one set with `set_client`. The `start_*` methods send
nothing: they return the requests, and `resume` takes the responses.

Runnable examples show the API, each as `cargo run -p nemo --example <name>`:

```
quickstart    # a program text in, typed rows out
construct     # one program built three ways: from text, with macros, with builders
diagnostics   # check a program, and read what the check reports
trace         # why a fact holds
incremental   # new rows, new statements and a new program after the fixpoint
modules       # one module, used twice
parameters    # give the parameters of a program their values
remote        # remote imports through a client set on the engine, or through `resume`
cancel        # stop a run from another thread, and continue it
cursor        # read a large predicate in bounded memory, and export into memory
```

Nemo is also available as the `nmo` command line ([nemo-cli](../nemo-cli/README.md)) and through
bindings for Python ([nemo-python](../nemo-python/README.md)) and WebAssembly
([nemo-wasm](../nemo-wasm/README.md)).
