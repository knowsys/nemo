# nemo

The logical layer of the Nemo rule engine, and the crate to depend on to use Nemo as a library.
It implements the Nemo language (parser, rule model, program transformations), reasoning
(normalization, planning, semi-naive execution), and tracing, on top of the database layer
[nemo-physical](../nemo-physical/README.md) and the format adapters [nemo-io](../nemo-io/README.md).

The engine does no network IO itself: a method that needs remote data suspends and lists the
requests it needs. With the `http` feature, the `driver` module performs them.

Two runnable examples show the API:

```
cargo run -p nemo --example api     # build a program, run it, read the results
cargo run -p nemo --example embed   # data in from the host, results out in bounded memory
```

Nemo is also available as the `nmo` command line ([nemo-cli](../nemo-cli/README.md)) and through
bindings for Python ([nemo-python](../nemo-python/README.md)) and WebAssembly
([nemo-wasm](../nemo-wasm/README.md)).
