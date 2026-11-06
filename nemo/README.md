# nemo

The logical layer of the Nemo rule engine, and the crate to depend on to use Nemo as a library.
It implements the Nemo language (parser, rule model, program transformations), reasoning
(normalization, planning, semi-naive execution), and tracing, on top of the database layer
[nemo-physical](../nemo-physical/README.md) and the format adapters [nemo-io](../nemo-io/README.md).

A method that needs remote data sends its requests through the engine's client: the HTTP client
of the `http` feature by default, or one set with `set_client`. The `start_*` methods send
nothing; they return the requests, and `resume` takes the responses.

## SPARQL imports

The `sparql` stage rewrites rules over `sparql` imports so that each endpoint is asked only for
what the rules need: atoms merge into one query, constants and conditions go into it, and the
values the rules derive go along as `VALUES` bindings. A rewritten program derives what the
program as written derives, under assumptions about the endpoint that common stores meet:

- **Values are held in their canonical terms.** Nemo reads `"3.0"^^xsd:decimal`,
  `"03"^^xsd:int` and `3` as one integer and sends it as `"3"^^xsd:integer`, and also as
  `"3.0"^^xsd:decimal`, as Wikidata holds its quantities; doubles go out as `"2.5E0"`,
  language tags in lower case. The endpoint compares terms. A value held in another datatype
  or spelling is not found through a binding, a constant or a join between merged atoms,
  though a plain import reads it, and a negated atom does not exclude it; a store comparing
  more loosely than Nemo may match more. A bound import never holds a solution its bindings
  do not match as Nemo compares values.
- **A blank node keeps its label.** A label names one null in every response of an endpoint,
  as stores labelling nodes by internal ids give them (Jena's TSV results, Virtuoso, Oxigraph,
  QLever), though SPARQL only promises a label within one result. No query can name a
  blank node, so
  binding rows holding one fetch nothing, which reasoning reports (N0338).
- **A result is one fixed set.** Queries with `LIMIT`, `OFFSET`, `SAMPLE` or a
  nondeterministic function are fetched once, as written. A query that may leave a projected
  variable unbound, a grouping, and a subquery are not bound.
- **A response is complete.** A response the endpoint cut short, by a trace in its body, a last
  row cut off, or Virtuoso's status 206, `X-SQL-State` or `X-SPARQL-MaxRows`, fails its import
  (or leaves it empty with a warning under `on_error = warn`); a bound import splits the
  request first.

The module documentation of `engine::transform::sparql_rewrite` gives the details.

Two runnable examples show the API:

```
cargo run -p nemo --example api     # build a program, run it, read the results
cargo run -p nemo --example embed   # data in from the host, results out in bounded memory
```

Nemo is also available as the `nmo` command line ([nemo-cli](../nemo-cli/README.md)) and through
bindings for Python ([nemo-python](../nemo-python/README.md)) and WebAssembly
([nemo-wasm](../nemo-wasm/README.md)).
