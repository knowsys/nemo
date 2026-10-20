# nmo — Python bindings for the Nemo rule engine

```python
import nmo

program = nmo.Program.parse("""
    parent(alice, bob) . parent(bob, carol) .
    ancestor(?x, ?y) :- parent(?x, ?y) .
    ancestor(?x, ?z) :- parent(?x, ?y), ancestor(?y, ?z) .
""")
assert program.ok

engine = nmo.Engine()
engine.import_rows("parent", [(nmo.Iri("carol"), nmo.Iri("dave"))])  # rows from Python
engine.load(program)
engine.reason()            # GIL released; Ctrl-C cancels at a rule boundary

engine.rows("ancestor")                 # list of tuples of typed values
for row in engine.cursor("ancestor"):    # bounded-memory paging
    ...
engine.to_arrow("ancestor")             # pyarrow.Table (also: arrow_stream,
                                        # to_pandas)
```

## Values

One conversion table, both directions, with the rule that **Python
equality mirrors term identity**: `str`, `bool`, `int`, `float`
(doubles), `decimal.Decimal`, `datetime.datetime`, `datetime.date`,
`datetime.time`, `datetime.timedelta` (dayTimeDurations), `tuple`, and
`dict` cross as themselves, as far as the Python type holds the value; IRIs, language-tagged strings, 32-bit floats,
typed literals, labeled structures, and nulls cross as the small
wrapper classes `Iri`, `LanguageString`, `Float`, `Literal`, `Labeled`,
`Null`. `Iri("a") != "a"` — the engine treats them as distinct terms,
so the binding does too. `str()` of any wrapper is canonical Nemo term
syntax.

## Errors

A failure of the engine raises `NemoError` or a subclass, a value that
does not convert raises `ValueError` or `TypeError`, and a panic, a bug
in Nemo, raises pyo3's `PanicException`. Parse/load/type failures
raise `DiagnosticsError`, whose `diagnostics` carry structured entries
(`code`, `severity`, `message`, resolved `labels`, `notes`, `hints`)
and whose message is the full terminal-style rendering with quoted
source snippets. `Program.parse` never raises — it is total, and
reports through `program.diagnostics`.

## Data in, data out

- `engine.import_rows(predicate, rows)` — rows from Python join the
  program as facts (before `load`, so they pass the same checks and type
  analysis).
- `engine.add_resource(name, text_or_bytes)` — an `@import` whose
  resource matches reads these bytes instead of the filesystem.
- `engine.add_module(name, text)` — a `@use` of the module `name`
  (as in `lib::tc`) reads this text instead of its file;
  `engine.add_module_root(name, dir)` makes `@use name::tc` read
  `tc.rls` in `dir`.
- A relative `@import` resource resolves beside the program file
  (`Program.parse_file`), or beside the working directory for parsed
  text; `engine.set_import_directory(dir)` resolves every one against
  `dir` instead.
- Loading stores only the columns and rows of an import its rules
  read, and a trace or a query over the rest raises; call
  `engine.retain_imports()` before `load` to keep every import whole.
- Remote imports (HTTP and SPARQL) are fetched by `load` and `reason`,
  which retry rate limits (429) and overloaded servers (502/503/504);
  Ctrl-C cancels a call while it waits. A resource registered with
  `add_resource` is read in place of the network.
- `engine.rows` / `engine.cursor` — typed values; `engine.export(dir)` —
  the program's own export directives.
- `engine.arrow_stream` — the Arrow C stream interface
  (`__arrow_c_stream__`), consumed by polars/duckdb/pyarrow directly;
  uniform integer/double/boolean columns are native arrays, everything
  else is text (the *dataframe projection*; the typed API keeps
  exactness).

## Building

```
pip install maturin
maturin develop --release    # in nemo-python/, inside a virtualenv
pytest tests/
```
