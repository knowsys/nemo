# nemo-python - Python bindings for nemo

This crate provide python bindings for the `nemo` crate.

> **Note**
> These bindings are currently in an experimental state and likely subject to change.

## Building

You will need to install [maturin](https://www.maturin.rs/). To try these bindings in a python virtualenv simply run:

```
maturin develop
```

## Example usage
```python
from nmo_python import load_string, NemoEngine, NemoOutputManager

rules="""
data(1,2) .
data(hi,42) .
data(hello,world) .

calculated(?x, !v) :- data(?y, ?x) .
"""

engine = NemoEngine(load_string(rules))
engine.reason()

print(list(engine.result("calculated")))

output_manager = NemoOutputManager("results", gzip=True)
engine.write_result("calculated", output_manager)
```

## Known limitations

Currently `any` values are not correctly transformed into python types. This will be fixed once a missing feature in nemo (knowsys/nemo#245) is implemented.