# nemo-python - Python bindings for nemo

This crate provide python bindings for the `nemo` crate.

> **Note**
> These bindings are currently in an experimental state and subject to change.

## Building

You will need to install [maturin](https://www.maturin.rs/). To try these bindings in a python virtualenv simply run:

```
maturin develop
```

### Build using **nix**

If you want to run a python session with with the `nmo_python` wheel installed, you can run the following command in the top-level directory of this repository:

```
nix run .#python
```

## Example usage
```python
from nmo_python import load_string, NemoEngine, NemoOutputManager

rules="""
data(1,2) .
data(hi,42) .
data(hello,world) .

calculated(?x, !v) :- data(?y, ?x) .
@export calculated :- csv {}.
"""

engine = NemoEngine(load_string(rules))
engine.reason()

print(list(engine.result("calculated")))

output_manager = NemoOutputManager("results", gzip=True)
engine.write_result("calculated", output_manager)
```
