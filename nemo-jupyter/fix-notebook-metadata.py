#!/usr/bin/env python
"""Remove stale ``nbconvert_exporter`` metadata from Nemo notebooks.

Notebooks created with early versions of the Nemo kernel carry
``language_info.nbconvert_exporter = "not.convert.None"`` in their
metadata, which breaks "Export as Executable Script" (nbconvert treats
the value as an exporter name and fails). This removes that key.

Usage:
    .venv/bin/python nemo-jupyter/fix-notebook-metadata.py NOTEBOOK.ipynb [...]
"""

from __future__ import annotations

import sys
from pathlib import Path

import nbformat


def fix(path: Path) -> bool:
    nb = nbformat.read(path, as_version=4)
    lang = nb.metadata.get("language_info", {})
    if "nbconvert_exporter" not in lang:
        return False
    value = lang.pop("nbconvert_exporter")
    nbformat.write(nb, path)
    print(f"fixed {path}: removed language_info.nbconvert_exporter = {value!r}")
    return True


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__.strip())
        return 1
    changed = 0
    for arg in sys.argv[1:]:
        if fix(Path(arg)):
            changed += 1
    print(f"{changed} notebook(s) updated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
