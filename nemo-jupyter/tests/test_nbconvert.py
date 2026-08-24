"""Unit tests for the Nemo nbconvert exporter (magic stripping, .rls export).

Run: .venv/bin/python -m pytest nemo-jupyter/tests/test_nbconvert.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import nbformat as nbf
import pytest

from nemo_jupyter.nbconvert import NemoScriptExporter
from nemo_jupyter.nemo_source import MAGIC_NAMES, clean_nemo_source


def test_clean_keeps_nemo_code_and_comments():
    source = "% a comment\nparent(ada, bob) .\nancestor(?x, ?y) :- parent(?x, ?y) ."
    assert clean_nemo_source(source) == source


def test_clean_strips_magics():
    source = "parent(ada, bob) .\n!trace ancestor(ada, bob)\n!program\n!reset"
    assert clean_nemo_source(source) == "parent(ada, bob) ."


def test_clean_drops_standalone_marker_keeps_program():
    source = "!standalone\niso(9, 9) .\n@export iso :- csv {}."
    cleaned = clean_nemo_source(source)
    assert "!standalone" not in cleaned
    assert "iso(9, 9) ." in cleaned
    assert "@export iso :- csv {}." in cleaned


def test_clean_inlines_load(tmp_path):
    data = tmp_path / "facts.rls"
    data.write_text("extra(7, 8) .")
    source = "!load facts.rls\nq(?x, ?y) :- extra(?x, ?y) ."
    cleaned = clean_nemo_source(source, base_dir=str(tmp_path))
    assert "extra(7, 8) ." in cleaned
    assert "!load" not in cleaned


def test_clean_keeps_existential_lines():
    # lines starting with ! that are not magics must survive
    source = "p(?x, !y) :- q(?x) .\n    !r(?x) :- s(?x) ."
    cleaned = clean_nemo_source(source)
    assert "p(?x, !y) :- q(?x) ." in cleaned
    assert "!r(?x) :- s(?x) ." in cleaned


def test_magic_names_shared_with_kernel():
    from nemo_jupyter.kernel import _MAGICS

    assert set(MAGIC_NAMES) == set(_MAGICS)


def _notebook(cells):
    nb = nbf.v4.new_notebook()
    nb.metadata["kernelspec"] = {"display_name": "Nemo", "language": "nemo", "name": "nemo"}
    nb.metadata["language_info"] = {"name": "nemo", "mimetype": "text/x-nemo", "file_extension": ".rls"}
    nb.cells = [nbf.v4.new_code_cell(c) for c in cells]
    return nb


def test_exporter_produces_rls():
    nb = _notebook([
        "parent(ada, bob) .",
        "ancestor(?x, ?y) :- parent(?x, ?y) .\n@export ancestor :- csv {}.",
        "!trace ancestor(ada, bob)",
    ])
    exporter = NemoScriptExporter()
    assert exporter.file_extension == ".rls"
    output, resources = exporter.from_notebook_node(nb)
    assert "parent(ada, bob) ." in output
    assert "ancestor(?x, ?y) :- parent(?x, ?y) ." in output
    assert "@export ancestor :- csv {}." in output
    assert "!trace" not in output
    assert resources["output_extension"] == ".rls"


def test_exporter_skips_markdown():
    nb = nbf.v4.new_notebook()
    nb.cells = [
        nbf.v4.new_markdown_cell("# heading\nnot nemo"),
        nbf.v4.new_code_cell("a(1) ."),
    ]
    output, _ = NemoScriptExporter().from_notebook_node(nb)
    assert "# heading" not in output
    assert "not nemo" not in output
    assert "a(1) ." in output


def test_exported_program_parses():
    import nmo_python  # noqa: F401  (ensures bindings importable)

    from nemo_jupyter.kernel import run_program

    nb = _notebook([
        "parent(ada, bob) .",
        "ancestor(?x, ?y) :- parent(?x, ?y) .\n@export ancestor :- csv {}.",
        "!trace ancestor(ada, bob)",
    ])
    output, _ = NemoScriptExporter().from_notebook_node(nb)
    result = run_program(output)
    assert result.ok, result.error
    assert result.predicates == ["ancestor"]
    assert result.counts["ancestor"] == 1
