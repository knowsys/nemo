"""Unit tests for the pure helpers of the Nemo kernel.

Run: .venv/bin/python -m pytest nemo-jupyter/tests/test_kernel_logic.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import nmo_python
import pytest

from nemo_jupyter.kernel import (
    format_fact,
    format_value,
    parse_export_predicates,
    parse_import_predicates,
    run_program,
    strip_comments,
)


def test_strip_comments_line():
    assert strip_comments("a(1) . % a comment\nb(2) .") == "a(1) .  \nb(2) ."


def test_strip_comments_block():
    assert strip_comments("a(1) . /* multi\nline */ b(2) .") == "a(1) .   b(2) ."


def test_strip_comments_preserves_strings():
    source = 'a("100% @export fake") .\n% @export fake2 :- csv {}.'
    stripped = strip_comments(source)
    assert '"100% @export fake"' not in stripped
    assert '""' in stripped
    assert "fake2" not in stripped


def test_parse_export_predicates_basic():
    source = "a(1) .\n@export a :- csv {}."
    assert parse_export_predicates(source) == ["a"]


def test_parse_export_predicates_multiple_and_order():
    source = "\n@export b :- csv {}.\n@export a :- csv {}.\n@export b :- csv {}."
    assert parse_export_predicates(source) == ["b", "a"]


def test_parse_export_predicates_with_arity():
    assert parse_export_predicates("@export p/2 :- csv {}.") == ["p"]
    assert parse_export_predicates("@export p(2) :- csv {}.") == ["p"]


def test_parse_export_predicates_ignores_comments_and_strings():
    source = """
        % @export fake :- csv {}.
        /* @export alsoFake :- csv {}. */
        a("text with @export inside") .
        @export real :- csv {}.
    """
    assert parse_export_predicates(source) == ["real"]


def test_parse_import_predicates():
    source = "@import data :- csv { resource = \"data.csv\" } ."
    assert parse_import_predicates(source) == ["data"]


def test_format_value():
    assert format_value(42) == "42"
    assert format_value(2.5) == "2.5"
    assert format_value(True) == "true"
    assert format_value(False) == "false"
    assert format_value("<http://ex.org/x>") == "<http://ex.org/x>"
    assert format_value('"hello"') == '"hello"'
    lit = nmo_python.NemoLiteral("hi", lang="en")
    assert format_value(lit) == '"hi"@en'
    lit2 = nmo_python.NemoLiteral("5")
    assert format_value(lit2) == '"5"^^<http://www.w3.org/2001/XMLSchema#string>'


def test_format_fact():
    assert format_fact("p", [1, "x", True]) == "p(1, x, true)"


def test_run_program_ok():
    result = run_program(
        "parent(ada, bob) .\nparent(bob, cyd) .\n"
        "anc(?x, ?y) :- parent(?x, ?y) .\n@export anc :- csv {}."
    )
    assert result.ok
    assert result.predicates == ["anc"]
    assert result.counts["anc"] == 2
    # bare names are relative IRIs, rendered in canonical <...> form
    assert "anc(<ada>, <bob>)" in result.text
    assert "anc(<bob>, <cyd>)" in result.text
    assert result.engine is not None


def test_run_program_no_exports():
    result = run_program("a(1) .")
    assert result.ok
    assert "no @export" in result.text


def test_run_program_error():
    result = run_program("this is not a nemo program(")
    assert not result.ok
    assert result.error


def test_run_program_language_tagged_values():
    result = run_program(
        'greet("hi"@en, "salut"@fr) .\n@export greet :- csv {}.'
    )
    assert result.ok
    assert '"hi"@en' in result.text
    assert '"salut"@fr' in result.text
