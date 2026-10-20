"""Writes syntaxes/nemo.tmLanguage.json from the language's own definitions.

The builtin names are read out of `Operator::NAMES` in the engine, so the
grammar cannot drift from the catalogue it highlights; `nemo/tests/integration/grammar.rs`
fails if this file is not re-run after the catalogue changes.

Usage: python tools/grammar.py
"""

import io
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
OPERATIONS = os.path.join(ROOT, "nemo", "src", "model", "term", "operation", "operator.rs")
AGGREGATION = os.path.join(ROOT, "nemo", "src", "model", "term", "aggregate.rs")
TARGET = os.path.join(os.path.dirname(HERE), "syntaxes", "nemo.tmLanguage.json")

# What a name is made of, exactly as the lexer has it: `%` continues a
# name, which is why a `%` inside one does not open a comment. A `-`
# joins two parts of a name without a marker (`my-lib`) where a letter,
# digit or `_` follows it; a name behind a marker (`?x`, `$x`, `@use`,
# `#sum`) has no `-`, so `?x-1` subtracts.
MARKED_NAME = "[A-Za-z_][A-Za-z0-9_%]*"
NAME = rf"{MARKED_NAME}(?:-[A-Za-z0-9_][A-Za-z0-9_%]*)*"
NOT_MARKED_NAME = "(?![A-Za-z0-9_%])"
NOT_NAME = "(?![A-Za-z0-9_%]|-[A-Za-z0-9_])"
# No match starts inside a hyphenated name, since the name patterns take
# it whole; looking behind for a `-` too would leave the `1` of `?x-1`
# uncoloured.
AFTER_NAME = "(?<![A-Za-z0-9_%])"

DIRECTIVES = [
    "base", "declare", "define", "export", "extern", "import",
    "input", "output", "parameter", "prefix", "type", "use",
]
# The base types and the column formats: one vocabulary where a type
# has a strict reading, plus the lexical formats `term` and `skip`.
TYPES = ["any", "string", "langstring", "iri", "int", "double", "float", "decimal", "number", "datetime", "date", "time", "duration", "daytimeduration", "yearmonthduration", "bool", "null", "tuple", "map", "term", "skip"]
# The aggregate scope word: `#sum(?v per ?w)`.
# Only before a variable sigil, so the plain constant `per` keeps its
# constant colour.
SCOPES = ["per"]

DIRECTIVE = {"name": "keyword.control.directive.nemo"}
CONNECTIVE = {"name": "keyword.control.connective.nemo"}
# A constant wears two scopes: VS Code's own themes colour
# `variable.other.constant` and leave `constant.other` plain, most other
# themes do the reverse, and the inner scope wins where a theme colours it.
CONSTANT = "variable.other.constant.nemo constant.other.nemo"
# An option's key, scoped like a JSON key rather than as a type.
OPTION = {"name": "support.type.property-name.nemo"}
TERMINATOR = {"name": "punctuation.terminator.statement.nemo"}
SECTION = {"name": "punctuation.section.nemo"}
# A directive's predicate, written right after the directive: the
# captures are its prefix or module name, the separator, and the name.
PREDICATE = rf"(?:\s*(?:({NAME})(::?))?({NAME}))?"
PREDICATE_CAPTURES = {
    "2": {"name": "entity.name.namespace.nemo"},
    "3": {"name": "punctuation.separator.namespace.nemo"},
    "4": {"name": "entity.name.function.nemo"},
}


def builtins():
    """The builtin names the engine accepts, from its own list."""
    source = io.open(OPERATIONS, encoding="utf-8").read()
    start = source.index("pub const NAMES:")
    end = source.index("];", start)
    return re.findall(r'"([A-Z0-9_]+)"', source[start:end])


def aggregates():
    """The aggregate names, from what the engine calls each one."""
    source = io.open(AGGREGATION, encoding="utf-8").read()
    body = source.split("pub fn name(&self)")[1].split("pub fn from_name")[0]
    return re.findall(r'=> "([a-z]+)"', body)


def alternation(words):
    """The words as one regex alternation, longest first so that a
    prefix of another word cannot win."""
    return "|".join(sorted(words, key=len, reverse=True))


def grammar():
    return {
        "$schema": "https://raw.githubusercontent.com/martinring/tmlanguage/master/tmlanguage.json",
        "name": "Nemo",
        "scopeName": "source.nemo",
        "patterns": [{"include": "#any"}],
        "repository": {
            "any": {
                "patterns": [
                    {"include": "#comment"},
                    {"include": "#attribute"},
                    {"include": "#language-tag"},
                    {"include": "#prefix-declaration"},
                    {"include": "#import-statement"},
                    {"include": "#declare-statement"},
                    {"include": "#type-statement"},
                    {"include": "#parameter-statement"},
                    {"include": "#directive-statement"},
                    {"include": "#directive"},
                    {"include": "#aggregate"},
                    {"include": "#format-string-block"},
                    {"include": "#format-string"},
                    {"include": "#string-block"},
                    {"include": "#string"},
                    {"include": "#iri"},
                    {"include": "#number"},
                    {"include": "#variable"},
                    {"include": "#boolean"},
                    {"include": "#builtin"},
                    {"include": "#qualified-call"},
                    {"include": "#qualified"},
                    {"include": "#prefixed-call"},
                    {"include": "#prefixed"},
                    {"include": "#predicate"},
                    {"include": "#aggregate-scope"},
                    {"include": "#label"},
                    {"include": "#constant"},
                    {"include": "#operator"},
                    {"include": "#punctuation"},
                ]
            },
            # `@prefix ex: <iri> .` in one stroke, so the declared prefix
            # gets its namespace colour rather than reading as a label.
            "prefix-declaration": {
                "match": rf"(@prefix){NOT_MARKED_NAME}\s*({NAME})?(:)",
                "captures": {
                    "1": DIRECTIVE,
                    "2": {"name": "entity.name.namespace.nemo"},
                    "3": {"name": "punctuation.separator.namespace.nemo"},
                },
            },
            # The words a directive gives meaning to (types, formats,
            # options, and connectives like `from`) are keywords only
            # where that directive reads them, and ordinary names
            # everywhere else. So each directive that reads such words
            # is a region of its own.
            #
            # `@import p from format { key = value, … }`, and `@export p to …`.
            "import-statement": {
                "begin": rf"(@(?:import|export)){NOT_MARKED_NAME}{PREDICATE}",
                "beginCaptures": {"1": DIRECTIVE, **PREDICATE_CAPTURES},
                "end": r"\.",
                "endCaptures": {"0": TERMINATOR},
                "patterns": [
                    {"include": "#comment"},
                    {"include": "#import-format"},
                    {"include": "#import-where"},
                    {"include": "#import-options"},
                    {"include": "#parenthesized"},
                    {"include": "#any"},
                ],
            },
            # `from` or `to`, and the format after it, which may be on
            # the next line.
            "import-format": {
                "begin": rf"{AFTER_NAME}(from|to){NOT_NAME}(?!\s*\()",
                "beginCaptures": {"1": CONNECTIVE},
                "end": rf"({NAME})|(?=[^\s%])",
                "endCaptures": {"1": {"name": "support.type.nemo"}},
                "patterns": [{"include": "#comment"}],
            },
            # An import's `where` clause of binding atoms.
            "import-where": {
                "match": rf"{AFTER_NAME}where{NOT_NAME}(?!\s*\()",
                "name": CONNECTIVE["name"],
            },
            # The option block: every key is an option, whatever its
            # name, and `format`'s value is column types.
            "import-options": {
                "begin": r"\{",
                "end": r"\}",
                "beginCaptures": {"0": SECTION},
                "endCaptures": {"0": SECTION},
                "patterns": [
                    {"include": "#comment"},
                    {"include": "#column-formats"},
                    {"match": rf"{AFTER_NAME}{NAME}(?=\s*=(?!=))", **OPTION},
                    {"include": "#braced"},
                    {"include": "#parenthesized"},
                    {"include": "#any"},
                ],
            },
            # `format = (int, term)`, or a single format.
            "column-formats": {
                "begin": rf"{AFTER_NAME}(format)\s*(=)(?!=)",
                "beginCaptures": {
                    "1": OPTION,
                    "2": {"name": "keyword.operator.comparison.nemo"},
                },
                "end": r"(?=[,}])",
                "patterns": [
                    {"include": "#type-tuple"},
                    {"include": "#type-expression"},
                ],
            },
            # Terms in brackets inside a directive, kept apart so that
            # the directive's own words are not read inside them.
            "parenthesized": {
                "begin": r"\(",
                "end": r"\)",
                "beginCaptures": {"0": SECTION},
                "endCaptures": {"0": SECTION},
                "patterns": [{"include": "#parenthesized"}, {"include": "#any"}],
            },
            "braced": {
                "begin": r"\{",
                "end": r"\}",
                "beginCaptures": {"0": SECTION},
                "endCaptures": {"0": SECTION},
                "patterns": [{"include": "#braced"}, {"include": "#any"}],
            },
            # `@declare p(column, …)`, `@input p(column, …)`, and
            # `@extern f(type, …) -> type`: types after the name.
            "declare-statement": {
                "begin": rf"(@(?:declare|extern|input)){NOT_MARKED_NAME}{PREDICATE}",
                "beginCaptures": {"1": DIRECTIVE, **PREDICATE_CAPTURES},
                "end": r"\.",
                "endCaptures": {"0": TERMINATOR},
                "patterns": [{"include": "#type-expression"}, {"include": "#operator"}],
            },
            # `@type point(double, double)`: the declared type, then its fields.
            "type-statement": {
                "begin": rf"(@type){NOT_MARKED_NAME}(?:\s*({NAME}))?",
                "beginCaptures": {"1": DIRECTIVE, "2": {"name": "entity.name.type.nemo"}},
                "end": r"\.",
                "endCaptures": {"0": TERMINATOR},
                "patterns": [{"include": "#type-expression"}],
            },
            # `@parameter $name: type = value`: a type after the colon,
            # a term after the `=`.
            "parameter-statement": {
                "begin": rf"(@parameter){NOT_MARKED_NAME}\s*(\${MARKED_NAME})?",
                "beginCaptures": {"1": DIRECTIVE, "2": {"name": "variable.parameter.nemo"}},
                "end": r"\.",
                "endCaptures": {"0": TERMINATOR},
                "patterns": [
                    {
                        "begin": r"\G\s*(:)",
                        "beginCaptures": {"1": {"name": "punctuation.separator.nemo"}},
                        "end": r"(?=[=.])",
                        "patterns": [{"include": "#type-expression"}],
                    },
                    {"include": "#any"},
                ],
            },
            # Inside the remaining directives, a bare name is a predicate
            # (`@output result`, a `@use` binding's both sides), so the
            # statement is a region where the fallback colours names as
            # predicates instead of as constants. `@define` holds terms
            # and needs no region.
            "directive-statement": {
                "begin": rf"(?=@(?:base|output|use){NOT_MARKED_NAME})",
                "end": r"\.",
                "endCaptures": {"0": TERMINATOR},
                "patterns": [
                    {"include": "#comment"},
                    {"include": "#use-module"},
                    {"include": "#directive"},
                    {"include": "#use-alias"},
                    {"include": "#format-string-block"},
                    {"include": "#format-string"},
                    {"include": "#string-block"},
                    {"include": "#string"},
                    {"include": "#iri"},
                    {"include": "#number"},
                    {"include": "#variable"},
                    {"include": "#boolean"},
                    {"include": "#builtin"},
                    {"include": "#label"},
                    {"include": "#qualified-call"},
                    {"include": "#qualified"},
                    {"include": "#prefixed-call"},
                    {"include": "#prefixed"},
                    {
                        "match": rf"{AFTER_NAME}{NAME}",
                        "name": "entity.name.function.nemo",
                    },
                    {"include": "#operator"},
                    {"include": "#punctuation"},
                ],
            },
            # The module a `@use` names, as `super::lib::tc`: directories and
            # the file name, each a namespace, after any `super` steps.
            "use-module": {
                "begin": rf"(@use){NOT_MARKED_NAME}",
                "beginCaptures": {"1": DIRECTIVE},
                "end": rf"(?=[{{.]|{AFTER_NAME}as{NOT_NAME})",
                "patterns": [
                    {"include": "#comment"},
                    {
                        "match": rf"{AFTER_NAME}super{NOT_NAME}",
                        "name": "keyword.other.super.nemo",
                    },
                    {"match": "::", "name": "punctuation.separator.namespace.nemo"},
                    {"match": rf"{AFTER_NAME}{NAME}", "name": "entity.name.namespace.nemo"},
                ],
            },
            # `as t` of a `@use`: the alias is the namespace its
            # `t::name` references wear.
            "use-alias": {
                "match": rf"{AFTER_NAME}(as)\s+({NAME})",
                "captures": {
                    "1": CONNECTIVE,
                    "2": {"name": "entity.name.namespace.nemo"},
                },
            },
            # A type: a base type, the name of a declared type, or a
            # tuple or map type labelled by either. Column and field
            # names wear their labels.
            "type-expression": {
                "patterns": [
                    {"include": "#comment"},
                    {"include": "#label"},
                    {"include": "#variable"},
                    {
                        # A base type's name before brackets labels the
                        # tuple or map instead.
                        "match": rf"{AFTER_NAME}(?:{alternation(TYPES)}){NOT_NAME}(?!\s*[({{])",
                        "name": "support.type.nemo",
                    },
                    {"match": rf"{AFTER_NAME}{NAME}", "name": "entity.name.type.nemo"},
                    {"include": "#punctuation"},
                ]
            },
            "type-tuple": {
                "begin": r"\(",
                "end": r"\)",
                "beginCaptures": {"0": SECTION},
                "endCaptures": {"0": SECTION},
                "patterns": [{"include": "#type-tuple"}, {"include": "#type-expression"}],
            },
            # A `%` opens a comment only where a name is not being
            # written: `Hello%20World` is one name, and the name pattern
            # consumes it before this is ever tried at that position.
            "comment": {
                "name": "comment.line.percentage.nemo",
                "begin": "%",
                "end": "$",
                "beginCaptures": {"0": {"name": "punctuation.definition.comment.nemo"}},
            },
            "attribute": {
                "name": "meta.attribute.nemo",
                "begin": r"(#\[)",
                "end": r"(\])",
                "beginCaptures": {"1": {"name": "punctuation.definition.attribute.nemo"}},
                "endCaptures": {"1": {"name": "punctuation.definition.attribute.nemo"}},
                "patterns": [
                    {"match": NAME, "name": "entity.other.attribute-name.nemo"},
                    {"include": "#any"},
                ],
            },
            # A language tag is an `@` touching a closing quote; every
            # other `@` opens a directive.
            "language-tag": {
                "match": r'(?<=")(@)([A-Za-z_][A-Za-z0-9_%]*(?:-[A-Za-z0-9][A-Za-z0-9_%]*)*)',
                "captures": {
                    "1": {"name": "punctuation.definition.tag.nemo"},
                    "2": {"name": "entity.name.tag.language.nemo"},
                },
            },
            # The marker is part of the keyword: themes rarely colour
            # `punctuation.definition.*`, and a bare `@` next to a
            # coloured word reads as a mistake.
            "directive": {
                "match": rf"@(?:{alternation(DIRECTIVES)}){NOT_MARKED_NAME}",
                "name": "keyword.control.directive.nemo",
            },
            "aggregate": {
                "match": rf"#(?:{alternation(aggregates())}){NOT_MARKED_NAME}",
                "name": "keyword.other.aggregate.nemo",
            },
            "format-string-block": {
                "name": "string.quoted.triple.nemo",
                "begin": 'f"""',
                "end": '"""',
                "beginCaptures": {"0": {"name": "punctuation.definition.string.begin.nemo"}},
                "endCaptures": {"0": {"name": "punctuation.definition.string.end.nemo"}},
                "patterns": [{"include": "#interpolation"}, {"include": "#escape"}],
            },
            "format-string": {
                "name": "string.quoted.double.nemo",
                "begin": 'f"',
                "end": '"',
                "beginCaptures": {"0": {"name": "punctuation.definition.string.begin.nemo"}},
                "endCaptures": {"0": {"name": "punctuation.definition.string.end.nemo"}},
                "patterns": [{"include": "#interpolation"}, {"include": "#escape"}],
            },
            "interpolation": {
                "patterns": [
                    # A doubled brace is a brace, not an expression.
                    {"match": r"\{\{|\}\}", "name": "constant.character.escape.nemo"},
                    {
                        "name": "meta.embedded.expression.nemo",
                        "begin": r"\{",
                        "end": r"\}",
                        "beginCaptures": {
                            "0": {"name": "punctuation.section.embedded.begin.nemo"}
                        },
                        "endCaptures": {"0": {"name": "punctuation.section.embedded.end.nemo"}},
                        "patterns": [{"include": "#any"}],
                    },
                ]
            },
            "string-block": {
                "name": "string.quoted.triple.nemo",
                "begin": '"""',
                "end": '"""',
                "beginCaptures": {"0": {"name": "punctuation.definition.string.begin.nemo"}},
                "endCaptures": {"0": {"name": "punctuation.definition.string.end.nemo"}},
                "patterns": [{"include": "#escape"}],
            },
            "string": {
                "name": "string.quoted.double.nemo",
                "begin": '"',
                "end": '"',
                "beginCaptures": {"0": {"name": "punctuation.definition.string.begin.nemo"}},
                "endCaptures": {"0": {"name": "punctuation.definition.string.end.nemo"}},
                "patterns": [{"include": "#escape"}],
            },
            # Exactly five escapes are recognized; anything else keeps
            # its backslash, so `\w` in a regex is not an error.
            "escape": {"match": r'\\[\\"ntr]', "name": "constant.character.escape.nemo"},
            # `<` opens an IRI only when a `>` follows before any
            # whitespace; otherwise it is the comparison operator.
            "iri": {
                "match": r"<[^\s>]*>",
                "name": "variable.other.constant.nemo constant.other.iri.nemo",
            },
            "number": {
                "patterns": [
                    {
                        "match": rf"{AFTER_NAME}0b[01]+",
                        "name": "constant.numeric.binary.nemo",
                    },
                    {
                        "match": rf"{AFTER_NAME}0o[0-7]+",
                        "name": "constant.numeric.octal.nemo",
                    },
                    {
                        "match": rf"{AFTER_NAME}0x[0-9a-fA-F]+",
                        "name": "constant.numeric.hex.nemo",
                    },
                    {
                        # The `f`/`d` suffix is only a suffix when no name
                        # character follows it: `1fx` is `1` and `fx`.
                        "match": (
                            rf"{AFTER_NAME}[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?"
                            rf"(?:[fFdD]{NOT_MARKED_NAME})?"
                        ),
                        "name": "constant.numeric.decimal.nemo",
                    },
                ]
            },
            "variable": {
                "patterns": [
                    {
                        "match": rf"\?{MARKED_NAME}",
                        "name": "variable.other.nemo",
                    },
                    {
                        "match": rf"!{MARKED_NAME}",
                        "name": "variable.other.existential.nemo",
                    },
                    {
                        "match": rf"\${MARKED_NAME}",
                        "name": "variable.parameter.nemo",
                    },
                    {
                        "match": rf"{AFTER_NAME}_{NOT_NAME}",
                        "name": "variable.language.anonymous.nemo",
                    },
                ]
            },
            # Applied to arguments, `true` and `false` name a predicate
            # or a function term.
            "boolean": {
                "match": rf"{AFTER_NAME}(?:true|false){NOT_NAME}(?!\s*\()",
                "name": "constant.language.boolean.nemo",
            },
            # Builtin names are reserved, case-insensitively, like the
            # aggregates; scoped as keywords, they also stand apart from
            # predicates in themes that colour `support.function` and
            # `entity.name.function` alike.
            "builtin": {
                "match": rf"{AFTER_NAME}(?i:{alternation(builtins())}){NOT_NAME}(?=\s*\()",
                "name": "keyword.other.builtin.nemo",
            },
            # A qualified or prefixed name applied to arguments is a
            # predicate; anywhere else it is a constant, like its bare
            # and `<…>` siblings.
            "qualified-call": {
                "match": rf"({NAME})(::)({NAME})(?=\s*\()",
                "captures": {
                    "1": {"name": "entity.name.namespace.nemo"},
                    "2": {"name": "punctuation.separator.namespace.nemo"},
                    "3": {"name": "entity.name.function.nemo"},
                },
            },
            "qualified": {
                "match": rf"({NAME})(::)({NAME})",
                "captures": {
                    "1": {"name": "entity.name.namespace.nemo"},
                    "2": {"name": "punctuation.separator.namespace.nemo"},
                    "3": {"name": CONSTANT},
                },
            },
            "prefixed-call": {
                "match": rf"({NAME})(:)({NAME})(?=\s*\()",
                "captures": {
                    "1": {"name": "entity.name.namespace.nemo"},
                    "2": {"name": "punctuation.separator.namespace.nemo"},
                    "3": {"name": "entity.name.function.nemo"},
                },
            },
            "prefixed": {
                "match": rf"({NAME})(:)({NAME})",
                "captures": {
                    "1": {"name": "entity.name.namespace.nemo"},
                    "2": {"name": "punctuation.separator.namespace.nemo"},
                    "3": {"name": CONSTANT},
                },
            },
            "predicate": {
                "match": rf"{AFTER_NAME}{NAME}(?=\s*\()",
                "name": "entity.name.function.nemo",
            },
            # An argument's or a column's `name:`, told from a prefixed
            # name the way the parser tells them apart: a prefixed
            # name's parts all touch.
            "label": {
                "match": rf"{AFTER_NAME}{NAME}(?=\s*:(?![A-Za-z0-9_%:]))",
                "name": "variable.parameter.nemo",
            },
            # `?v per ?w` in aggregates; the lookahead keeps `p(per)`'s
            # constant a constant.
            "aggregate-scope": {
                "match": rf"{AFTER_NAME}(?:{alternation(SCOPES)}){NOT_NAME}(?=\s*[?$!])",
                "name": CONNECTIVE["name"],
            },
            # Every remaining name is an IRI constant in argument
            # position — matched with a scope of its own, and so that a
            # `%` inside one is part of it rather than a comment.
            "constant": {
                "match": rf"{AFTER_NAME}{NAME}",
                "name": CONSTANT,
            },
            "operator": {
                "patterns": [
                    {"match": r":-", "name": "keyword.operator.rule.nemo"},
                    {"match": r"->", "name": "keyword.operator.arrow.nemo"},
                    {"match": r"\^\^", "name": "keyword.operator.datatype.nemo"},
                    # Longest first: `!==` before `!=`, `==` before `=`.
                    {"match": r"!==|!=|==|>=|<=|=|>|<", "name": "keyword.operator.comparison.nemo"},
                    {"match": r"[+\-*/]", "name": "keyword.operator.arithmetic.nemo"},
                    {"match": r"~", "name": "keyword.operator.negation.nemo"},
                ]
            },
            "punctuation": {
                "patterns": [
                    {"match": r"\.", "name": "punctuation.terminator.statement.nemo"},
                    {"match": r",", "name": "punctuation.separator.nemo"},
                    {"match": r"[()\[\]{}]", "name": "punctuation.section.nemo"},
                    {"match": r":", "name": "punctuation.separator.namespace.nemo"},
                ]
            },
        },
    }


def main():
    os.makedirs(os.path.dirname(TARGET), exist_ok=True)
    text = json.dumps(grammar(), indent=2) + "\n"
    io.open(TARGET, "w", encoding="utf-8", newline="\n").write(text)
    print(f"wrote {TARGET} with {len(builtins())} builtin names")


if __name__ == "__main__":
    main()
