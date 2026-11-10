"""How values convert between Python and the engine, in both directions.

Usage: `python values.py`, with the `nmo` module installed (`maturin develop`).
"""

import datetime
import decimal

import nmo

# Python values go in through `import_rows`. A builtin type that holds a value without loss
# stands for itself, and the wrapper classes of `nmo` stand for the other kinds of terms.
SENT = [
    "a string",
    nmo.Iri("https://example.org/alice"),
    nmo.LanguageString("hallo", "de"),
    True,
    42,
    10**30,
    decimal.Decimal("2.50"),
    2.5,
    nmo.Float(2.5),
    datetime.date(2024, 3, 1),
    datetime.datetime(2024, 3, 1, 12, 30, tzinfo=datetime.timezone.utc),
    datetime.timedelta(hours=2),
    (1, "two"),
    {nmo.Iri("port"): 8080},
    nmo.Labeled("point", (1, 2)),
    nmo.Literal("5cm", "https://example.org/length"),
]

engine = nmo.Engine()
engine.import_rows("sent", [(index, value) for index, value in enumerate(SENT)])

# The program's own values come out the same way. The last rule invents a value:
# `!owner` is an existential variable, and the engine fills it with a null.
engine.load(
    nmo.Program.parse(
        """
        received(?index, ?value) :- sent(?index, ?value) .
        written(point(1, 2)) . written("hallo"@de) . written(2.5f) . written(alice) .
        owned(?thing, !owner) :- written(?thing) .
        """
    )
)
engine.reason()

# Every value comes back as the value that went in.
for index, value in sorted(engine.rows("received")):
    assert value == SENT[index], (value, SENT[index])
    print(repr(value))

# `str` of an instance of one of the wrapper classes is the term as a program writes it.
print()
for (value,) in engine.rows("written"):
    print(f"{value!r:32} is written {value}")

# Python equality follows the engine's: a name is not the string of its characters,
# and a 32-bit float is not the double of the same value.
assert nmo.Iri("alice") != "alice"
assert nmo.Float(2.5) != 2.5

# A null is a value that reasoning created. It can be read and compared, but not passed in.
(thing, owner), *_ = engine.rows("owned")
print(f"\n{thing} is owned by {owner!r}")
try:
    nmo.Engine().import_rows("p", [(owner,)])
except ValueError as refused:
    print("refused:", refused)

# The inferred column types, in the syntax of `@declare`.
print(engine.column_types("owned"))
