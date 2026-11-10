"""The shortest path through the bindings: a program text in, rows out.

Usage: `python quickstart.py`, with the `nmo` module installed (`maturin develop`).
"""

import nmo

PROGRAM = """
    age(alice, 34) . age(bob, 17) . age(carol, 52) .
    adult(?person, ?age) :- age(?person, ?age), ?age >= 18 .
"""

# Parsing never raises. What did not parse is in `program.diagnostics`.
program = nmo.Program.parse(PROGRAM, name="people.rls")
assert program.ok, program.diagnostics
print(program.predicates)

engine = nmo.Engine()
engine.load(program)  # raises `nmo.DiagnosticsError` for a program that does not load
engine.reason()  # releases the GIL, and Ctrl-C cancels it

# A row is a tuple of values. A name such as `alice` is an `nmo.Iri`, never a `str`,
# and an integer is an `int`.
for person, age in sorted(engine.rows("adult"), key=str):
    print(f"{person.value} is an adult, aged {age}")

# A cursor reads the rows in bounded chunks, for predicates too large for one list.
cursor = engine.cursor("adult")
print(cursor.remaining, "rows to read")
for row in cursor:
    print(row)

# `str` of an `Iri`, as of the other classes of `nmo`, is the term as a program writes it.
print(str(nmo.Iri("alice")), str(nmo.Iri("https://example.org/alice")))

print(engine.predicates())
print(engine.column_types("adult"))
