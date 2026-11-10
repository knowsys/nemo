"""What goes wrong, as data: the diagnostics of parsing, checking and loading.

Usage: `python diagnostics.py`, with the `nmo` module installed (`maturin develop`).
"""

import nmo

# One error and one warning: the head of `unbound` has a variable that its body does not
# bind, and `confused` compares a name with an age.
PROGRAM = """
    @declare person(name: string, age: int) .
    person("Ann", 34) . person("Bo", 17) .
    confused(?x) :- person(?x, ?a), ?x > ?a .
    unbound(?x, ?z) :- person(?x, ?a) .
"""


def summary(diagnostic):
    """Returns a diagnostic on one line, with the place of its first label."""
    place = "no place"
    if diagnostic.labels and diagnostic.labels[0].line is not None:
        label = diagnostic.labels[0]
        place = f"{label.source}:{label.line}:{label.column}"
    return f"{diagnostic.severity} {diagnostic.code} at {place}: {diagnostic.message}"


# Parsing never raises. The statements that parse form the program,
# and the others are in `diagnostics`.
typo = nmo.Program.parse("p(1) . q(?x :- p(?x) .", name="typo.rls")
print(typo.ok, len(typo), [summary(diagnostic) for diagnostic in typo.diagnostics])

# A check reads no data and stores nothing. It returns every problem, warnings included.
program = nmo.Program.parse(PROGRAM, name="people.rls")
for diagnostic in nmo.Engine().check(program):
    print(summary(diagnostic))

# A load raises at the first error. The exception's message is the rendering that a
# terminal shows, and its `diagnostics` holds the same as data.
try:
    nmo.Engine().load(program)
except nmo.DiagnosticsError as error:
    print()
    print(error)
    print([(diagnostic.code, diagnostic.message) for diagnostic in error.diagnostics])

# The warnings of a load that succeeds are in `reports()`.
engine = nmo.Engine()
engine.load(nmo.Program.parse(PROGRAM.replace("unbound(?x, ?z)", "bound(?x)"), name="fixed.rls"))
for report in engine.reports():
    print(summary(report))
