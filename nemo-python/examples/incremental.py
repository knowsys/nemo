"""Changing a reasoned engine: new rows, new statements, and a replaced program.

Usage: `python incremental.py`, with the `nmo` module installed (`maturin develop`).
"""

import nmo

# A task is ready unless it is blocked, and it is blocked while a task it needs is not done.
PROGRAM = """
    task(build) . task(test) . task(deploy) .
    needs(test, build) . needs(deploy, test) .
    blocked(?task) :- needs(?task, ?other), ~done(?other) .
    ready(?task) :- task(?task), ~blocked(?task) .
"""


def names(engine, predicate):
    """Returns the first column of a predicate's rows in term syntax, sorted."""
    return ", ".join(sorted(str(row[0]) for row in engine.rows(predicate)))


engine = nmo.Engine()
engine.load(nmo.Program.parse(PROGRAM, name="tasks.rls"))
engine.reason()
print("ready at first:", names(engine, "ready"))

# Rows from Python are new facts. `done` is read under a negation, so the next run drops the
# conclusions that depended on it and derives them again.
# The program's `build` is a name, so the row holds an `Iri`, not a `str`.
engine.import_rows("done", [(nmo.Iri("build"),)])
engine.reason()
print("ready once build is done:", names(engine, "ready"))

# A trace explains a fact by the rules and facts that derive it.
print(engine.trace_ascii("ready(test)"))

# An extension adds statements, and the next run continues from what is stored.
engine.extend(
    nmo.Program.parse(
        """
        task(release) . needs(release, deploy) .
        awaited(?task) :- ready(?task), needs(?next, ?task) .
        """,
        name="more.rls",
    )
)
engine.reason()
print("awaited:", names(engine, "awaited"))

# A reload replaces the program, which is how statements are retracted.
# `done` has no statements, so the rows given to it stay.
engine.reload(nmo.Program.parse(PROGRAM + "blocked(deploy) .", name="tasks.rls"))
engine.reason()
print("ready after the reload:", names(engine, "ready"))
print("predicates:", [name for name, _, _ in engine.predicates()])
print(engine.loaded_facts, "loaded and", engine.derived_facts, "derived facts")
