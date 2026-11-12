//! How the tables and the plans of rules are laid out.

use super::*;

/// A table that a stratum leaves final is merged into one block before a later stratum
/// reads it.
#[test]
fn a_finished_table_merges_whole_before_later_strata_read_it() {
    // `path` grows over several iterations, and `reach` reads it in a later
    // stratum, which does not write `path`.
    let source = "e(1, 2) . e(2, 3) . e(3, 4) . e(4, 5) . e(5, 6) .\n\
                  path(?x, ?y) :- e(?x, ?y) .\n\
                  path(?x, ?z) :- path(?x, ?y), e(?y, ?z) .\n\
                  reach(?y) :- path(1, ?y) .";
    let program = parse(source);
    let mut engine = Engine::new();
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");

    let &Registered { table: path, .. } = engine.stores.predicates.get("path").expect("registered");
    assert_eq!(engine.stores.manager.blocks(path).len(), 1);
    assert_eq!(count(&mut engine, "reach"), 5);
}

/// The plan layout runs a rule's operators inside its joins,
/// so each shape runs in as few units as the executor allows:
///
/// - One unit for a plain rule, and one for a negation over variables.
/// - A computed head column, a filter calling a nondeterministic function, or a
///   negation over a computed column ends a unit below it.
/// - Several heads read one shared result.
/// - An aggregate and an existential check read units of their own.
///
/// A shape that runs in more units than these lost a fusion.
#[test]
fn rule_shapes_run_in_few_units() {
    let facts = "e(1, 2) . e(2, 3) . e(3, 4) . f(3) . g(3, 3) .";
    let cases: &[(&str, &str)] = &[
        ("plain", "p(?x, ?y) :- e(?x, ?z), e(?z, ?y) ."),
        ("computed head", "p(?x, ?y + 1) :- e(?x, ?z), e(?z, ?y) ."),
        ("drawing filter", "p(?x) :- e(?x, ?y), RAND() < 2 ."),
        ("several heads", "p(?x), q(?y) :- e(?x, ?z), e(?z, ?y) ."),
        ("negation", "p(?x, ?y) :- e(?x, ?y), ~f(?y) ."),
        (
            "negation over a computed column",
            "p(?x, ?y) :- e(?x, ?y), ?z = ?y + 1, ~f(?z) .",
        ),
        (
            "negation with a constant",
            "p(?x, ?y) :- e(?x, ?y), ~g(?y, 3) .",
        ),
        ("aggregate", "c(?x, #count(?y)) :- e(?x, ?z), e(?z, ?y) ."),
        ("existential", "p(?x, !v) :- e(?x, ?y) ."),
    ];
    let expected = [1, 2, 2, 3, 1, 2, 2, 3, 3];
    for ((name, rule), steps) in cases.iter().zip(expected) {
        let engine = run(&format!(
            "{facts}
{rule}"
        ));
        assert_eq!(rule_steps(&engine), vec![steps], "{name}");
    }
}
