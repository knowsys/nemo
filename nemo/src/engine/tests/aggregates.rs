//! Aggregates: what they count and report beyond their results.

use super::*;

#[test]
fn a_key_with_two_values_is_a_malformed_group() {
    // The conflicted group is dropped and counted; the clean group
    // survives. Identical duplicate contributions collapse.
    let mut engine = run("
        setting(api, port, 8080). setting(api, port, 9090).
        setting(db, port, 5432). setting(db2, port, 5432). setting(db2, port, 5432).
        config(?service, #map(?key = ?value)) :- setting(?service, ?key, ?value).
    ");
    assert_eq!(count(&mut engine, "config"), 2);
    let errors = engine.evaluation_errors();
    assert_eq!(errors.count(EvaluationError::Malformed), 1);
}

#[test]
fn tuples_of_different_shapes_do_not_compare() {
    // Only the two `point`s compare (elementwise: (1,2) < (1,3));
    // every cross-label pair errors and is counted, never silently false.
    let mut engine = run("
        p(point(1, 2)) . p(pixel(1, 2)) . p(point(1, 3)) .
        q(?a, ?b) :- p(?a), p(?b), ?a < ?b .
    ");
    assert_eq!(count(&mut engine, "q"), 1);
    let errors = engine.evaluation_errors();
    assert!(errors.count(EvaluationError::Incomparable) > 0);
}

#[test]
fn the_legacy_aggregate_list_means_per_and_warns() {
    // `#sum(?b, ?a)` is the `per` form under its old spelling: the
    // results agree, and a warning nudges toward the keyword.
    let mut engine = run("data(1, 2, 7) . data(1, 2, 8) . data(1, 3, 7) .
         old(?g, #sum(?b, ?a)) :- data(?g, ?a, ?b) .
         new(?g, #sum(?b per ?a)) :- data(?g, ?a, ?b) .");
    assert_eq!(rows(&mut engine, "old"), rows(&mut engine, "new"));
    let [warning] = engine.reports() else {
        panic!("expected the one warning: {:?}", engine.reports());
    };
    assert_eq!(warning.code(), codes::LEGACY_SCOPE_LIST, "{warning:?}");
}
