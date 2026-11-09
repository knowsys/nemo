//! Aggregates and comparisons of structures: what they count and report beyond their results.

use super::*;

/// A map aggregate's group with two values for one key is dropped and counted as malformed,
/// while identical contributions collapse into one.
#[test]
fn a_key_with_two_values_is_a_malformed_group() {
    // `api` has two values for `port`, and `db2` contributes the same pair twice.
    let mut engine = run("
        setting(api, port, 8080). setting(api, port, 9090).
        setting(db, port, 5432). setting(db2, port, 5432). setting(db2, port, 5432).
        config(?service, #map(?key = ?value)) :- setting(?service, ?key, ?value).
    ");
    assert_eq!(count(&mut engine, "config"), 2);
    let errors = engine.evaluation_errors();
    assert_eq!(errors.count(EvaluationError::Malformed), 1);
}

/// Comparing tuples with different labels is a counted evaluation error,
/// not a comparison that is silently false.
#[test]
fn tuples_of_different_shapes_do_not_compare() {
    // Only the two `point`s compare, elementwise, and (1, 2) < (1, 3).
    let mut engine = run("
        p(point(1, 2)) . p(pixel(1, 2)) . p(point(1, 3)) .
        q(?a, ?b) :- p(?a), p(?b), ?a < ?b .
    ");
    assert_eq!(count(&mut engine, "q"), 1);
    let errors = engine.evaluation_errors();
    assert!(errors.count(EvaluationError::Incomparable) > 0);
}

/// The legacy aggregate list `#sum(?b, ?a)` means `#sum(?b per ?a)`,
/// and is reported with a warning that suggests the keyword.
#[test]
fn the_legacy_aggregate_list_means_per_and_warns() {
    let mut engine = run("data(1, 2, 7) . data(1, 2, 8) . data(1, 3, 7) .
         old(?g, #sum(?b, ?a)) :- data(?g, ?a, ?b) .
         new(?g, #sum(?b per ?a)) :- data(?g, ?a, ?b) .");
    assert_eq!(rows(&mut engine, "old"), rows(&mut engine, "new"));
    let [warning] = engine.reports() else {
        panic!("expected the one warning: {:?}", engine.reports());
    };
    assert_eq!(warning.code(), codes::LEGACY_SCOPE_LIST, "{warning:?}");
}
