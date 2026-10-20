//! Consolidation's write amplification, bounded.
//!
//! The engine reports how many rows its consolidation merges rewrote.
//! On the two-level schedule that stays within O(n·√n) of the rows a table holds.
//! A schedule that rewrites a table once per iteration is quadratic instead,
//! for example if a seed fact's block walls off a derived segment whose fold can never settle
//! the table's merge debt.

use crate::common::run;

/// A chain of `n` iterations deriving one row each into a predicate that
/// also holds a seed fact.
fn chain(n: usize) -> String {
    format!("p(0) . p(?x + 1) :- p(?x), ?x < {} .", n - 1)
}

#[test]
fn a_seed_fact_does_not_make_consolidation_quadratic() {
    let n = 4096;
    let engine = run(&chain(n));
    assert_eq!(engine.derived_facts(), n - 1);

    let rewritten = engine.timings().consolidated_rows;
    // The quadratic schedule rewrites about n²/2 rows (8.4 M here); the
    // two-level one measures just under n·√n (243 k).
    let ceiling = (4 * n * n.isqrt()) as u64;
    assert!(
        rewritten <= ceiling,
        "consolidation rewrote {rewritten} rows for {n} derived, above {ceiling}"
    );
}
