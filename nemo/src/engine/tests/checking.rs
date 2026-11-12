//! What a check leaves in the engine.

use super::*;

/// A check leaves no statement or revision in the pipeline,
/// whether the program passes, has problems, or ends the check early.
#[test]
fn a_check_leaves_no_statements_behind() {
    let mut engine = Engine::new();
    let before = engine.pipeline.mark();

    let report = engine.check(parse("p(1) . q(?x) :- p(?x) ."));
    assert!(report.diagnostics().is_empty(), "{report:?}");
    assert_eq!(engine.pipeline.mark(), before);

    let report = engine.check(parse("q(?x, ?y) :- p(?x) ."));
    assert!(!report.diagnostics().is_empty());
    assert_eq!(engine.pipeline.mark(), before);

    let report = engine.check_source(
        "test.rls",
        "@use lib as l .
",
    );
    assert!(!report.diagnostics().is_empty());
    assert_eq!(engine.pipeline.mark(), before);
}
