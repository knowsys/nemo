//! Tests of the engine through its methods, with one module per subject.
//!
//! The modules share the helpers below.

use super::*;

use nemo_physical::{
    management::Revision,
    value::{DataValue, EvaluationError},
};

use crate::{
    codes,
    engine::{PredicateRole, registered::Registered},
    model::{Fact, FileId, Program, Term},
    testing::{TempDirectory, arities, parse, roles, run},
    values::TypeSyntax,
};

mod aggregates;
mod checking;
mod diagnostics;
mod documentation;
mod imported_rows;
mod imports;
mod methods;
mod parameters;
mod rules;
mod structures;
mod term_syntax;
mod types;

/// Returns the rows of a predicate as displayed values, sorted.
fn rows(engine: &mut Engine, predicate: &str) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = engine
        .rows(predicate)
        .expect("the predicate exists")
        .into_iter()
        .map(|row| row.iter().map(DataValue::to_string).collect())
        .collect();
    rows.sort();
    rows
}

/// Returns the number of stored rows of a predicate.
fn count(engine: &mut Engine, predicate: &str) -> usize {
    engine.rows(predicate).expect("the predicate exists").len()
}

/// Returns the numbers of rows and of columns of a predicate's table,
/// whether or not it is stored whole, which shows what the import passes left of it.
fn stored(engine: &Engine, predicate: &str) -> (usize, usize) {
    let &Registered { table, arity } = engine
        .predicates
        .get(predicate)
        .expect("the predicate has a table");
    let rows = engine
        .manager
        .row_count(table, Revision(0)..Revision(engine.step.max(1)));
    (rows, arity)
}

/// Loads a program over in-memory resources, given by name and content, and reasons to the
/// fixpoint.
///
/// Returns the engine, with its reports.
fn load_resources(source: &str, resources: &[(&str, &str)]) -> Engine {
    let program = parse(source);
    let mut engine = Engine::new();
    for (name, content) in resources {
        engine.add_resource(*name, content.as_bytes().to_vec());
    }
    engine.load(program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    engine
}

/// Returns the `count` that the first warning of the given code reports,
/// or `None` if there is no such warning.
fn reported_count(engine: &Engine, code: nemo_physical::diagnostics::Code) -> Option<u64> {
    engine.reports().iter().find_map(|report| {
        (report.code() == code).then(|| {
            report
                .field("count")
                .and_then(|count| count.parse().ok())
                .expect("the warning reports its count")
        })
    })
}

/// Returns the problems that a check of a source reports.
fn check(source: &str) -> Vec<Diagnostic> {
    let program = parse(source);
    Engine::new().check(program).into_diagnostics()
}

/// Returns, per rule, the number of top-level operations that its plan runs per application,
/// which are its execution units and aggregations, read off its layout.
fn rule_steps(engine: &Engine) -> Vec<usize> {
    let program = engine.program.as_ref().expect("the program is loaded");
    (0..program.rules.len())
        .map(|position| {
            let layout = engine
                .skeletons
                .get(position)
                .expect("every rule was applied")
                .layout()
                .to_string();
            layout.lines().filter(|line| !line.starts_with(' ')).count()
        })
        .collect()
}
