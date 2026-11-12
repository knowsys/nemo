//! Tests of what the engine holds after a call: its tables, its pipeline and its run state.
//!
//! They read the engine's private parts.
//! The tests of the engine through its public methods alone are in `tests/integration`,
//! under the same module names.
//! The modules here share the helpers below.

use super::*;

use nemo_physical::{management::Revision, value::DataValue};

use crate::{
    codes,
    engine::stores::Registered,
    model::Program,
    testing::{TempDirectory, arities, parse, run},
};

mod checking;
mod imported_rows;
mod imports;
mod methods;
mod rules;
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
        .stores
        .predicates
        .get(predicate)
        .expect("the predicate has a table");
    let rows = engine
        .stores
        .manager
        .row_count(table, Revision(0)..Revision(engine.run.step.max(1)));
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

/// Returns, per rule, the number of top-level operations that its plan runs per application,
/// which are its execution units and aggregations, read off its layout.
fn rule_steps(engine: &Engine) -> Vec<usize> {
    let program = &engine
        .loaded
        .as_ref()
        .expect("the program is loaded")
        .compiled;

    (0..program.rules.len())
        .map(|position| {
            let layout = engine
                .run
                .skeletons
                .get(position)
                .expect("every rule was applied")
                .layout()
                .to_string();
            layout.lines().filter(|line| !line.starts_with(' ')).count()
        })
        .collect()
}
