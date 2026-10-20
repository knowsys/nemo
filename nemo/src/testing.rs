//! Helpers shared by the unit tests: programs and rules parsed from
//! source, engines reasoned to their fixpoint, and temporary directories.

mod temp_directory;

pub(crate) use temp_directory::TempDirectory;

use crate::{
    engine::{Engine, PredicateRole},
    model::{FileId, Program, Rule},
};

/// Parses a program, panicking on diagnostics.
pub(crate) fn parse(source: &str) -> Program {
    let (program, diagnostics) = Program::parse(source, FileId::new(0));
    assert!(diagnostics.is_empty(), "{source}: {diagnostics:?}");
    program
}

/// Parses a rule, panicking on a problem.
pub(crate) fn rule(text: &str) -> Rule {
    Rule::parse(text).expect("the rule parses")
}

/// Parses, loads, and reasons a program to its fixpoint.
pub(crate) fn run(source: &str) -> Engine {
    let mut engine = Engine::new();
    engine.load(parse(source)).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    engine
}

/// Returns the engine's predicates as name and arity.
pub(crate) fn arities(engine: &Engine) -> Vec<(String, usize)> {
    engine
        .predicates()
        .into_iter()
        .map(|info| (info.name, info.arity))
        .collect()
}

/// Returns the engine's predicates as name and role.
pub(crate) fn roles(engine: &Engine) -> Vec<(String, PredicateRole)> {
    engine
        .predicates()
        .into_iter()
        .map(|info| (info.name, info.role))
        .collect()
}
