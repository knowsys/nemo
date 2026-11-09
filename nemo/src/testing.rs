//! Helpers shared by the unit tests.
//!
//! They parse programs and rules, run programs to their fixpoint, and create temporary
//! directories.

mod temp_directory;

pub(crate) use temp_directory::TempDirectory;

use crate::{
    engine::{Engine, PredicateRole},
    model::{FileId, Program, Rule},
};

/// Parses a program, and panics if parsing reports any diagnostic.
pub(crate) fn parse(source: &str) -> Program {
    let (program, diagnostics) = Program::parse(source, FileId::new(0));
    assert!(diagnostics.is_empty(), "{source}: {diagnostics:?}");
    program
}

/// Parses a rule, and panics if the text is not a rule.
pub(crate) fn rule(text: &str) -> Rule {
    Rule::parse(text).expect("the rule parses")
}

/// Returns a new engine that has loaded the program in `source` and reasoned to the fixpoint.
///
/// Panics if parsing, loading or reasoning fails.
pub(crate) fn run(source: &str) -> Engine {
    let mut engine = Engine::new();
    engine.load(parse(source)).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    engine
}

/// Returns the name and arity of each of the engine's predicates.
pub(crate) fn arities(engine: &Engine) -> Vec<(String, usize)> {
    engine
        .predicates()
        .into_iter()
        .map(|info| (info.name, info.arity))
        .collect()
}

/// Returns the name and role of each of the engine's predicates.
pub(crate) fn roles(engine: &Engine) -> Vec<(String, PredicateRole)> {
    engine
        .predicates()
        .into_iter()
        .map(|info| (info.name, info.role))
        .collect()
}
