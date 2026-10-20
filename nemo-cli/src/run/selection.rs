//! Which predicates a run selects, and the predicates that reaches.

use nemo::{
    codes::NOT_STORED_WHOLE,
    engine::{Engine, PredicateRole},
};
use nemo_physical::diagnostics::Diagnostic;

use crate::{arguments::Mode, error::CliError};

/// Which predicates a run selects for writing or printing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Selection {
    /// What the program declares: its exports, and for printing its
    /// outputs.
    Declared,
    /// Every predicate rules derive into.
    Derived,
    /// Every predicate fed from outside.
    Extensional,
    /// Every predicate.
    All,
    /// None.
    None,
}

/// The predicates a selection reaches.
#[derive(Debug, Default)]
pub(super) struct Reached {
    /// The predicates to write, in order.
    pub(super) predicates: Vec<String>,
    /// The predicates left out, since the engine does not store them
    /// whole.
    pub(super) skipped: Vec<String>,
}

impl Reached {
    /// Splits role-selected predicates into the ones the engine can
    /// deliver and the ones it does not store whole.
    pub(super) fn split(engine: &Engine, predicates: Vec<String>) -> Self {
        let mut reached = Self::default();
        for predicate in predicates {
            match engine.count(&predicate) {
                Err(error) if error.code() == NOT_STORED_WHOLE => reached.skipped.push(predicate),
                _ => reached.predicates.push(predicate),
            }
        }
        reached
    }

    /// Returns the warning naming the skipped predicates,
    /// or `None` if nothing was skipped.
    pub(super) fn warning(&self) -> Option<Diagnostic> {
        if self.skipped.is_empty() {
            return None;
        }
        let names: Vec<String> = self
            .skipped
            .iter()
            .map(|predicate| format!("`{predicate}`"))
            .collect();
        Some(
            CliError::SkippedPredicates {
                count: names.len().into(),
                predicates: names.join(", "),
            }
            .diagnostic(),
        )
    }
}

impl Selection {
    /// Returns the selection a mode asks for.
    pub(super) fn of(mode: Mode) -> Self {
        match mode {
            Mode::Keep => Selection::Declared,
            Mode::Derived => Selection::Derived,
            Mode::Extensional => Selection::Extensional,
            Mode::All => Selection::All,
            Mode::None => Selection::None,
        }
    }

    /// Returns whether a predicate of the given role is selected,
    /// for the role-driven selections.
    fn admits(self, role: PredicateRole) -> bool {
        match self {
            Selection::Derived => role.is_derived(),
            Selection::Extensional => role.is_extensional(),
            Selection::All => true,
            Selection::Declared | Selection::None => false,
        }
    }

    /// Returns the predicates the selection reaches, in order:
    /// the exported ones as the program declares them,
    /// or the predicates whose role is admitted as the engine lists them, by name.
    ///
    /// A declared export of a predicate no statement used is reached by the declared selection
    /// alone, since the engine lists no role for it.
    /// A selection by role skips what the engine does not store whole,
    /// and a declared predicate is always stored whole.
    pub(super) fn reaches(self, engine: &Engine) -> Reached {
        match self {
            Selection::None => Reached::default(),
            Selection::Declared => Reached {
                predicates: engine
                    .exports()
                    .iter()
                    .map(|specification| specification.predicate.clone())
                    .collect(),
                skipped: Vec::new(),
            },
            selection => Reached::split(
                engine,
                engine
                    .predicates()
                    .into_iter()
                    .filter(|info| selection.admits(info.role))
                    .map(|info| info.name)
                    .collect(),
            ),
        }
    }
}
