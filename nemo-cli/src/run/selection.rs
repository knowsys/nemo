//! Selecting the predicates that a run writes or prints.

use nemo::{
    codes::NOT_STORED_WHOLE,
    engine::{Engine, PredicateRole},
};
use nemo_physical::diagnostics::Diagnostic;

use crate::{arguments::Mode, error::CliError};

/// Which predicates a run selects for writing or printing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Selection {
    /// The predicates that the program declares: its exports, and for printing also its
    /// outputs.
    Declared,
    /// Every predicate that rules derive facts for.
    Derived,
    /// Every extensional predicate.
    Extensional,
    /// Every predicate.
    All,
    /// No predicate.
    None,
}

/// The predicates that a selection includes.
#[derive(Debug, Default)]
pub(super) struct Reached {
    /// The predicates to write, in order.
    pub(super) predicates: Vec<String>,
    /// The predicates left out, since the engine does not store them whole.
    pub(super) skipped: Vec<String>,
}

impl Reached {
    /// Splits `predicates` into those that the engine stores whole and those it does not.
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

    /// Returns whether a selection by role includes a predicate with `role`.
    ///
    /// [Declared](Self::Declared) and [None](Self::None) include no predicate by its role.
    fn admits(self, role: PredicateRole) -> bool {
        match self {
            Selection::Derived => role.is_derived(),
            Selection::Extensional => role.is_extensional(),
            Selection::All => true,
            Selection::Declared | Selection::None => false,
        }
    }

    /// Returns the predicates that the selection includes, in order.
    ///
    /// - [Declared](Self::Declared) includes the exported predicates,
    ///   in the order of the program's exports.
    /// - A selection by role includes the predicates with a matching role, sorted by name,
    ///   and skips those that the engine does not store whole.
    /// - [None](Self::None) includes no predicate.
    ///
    /// An export of a predicate that no statement uses is included only by
    /// [Declared](Self::Declared), since the engine lists no such predicate.
    /// [Declared](Self::Declared) skips nothing, since the engine stores every exported
    /// predicate whole.
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
