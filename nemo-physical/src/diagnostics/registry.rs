//! The registry of the documentation of diagnostic codes.

use std::{collections::HashMap, sync::LazyLock};

use crate::{
    diagnostics::{Code, DiagnosticKind},
    plan::PlanError,
};

/// The documentation of one diagnostic code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodeEntry {
    /// The code.
    pub code: Code,
    /// The name of the kind, as declared.
    pub name: &'static str,
    /// The message template, with its `{field}` placeholders not filled in.
    pub summary: &'static str,
    /// The explanation of the kind.
    pub explanation: &'static str,
    /// The note that every diagnostic of the kind has, if there is one.
    pub note: Option<&'static str>,
    /// The hint that every diagnostic of the kind has, if there is one.
    pub hint: Option<&'static str>,
}

impl CodeEntry {
    /// Creates the entry of a declared kind.
    pub const fn new(
        code: Code,
        name: &'static str,
        summary: &'static str,
        explanation: &'static str,
        note: Option<&'static str>,
        hint: Option<&'static str>,
    ) -> Self {
        Self {
            code,
            name,
            summary,
            explanation,
            note,
            hint,
        }
    }
}

/// The note that every internal error has.
pub(crate) const INTERNAL_NOTE: &str = "this is a bug in Nemo, not in your program; please report it at \
     https://github.com/knowsys/nemo/issues";

/// The entries of the codes that belong to no declared kind, which is only the internal error.
const ENTRIES: &[CodeEntry] = &[CodeEntry::new(
    Code::INTERNAL,
    "Internal",
    "internal error",
    "Nemo reached a state it did not expect. \
     This is a bug in Nemo. Please report it, \
     together with the program and data that triggered it.",
    Some(INTERNAL_NOTE),
    None,
)];

/// The registry of every error and warning that this crate reports.
pub static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    Registry::new()
        .with_entries(ENTRIES)
        .with_kinds::<PlanError>()
});

/// The documentation of diagnostic codes, looked up by code.
///
/// It provides the explanation of a code, for example to show in a log or on a web page.
/// Two different entries never share a code:
/// adding an entry with the code of a different entry in the registry panics.
///
/// # Example
///
/// ```
/// use nemo_physical::{diagnostic_kinds, diagnostics::{Code, Registry, REGISTRY}};
///
/// diagnostic_kinds! {
///     /// Problems with a greeting.
///     pub enum GreetingError {
///         Nobody => NOBODY = 42, error, "the greeting names nobody",
///             explain "The greeting named nobody.";
///     }
/// }
///
/// let registry = Registry::new()
///     .with_registry(&REGISTRY)
///     .with_kinds::<GreetingError>();
///
/// let entry = registry.explain(codes::NOBODY).unwrap();
/// assert_eq!(entry.name, "Nobody");
/// assert_eq!(entry.explanation, "The greeting named nobody.");
/// assert_eq!(registry.explain(Code::INTERNAL).map(|entry| entry.name), Some("Internal"));
/// assert_eq!(registry.explain(Code::new(7)), None);
/// ```
#[derive(Debug, Default)]
pub struct Registry {
    /// The entries, by code.
    entries: HashMap<Code, &'static CodeEntry>,
}

impl Registry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds every entry of another registry.
    pub fn with_registry(mut self, other: &Registry) -> Self {
        for &entry in other.entries.values() {
            self.insert(entry);
        }
        self
    }

    /// Adds the entries of the kinds that the enum `K` declares.
    pub fn with_kinds<K: DiagnosticKind>(self) -> Self {
        self.with_entries(K::ENTRIES)
    }

    /// Adds the given entries.
    pub fn with_entries(mut self, entries: &'static [CodeEntry]) -> Self {
        for entry in entries {
            self.insert(entry);
        }
        self
    }

    /// Adds one entry, which is kept once if it is added again.
    ///
    /// Panics if the registry has a different entry with the same code.
    fn insert(&mut self, entry: &'static CodeEntry) {
        match self.entries.get(&entry.code) {
            Some(previous) if *previous != entry => {
                panic!(
                    "{} is both {} and {}",
                    entry.code, previous.name, entry.name
                )
            }
            Some(_) => {}
            None => {
                self.entries.insert(entry.code, entry);
            }
        }
    }

    /// Returns the entry documenting a code, if the registry has one.
    pub fn explain(&self, code: Code) -> Option<&'static CodeEntry> {
        self.entries.get(&code).copied()
    }

    /// Returns every entry, in code order.
    pub fn entries(&self) -> Vec<&'static CodeEntry> {
        let mut entries: Vec<_> = self.entries.values().copied().collect();
        entries.sort_by_key(|entry| entry.code);
        entries
    }
}
