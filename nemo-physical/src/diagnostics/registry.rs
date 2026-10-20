//! The registry documenting diagnostic codes,
//! e.g. for looking up the additional information for errors.

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
    /// The message template, its `{field}` placeholders unfilled.
    pub summary: &'static str,
    /// The explanation of the kind.
    pub explanation: &'static str,
    /// The note every diagnostic of the kind carries, if any.
    pub note: Option<&'static str>,
    /// The hint every diagnostic of the kind carries, if any.
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

/// The note every internal error carries.
pub(crate) const INTERNAL_NOTE: &str = "this is a bug in Nemo, not in your program; please report it at \
     https://github.com/knowsys/nemo/issues";

/// The entries of the codes without a kind:
/// the internal error alone.
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

/// The registry of every error or warning this layer reports.
pub static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    Registry::new()
        .with_entries(ENTRIES)
        .with_kinds::<PlanError>()
});

/// The documentation of every diagnostic that is reported, looked up by code.
///
/// This is used to show the explanation of an error, e.g. in a log or on a web page.
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

    /// Adds the entries of the kinds an enum declares.
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

    /// Adds one entry.
    ///
    /// The same entry added again is kept once,
    /// a different one under its code is refused.
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
