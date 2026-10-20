//! The program's predicates as results refer to them.
//!
//! The written program names them,
//! and a method that delivers a predicate's facts fails with N0489 for one the engine does not
//! store whole.
//!
//! Every derived predicate is stored whole,
//! and so is every predicate the program writes out (`@output`, `@export`).
//! Only an import can be stored partially, if the optimization passes left out columns or rows
//! no rule reads, or not at all, if the rules query it at its endpoint.

use nemo_physical::{
    diagnostics::Diagnostic,
    management::{Revision, TableId},
};

use crate::{
    diagnostics::EngineError,
    engine::{Engine, evaluate::extensional::ExtensionalOrigin, registered::Registered},
};

/// Where a predicate's rows come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateRole {
    /// Not derived by any rule.
    ///
    /// Its facts, if any, come from facts, imports, `@input`,
    /// or rows added through [import_rows](Engine::import_rows).
    Extensional,
    /// Derived by rules only.
    Derived,
    /// Derived by rules, and given input facts as well.
    Both,
}

impl PredicateRole {
    /// Returns the role's name: `extensional`, `derived`, or `both`.
    pub fn name(self) -> &'static str {
        match self {
            PredicateRole::Extensional => "extensional",
            PredicateRole::Derived => "derived",
            PredicateRole::Both => "both",
        }
    }

    /// Returns whether rules derive into the predicate.
    pub fn is_derived(self) -> bool {
        matches!(self, PredicateRole::Derived | PredicateRole::Both)
    }

    /// Returns whether the predicate holds input facts.
    pub fn is_extensional(self) -> bool {
        matches!(self, PredicateRole::Extensional | PredicateRole::Both)
    }
}

/// One predicate of the program, as the engine lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredicateInfo {
    /// The predicate's resolved name.
    pub name: String,
    /// Its number of columns.
    pub arity: usize,
    /// Where its rows come from.
    pub role: PredicateRole,
}

/// How much of a predicate's facts the engine stores.
enum StoredExtent {
    /// Every fact, in the table.
    Whole(TableId),
    /// The table leaves out what no rule reads.
    Partial {
        /// The columns left out, by position.
        pruned: Vec<usize>,
        /// Whether the rows no rule can use were left out.
        filtered: bool,
    },
    /// No table: the rules query the predicate's import at its endpoint.
    Absent,
}

impl Engine {
    /// Returns the number of columns of a predicate of the program,
    /// or `None` for a name the program never used.
    pub(crate) fn arity_of(&self, predicate: &str) -> Option<usize> {
        if !self.names(predicate) {
            return None;
        }
        match self.predicates.get(predicate) {
            Some(&Registered {
                arity: delivered, ..
            }) => Some(
                self.translation
                    .plan
                    .stored_layout(predicate, delivered)
                    .kept(),
            ),
            None => self.written.as_ref()?.arity(predicate).or_else(|| {
                self.translation
                    .declarations
                    .get(predicate)
                    .map(|declaration| declaration.types.len())
            }),
        }
    }

    /// Returns the names of the program's outputs.
    pub(in crate::engine) fn output_names(&self) -> Vec<String> {
        match &self.written {
            Some(written) if !written.declared_outputs().is_empty() => {
                written.declared_outputs().to_vec()
            }
            Some(_) => self
                .predicates()
                .into_iter()
                .filter(|info| info.role.is_derived())
                .map(|info| info.name)
                .collect(),
            None => Vec::new(),
        }
    }

    /// Returns where a predicate's rows come from (see [predicate](Self::predicate)).
    pub(in crate::engine) fn role_of(&self, name: &str) -> PredicateRole {
        let (derives, has_input_facts) = match &self.written {
            Some(written) => (written.derives(name), written.has_input_facts(name)),
            None => (false, true),
        };
        match (derives, has_input_facts || self.holds_imported_rows(name)) {
            (true, true) => PredicateRole::Both,
            (true, false) => PredicateRole::Derived,
            (false, _) => PredicateRole::Extensional,
        }
    }

    /// Returns the facts of the program's predicates, split into the loaded and the derived.
    ///
    /// A block of extensional rows counts as loaded if it was added through `import_rows`,
    /// or if a written fact, import, or `@input` produces facts for the predicate.
    /// Every other row counts as derived,
    /// so the rows of an import that a stage put in place of a written rule count as derived.
    pub(in crate::engine) fn fact_counts(&self) -> (usize, usize) {
        let range = Revision(0)..Revision(self.step.max(1));
        let (mut loaded, mut derived) = (0, 0);
        for name in self.listed() {
            let Some(&Registered { table, .. }) = self.predicates.get(&name) else {
                continue;
            };
            let has_input_facts = self
                .written
                .as_ref()
                .is_none_or(|written| written.has_input_facts(&name));
            let outside: usize = self.extensional.get(&table).map_or(0, |blocks| {
                blocks
                    .iter()
                    .filter(|block| {
                        has_input_facts || block.origin == ExtensionalOrigin::ImportRows
                    })
                    .map(|block| self.manager.row_count(table, block.range.clone()))
                    .sum()
            });
            let total = self.manager.row_count(table, range.clone());
            loaded += outside;
            derived += total - outside;
        }
        (loaded, derived)
    }

    /// Returns whether rows were added to a predicate through `import_rows`.
    fn holds_imported_rows(&self, predicate: &str) -> bool {
        self.predicates
            .get(predicate)
            .is_some_and(|Registered { table, .. }| {
                self.extensional.get(table).is_some_and(|blocks| {
                    blocks
                        .iter()
                        .any(|block| block.origin == ExtensionalOrigin::ImportRows)
                })
            })
    }

    /// Returns the names of the program's predicates:
    /// the written program's, or every registered one if no program is loaded.
    pub(in crate::engine) fn listed(&self) -> Vec<String> {
        match &self.written {
            Some(written) => written.predicates().map(str::to_owned).collect(),
            None => self.predicates.keys().cloned().collect(),
        }
    }

    /// Returns whether a name is one of the program's predicates.
    fn names(&self, predicate: &str) -> bool {
        match &self.written {
            Some(written) => written.names(predicate),
            None => self.predicates.contains_key(predicate),
        }
    }

    /// Returns the table and the number of columns of a predicate of
    /// the program.
    ///
    /// # Errors
    ///
    /// Returns N0486 for a predicate the program never used, and for one
    /// a pipeline stage introduced, which no user wrote.
    pub(crate) fn written_predicate(
        &self,
        predicate: &str,
    ) -> Result<(TableId, usize), Diagnostic> {
        match self.predicates.get(predicate) {
            Some(&Registered { table, arity }) if self.names(predicate) => Ok((table, arity)),
            _ => Err(EngineError::UnknownPredicate {
                predicate: predicate.to_owned(),
            }
            .diagnostic()),
        }
    }

    /// Returns the table and the number of columns of a predicate of the program the engine
    /// stores whole, which the methods delivering the predicate's facts read.
    ///
    /// # Errors
    ///
    /// Returns N0482 if no program is loaded,
    /// N0486 for a name that is not one of the program's predicates,
    /// and N0489 for a predicate the engine does not store whole, naming what it leaves out.
    pub(crate) fn whole_predicate(&self, predicate: &str) -> Result<(TableId, usize), Diagnostic> {
        if self.program.is_none() {
            return Err(EngineError::NoProgram.diagnostic());
        }
        let Some(arity) = self.arity_of(predicate) else {
            return Err(EngineError::UnknownPredicate {
                predicate: predicate.to_owned(),
            }
            .diagnostic());
        };
        match self.stored_extent(predicate) {
            StoredExtent::Whole(table) => Ok((table, arity)),
            extent => Err(not_stored_whole(predicate, extent)),
        }
    }

    /// Returns how much of a predicate's facts the engine stores.
    fn stored_extent(&self, predicate: &str) -> StoredExtent {
        let Some(&Registered {
            table,
            arity: delivered,
        }) = self.predicates.get(predicate)
        else {
            return StoredExtent::Absent;
        };
        let layout = self.translation.plan.stored_layout(predicate, delivered);
        let pruned: Vec<usize> = (0..layout.kept())
            .filter(|&column| layout.is_pruned(column))
            .collect();
        let filtered = self.translation.plan.filter(predicate).is_some();
        if pruned.is_empty() && !filtered {
            StoredExtent::Whole(table)
        } else {
            StoredExtent::Partial { pruned, filtered }
        }
    }
}

/// Returns the error (N0489) for a predicate the engine does not store whole, naming
/// what it leaves out in the program's terms.
fn not_stored_whole(predicate: &str, extent: StoredExtent) -> Diagnostic {
    let (missing, hint) = match extent {
        StoredExtent::Whole(_) => unreachable!("a predicate stored whole is delivered"),
        StoredExtent::Partial { pruned, filtered } => {
            let columns: Vec<String> = pruned
                .iter()
                .map(|column| (column + 1).to_string())
                .collect();
            let columns = match columns.split_last() {
                None => None,
                Some((last, [])) => Some((format!("column {last} is"), "it")),
                Some((last, rest)) => Some((
                    format!("columns {} and {last} are", rest.join(", ")),
                    "them",
                )),
            };
            let missing = match (columns, filtered) {
                (Some((columns, them)), false) => {
                    format!("{columns} not stored, since no rule reads {them}")
                }
                (Some((columns, _)), true) => format!(
                    "{columns} not stored, and only the rows the rules reading it can use are \
                     kept"
                ),
                (None, _) => String::from("only the rows the rules reading it can use are stored"),
            };
            (
                missing,
                "declare it `@output`, or call `retain_imports()` before loading",
            )
        }
        StoredExtent::Absent => (
            String::from("it is not stored, since the rules reading it query it at its endpoint"),
            "declare it `@output`",
        ),
    };
    EngineError::NotStoredWhole {
        predicate: predicate.to_owned(),
        missing,
    }
    .diagnostic()
    .with_hint(hint)
}
