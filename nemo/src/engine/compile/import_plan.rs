//! What the loaders of import-fed predicates may leave out.

use std::collections::HashMap;

use nemo_io::{row_filter::RowFilter, selection::RowSelection};
use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::{AboutPredicate, TranslationError},
    engine::compile::layout::ColumnLayout,
    model::Operation,
};

/// Per import-fed predicate, the columns its loader prunes and the condition its rows must pass.
///
/// The import passes decide the plan from what the rules read.
/// The plan is kept with the stored tables,
/// since a later translation, after `extend` or `reload`, must not expect what the loader left
/// out (see [check_stability](Self::check_stability)).
/// Column positions refer to the kept columns of a [ColumnLayout],
/// and every consumer takes its widths and positions from the layout the plan returns.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct ImportPlan {
    /// Per predicate, per kept column, whether the loader prunes it.
    pruned: HashMap<String, Vec<bool>>,
    /// Per predicate, the condition over its delivered columns.
    filters: HashMap<String, RowFilter>,
    /// Per predicate, the condition [filters](Self::filters) compiled from,
    /// over the `column N` pseudo-variables.
    ///
    /// A loader that can ask its source for less (a SPARQL endpoint) translates it.
    conditions: HashMap<String, Operation>,
}

impl ImportPlan {
    /// Returns the number of predicates the plan has entries for.
    pub(crate) fn len(&self) -> usize {
        self.pruned.len().max(self.filters.len())
    }

    /// Returns the layout of a predicate used at the given width with every column kept
    /// (no `skip` format), pruned as the plan records for that width.
    pub(crate) fn layout_of(&self, name: &str, kept: usize) -> ColumnLayout {
        ColumnLayout::whole(kept, self.pruned.get(name).map(Vec::as_slice))
    }

    /// Returns the layout of an import whose directive keeps the given source columns,
    /// the plan pruning among the kept ones.
    pub(crate) fn layout(&self, name: &str, kept: Vec<bool>) -> ColumnLayout {
        ColumnLayout::new(kept, self.pruned.get(name).map(Vec::as_slice))
    }

    /// Returns the layout of a predicate stored at the given delivered arity:
    /// the plan's pruning if its delivered count matches, the whole width otherwise.
    ///
    /// This is how a consumer that knows only the table's arity learns the kept width.
    pub(crate) fn stored_layout(&self, name: &str, delivered: usize) -> ColumnLayout {
        match self
            .pruned
            .get(name)
            .filter(|pruned| pruned.iter().filter(|&&pruned| !pruned).count() == delivered)
        {
            Some(pruned) => ColumnLayout::whole(pruned.len(), Some(pruned)),
            None => ColumnLayout::whole(delivered, None),
        }
    }

    /// Returns the condition a predicate's delivered rows must pass.
    pub(crate) fn filter(&self, name: &str) -> Option<&RowFilter> {
        self.filters.get(name)
    }

    /// Returns the condition a predicate's delivered rows must pass, as the
    /// operation it was compiled from.
    pub(crate) fn condition(&self, name: &str) -> Option<&Operation> {
        self.conditions.get(name)
    }

    /// Returns what a loader delivers of the predicate under a layout:
    /// the delivered columns and the row condition.
    pub(crate) fn selection(&self, name: &str, layout: &ColumnLayout) -> RowSelection {
        RowSelection {
            columns: (layout.delivered() != layout.source_width())
                .then(|| layout.delivered_columns()),
            filter: self.filters.get(name).cloned(),
        }
    }

    /// Records the columns a predicate's loader prunes.
    pub(crate) fn prune(&mut self, name: String, pruned: Vec<bool>) {
        self.pruned.insert(name, pruned);
    }

    /// Records a predicate's row condition, compiled and as written.
    pub(crate) fn restrict(&mut self, name: String, filter: RowFilter, condition: Operation) {
        self.conditions.insert(name.clone(), condition);
        self.filters.insert(name, filter);
    }

    /// Forgets a predicate whose table is being rebuilt.
    pub(crate) fn remove(&mut self, name: &str) {
        self.pruned.remove(name);
        self.filters.remove(name);
        self.conditions.remove(name);
    }

    /// Checks this freshly computed plan against the one the stored tables were loaded under.
    ///
    /// A column left out then must be unread now,
    /// and a row condition applied then must be the condition now,
    /// since the stored relation is what the old plan admitted,
    /// and a program that reads more cannot get it back.
    /// Predicates that already have tables keep the entries they were loaded under,
    /// or none if they were stored whole, whatever this plan computed.
    /// Predicates are checked in name order,
    /// so the first instability reported is always the same one.
    pub(crate) fn check_stability<'n>(
        &mut self,
        stored: ImportPlan,
        loaded: impl IntoIterator<Item = &'n String>,
    ) -> Result<(), Diagnostic> {
        for name in loaded {
            if !stored.pruned.contains_key(name) {
                self.pruned.remove(name);
            }
            if !stored.filters.contains_key(name) {
                self.filters.remove(name);
                self.conditions.remove(name);
            }
        }
        let mut pruned: Vec<(String, Vec<bool>)> = stored.pruned.into_iter().collect();
        pruned.sort_by(|left, right| left.0.cmp(&right.0));
        for (name, was_pruned) in pruned {
            let live_again = match self.pruned.get(&name) {
                Some(recomputed) => was_pruned
                    .iter()
                    .zip(recomputed)
                    .any(|(&was_pruned, &still_pruned)| was_pruned && !still_pruned),
                // No longer prunable at all: every pruned column counts as
                // read.
                None => was_pruned.contains(&true),
            };
            if live_again {
                return Err(TranslationError::PruningInstability {
                    name: name.clone(),
                    change: String::from("the program now reads some of those columns"),
                }
                .diagnostic()
                .with_predicate(name));
            }
            self.pruned.insert(name, was_pruned);
        }
        let mut filters: Vec<(String, RowFilter)> = stored.filters.into_iter().collect();
        filters.sort_by(|left, right| left.0.cmp(&right.0));
        for (name, filter) in filters {
            if self.filters.get(&name) != Some(&filter) {
                return Err(TranslationError::PruningInstability {
                    name: name.clone(),
                    change: String::from("the program now selects its rows by another condition"),
                }
                .diagnostic()
                .with_predicate(name));
            }
            if let Some(condition) = stored.conditions.get(&name) {
                self.conditions.insert(name.clone(), condition.clone());
            }
            self.filters.insert(name, filter);
        }
        Ok(())
    }
}
