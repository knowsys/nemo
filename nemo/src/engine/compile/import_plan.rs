//! The import plan: what the loads of imported predicates may leave out.

use std::collections::HashMap;

use nemo_io::selection::{RowCondition, RowSelection};
use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::{AboutPredicate, TranslationError},
    engine::compile::layout::ColumnLayout,
    model::Operation,
};

/// For each imported predicate, the columns that its load prunes
/// and the condition that its rows must satisfy.
///
/// The [import passes](crate::engine::compile::optimize::ImportPass) compute the plan from
/// what the rules read.
/// The plan is kept as long as the stored tables,
/// since a later translation must not read what a load left out
/// (see [check_stability](Self::check_stability)).
/// Column positions are positions among the kept columns of a [ColumnLayout].
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct ImportPlan {
    /// Per predicate, whether the load prunes each kept column.
    pruned: HashMap<String, Vec<bool>>,
    /// Per predicate, the condition over its delivered columns.
    filters: HashMap<String, RowCondition>,
    /// Per predicate, the condition as written, from which its [filter](Self::filters) was
    /// compiled.
    ///
    /// It refers to delivered column `N` by the variable `column N`.
    /// A load that can ask its source for fewer rows, such as a SPARQL query, translates it.
    conditions: HashMap<String, Operation>,
}

impl ImportPlan {
    /// Returns the number of predicates the plan has entries for.
    pub(crate) fn len(&self) -> usize {
        self.pruned.len().max(self.filters.len())
    }

    /// Returns the layout of a predicate with `kept` columns and no `skip` format,
    /// pruned as the plan records for that number of columns.
    pub(crate) fn layout_of(&self, name: &str, kept: usize) -> ColumnLayout {
        ColumnLayout::whole(kept, self.pruned.get(name).map(Vec::as_slice))
    }

    /// Returns the layout of an import, pruned as the plan records.
    ///
    /// `kept` says for each source column whether the directive keeps it.
    pub(crate) fn layout(&self, name: &str, kept: Vec<bool>) -> ColumnLayout {
        ColumnLayout::new(kept, self.pruned.get(name).map(Vec::as_slice))
    }

    /// Returns the layout of a predicate whose table has `delivered` columns.
    ///
    /// The layout has the plan's pruning if that pruning delivers `delivered` columns,
    /// and no pruning otherwise.
    /// It gives the kept columns of a predicate when only its table's arity is known.
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

    /// Returns the condition that the delivered rows of a predicate must satisfy, if it has one.
    pub(crate) fn filter(&self, name: &str) -> Option<&RowCondition> {
        self.filters.get(name)
    }

    /// Returns the condition of a predicate as written,
    /// the operation from which [filter](Self::filter) was compiled.
    pub(crate) fn condition(&self, name: &str) -> Option<&Operation> {
        self.conditions.get(name)
    }

    /// Returns the selection of a predicate's load under a layout:
    /// its delivered columns and its condition.
    ///
    /// The columns are `None` if every source column is delivered.
    pub(crate) fn selection(&self, name: &str, layout: &ColumnLayout) -> RowSelection {
        RowSelection {
            columns: (layout.delivered() != layout.source_width())
                .then(|| layout.delivered_columns()),
            condition: self.filters.get(name).cloned(),
        }
    }

    /// Records which kept columns of a predicate its load prunes.
    pub(crate) fn prune(&mut self, name: String, pruned: Vec<bool>) {
        self.pruned.insert(name, pruned);
    }

    /// Records a predicate's row condition, compiled and as written.
    pub(crate) fn restrict(&mut self, name: String, filter: RowCondition, condition: Operation) {
        self.conditions.insert(name.clone(), condition);
        self.filters.insert(name, filter);
    }

    /// Forgets a predicate whose table is being rebuilt.
    pub(crate) fn remove(&mut self, name: &str) {
        self.pruned.remove(name);
        self.filters.remove(name);
        self.conditions.remove(name);
    }

    /// Checks this newly computed plan against `stored`, the plan that the stored tables were
    /// loaded under.
    ///
    /// A column that `stored` pruned must not be read now,
    /// and a condition of `stored` must equal the condition now,
    /// since the stored tables contain only the rows that `stored` admitted,
    /// and a program that reads more cannot get the rest back.
    /// The predicates in `loaded`, which already have tables, keep the entries they were
    /// loaded under, or none if they were stored whole, whatever this plan computed.
    /// The predicates are checked in name order,
    /// so that the error is always about the same predicate.
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
                // This plan prunes nothing of the predicate,
                // so every column that `stored` pruned counts as read.
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
        let mut filters: Vec<(String, RowCondition)> = stored.filters.into_iter().collect();
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
