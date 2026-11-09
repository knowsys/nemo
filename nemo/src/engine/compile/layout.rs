//! How a predicate's columns map from an import source to its table.

/// How the columns of an import source map to a predicate's table.
///
/// - The *source columns* are all columns of the import.
/// - A directive *keeps* the source columns whose format is not `skip`.
///   The kept columns are the predicate's columns as the program sees them,
///   the ones its rules, facts and declaration refer to.
/// - The [import plan](super::import_plan::ImportPlan) *prunes* the kept columns that no rule
///   reads, and the rest are *delivered* to the table.
///
/// Positions are mapped between these lists only through a layout,
/// so that no two parts of the compilation can disagree on what a position means.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnLayout {
    /// Per source column, whether the directive keeps it.
    kept: Vec<bool>,
    /// Per kept column, whether the plan prunes it.
    pruned: Vec<bool>,
}

impl ColumnLayout {
    /// Creates a layout from the source columns that the directive keeps,
    /// and the kept columns that the plan prunes.
    ///
    /// `kept` has one flag per source column, and `pruned` one per kept column.
    /// The pruning applies only if its length is the number of kept columns.
    /// Otherwise nothing is pruned, since the predicate is used at another arity,
    /// which is an error of its own.
    pub(super) fn new(kept: Vec<bool>, pruned: Option<&[bool]>) -> Self {
        let width = kept.iter().filter(|&&keep| keep).count();
        let pruned = match pruned {
            Some(pruned) if pruned.len() == width => pruned.to_vec(),
            _ => vec![false; width],
        };
        Self { kept, pruned }
    }

    /// Creates a layout of `width` source columns, all of them kept,
    /// with `pruned` as for [new](Self::new).
    pub(super) fn whole(width: usize, pruned: Option<&[bool]>) -> Self {
        Self::new(vec![true; width], pruned)
    }

    /// Returns the number of source columns.
    pub(crate) fn source_width(&self) -> usize {
        self.kept.len()
    }

    /// Returns the number of kept columns, which is the predicate's arity in the program.
    pub(crate) fn kept(&self) -> usize {
        self.pruned.len()
    }

    /// Returns the number of delivered columns, which is the table's arity.
    pub(crate) fn delivered(&self) -> usize {
        self.pruned.iter().filter(|&&pruned| !pruned).count()
    }

    /// Returns whether the plan prunes any column.
    pub(crate) fn prunes(&self) -> bool {
        self.pruned.contains(&true)
    }

    /// Returns whether the kept column at position `kept` is pruned.
    pub(crate) fn is_pruned(&self, kept: usize) -> bool {
        self.pruned.get(kept).copied().unwrap_or(false)
    }

    /// Returns the position among the delivered columns of the kept column at position `kept`,
    /// or `None` if it is pruned.
    pub(crate) fn delivered_position(&self, kept: usize) -> Option<usize> {
        if self.is_pruned(kept) {
            return None;
        }
        Some(
            self.pruned
                .iter()
                .take(kept)
                .filter(|&&pruned| !pruned)
                .count(),
        )
    }

    /// Returns the position among the kept columns of the delivered column at position
    /// `delivered`, or `None` if fewer columns are delivered.
    pub(crate) fn kept_position(&self, delivered: usize) -> Option<usize> {
        self.pruned
            .iter()
            .enumerate()
            .filter(|&(_, &pruned)| !pruned)
            .nth(delivered)
            .map(|(kept, _)| kept)
    }

    /// Returns whether a source column is delivered, which means kept and not pruned.
    pub(crate) fn delivers(&self, column: usize) -> bool {
        self.delivered_source(column).is_some()
    }

    /// Returns whether the directive skips a source column.
    pub(crate) fn skips(&self, column: usize) -> bool {
        !self.kept.get(column).copied().unwrap_or(false)
    }

    /// Returns the position among the delivered columns of a source column,
    /// or `None` if the directive skips it or the plan prunes it.
    pub(crate) fn delivered_source(&self, column: usize) -> Option<usize> {
        if !self.kept.get(column).copied().unwrap_or(false) {
            return None;
        }
        let kept = self.kept[..column].iter().filter(|&&keep| keep).count();
        self.delivered_position(kept)
    }

    /// Returns, per source column, whether it is delivered.
    pub(crate) fn delivered_columns(&self) -> Vec<bool> {
        (0..self.source_width())
            .map(|column| self.delivers(column))
            .collect()
    }

    /// Returns a list with one item per kept column, reduced to the items of the delivered
    /// columns.
    ///
    /// A list of another length is returned unchanged,
    /// since it belongs to a use of the predicate at another arity, which is an error of its own.
    pub(crate) fn deliver<T>(&self, kept: Vec<T>) -> Vec<T> {
        if kept.len() != self.kept() {
            return kept;
        }
        kept.into_iter()
            .zip(&self.pruned)
            .filter(|(_, pruned)| !**pruned)
            .map(|(item, _)| item)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A directive's skipped columns and the plan's pruned columns combine,
    /// and positions map correctly between source, kept and delivered columns.
    #[test]
    fn skips_and_pruning_compose_over_the_source_columns() {
        // Four source columns: the directive skips the second, and the
        // plan prunes the first of the three kept.
        let layout = ColumnLayout::new(vec![true, false, true, true], Some(&[true, false, false]));
        assert_eq!(layout.source_width(), 4);
        assert_eq!(layout.kept(), 3);
        assert_eq!(layout.delivered(), 2);
        assert_eq!(layout.delivered_columns(), [false, false, true, true]);
        assert_eq!(layout.delivered_position(0), None);
        assert_eq!(layout.delivered_position(2), Some(1));
        assert_eq!(layout.kept_position(0), Some(1));
        assert_eq!(layout.kept_position(1), Some(2));
        assert_eq!(layout.kept_position(2), None);
        assert_eq!(layout.deliver(vec!['a', 'b', 'c']), ['b', 'c']);
    }

    /// A pruning whose length is not the number of kept columns prunes nothing.
    #[test]
    fn pruning_at_another_width_does_not_apply() {
        let layout = ColumnLayout::whole(2, Some(&[true, false, false]));
        assert!(!layout.prunes());
        assert_eq!(layout.delivered(), 2);
        assert_eq!(layout.deliver(vec![1, 2, 3]), [1, 2, 3]);
    }
}
