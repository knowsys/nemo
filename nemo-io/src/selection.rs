//! Selections: which columns of a resource a reader delivers, and which rows it admits.
//!
//! The *written* columns are the columns of the resource,
//! and the *delivered* columns are those the reader keeps.

use nemo_physical::{
    function::EvaluationContext,
    value::{DataValue, EvaluationError},
};

use crate::row_filter::RowFilter;

/// The projection and restriction that a reader applies to its resource.
///
/// A selection says which written columns are delivered,
/// and gives a [condition](RowFilter) over the delivered columns that a row must pass.
/// A column that is not delivered still decides whether a row is well-formed.
/// A row that the condition excludes is never interned.
///
/// The default selection delivers every column and admits every row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowSelection {
    /// For each written column, whether it is delivered, or `None` to deliver all.
    ///
    /// A column past the end of the list is delivered.
    pub columns: Option<Vec<bool>>,
    /// The condition that delivered rows must pass, or `None` to admit all rows.
    pub filter: Option<RowFilter>,
}

impl RowSelection {
    /// Returns whether the written column at `position` is delivered.
    pub fn keeps(&self, position: usize) -> bool {
        self.columns
            .as_ref()
            .is_none_or(|columns| columns.get(position).copied().unwrap_or(true))
    }

    /// Returns the number of delivered columns of a resource with `written` columns.
    pub fn arity(&self, written: usize) -> usize {
        (0..written)
            .filter(|&position| self.keeps(position))
            .count()
    }

    /// Returns whether a delivered row passes the condition, which every row passes if there is
    /// none.
    ///
    /// `context` is the evaluation context of the condition, such as the instant its `NOW()`
    /// reads.
    ///
    /// # Errors
    ///
    /// Returns the evaluation error that left the row undecided, as [RowFilter::evaluate] does.
    ///
    /// # Panics
    ///
    /// Panics if the row has no value at a column that the condition reads.
    pub fn evaluate(
        &self,
        context: EvaluationContext,
        row: &[DataValue],
    ) -> Result<bool, EvaluationError> {
        self.filter
            .as_ref()
            .map_or(Ok(true), |filter| filter.evaluate(context, row))
    }

    /// Returns whether a delivered row passes the condition,
    /// counting an evaluation error as a failure.
    ///
    /// # Panics
    ///
    /// Panics if the row has no value at a column that the condition reads.
    pub fn admits(&self, context: EvaluationContext, row: &[DataValue]) -> bool {
        self.evaluate(context, row).unwrap_or(false)
    }
}
