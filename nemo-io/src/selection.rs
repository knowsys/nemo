//! What a reader delivers of what a resource holds: the columns kept and
//! the rows admitted.

use nemo_physical::{
    function::EvaluationContext,
    value::{DataValue, EvaluationError},
};

use crate::row_filter::RowFilter;

/// The projection and restriction a reader applies to its resource.
///
/// A selection says which written columns are delivered,
/// and a [condition](RowFilter) over the delivered columns that rows must pass.
/// A dropped column is never parsed or validated, so only what is read makes a row malformed.
/// A row the condition excludes is counted and never interned.
/// The default delivers everything.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowSelection {
    /// Per written column, whether it is delivered, or `None` to keep all.
    pub columns: Option<Vec<bool>>,
    /// The condition delivered rows must pass, or `None` to admit all.
    pub filter: Option<RowFilter>,
}

impl RowSelection {
    /// Returns whether a written column is delivered.
    pub fn keeps(&self, position: usize) -> bool {
        self.columns
            .as_ref()
            .is_none_or(|columns| columns.get(position).copied().unwrap_or(true))
    }

    /// Returns the delivered arity of a resource of the given written width.
    pub fn arity(&self, written: usize) -> usize {
        (0..written)
            .filter(|&position| self.keeps(position))
            .count()
    }

    /// Returns the condition's verdict on a delivered row, which passes if there is no condition.
    ///
    /// # Errors
    ///
    /// Returns the evaluation error that left the row undecided, as [RowFilter::evaluate] does.
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
    /// an evaluation error counting as a rejection.
    pub fn admits(&self, context: EvaluationContext, row: &[DataValue]) -> bool {
        self.evaluate(context, row).unwrap_or(false)
    }
}
