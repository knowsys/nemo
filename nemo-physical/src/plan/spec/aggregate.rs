//! Specification of the aggregation operation.

use crate::plan::{Marker, spec::AggregateFunction};

/// One aggregate of an [aggregation](crate::plan::OperationPlan::aggregate).
///
/// Within each group, the aggregate reads each distinct tuple
/// of its function's columns and its `per` columns once:
/// `Sum(v)` per `[w]` adds a value once for every `w` it occurs with.
///
/// # Example
///
/// ```
/// use nemo_physical::{
///     dictionary::Dictionary,
///     function::EvaluationContext,
///     management::{ColumnOrder, Revision, TableManager},
///     plan::{AggregateColumnSpec, AggregateFunction, OperationPlan},
///     storage::StorageValue,
///     trie::RowBuffer,
/// };
///
/// // The sales of items by store.
/// let int = StorageValue::integer;
/// let mut manager = TableManager::new();
/// let sales = manager.register("sale", 3);
/// let mut rows = RowBuffer::new(3);
/// for (store, item, amount) in [(1, 10, 5), (1, 11, 5), (2, 10, 7)] {
///     rows.push_row(&[int(store), int(item), int(amount)]);
/// }
/// manager.add_rows(sales, Revision(1), ColumnOrder::identity(3), rows);
///
/// // The total per store: the amount is summed per item,
/// // so the two items of store 1 that share an amount both count.
/// let mut plan = OperationPlan::new();
/// let [store, item, amount, total] = plan.markers();
/// let rows = plan.load(sales, Revision(0)..Revision(2), vec![store, item, amount]);
/// let sum = AggregateColumnSpec::new(AggregateFunction::Sum(amount), total).per(vec![item]);
/// let totals = plan.aggregate(rows, vec![store], vec![sum]);
/// plan.output(totals);
///
/// let mut dictionary = Dictionary::new();
/// let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
/// let mut found = Vec::new();
/// results.outputs[0].for_each_row(|row| found.push(row.to_vec()));
/// assert_eq!(found, [[int(1), int(10)], [int(2), int(7)]]);
/// ```
#[derive(Debug, Clone)]
pub struct AggregateColumnSpec {
    /// The function, with the columns it reads.
    pub(crate) function: AggregateFunction,
    /// The markers that, beside the function's columns, tell the rows it reads apart.
    pub(crate) per: Vec<Marker>,
    /// The fresh marker naming the result column.
    pub(crate) result: Marker,
}

impl AggregateColumnSpec {
    /// Creates an aggregate reading each distinct value of its function's columns once per group,
    /// named by a fresh marker.
    pub fn new(function: AggregateFunction, result: Marker) -> Self {
        Self {
            function,
            per: Vec::new(),
            result,
        }
    }

    /// Returns the aggregate reading each distinct tuple
    /// of its function's columns and the given columns once per group.
    pub fn per(self, per: Vec<Marker>) -> Self {
        Self { per, ..self }
    }

    /// Returns the markers the aggregate reads: its function's columns, then the `per` markers.
    pub(crate) fn read(&self) -> impl Iterator<Item = Marker> + '_ {
        let (value, pair) = self.function.columns();
        value
            .into_iter()
            .chain(pair)
            .chain(self.per.iter().copied())
    }
}
