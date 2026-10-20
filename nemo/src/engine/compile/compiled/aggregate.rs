//! Aggregate heads.

use nemo_physical::management::TableId;

use crate::engine::compile::compiled::ComputedColumn;

/// One aggregate head atom.
///
/// The body result is projected to the group columns and every aggregate's scope columns,
/// deduplicated, and aggregated per group in a single scan,
/// each aggregate ranging over the distinct combinations of its own scope columns.
/// The head atom then builds its rows from the groups, the results, and the post programs.
#[derive(Debug)]
pub(crate) struct CompiledAggregate {
    /// The receiving table.
    pub(crate) table: TableId,
    /// The group-by columns, as rule columns, in first-mention
    /// order over the head atom.
    pub(crate) group: Vec<usize>,
    /// The aggregates, in head order of first appearance.
    pub(crate) functions: Vec<CompiledAggregateFunction>,
    /// The programs over the aggregation's output,
    /// computing head expressions that combine an aggregate result.
    ///
    /// Their inputs index the `(group…, results…)` space.
    pub(crate) post: Vec<ComputedColumn>,
    /// The head atom's columns over the aggregation's full output.
    pub(crate) columns: Vec<AggregateColumn>,
}

/// One aggregate of an aggregate head, over the rule's columns.
#[derive(Debug)]
pub(crate) struct CompiledAggregateFunction {
    /// The aggregation function.
    pub(crate) kind: crate::model::AggregateKind,
    /// The aggregated column, never a group column, or `None` for `#count()`,
    /// which counts the contributions and reads no column.
    ///
    /// A map aggregate aggregates its keys here.
    pub(crate) input: Option<usize>,
    /// A map aggregate's value column, or `None` for every other kind.
    pub(crate) value: Option<usize>,
    /// The scope columns beyond the input,
    /// whose distinct combinations the aggregate ranges over within each group,
    /// or `None` for `#count()`, for which every body variable is a scope column.
    pub(crate) scope: Option<Vec<usize>>,
}

/// One column of an aggregate head atom.
#[derive(Debug, Clone, Copy)]
pub(crate) enum AggregateColumn {
    /// A group-by column, by index into the group list.
    Group(usize),
    /// An aggregate's result, by index into the function list.
    Result(usize),
    /// A post program's result, by index into the post list.
    Post(usize),
}
