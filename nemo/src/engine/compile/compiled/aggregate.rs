//! Compiled aggregate heads.

use nemo_physical::management::TableId;

use crate::engine::compile::compiled::ComputedColumn;

/// One head atom with aggregates.
///
/// The body matches are grouped by their values in the group columns.
/// Within a group, each aggregate receives one contribution per distinct combination of
/// values in its input, value and [scope](CompiledAggregateFunction::scope) columns.
/// The head atom's rows are built from the group columns, the aggregates' results and the
/// post programs.
#[derive(Debug)]
pub(crate) struct CompiledAggregate {
    /// The table of the head predicate.
    pub(crate) table: TableId,
    /// The group columns, as rule columns.
    ///
    /// They are the head atom's columns without an aggregate, in order of first mention,
    /// followed by the variables that a post program combines with an aggregate's result.
    pub(crate) group: Vec<usize>,
    /// The aggregates, in the order of the head positions that contain them.
    pub(crate) functions: Vec<CompiledAggregateFunction>,
    /// The *post programs*, which compute the head expressions that combine an aggregate's
    /// result with other values.
    ///
    /// A post program reads a row of the group columns followed by the aggregates' results,
    /// so its inputs are positions in that row.
    pub(crate) post: Vec<ComputedColumn>,
    /// Where the value of each column of the head atom comes from.
    pub(crate) columns: Vec<AggregateColumn>,
}

/// One aggregate of an aggregate head, over rule columns.
#[derive(Debug)]
pub(crate) struct CompiledAggregateFunction {
    /// The aggregation function.
    pub(crate) kind: crate::model::AggregateKind,
    /// The aggregated column, or `None` for a count without an input,
    /// which reads no column.
    ///
    /// It is never a group column. For a map aggregate, it is the column of the keys.
    pub(crate) input: Option<usize>,
    /// The value column of a map aggregate, or `None` for every other kind.
    pub(crate) value: Option<usize>,
    /// The columns of the aggregate's `per` list, without the input and value columns,
    /// or `None` for `#count()`.
    ///
    /// `#count()` receives one contribution per body match,
    /// so all the rule's variables act as its scope columns.
    pub(crate) scope: Option<Vec<usize>>,
}

/// Where the value of one column of an aggregate head atom comes from.
#[derive(Debug, Clone, Copy)]
pub(crate) enum AggregateColumn {
    /// A group column, by its index in [group](CompiledAggregate::group).
    Group(usize),
    /// An aggregate's result, by its index in [functions](CompiledAggregate::functions).
    Result(usize),
    /// A post program's result, by its index in [post](CompiledAggregate::post).
    Post(usize),
}
