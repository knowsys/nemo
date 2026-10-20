//! The execution of database operations over tries: joins, and the consumers of their rows.
//!
//! A join combines inputs, each a trie, and chooses one value per layer.
//! The values of a layer are those that every input column mapped to it holds
//! below the values bound at the shallower layers.
//! The leading layers are emitted as the join's rows.
//! The deeper layers only establish that a matching row exists.
//! A join may carry hooks, which decide whether a value found at a layer stands:
//! - a subtracted input drops the rows it matches,
//! - a filter drops the rows on which a condition over some layers does not hold,
//! - a bound layer takes the value a program computes from the shallower layers.
//!
//! A join yields its rows in storage order, each once,
//! with the shallowest layer that changed since the previous row.
//! Its rows thus form a sorted run, which a trie is built from without sorting,
//! and a change at one of the leading layers marks the start of a group.
//!
//! A join too large for one thread is split into cells, which run on the worker pool.
//!
//! The module follows the rows:
//! the shapes describe what a join and the consumers of its rows compute,
//! the join produces the rows, sequentially or in cells,
//! and the sinks consume them: evaluating programs on them, projecting or aggregating them.

mod join;
mod partition;
mod shape;
mod sink;

pub use join::JoinArena;
pub(crate) use join::{JoinTemplate, TrieJoin};
pub(crate) use partition::Cells;
pub(crate) use shape::AggregateKind;
#[cfg(test)]
pub(crate) use shape::OwnedJoinShape;
pub(crate) use shape::{
    AggregateShape, AggregationShape, BoundLayerShape, BoundValue, ComputedShape, FilterShape,
    InputColumn, JoinShape, PostFilterShape, ProjectionShape, RowInput, RowStageShape,
    SubtractedShape,
};
pub(crate) use sink::{
    Aggregator, PendingRows, ProjectingSink, ProjectionScratch, RowEvaluator, RowSink,
};

#[cfg(test)]
mod tests;
