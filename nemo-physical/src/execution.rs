//! The execution of database operations over tries: joins, and the sinks that consume their rows.
//!
//! A join combines inputs and chooses one value per *layer*.
//! An input is stored in one trie or several, its *blocks*,
//! and its rows are the rows of all its blocks together.
//! At each layer, the join chooses a value that every input column mapped to the layer has
//! below the values chosen at the shallower layers.
//! The leading layers are *emitted*: their values form the join's rows.
//! The deeper layers only check that a matching row exists.
//! A join may have *hooks*, which decide whether to keep a value found at a layer:
//! - a subtracted input drops the rows it matches,
//! - a filter drops the rows on which a condition over some layers does not hold,
//! - a bound layer has a single value, a constant or one that a
//!   [program](crate::function::StackProgram) computes from the shallower layers.
//!
//! A join yields its rows in storage order, each once,
//! together with the shallowest layer that changed since the previous row.
//! So its rows form a sorted run, from which a trie is built without sorting,
//! and a change at one of the leading layers marks the start of a *group*.
//!
//! A join too large for one thread is split into *cells*, which run on the worker pool.
//!
//! The module is organized along the rows.
//! The shapes describe what a join and the sinks of its rows compute,
//! the join produces the rows, sequentially or in cells,
//! and the sinks consume them, by evaluating programs on them, projecting or aggregating them.

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
