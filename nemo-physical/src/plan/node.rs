//! Nodes: the operations of a plan, as the plan stores them.

use std::{ops::Range, slice};

use crate::{
    management::{Revision, TableId},
    plan::{
        Marker, MarkerOrder,
        spec::{AggregateColumnSpec, ComputedColumnSpec, FilterSpec},
    },
};

/// Identifies a node within its [OperationPlan](crate::plan::OperationPlan).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub(crate) u32);

impl NodeId {
    /// Creates the id of the node at the given index.
    pub(crate) fn new(index: usize) -> Self {
        Self(u32::try_from(index).expect("fewer than 2^32 nodes are added"))
    }

    /// Returns the index of the node.
    pub(crate) fn index(self) -> usize {
        self.0 as usize
    }
}

/// One operation of a plan, producing a relation whose columns are named by markers.
#[derive(Debug)]
pub(crate) enum Node {
    /// A range of revisions of a stored table; marker `i` names the table's column `i`.
    Load {
        /// The table.
        table: TableId,
        /// The revisions read.
        range: Range<Revision>,
        /// The column markers.
        markers: MarkerOrder,
    },
    /// The input's rows under new markers: marker `i` renames the input's column `i`.
    Alias {
        /// The renamed relation.
        input: NodeId,
        /// The new column markers.
        markers: MarkerOrder,
    },
    /// The union of relations sharing one marker set.
    Union {
        /// The united relations.
        inputs: Vec<NodeId>,
        /// The shared markers, in the first input's order.
        markers: MarkerOrder,
    },
    /// The rows over the inputs' markers that every input matches.
    Join {
        /// The joined relations.
        inputs: Vec<NodeId>,
        /// The node's markers: the inputs' markers, in the order the join binds them.
        order: MarkerOrder,
    },
    /// The input's rows for which a condition holds.
    Filter {
        /// The filtered relation.
        input: NodeId,
        /// The condition.
        spec: FilterSpec,
        /// The node's markers, which are the input's.
        markers: MarkerOrder,
    },
    /// The input's rows, each extended by a computed column.
    Compute {
        /// The extended relation.
        input: NodeId,
        /// The computed column.
        spec: ComputedColumnSpec,
        /// The node's markers: the input's, then the result.
        markers: MarkerOrder,
    },
    /// The input's rows that no row of a relation matches on their shared markers.
    Subtract {
        /// The relation rows are dropped from.
        input: NodeId,
        /// The relation whose matches are dropped.
        relation: NodeId,
        /// The node's markers, which are the input's.
        markers: MarkerOrder,
    },
    /// The input's rows restricted to some of its markers, each row once.
    Project {
        /// The projected relation.
        input: NodeId,
        /// The kept markers, in output order.
        markers: MarkerOrder,
    },
    /// A grouped aggregation.
    Aggregate(AggregateNode),
}

impl Node {
    /// Returns the markers of the node's columns in its canonical order.
    pub(crate) fn markers(&self) -> &MarkerOrder {
        match self {
            Node::Load { markers, .. }
            | Node::Alias { markers, .. }
            | Node::Union { markers, .. }
            | Node::Filter { markers, .. }
            | Node::Compute { markers, .. }
            | Node::Subtract { markers, .. }
            | Node::Project { markers, .. } => markers,
            Node::Join { order, .. } => order,
            Node::Aggregate(aggregate) => &aggregate.markers,
        }
    }

    /// Returns the nodes this node reads.
    pub(crate) fn inputs(&self) -> impl Iterator<Item = NodeId> + '_ {
        let (read, relation): (&[NodeId], Option<NodeId>) = match self {
            Node::Load { .. } => (&[], None),
            Node::Alias { input, .. }
            | Node::Filter { input, .. }
            | Node::Compute { input, .. }
            | Node::Project { input, .. } => (slice::from_ref(input), None),
            Node::Subtract {
                input, relation, ..
            } => (slice::from_ref(input), Some(*relation)),
            Node::Union { inputs, .. } | Node::Join { inputs, .. } => (inputs, None),
            Node::Aggregate(aggregate) => (slice::from_ref(&aggregate.input), None),
        };

        read.iter().copied().chain(relation)
    }
}

/// An aggregation node: a group key of input markers, then one column per aggregate.
#[derive(Debug)]
pub(crate) struct AggregateNode {
    /// The aggregated relation.
    pub(crate) input: NodeId,
    /// The aggregates computed per group.
    pub(crate) aggregates: Vec<AggregateColumnSpec>,
    /// The node's markers: the group key, then one per aggregate.
    pub(crate) markers: MarkerOrder,
}

impl AggregateNode {
    /// Returns the input markers forming the group key, in order.
    pub(crate) fn group(&self) -> &[Marker] {
        &self.markers[..self.markers.len() - self.aggregates.len()]
    }
}
