//! Aggregations: laying out an aggregation node,
//! which counts its input's rows or makes one pass over them.

use crate::{
    execution::{AggregateShape, InputColumn},
    plan::{
        AggregateFunction, Marker, MarkerOrder, NodeId,
        layout::{LayoutBuilder, RequiredOrder, Step},
        node::AggregateNode,
    },
};

/// The layout of an aggregation.
#[derive(Debug)]
pub(crate) struct AggregateLayout {
    /// The aggregation node.
    pub(crate) node: NodeId,
    /// The order the aggregation reads its input in: the group key first.
    pub(crate) input: RequiredOrder,
    /// How the aggregation computes its aggregates.
    pub(crate) pass: AggregatePass,
}

/// How an aggregation computes its aggregates.
#[derive(Debug)]
pub(crate) enum AggregatePass {
    /// A count of the input's rows, which needs no pass over them:
    /// the single aggregate of an ungrouped aggregation is a count
    /// that tells the rows apart by every column of the input.
    CountRows,
    /// One pass of an aggregating sink over the input.
    Scan(ScanLayout),
}

/// The pass of an aggregating sink over the input of an aggregation.
#[derive(Debug)]
pub(crate) struct ScanLayout {
    /// The layers of the pass, one per column of the input.
    pub(crate) layers: Vec<Vec<InputColumn>>,
    /// The number of leading layers forming the group key.
    pub(crate) group_layers: usize,
    /// The aggregates computed per group, reading layers by index.
    pub(crate) aggregates: Vec<AggregateShape>,
}

impl LayoutBuilder<'_> {
    /// Lays out an aggregation node, requires its input in the order it reads it,
    /// and records what it materializes.
    pub(super) fn lay_out_aggregate(&mut self, node: NodeId, aggregate: &AggregateNode) {
        let input_markers = self.plan.node_markers(aggregate.input);
        self.need(aggregate.input, input_markers.iter().copied());
        let order = read_order(aggregate, input_markers);
        let pass = AggregatePass::new(aggregate, &order);
        let input = self.require(aggregate.input, order);

        let materialization = self.materialization(node, self.plan.node_markers(node).clone());
        self.materializations[node.index()] = Some(materialization);
        self.steps
            .push(Step::Aggregate(AggregateLayout { node, input, pass }));
    }
}

impl AggregatePass {
    /// Returns how an aggregation computes its aggregates over its input, read in the given order.
    fn new(aggregate: &AggregateNode, order: &MarkerOrder) -> Self {
        if aggregate.group().is_empty()
            && let [column] = aggregate.aggregates.as_slice()
            && column.function == AggregateFunction::Count
            && order.iter().all(|marker| column.per.contains(marker))
        {
            return Self::CountRows;
        }

        let position = |&marker: &Marker| {
            order
                .position(marker)
                .expect("plans validate aggregate inputs")
        };
        let aggregates = aggregate
            .aggregates
            .iter()
            .map(|column| {
                let (value, pair) = column.function.columns();
                AggregateShape {
                    kind: column.function.kind(),
                    input: value.as_ref().map(position),
                    pair: pair.as_ref().map(position),
                    per: column.per.iter().map(position).collect(),
                }
            })
            .collect();

        Self::Scan(ScanLayout {
            layers: (0..order.len())
                .map(|column| vec![InputColumn { input: 0, column }])
                .collect(),
            group_layers: aggregate.group().len(),
            aggregates,
        })
    }
}

/// Returns the order an aggregation reads its input in, whose markers are given:
/// the group key, then the input's other markers.
fn read_order(aggregate: &AggregateNode, input_markers: &[Marker]) -> MarkerOrder {
    let group = aggregate.group();
    let others = input_markers
        .iter()
        .copied()
        .filter(|marker| !group.contains(marker));

    group.iter().copied().chain(others).collect()
}
