//! Members: laying out one join of a unit with the operators that run inside it.
//!
//! Every member of a unit runs the unit's chain over the unit's layers
//! and produces the unit's columns, so that the rows of all members build one relation.

use crate::{
    execution::{
        BoundLayerShape, ComputedShape, FilterShape, InputColumn, PostFilterShape, ProjectionShape,
        RowInput, RowStageShape,
    },
    function::SpecializedProgram,
    plan::{
        Marker, MarkerOrder, NodeId,
        layout::{
            LayoutBuilder, RequiredOrder,
            chain::{Chain, ChainColumn, ChainComputation, ChainCondition},
        },
    },
};

/// The layout of one member of a unit: a join with the operators running inside it.
#[derive(Debug)]
pub(crate) struct MemberLayout {
    /// Per joined input, the order the join reads it in.
    pub(crate) inputs: Vec<RequiredOrder>,
    /// Per join layer, the input columns intersected there.
    pub(crate) layers: Vec<Vec<InputColumn>>,
    /// The number of leading layers the join emits.
    pub(crate) emitted: usize,
    /// The subtracted relations, compared in the descent.
    pub(crate) subtracted: Vec<SubtractedLayout>,
    /// The filters checked in the descent, reading layers by index.
    pub(crate) filters: Vec<FilterShape>,
    /// The layers whose values a program computes from layers above them, looked up in the descent.
    pub(crate) bound_layers: Vec<BoundLayerShape>,
    /// What becomes of the emitted rows.
    pub(crate) delivery: Delivery,
}

/// What becomes of the rows a member's join emits.
#[derive(Debug)]
pub(crate) enum Delivery {
    /// The emitted rows are the unit's rows.
    Direct,
    /// Programs evaluate on each emitted row:
    /// computed columns extend it, and post-filters may drop it.
    RowStage(RowStageShape),
    /// The emitted rows pass through a projection that merges or reorders them.
    Sink(ProjectionShape),
}

/// The layout of one subtracted relation.
#[derive(Debug)]
pub(crate) struct SubtractedLayout {
    /// The order the join reads the relation in: its compared markers in layer order,
    /// then the rest.
    pub(crate) order: RequiredOrder,
    /// The join layers its leading columns are compared with, strictly increasing.
    pub(crate) layers: Vec<usize>,
}

/// The columns of a unit's rows, decided once for all its members:
/// the layers the joins emit, and the markers of the top that reach the consumers.
#[derive(Debug)]
pub(super) struct UnitColumns {
    /// The number of leading layers the joins emit:
    /// one past the deepest layer delivered or read by a row stage, over every member.
    pub(super) emitted: usize,
    /// The base markers the consumers need, in the top's order.
    pub(super) base: MarkerOrder,
    /// The computed columns the consumers need, each with its index among the chain's,
    /// in the top's order.
    pub(super) computed: Vec<(Marker, usize)>,
    /// Whether a sink projects the emitted rows to the base markers.
    ///
    /// A unit needs a sink if its projection merges rows.
    /// It also needs one if it delivers base markers other than the leading layers in order,
    /// unless a row stage needs the rows in the drain's order.
    /// In that case the builds reorder the rows instead.
    pub(super) sink: bool,
}

impl UnitColumns {
    /// Decides the columns of a unit from its members' chains, which agree on their top,
    /// the layers the members descend through, and the markers of the top the consumers need.
    pub(super) fn new(chains: &[Chain], layers: &MarkerOrder, needed: &[Marker]) -> Self {
        let chain = &chains[0];
        let base = MarkerOrder::new(chain.delivered_base(needed));
        let computed = chain
            .output
            .iter()
            .filter_map(|&(marker, column)| match column {
                ChainColumn::Computed(index) if needed.contains(&marker) => Some((marker, index)),
                _ => None,
            })
            .collect();

        let layer_of = |&marker: &Marker| {
            layers
                .position(marker)
                .expect("a unit's base markers are layers")
        };
        let row_reads = chains.iter().flat_map(|chain| chain.row_reads.iter());
        let emitted = base
            .iter()
            .chain(row_reads)
            .map(|marker| layer_of(marker) + 1)
            .max()
            .unwrap_or(0);

        let sink = chain.deduplicates
            || (!chain.projections.is_empty()
                && !chain.has_row_stage()
                && layers.get(..base.len()) != Some(&base[..]));

        Self {
            emitted,
            base,
            computed,
            sink,
        }
    }

    /// Returns the columns of the unit's rows as the members produce them:
    /// the base markers if a sink projects to them,
    /// and the emitted layers followed by the delivered computed columns otherwise.
    pub(super) fn produced(&self, layers: &MarkerOrder) -> MarkerOrder {
        if self.sink {
            return self.base.clone();
        }

        layers[..self.emitted]
            .iter()
            .copied()
            .chain(self.computed.iter().map(|&(marker, _)| marker))
            .collect()
    }
}

impl LayoutBuilder<'_> {
    /// Lays out one member of a unit, a join of the given inputs running the given chain
    /// over the unit's layers, and requires what it reads.
    pub(super) fn member(
        &mut self,
        inputs: &[NodeId],
        layers: &MarkerOrder,
        chain: &Chain,
        columns: &UnitColumns,
    ) -> MemberLayout {
        let layer_of = |&marker: &Marker| layers.position(marker);

        let mut join_layers = vec![Vec::new(); layers.len()];
        let mut input_orders = Vec::with_capacity(inputs.len());
        for (index, &input) in inputs.iter().enumerate() {
            let (bound, order) = aligned(self.plan.node_markers(input), layer_of);
            for (column, &(layer, _)) in bound.iter().enumerate() {
                join_layers[layer].push(InputColumn {
                    input: index,
                    column,
                });
            }
            self.need(input, bound.iter().map(|&(_, marker)| marker));
            input_orders.push(self.require(input, order));
        }

        let mut subtracted = Vec::with_capacity(chain.subtracted.len());
        for (relation, shared) in &chain.subtracted {
            let compared =
                |marker: &Marker| shared.contains(marker).then(|| layer_of(marker)).flatten();
            let (bound, order) = aligned(self.plan.node_markers(*relation), compared);
            self.need(*relation, shared.iter().copied());
            subtracted.push(SubtractedLayout {
                order: self.require(*relation, order),
                layers: bound.iter().map(|&(layer, _)| layer).collect(),
            });
        }

        let (filters, bound_layers) = descent_conditions(&chain.filters, layer_of);
        let delivery = match (columns.sink, chain.has_row_stage()) {
            (false, false) => Delivery::Direct,
            (false, true) => {
                let (computed, post_filters) =
                    row_programs(&chain.computed, &chain.post_filters, layer_of);
                Delivery::RowStage(RowStageShape {
                    computed,
                    post_filters,
                    delivered: columns.computed.iter().map(|&(_, index)| index).collect(),
                })
            }
            (true, false) => {
                let projected = columns
                    .base
                    .iter()
                    .map(|marker| layer_of(marker).expect("projected markers are layers"))
                    .collect();
                Delivery::Sink(ProjectionShape::new(projected))
            }
            (true, true) => unreachable!("a chain with a row stage never sinks"),
        };

        MemberLayout {
            inputs: input_orders,
            layers: join_layers,
            emitted: columns.emitted,
            subtracted,
            filters,
            bound_layers,
            delivery,
        }
    }
}

/// Returns a relation's layer markers with their layers, sorted by layer,
/// and the order in which a join reads the relation:
/// its layer markers in layer order, then its other markers.
fn aligned(
    markers: &[Marker],
    layer_of: impl Fn(&Marker) -> Option<usize>,
) -> (Vec<(usize, Marker)>, MarkerOrder) {
    let mut bound: Vec<(usize, Marker)> = markers
        .iter()
        .filter_map(|&marker| layer_of(&marker).map(|layer| (layer, marker)))
        .collect();
    bound.sort_unstable_by_key(|&(layer, _)| layer);

    let unbound = markers
        .iter()
        .copied()
        .filter(|marker| layer_of(marker).is_none());
    let order = bound
        .iter()
        .map(|&(_, marker)| marker)
        .chain(unbound)
        .collect();

    (bound, order)
}

/// Returns the filters and bound layers of a member's descent, reading layers by index.
///
/// A filter requiring a layer to be the same term as a constant,
/// or as an expression over shallower layers, becomes a bound layer:
/// the join looks up that value instead of enumerating the layer.
/// Each layer is bound at most once, to a constant if it can be, since a constant needs no
/// evaluation per descent; a further binding stays a filter.
fn descent_conditions(
    conditions: &[ChainCondition<Marker>],
    layer_of: impl Fn(&Marker) -> Option<usize>,
) -> (Vec<FilterShape>, Vec<BoundLayerShape>) {
    let positions = |condition: &ChainCondition<Marker>| -> Vec<usize> {
        condition
            .inputs
            .iter()
            .map(|marker| layer_of(marker).expect("descent conditions read layers"))
            .collect()
    };
    let is_bound = |bound_layers: &[BoundLayerShape], layer: usize| {
        bound_layers.iter().any(|bound| bound.layer == layer)
    };

    let mut bound_layers: Vec<BoundLayerShape> = Vec::new();
    let mut rest = Vec::with_capacity(conditions.len());
    for condition in conditions {
        if let Some((compared, constant)) = condition.program.constant_binding() {
            let target = positions(condition)[compared];
            if !is_bound(&bound_layers, target) {
                bound_layers.push(BoundLayerShape::constant(target, constant));
                continue;
            }
        }
        rest.push(condition);
    }

    let mut filters = Vec::with_capacity(rest.len());
    for condition in rest {
        let positions = positions(condition);
        if let Some((compared, expression)) = condition.program.binding() {
            let target = positions[compared];
            let inputs: Vec<usize> = positions
                .iter()
                .enumerate()
                .filter(|&(index, _)| index != compared)
                .map(|(_, &layer)| layer)
                .collect();
            if !is_bound(&bound_layers, target) && inputs.iter().all(|&input| input < target) {
                bound_layers.push(BoundLayerShape::computed(target, inputs, expression));
                continue;
            }
        }
        filters.push(FilterShape::new(positions, condition.program.clone()));
    }

    (filters, bound_layers)
}

/// Returns the shapes of a member's computed columns and post-filters.
fn row_programs(
    computations: &[ChainComputation],
    conditions: &[ChainCondition<ChainColumn>],
    layer_of: impl Fn(&Marker) -> Option<usize>,
) -> (Vec<ComputedShape>, Vec<PostFilterShape>) {
    let row_input = |column: &ChainColumn| match *column {
        ChainColumn::Base(marker) => {
            RowInput::Layer(layer_of(&marker).expect("the row stage reads emitted layers"))
        }
        ChainColumn::Computed(index) => RowInput::Computed(index),
    };

    let mut computed: Vec<ComputedShape> = Vec::with_capacity(computations.len());
    for computation in computations {
        let inputs: Vec<RowInput> = computation.inputs.iter().map(row_input).collect();
        // Staleness and volatility reach through computed inputs.
        let deepest = inputs
            .iter()
            .filter_map(|input| match *input {
                RowInput::Layer(layer) => Some(layer),
                RowInput::Computed(column) => computed[column].deepest,
            })
            .max();
        let volatile = computation.program.is_nondeterministic()
            || inputs.iter().any(
                |input| matches!(*input, RowInput::Computed(column) if computed[column].volatile),
            );
        computed.push(ComputedShape {
            inputs,
            program: SpecializedProgram::new(computation.program.clone()),
            deepest,
            volatile,
        });
    }

    let post_filters = conditions
        .iter()
        .map(|condition| PostFilterShape {
            inputs: condition.inputs.iter().map(row_input).collect(),
            program: SpecializedProgram::new(condition.program.clone()),
        })
        .collect();

    (computed, post_filters)
}
