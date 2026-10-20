//! Chains: the operators above a unit's base, sorted into the stages they execute in.

use crate::{
    function::StackProgram,
    plan::{Marker, NodeId, OperationPlan, node::Node},
};

/// The base of a chain, as its operators see it.
#[derive(Debug, Clone, Copy)]
pub(super) enum ChainBase<'a> {
    /// A join, with the layers its descent binds.
    Join {
        /// The join's markers.
        markers: &'a [Marker],
        /// The layers, outermost first: the markers, without those that left the descent.
        layers: &'a [Marker],
    },
    /// The markers of a relation read as a join of one input,
    /// whose layers are chosen once the chain is known.
    Relation(&'a [Marker]),
}

/// A column an operator reads: a marker of the base, or the result of a computed column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ChainColumn {
    /// A marker of the base.
    Base(Marker),
    /// The result of the computed column of the given index.
    Computed(usize),
}

/// A condition of a chain on the columns it reads.
#[derive(Debug, Clone)]
pub(super) struct ChainCondition<C> {
    /// The columns feeding the program, in program input order.
    pub(super) inputs: Vec<C>,
    /// The condition.
    pub(super) program: StackProgram,
}

/// A computed column of a chain.
#[derive(Debug, Clone)]
pub(super) struct ChainComputation {
    /// The columns feeding the program, in program input order.
    pub(super) inputs: Vec<ChainColumn>,
    /// The program computing the value.
    pub(super) program: StackProgram,
    /// The marker naming the result.
    pub(super) result: Marker,
}

/// A chain of operators, sorted into the stages of a unit.
///
/// The descent holds the filters reading only base markers deterministically,
/// and the subtractions comparing only base markers.
/// The row stage holds the computed columns and the other filters,
/// evaluated on each row the descent emits.
/// The operators move by exact commutations only:
/// a filter or a subtraction passes below a computed column it does not read and below a projection.
#[derive(Debug, Clone)]
pub(super) struct Chain {
    /// The conditions checked in the descent, on base markers.
    pub(super) filters: Vec<ChainCondition<Marker>>,
    /// The subtracted relations, each with the base markers it shares, compared in the descent.
    pub(super) subtracted: Vec<(NodeId, Vec<Marker>)>,
    /// The computed columns, in chain order.
    pub(super) computed: Vec<ChainComputation>,
    /// The conditions checked on each row after the computed columns.
    pub(super) post_filters: Vec<ChainCondition<ChainColumn>>,
    /// The base markers the row stage reads on each row.
    ///
    /// A nondeterministic operator reads every base marker of its input,
    /// since it evaluates once per row of that input.
    pub(super) row_reads: Vec<Marker>,
    /// Per projection, bottom-up, the base markers it keeps, in its order.
    pub(super) projections: Vec<Vec<Marker>>,
    /// Whether a projection merges rows, so that the unit deduplicates them in a sink.
    pub(super) deduplicates: bool,
    /// The columns of the chain's top, in the top's marker order.
    pub(super) output: Vec<(Marker, ChainColumn)>,
}

impl Chain {
    /// Sorts a chain of operators above a base, listed bottom-up, into the stages of a unit.
    ///
    /// Returns the position of the first operator that cannot join the unit of those below it:
    /// - a subtraction comparing a computed column, since only the descent subtracts,
    /// - a projection merging rows above the row stage, or an operator of the row stage above it,
    ///   since the sink deduplicates storage values,
    ///   which computed values have only once the join is done.
    ///
    /// Every operator resolves its markers against its own input,
    /// so a marker a projection dropped below it is out of its scope,
    /// even if the descent still carries it as a layer.
    pub(super) fn sort(
        plan: &OperationPlan,
        base: ChainBase<'_>,
        operators: &[NodeId],
    ) -> Result<Self, usize> {
        let (markers, layers) = match base {
            ChainBase::Join { markers, layers } => (markers, Some(layers)),
            ChainBase::Relation(markers) => (markers, None),
        };
        let mut chain = Self {
            filters: Vec::new(),
            subtracted: Vec::new(),
            computed: Vec::new(),
            post_filters: Vec::new(),
            row_reads: Vec::new(),
            projections: Vec::new(),
            deduplicates: false,
            output: markers
                .iter()
                .map(|&marker| (marker, ChainColumn::Base(marker)))
                .collect(),
        };

        for (position, &operator) in operators.iter().enumerate() {
            match plan.node(operator) {
                Node::Filter { spec, .. } => {
                    let inputs = chain.resolve(&spec.inputs);
                    let descends = !spec.program.is_nondeterministic()
                        && !inputs.is_empty()
                        && inputs
                            .iter()
                            .all(|column| matches!(column, ChainColumn::Base(_)));
                    if descends {
                        chain.filters.push(ChainCondition {
                            inputs: spec.inputs.clone(),
                            program: spec.program.clone(),
                        });
                    } else {
                        chain.enter_row_stage(position, &inputs, &spec.program)?;
                        chain.post_filters.push(ChainCondition {
                            inputs,
                            program: spec.program.clone(),
                        });
                    }
                }
                Node::Compute { spec, .. } => {
                    let inputs = chain.resolve(&spec.inputs);
                    chain.enter_row_stage(position, &inputs, &spec.program)?;
                    chain
                        .output
                        .push((spec.result, ChainColumn::Computed(chain.computed.len())));
                    chain.computed.push(ChainComputation {
                        inputs,
                        program: spec.program.clone(),
                        result: spec.result,
                    });
                }
                Node::Subtract { relation, .. } => {
                    let relation_markers = plan.node_markers(*relation);
                    let mut shared = Vec::new();
                    for &(marker, column) in &chain.output {
                        if relation_markers.contains(&marker) {
                            let ChainColumn::Base(marker) = column else {
                                return Err(position);
                            };
                            shared.push(marker);
                        }
                    }
                    chain.subtracted.push((*relation, shared));
                }
                // A join of one input above an absorbed node projects to its order.
                Node::Project { markers, .. } | Node::Join { order: markers, .. } => {
                    chain.project(position, markers, layers)?;
                }
                _ => unreachable!("chains hold single-input operators"),
            }
        }

        Ok(chain)
    }

    /// Returns whether the chain evaluates programs on each emitted row.
    ///
    /// A chain with a row stage never deduplicates:
    /// sorting cuts it where a projection merging rows would meet the row stage.
    pub(super) fn has_row_stage(&self) -> bool {
        !self.computed.is_empty() || !self.post_filters.is_empty()
    }

    /// Returns the base markers among the top's columns that are needed, in the top's order.
    pub(super) fn delivered_base(&self, needed: &[Marker]) -> Vec<Marker> {
        self.output
            .iter()
            .filter_map(|&(marker, column)| match column {
                ChainColumn::Base(base) if needed.contains(&marker) => Some(base),
                _ => None,
            })
            .collect()
    }

    /// Returns the base markers the chain reads or delivers:
    /// those needed of its top, those its descent compares, and those its row stage reads.
    pub(super) fn used_markers(&self, needed: &[Marker]) -> Vec<Marker> {
        let mut used = self.delivered_base(needed);
        let read = self
            .filters
            .iter()
            .flat_map(|filter| filter.inputs.iter())
            .chain(self.subtracted.iter().flat_map(|(_, shared)| shared.iter()))
            .chain(&self.row_reads);
        for &marker in read {
            if !used.contains(&marker) {
                used.push(marker);
            }
        }

        used
    }

    /// Returns the columns the given markers name at the current top of the chain.
    fn resolve(&self, markers: &[Marker]) -> Vec<ChainColumn> {
        markers
            .iter()
            .map(|marker| {
                self.output
                    .iter()
                    .find(|(candidate, _)| candidate == marker)
                    .map(|&(_, column)| column)
                    .expect("plans validate the markers operators read")
            })
            .collect()
    }

    /// Records an operator of the row stage reading the given columns,
    /// or returns its position if a projection below it merges rows.
    fn enter_row_stage(
        &mut self,
        position: usize,
        inputs: &[ChainColumn],
        program: &StackProgram,
    ) -> Result<(), usize> {
        if self.deduplicates {
            return Err(position);
        }

        let read: Vec<Marker> = if program.is_nondeterministic() {
            self.output
                .iter()
                .filter_map(|&(_, column)| match column {
                    ChainColumn::Base(marker) => Some(marker),
                    ChainColumn::Computed(_) => None,
                })
                .collect()
        } else {
            inputs
                .iter()
                .filter_map(|&column| match column {
                    ChainColumn::Base(marker) => Some(marker),
                    ChainColumn::Computed(_) => None,
                })
                .collect()
        };
        for marker in read {
            if !self.row_reads.contains(&marker) {
                self.row_reads.push(marker);
            }
        }

        Ok(())
    }

    /// Records a projection to the given markers,
    /// or returns its position if it merges rows above the row stage.
    ///
    /// A projection merges rows if it drops a base marker the row stage reads,
    /// or, over the layers of a join, if it drops a layer above one it keeps.
    /// A marker that left the descent is not a layer, so dropping it merges nothing.
    /// Over a relation, the layers are chosen with the kept markers leading,
    /// so that only the row stage's reads can make it merge rows.
    fn project(
        &mut self,
        position: usize,
        kept: &[Marker],
        layers: Option<&[Marker]>,
    ) -> Result<(), usize> {
        let kept_columns = self.resolve(kept);
        let kept_base: Vec<Marker> = kept_columns
            .iter()
            .filter_map(|&column| match column {
                ChainColumn::Base(marker) => Some(marker),
                ChainColumn::Computed(_) => None,
            })
            .collect();
        let drops_read = self
            .row_reads
            .iter()
            .any(|marker| !kept_base.contains(marker));
        let drops_shallower = layers.is_some_and(|layers| {
            let deepest = layers.iter().rposition(|layer| kept_base.contains(layer));
            deepest.is_some_and(|deepest| {
                layers[..deepest]
                    .iter()
                    .any(|layer| !kept_base.contains(layer))
            })
        });

        if drops_read || drops_shallower {
            if self.has_row_stage() {
                return Err(position);
            }
            self.deduplicates = true;
        }
        self.output = kept.iter().copied().zip(kept_columns).collect();
        self.projections.push(kept_base);

        Ok(())
    }
}
