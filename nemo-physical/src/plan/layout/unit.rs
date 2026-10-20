//! Units: laying out the nodes that execute together.
//!
//! A unit is a base with a chain of single-input operators above it.
//! The base is a join, the joins a union unites, or a relation read as a join of one input;
//! these joins are the unit's members.
//! Each member runs the whole chain, sorted into stages:
//! - the descent checks filters and subtractions while the join binds its layers,
//! - the row stage evaluates computed columns and the remaining filters on each emitted row,
//! - a sink projects the rows if the projection merges or reorders them.
//!
//! The unit materializes the rows of its top, the last node of the chain.
//! If an operator cannot run inside the unit below it,
//! that unit ends below the operator, and the unit above reads its result.
//!
//! A join of one input plays two roles.
//! Over a load, an alias, an aggregation, or a node that other nodes read as well,
//! it is the base of a unit: the relation read as a join of one input.
//! Over a node its unit absorbs, it is an operator of the chain:
//! a projection to its order, which reorders the markers and drops some.

use crate::plan::{
    Marker, MarkerOrder, NodeId,
    layout::{
        LayoutBuilder, Step,
        chain::{Chain, ChainBase},
        member::{MemberLayout, UnitColumns},
    },
    node::Node,
};

/// The layout of one unit.
#[derive(Debug)]
pub(crate) struct UnitLayout {
    /// The unit's top, the node whose rows the unit materializes.
    pub(crate) top: NodeId,
    /// The members, each a join running the unit's chain.
    pub(crate) members: Vec<MemberLayout>,
}

/// The base of a unit, as the walk down from its top finds it.
#[derive(Debug, Clone)]
enum UnitBase {
    /// A join, or the joins a union unites, each with its own operators below the union, bottom-up.
    Members {
        /// The union, if the members are its inputs.
        union: Option<NodeId>,
        /// The joins, each with its own operators.
        members: Vec<(NodeId, Vec<NodeId>)>,
    },
    /// A relation read as a join of one input.
    Relation(NodeId),
}

impl LayoutBuilder<'_> {
    /// Lays out the unit whose top is the given node and requires what it reads,
    /// and returns whether the node is a unit's top.
    ///
    /// Every operator is. A union is only if its inputs are joins a unit can run,
    /// and passes their blocks through otherwise.
    pub(super) fn lay_out_unit(&mut self, top: NodeId) -> bool {
        let Some((mut base, mut operators)) = self.find_base(top) else {
            return false;
        };

        let (chains, layers) = loop {
            match self.sort_chains(top, &base, &operators) {
                // The operators below a cut form a unit of their own,
                // and this unit reads its result as a relation.
                Err(cut) => {
                    let below = match cut {
                        0 => chain_input(self.plan.node(operators[0])),
                        _ => Some(operators[cut - 1]),
                    };
                    base = UnitBase::Relation(below.expect("a cut operator reads one relation"));
                    operators.drain(..cut);
                }
                Ok((chains, layers)) if consistent(&chains) => break (chains, layers),
                // Members whose chains differ cannot write into one set of runs.
                // Without operators above it, the union is the top and passes its members through;
                // otherwise the operators read it as a relation, materialized by a unit of its own.
                Ok(_) if operators.is_empty() => return false,
                Ok(_) => {
                    let below = chain_input(self.plan.node(operators[0]));
                    base = UnitBase::Relation(below.expect("chain operators read one relation"));
                }
            }
        };

        self.absorb(&base, &operators);
        let unit = self.unit(top, &base, &chains, &layers);
        self.steps.push(Step::Unit(unit));

        true
    }

    /// Finds the base of the unit with the given top, and the operators above it, bottom-up.
    ///
    /// Walks down while the next node is read by the chain alone and can run inside it.
    /// Returns `None` for a union whose inputs the unit cannot run.
    fn find_base(&self, top: NodeId) -> Option<(UnitBase, Vec<NodeId>)> {
        let mut operators = Vec::new();
        let mut cursor = top;
        let base = loop {
            let node = self.plan.node(cursor);
            if matches!(node, Node::Union { .. }) {
                match self.union_members(cursor) {
                    Some(members) => {
                        break UnitBase::Members {
                            union: Some(cursor),
                            members,
                        };
                    }
                    None if cursor == top => return None,
                    None => break UnitBase::Relation(cursor),
                }
            }
            match chain_input(node) {
                Some(input) if !matches!(node, Node::Join { .. }) || self.absorbable(input) => {
                    operators.push(cursor);
                    if self.absorbable(input) {
                        cursor = input;
                    } else {
                        break UnitBase::Relation(input);
                    }
                }
                _ => {
                    break UnitBase::Members {
                        union: None,
                        members: vec![(cursor, Vec::new())],
                    };
                }
            }
        };
        operators.reverse();

        Some((base, operators))
    }

    /// Returns whether a node can run inside the unit of its only consumer.
    ///
    /// Only a node consumed once, by a node of the same unit, may execute inside that unit.
    /// An operator always can, a union if a unit can run its members,
    /// and a load, an alias or an aggregation never does.
    fn absorbable(&self, node: NodeId) -> bool {
        if !self.exclusive(node) {
            return false;
        }
        match self.plan.node(node) {
            Node::Union { .. } => self.union_members(node).is_some(),
            Node::Load { .. } | Node::Alias { .. } | Node::Aggregate(_) => false,
            _ => true,
        }
    }

    /// Returns whether a node that another node reads has no other consumer:
    /// it is live, read once, and not declared an output.
    fn exclusive(&self, node: NodeId) -> bool {
        self.live[node.index()] && self.consumers[node.index()] == 1
    }

    /// Returns the members of a union a unit can run, each a join with its own operators, bottom-up,
    /// or `None` if it cannot.
    ///
    /// Every input must be a join in the same order, below operators only the union reads,
    /// and those operators must form one unit with the join.
    /// They do so over the join's whole order if they do over fewer layers,
    /// since fewer layers can only spare a projection from merging rows.
    fn union_members(&self, union: NodeId) -> Option<Vec<(NodeId, Vec<NodeId>)>> {
        let Node::Union { inputs, .. } = self.plan.node(union) else {
            unreachable!("members are found under unions");
        };

        let mut members = Vec::with_capacity(inputs.len());
        let mut shared_order: Option<&MarkerOrder> = None;
        for &input in inputs {
            if !self.exclusive(input) {
                return None;
            }
            let mut local = Vec::new();
            let mut cursor = input;
            let join = loop {
                let node = self.plan.node(cursor);
                match (node, chain_input(node)) {
                    (Node::Join { .. }, Some(next)) if !self.exclusive_operator(next) => {
                        break cursor;
                    }
                    (Node::Join { inputs, .. }, _) if inputs.len() > 1 => break cursor,
                    (_, Some(next)) if self.exclusive_operator(next) => {
                        local.push(cursor);
                        cursor = next;
                    }
                    _ => return None,
                }
            };
            local.reverse();

            let Node::Join { order, .. } = self.plan.node(join) else {
                unreachable!("members end in joins");
            };
            if *shared_order.get_or_insert(order) != order {
                return None;
            }
            let chain_base = ChainBase::Join {
                markers: order,
                layers: order,
            };
            Chain::sort(self.plan, chain_base, &local).ok()?;
            members.push((join, local));
        }

        Some(members)
    }

    /// Returns whether a node is a join or a chain operator that only its consumer reads,
    /// as a member of a union is walked down.
    fn exclusive_operator(&self, node: NodeId) -> bool {
        self.exclusive(node)
            && matches!(
                self.plan.node(node),
                Node::Join { .. }
                    | Node::Filter { .. }
                    | Node::Compute { .. }
                    | Node::Subtract { .. }
                    | Node::Project { .. }
            )
    }

    /// Sorts the chain of every member and chooses the layers the members descend through,
    /// or returns the position among the operators of the lowest cut any member needs.
    fn sort_chains(
        &self,
        top: NodeId,
        base: &UnitBase,
        operators: &[NodeId],
    ) -> Result<(Vec<Chain>, MarkerOrder), usize> {
        let needed = &self.needed[top.index()];
        match base {
            UnitBase::Relation(relation) => {
                let markers = self.plan.node_markers(*relation);
                let chain = Chain::sort(self.plan, ChainBase::Relation(markers), operators)?;
                let layers = self.relation_layers(markers, &chain, needed, top);
                Ok((vec![chain], layers))
            }
            UnitBase::Members { members, .. } => {
                let order = self.plan.node_markers(members[0].0);
                let chained: Vec<Vec<NodeId>> = members
                    .iter()
                    .map(|(_, local)| [local.as_slice(), operators].concat())
                    .collect();
                let layers = self.descent_layers(members, &chained, needed);
                let chain_base = ChainBase::Join {
                    markers: order,
                    layers: &layers,
                };

                let mut chains = Vec::with_capacity(members.len());
                let mut cut: Option<usize> = None;
                for ((_, local), all) in members.iter().zip(&chained) {
                    match Chain::sort(self.plan, chain_base, all) {
                        Ok(chain) => chains.push(chain),
                        Err(position) => {
                            // A member's own operators sorted over every layer when the member was found,
                            // so they sort over the unit's layers as well: the cut lies among the shared operators.
                            debug_assert!(
                                position >= local.len(),
                                "a member's own operators form one unit with its join"
                            );
                            let position = position - local.len();
                            cut = Some(cut.map_or(position, |cut| cut.min(position)));
                        }
                    }
                }
                match cut {
                    Some(cut) => Err(cut),
                    None => Ok((chains, layers)),
                }
            }
        }
    }

    /// Chooses the layers the members of a unit descend through, given each member's operators:
    /// the order of their joins, without the markers that leave the descent.
    ///
    /// A marker leaves the descent if nothing in the unit reads it,
    /// and if in every member exactly one input carries it,
    /// as that input's deepest marker in the order.
    /// The markers are decided from the deepest up,
    /// so a marker leaves only after its input's deeper markers have left.
    ///
    /// The join never binds a marker that left.
    /// Wherever a row continues the bound values, the input's trie holds a value below them,
    /// and the input is read with the same leading columns as with the marker bound.
    fn descent_layers(
        &self,
        members: &[(NodeId, Vec<NodeId>)],
        chained: &[Vec<NodeId>],
        needed: &[Marker],
    ) -> MarkerOrder {
        let order = self.plan.node_markers(members[0].0);

        // Which markers a chain reads does not depend on the layers,
        // so sorting the chains without layers finds them.
        let mut used: Vec<Marker> = Vec::new();
        for operators in chained {
            let Ok(chain) = Chain::sort(self.plan, ChainBase::Relation(order), operators) else {
                return order.clone();
            };
            for marker in chain.used_markers(needed) {
                if !used.contains(&marker) {
                    used.push(marker);
                }
            }
        }

        let position = |&marker: &Marker| {
            order
                .position(marker)
                .expect("a join's order lists its inputs' markers")
        };
        let mut bound = vec![true; order.len()];
        for (depth, marker) in order.iter().enumerate().rev() {
            if used.contains(marker) {
                continue;
            }
            let leaves = members.iter().all(|&(join, _)| {
                let Node::Join { inputs, .. } = self.plan.node(join) else {
                    unreachable!("members are joins");
                };
                let mut carriers = inputs
                    .iter()
                    .map(|&input| self.plan.node_markers(input))
                    .filter(|markers| markers.contains(marker));
                let (Some(carried), None) = (carriers.next(), carriers.next()) else {
                    return false;
                };

                carried.iter().any(|other| position(other) < depth)
                    && carried.iter().all(|other| {
                        let other = position(other);
                        other <= depth || !bound[other]
                    })
            });
            bound[depth] = !leaves;
        }

        order
            .iter()
            .zip(&bound)
            .filter_map(|(&marker, &bound)| bound.then_some(marker))
            .collect()
    }

    /// Chooses the layers of a join of one input over a relation:
    /// the markers the chain uses, those its projections keep leading.
    ///
    /// The last projection's kept markers come first, in its order,
    /// then those each earlier projection keeps besides.
    /// Without a projection, the top's first required order leads instead,
    /// so the unit's rows arrive in the order its first consumer reads.
    /// The other markers the chain uses follow, in the relation's order.
    fn relation_layers(
        &self,
        markers: &[Marker],
        chain: &Chain,
        needed: &[Marker],
        top: NodeId,
    ) -> MarkerOrder {
        let used = chain.used_markers(needed);

        let mut layers: Vec<Marker> = Vec::with_capacity(used.len());
        let lead = |markers: &[Marker], layers: &mut Vec<Marker>| {
            for marker in markers {
                if used.contains(marker) && !layers.contains(marker) {
                    layers.push(*marker);
                }
            }
        };
        if chain.projections.is_empty() {
            if let Some(order) = self.required[top.index()].first() {
                lead(order, &mut layers);
            }
        } else {
            for kept in chain.projections.iter().rev() {
                lead(kept, &mut layers);
            }
        }
        lead(markers, &mut layers);

        // A unit reading nothing of its relation still needs a layer to find out whether a row exists,
        // unless the relation has no column; then its only possible row is the empty one.
        if layers.is_empty()
            && let Some(&first) = markers.first()
        {
            layers.push(first);
        }

        MarkerOrder::new(layers)
    }

    /// Marks the nodes a unit runs below its top, which execute with it.
    fn absorb(&mut self, base: &UnitBase, operators: &[NodeId]) {
        for &operator in operators {
            self.absorbed[operator.index()] = true;
        }
        if let UnitBase::Members { union, members } = base {
            if let Some(union) = union {
                self.absorbed[union.index()] = true;
            }
            for (join, local) in members {
                self.absorbed[join.index()] = true;
                for &operator in local {
                    self.absorbed[operator.index()] = true;
                }
            }
        }
    }

    /// Lays out a unit from its sorted chains and the layers its members descend through,
    /// requires what its members read, and records what it materializes.
    fn unit(
        &mut self,
        top: NodeId,
        base: &UnitBase,
        chains: &[Chain],
        layers: &MarkerOrder,
    ) -> UnitLayout {
        let plan = self.plan;
        let columns = UnitColumns::new(chains, layers, &self.needed[top.index()]);

        let members = match base {
            UnitBase::Members { members, .. } => members
                .iter()
                .zip(chains)
                .map(|((join, _), chain)| {
                    let Node::Join { inputs, .. } = plan.node(*join) else {
                        unreachable!("members are joins");
                    };
                    self.member(inputs, layers, chain, &columns)
                })
                .collect(),
            UnitBase::Relation(relation) => {
                vec![self.member(&[*relation], layers, &chains[0], &columns)]
            }
        };

        let produced = columns.produced(layers);
        self.materializations[top.index()] = Some(self.materialization(top, produced));

        UnitLayout { top, members }
    }
}

/// Returns the input of an operator a chain can run:
/// a filter, a computed column, a subtraction, a projection, or a join of one input.
///
/// No other node is a chain operator, a join of several inputs included.
fn chain_input(node: &Node) -> Option<NodeId> {
    match node {
        Node::Filter { input, .. }
        | Node::Compute { input, .. }
        | Node::Subtract { input, .. }
        | Node::Project { input, .. } => Some(*input),
        Node::Join { inputs, .. } => match inputs.as_slice() {
            [input] => Some(*input),
            _ => None,
        },
        _ => None,
    }
}

/// Returns whether the chains of a unit's members produce their rows alike,
/// so that their runs build one relation.
fn consistent(chains: &[Chain]) -> bool {
    let [first, rest @ ..] = chains else {
        return true;
    };
    let results = |chain: &Chain| -> Vec<Marker> {
        chain
            .computed
            .iter()
            .map(|computation| computation.result)
            .collect()
    };

    rest.iter().all(|chain| {
        chain.output == first.output
            && chain.deduplicates == first.deduplicates
            && chain.has_row_stage() == first.has_row_stage()
            && chain.projections.last() == first.projections.last()
            && results(chain) == results(first)
    })
}
