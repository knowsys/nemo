//! The layout of a plan: what executing it needs, derived once from the plan's structure.
//!
//! A consumer reads a node in a column order, a sequence of the node's markers;
//! the node's canonical order is the order of its markers.
//! The layout collects the orders each node is read in, its required orders.
//! A load is read from the table manager in each of them.
//! A materialized node produces only the columns its consumers need, in an order of its own,
//! and builds one trie from its rows per required order.
//!
//! The layout also lists the steps of an execution.
//! A step is a unit, an aggregation,
//! or an output that only merges the blocks of a load, alias or union.
//! A unit is a join, or the joins a union unites,
//! together with the single-input operators above it, which run inside the joins.
//!
//! Terms used throughout the layout, each defined in its module:
//! - a *unit* executes a *base* and the *chain* of single-input operators above it,
//!   and materializes the rows of its *top*, the last node of the chain;
//! - the base is a join, the joins a union unites, or a relation read as a join of one input,
//!   and each of these joins is a *member*, which runs the whole chain;
//! - a unit *absorbs* a node that executes inside it instead of being materialized,
//!   and ends below an operator that cannot run inside it;
//! - a member runs its chain in *stages*: the *descent* while its join binds the layers,
//!   the *row stage* on each emitted row, and a *sink* that projects the rows;
//! - a member's *delivery* says what becomes of the rows its join emits;
//! - a marker *leaves* the descent if the join need not bind it.

mod aggregate;
mod chain;
mod member;
mod unit;

pub(crate) use self::{
    aggregate::{AggregateLayout, AggregatePass, ScanLayout},
    member::{Delivery, MemberLayout},
    unit::UnitLayout,
};

use std::fmt;

use crate::{
    management::{ColumnOrder, TableId},
    plan::{Marker, MarkerOrder, NodeId, OperationPlan, node::Node},
};

/// The layout of a plan: what an execution reads, joins and builds,
/// worked out once from the plan's structure.
///
/// The layout is valid as long as the plan's structure is. See [OperationPlan::layout].
///
/// # Example
///
/// ```
/// use nemo_physical::{
///     dictionary::Dictionary,
///     function::EvaluationContext,
///     management::{ColumnOrder, Revision, TableManager},
///     plan::{JoinArena, OperationPlan},
///     storage::StorageValue,
///     trie::RowBuffer,
/// };
///
/// // Edges that arrive in batches, one revision each.
/// let mut manager = TableManager::new();
/// let edge = manager.register("edge", 2);
/// for (revision, (from, to)) in (1..).zip([(1, 2), (2, 3), (3, 4)]) {
///     let mut rows = RowBuffer::new(2);
///     rows.push_row(&[StorageValue::integer(from), StorageValue::integer(to)]);
///     manager.add_rows(edge, Revision(revision), ColumnOrder::identity(2), rows);
/// }
///
/// // The paths of two edges whose first edge is new, laid out once
/// // and executed for each batch with the same layout and arena.
/// let mut plan = OperationPlan::new();
/// let [x, y, z] = plan.markers();
/// let new = plan.load(edge, Revision(1)..Revision(2), vec![x, y]);
/// let all = plan.load(edge, Revision(0)..Revision(4), vec![y, z]);
/// let paths = plan.join(vec![new, all], vec![x, y, z]);
/// plan.output(paths);
///
/// let layout = plan.layout();
/// let arena = JoinArena::new();
/// let mut dictionary = Dictionary::new();
/// let mut found = Vec::new();
/// for revision in 1..=3 {
///     plan.set_load_range(new, Revision(revision)..Revision(revision + 1));
///     let context = EvaluationContext::now();
///     let results =
///         plan.execute_with_layout(&layout, &mut manager, &mut dictionary, context, &arena);
///     found.push(results.outputs[0].row_count());
/// }
/// assert_eq!(found, [1, 1, 0]);
/// ```
#[derive(Debug)]
pub struct PlanLayout {
    /// Per node, whether an output depends on it.
    pub(crate) live: Vec<bool>,
    /// Per node and required order, where a read in that order finds its blocks;
    /// a [RequiredOrder] indexes them.
    pub(crate) sources: Vec<Vec<Vec<BlockSource>>>,
    /// Per materializing node, what its materialization builds.
    pub(crate) materializations: Vec<Option<Materialization>>,
    /// The steps of an execution, in topological order.
    pub(crate) steps: Vec<Step>,
    /// Per step, the materialized results it is the last to read.
    ///
    /// The execution drops them once the step has run.
    /// Declared outputs are never listed, since their rows leave with the results.
    pub(crate) released: Vec<Vec<NodeId>>,
}

impl PlanLayout {
    /// Lays out a plan.
    ///
    /// The walk visits the nodes in reverse topological order,
    /// so every consumer of a node has stated what it reads before the node is laid out.
    pub(crate) fn new(plan: &OperationPlan) -> Self {
        let mut builder = LayoutBuilder::new(plan);
        for index in (0..plan.nodes().len()).rev() {
            builder.visit(NodeId::new(index));
        }

        builder.finish()
    }

    /// Returns the number of members of the unit with the given top,
    /// or `None` if no unit has that top.
    #[cfg(test)]
    pub(crate) fn unit_members(&self, top: NodeId) -> Option<usize> {
        self.steps.iter().find_map(|step| match step {
            Step::Unit(unit) if unit.top == top => Some(unit.members.len()),
            _ => None,
        })
    }
}

/// One step of an execution.
#[derive(Debug)]
pub(crate) enum Step {
    /// An execution unit, materializing its top.
    Unit(UnitLayout),
    /// An aggregation, reading its input group key first.
    Aggregate(AggregateLayout),
    /// A load, alias or union declared as an output: its blocks merged into one trie.
    Concatenate {
        /// The node.
        node: NodeId,
        /// Its canonical order, among its required orders.
        order: RequiredOrder,
    },
}

impl Step {
    /// Returns the node the step executes, whose result it delivers.
    pub(crate) fn node(&self) -> NodeId {
        match self {
            Step::Unit(unit) => unit.top,
            Step::Aggregate(aggregate) => aggregate.node,
            Step::Concatenate { node, .. } => *node,
        }
    }
}

/// What a materializing node builds from its rows.
///
/// The rows arrive in the produced order, and a trie is built per order a consumer reads:
/// directly from the rows if that is the produced order,
/// and from a sorted permutation otherwise.
/// No trie is built in the produced order unless a consumer reads it.
#[derive(Debug)]
pub(crate) struct Materialization {
    /// The columns of the node's rows as it produces them, in order.
    pub(crate) produced: MarkerOrder,
    /// The orders the node's tries are built in, each over the produced columns, without repeats.
    pub(crate) orders: Vec<MarkerOrder>,
    /// Per order, the column order its build lays the produced columns out in;
    /// `None` for the produced order, whose rows build as they are.
    pub(crate) builds: Vec<Option<ColumnOrder>>,
    /// For a declared output, the index of the order its trie is delivered in, the node's markers.
    pub(crate) output: Option<usize>,
}

impl Materialization {
    /// Creates the materialization of rows produced in the given columns
    /// for the given required orders, and for a declared output,
    /// for the order its trie is delivered in, the node's markers.
    fn new(produced: MarkerOrder, required: &[MarkerOrder], output: Option<&MarkerOrder>) -> Self {
        let mut orders: Vec<MarkerOrder> = Vec::with_capacity(required.len());
        for order in required {
            let order = order.restricted_to(&produced);
            if !orders.contains(&order) {
                orders.push(order);
            }
        }
        let output = output.map(|delivered| {
            let consumed = orders.iter().position(|order| order == delivered);
            consumed.unwrap_or_else(|| {
                orders.push(delivered.clone());
                orders.len() - 1
            })
        });
        let builds = orders
            .iter()
            .map(|order| (order != &produced).then(|| produced.column_order_to(order)))
            .collect();

        Self {
            produced,
            orders,
            builds,
            output,
        }
    }

    /// Returns the index of the trie a read in the given order receives.
    fn trie(&self, order: &MarkerOrder) -> usize {
        let order = order.restricted_to(&self.produced);
        self.orders
            .iter()
            .position(|candidate| *candidate == order)
            .expect("consumed orders were required")
    }
}

/// Where a read of a node in one of its required orders finds its blocks,
/// once aliases and the unions passing their inputs through are resolved.
#[derive(Debug, Clone)]
pub(crate) enum BlockSource {
    /// A load of a stored table.
    ///
    /// The range of revisions is the load node's at execution time.
    Load {
        /// The load node.
        node: NodeId,
        /// The table it reads.
        table: TableId,
        /// The order of the table's columns that lays them out in the required order.
        order: ColumnOrder,
    },
    /// A trie of a materialized node.
    Materialized {
        /// The materializing node.
        node: NodeId,
        /// The index of its trie.
        trie: usize,
    },
}

/// One of a node's required orders, which a consumer reads the node in.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RequiredOrder {
    /// The node.
    pub(crate) node: NodeId,
    /// The index of the order among the node's required orders.
    pub(crate) index: usize,
}

/// The layout under construction: the walk over the nodes, consumers first.
#[derive(Debug)]
pub(super) struct LayoutBuilder<'a> {
    /// The plan.
    plan: &'a OperationPlan,
    /// Per node, whether an output depends on it.
    live: Vec<bool>,
    /// Per node, whether it is a declared output.
    is_output: Vec<bool>,
    /// Per node, how many live nodes and declared outputs consume it.
    consumers: Vec<usize>,
    /// Per node, whether it runs inside a unit whose top is another node.
    absorbed: Vec<bool>,
    /// Per node, the markers its consumers need the values of.
    needed: Vec<Vec<Marker>>,
    /// Per node, the column orders its consumers read.
    required: Vec<Vec<MarkerOrder>>,
    /// Per materializing node, what its materialization builds.
    materializations: Vec<Option<Materialization>>,
    /// The steps, in reverse topological order.
    steps: Vec<Step>,
}

impl<'a> LayoutBuilder<'a> {
    /// Starts the layout of a plan, with every declared output needing all of its markers.
    fn new(plan: &'a OperationPlan) -> Self {
        let count = plan.nodes().len();
        let mut is_output = vec![false; count];
        for output in plan.declared_outputs() {
            is_output[output.index()] = true;
        }

        let live = liveness(plan);
        let consumers = consumer_counts(plan, &live);

        let mut builder = Self {
            plan,
            live,
            is_output,
            consumers,
            absorbed: vec![false; count],
            needed: vec![Vec::new(); count],
            required: vec![Vec::new(); count],
            materializations: (0..count).map(|_| None).collect(),
            steps: Vec::new(),
        };

        for &output in plan.declared_outputs() {
            builder.need(output, plan.node_markers(output).iter().copied());
        }

        builder
    }

    /// Lays out one node, unless a unit above runs it.
    ///
    /// A load, an alias, and a union passing its inputs through
    /// pass the orders and needs of their consumers on.
    /// Declared as an output, they are concatenated.
    fn visit(&mut self, node: NodeId) {
        let index = node.index();
        if !self.live[index] || self.absorbed[index] {
            return;
        }

        match self.plan.node(node) {
            Node::Aggregate(aggregate) => self.lay_out_aggregate(node, aggregate),
            Node::Load { .. } | Node::Alias { .. } => self.pass_through(node),
            Node::Union { .. } => {
                if !self.lay_out_unit(node) {
                    self.pass_through(node);
                }
            }
            _ => {
                let laid_out = self.lay_out_unit(node);
                debug_assert!(laid_out, "operators are unit tops");
            }
        }
    }

    /// Lays out a node that passes the blocks of its inputs through:
    /// its consumers' orders and needs go on to its inputs.
    fn pass_through(&mut self, node: NodeId) {
        let index = node.index();
        if self.is_output[index] {
            let order = self.require(node, self.plan.node_markers(node).clone());
            self.steps.push(Step::Concatenate { node, order });
        }

        let orders = self.required[index].clone();
        let needed = self.needed[index].clone();
        match self.plan.node(node) {
            Node::Alias { input, markers } => {
                let input_markers = self.plan.node_markers(*input);
                for order in &orders {
                    self.require(*input, markers.translate(order, input_markers));
                }
                self.need(*input, markers.translate(&needed, input_markers));
            }
            Node::Union { inputs, .. } => {
                for &input in inputs {
                    for order in &orders {
                        self.require(input, order.clone());
                    }
                    self.need(input, needed.iter().copied());
                }
            }
            _ => {}
        }
    }

    /// Records a required order of a node, unless it is recorded already, and returns it.
    fn require(&mut self, node: NodeId, order: MarkerOrder) -> RequiredOrder {
        let entry = &mut self.required[node.index()];
        let position = match entry.iter().position(|candidate| *candidate == order) {
            Some(position) => position,
            None => {
                entry.push(order);
                entry.len() - 1
            }
        };

        RequiredOrder {
            node,
            index: position,
        }
    }

    /// Records that a consumer needs the values of the given markers of a node.
    fn need(&mut self, node: NodeId, markers: impl IntoIterator<Item = Marker>) {
        let entry = &mut self.needed[node.index()];
        for marker in markers {
            if !entry.contains(&marker) {
                entry.push(marker);
            }
        }
    }

    /// Returns what a materializing node builds from its rows, produced in the given columns:
    /// a trie per order its consumers read, and for a declared output,
    /// one in the order it is delivered in, the node's markers.
    fn materialization(&self, node: NodeId, produced: MarkerOrder) -> Materialization {
        let output = self.is_output[node.index()].then(|| self.plan.node_markers(node));
        Materialization::new(produced, &self.required[node.index()], output)
    }

    /// Finishes the layout once every node is visited:
    /// puts the steps in execution order, resolves every required order to its sources,
    /// and finds the step that releases each materialized result.
    fn finish(mut self) -> PlanLayout {
        self.steps.reverse();
        let sources = self.resolve_sources();
        let released = self.release_after_last_use();

        PlanLayout {
            live: self.live,
            sources,
            materializations: self.materializations,
            steps: self.steps,
            released,
        }
    }

    /// Resolves every required order of every live node to its sources.
    fn resolve_sources(&self) -> Vec<Vec<Vec<BlockSource>>> {
        (0..self.plan.nodes().len())
            .map(|index| {
                if !self.live[index] {
                    return Vec::new();
                }

                self.required[index]
                    .iter()
                    .map(|order| {
                        let mut resolved = Vec::new();
                        self.resolve(NodeId::new(index), order, &mut resolved);
                        resolved
                    })
                    .collect()
            })
            .collect()
    }

    /// Resolves a read of a node in an order to its sources.
    ///
    /// A load resolves to itself with the order of its columns,
    /// a materializing node to its trie for the order,
    /// and an alias or a union passing its inputs through to the sources of its inputs.
    fn resolve(&self, node: NodeId, order: &MarkerOrder, into: &mut Vec<BlockSource>) {
        if let Some(materialization) = &self.materializations[node.index()] {
            into.push(BlockSource::Materialized {
                node,
                trie: materialization.trie(order),
            });
            return;
        }

        match self.plan.node(node) {
            Node::Load { table, markers, .. } => into.push(BlockSource::Load {
                node,
                table: *table,
                order: markers.column_order_to(order),
            }),
            Node::Alias { input, markers } => {
                let translated = markers.translate(order, self.plan.node_markers(*input));
                self.resolve(*input, &translated, into);
            }
            Node::Union { inputs, .. } => {
                for &input in inputs {
                    self.resolve(input, order, into);
                }
            }
            _ => unreachable!("every other live node materializes"),
        }
    }

    /// Returns, per step, the materialized results it is the last to read,
    /// which it releases once it has run; declared outputs are never released.
    ///
    /// The steps are walked in execution order, so the last reader wins.
    fn release_after_last_use(&self) -> Vec<Vec<NodeId>> {
        let mut last_use: Vec<Option<usize>> = vec![None; self.plan.nodes().len()];
        for (position, step) in self.steps.iter().enumerate() {
            let mut read = Vec::new();
            match step {
                Step::Unit(unit) => {
                    for member in &unit.members {
                        for input in &member.inputs {
                            self.materialized_sources(input.node, &mut read);
                        }
                        for subtracted in &member.subtracted {
                            self.materialized_sources(subtracted.order.node, &mut read);
                        }
                    }
                }
                Step::Aggregate(aggregate) => {
                    self.materialized_sources(aggregate.input.node, &mut read);
                }
                Step::Concatenate { node, .. } => self.materialized_sources(*node, &mut read),
            }
            for source in read {
                last_use[source.index()] = Some(position);
            }
        }

        let mut released = vec![Vec::new(); self.steps.len()];
        for (source, last) in last_use.iter().enumerate() {
            if let Some(reader) = last
                && !self.is_output[source]
            {
                released[*reader].push(NodeId::new(source));
            }
        }

        released
    }

    /// Collects the materializing nodes a read of a node reaches through aliases and unions,
    /// the results the reader consumes.
    fn materialized_sources(&self, node: NodeId, into: &mut Vec<NodeId>) {
        if self.materializations[node.index()].is_some() {
            into.push(node);
            return;
        }

        match self.plan.node(node) {
            Node::Alias { input, .. } => self.materialized_sources(*input, into),
            Node::Union { inputs, .. } => {
                for &input in inputs {
                    self.materialized_sources(input, into);
                }
            }
            _ => {}
        }
    }
}

impl fmt::Display for PlanLayout {
    /// Lists the steps of an execution, a unit with its members and the stages of their operators.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for step in &self.steps {
            match step {
                Step::Unit(unit) => {
                    let produced = self.materializations[unit.top.index()]
                        .as_ref()
                        .map_or(0, |materialization| materialization.produced.len());
                    writeln!(
                        f,
                        "unit at node {}: {} member(s), {} column(s)",
                        unit.top.0,
                        unit.members.len(),
                        produced
                    )?;
                    for member in &unit.members {
                        let (computed, post_filters) = match &member.delivery {
                            Delivery::RowStage(stage) => {
                                (stage.computed.len(), stage.post_filters.len())
                            }
                            Delivery::Direct | Delivery::Sink(_) => (0, 0),
                        };
                        let sink = matches!(member.delivery, Delivery::Sink(_));
                        writeln!(
                            f,
                            "  join of {} input(s), {} layer(s), {} emitted; descent: {} filter(s), {} bound layer(s), {} subtraction(s); rows: {} computed, {} filter(s); sink: {}",
                            member.inputs.len(),
                            member.layers.len(),
                            member.emitted,
                            member.filters.len(),
                            member.bound_layers.len(),
                            member.subtracted.len(),
                            computed,
                            post_filters,
                            if sink { "yes" } else { "no" },
                        )?;
                    }
                }
                Step::Aggregate(aggregate) => {
                    let pass = match aggregate.pass {
                        AggregatePass::CountRows => "count of rows",
                        AggregatePass::Scan(_) => "one pass",
                    };
                    writeln!(f, "aggregate at node {}: {}", aggregate.node.0, pass)?;
                }
                Step::Concatenate { node, .. } => writeln!(f, "output at node {}", node.0)?,
            }
        }

        Ok(())
    }
}

/// Returns, per node, whether a declared output depends on it.
fn liveness(plan: &OperationPlan) -> Vec<bool> {
    let mut live = vec![false; plan.nodes().len()];
    let mut pending: Vec<NodeId> = plan.declared_outputs().to_vec();
    while let Some(node) = pending.pop() {
        if std::mem::replace(&mut live[node.index()], true) {
            continue;
        }
        pending.extend(plan.node(node).inputs());
    }

    live
}

/// Returns, per node, how many live nodes and declared outputs consume it.
///
/// A declared output counts as a consumer, so an output is always a unit's top.
fn consumer_counts(plan: &OperationPlan, live: &[bool]) -> Vec<usize> {
    let mut consumers = vec![0usize; plan.nodes().len()];
    for output in plan.declared_outputs() {
        consumers[output.index()] += 1;
    }
    for (index, node) in plan.nodes().iter().enumerate() {
        if live[index] {
            for input in node.inputs() {
                consumers[input.index()] += 1;
            }
        }
    }

    consumers
}
