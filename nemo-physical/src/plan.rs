//! Operation plans: descriptions of database computations.
//!
//! An [OperationPlan] is a graph of nodes, each an operation producing a relation
//! whose columns are named by [markers](Marker):
//! - a [load](OperationPlan::load) reads a range of revisions of a stored table,
//! - an [alias](OperationPlan::alias) renames the columns of a relation,
//! - a [union](OperationPlan::union) unites relations,
//! - a [join](OperationPlan::join) joins relations on their shared markers,
//! - a [filter](OperationPlan::filter) keeps the rows for which a condition holds,
//! - a [computed column](OperationPlan::compute) extends every row by a computed value,
//! - a [subtraction](OperationPlan::subtract) drops the rows another relation matches,
//! - a [projection](OperationPlan::project) keeps some of the columns of a relation,
//! - an [aggregation](OperationPlan::aggregate) aggregates the rows of a relation by group.
//!
//! Some nodes are declared [outputs](OperationPlan::output),
//! and [executing](OperationPlan::execute) the plan delivers their rows.
//! A plan with one output can also be executed only until that output
//! [has a row](OperationPlan::find_row) that the caller accepts.

mod error;
mod execute;
mod layout;
mod marker;
mod marker_order;
mod node;
mod spec;

pub use self::{
    error::{PlanError, codes},
    execute::{PhaseTimings, PlanResults, WorkCounts},
    layout::PlanLayout,
    marker::Marker,
    node::NodeId,
    spec::{AggregateColumnSpec, AggregateFunction, ComputedColumnSpec, Expression, FilterSpec},
};

pub(crate) use self::marker_order::MarkerOrder;

use std::ops::Range;

pub use crate::execution::{JoinArena, Verdict};

use crate::{
    dictionary::Dictionary,
    function::EvaluationContext,
    management::{Revision, TableId, TableManager},
    plan::{
        execute::Execution,
        node::{AggregateNode, Node},
    },
    storage::StorageValue,
};

/// A plan of database operations: a graph of nodes, each producing a relation.
///
/// Each node constructor checks the node against the plan,
/// and panics if the node breaks one of the requirements its `# Panics` section lists.
/// Every constructor requires that each node id it is given names a node of this plan,
/// and that the markers of a new relation are allocated from this plan and pairwise distinct.
///
/// # Example
///
/// ```
/// use nemo_physical::{
///     dictionary::Dictionary,
///     function::EvaluationContext,
///     management::{ColumnOrder, Revision, TableManager},
///     plan::OperationPlan,
///     storage::StorageValue,
///     trie::RowBuffer,
/// };
///
/// // The edges 1 → 2 → 3, stored at revision 1.
/// let mut manager = TableManager::new();
/// let edge = manager.register("edge", 2);
/// let mut rows = RowBuffer::new(2);
/// for (from, to) in [(1, 2), (2, 3)] {
///     rows.push_row(&[StorageValue::integer(from), StorageValue::integer(to)]);
/// }
/// manager.add_rows(edge, Revision(1), ColumnOrder::identity(2), rows);
///
/// // The paths of two edges: edge(x, y) joined with edge(y, z).
/// let mut plan = OperationPlan::new();
/// let [x, y, z] = plan.markers();
/// let first = plan.load(edge, Revision(0)..Revision(2), vec![x, y]);
/// let second = plan.load(edge, Revision(0)..Revision(2), vec![y, z]);
/// let paths = plan.join(vec![first, second], vec![x, y, z]);
/// plan.output(paths);
///
/// let mut dictionary = Dictionary::new();
/// let results = plan.execute(&mut manager, &mut dictionary, EvaluationContext::now());
/// assert_eq!(results.outputs[0].row_count(), 1);
/// ```
#[derive(Debug, Default)]
pub struct OperationPlan {
    /// The nodes, in the order they were added.
    nodes: Vec<Node>,
    /// The declared outputs.
    outputs: Vec<NodeId>,
    /// The number of allocated markers.
    next_marker: u32,
}

impl OperationPlan {
    /// Creates an empty plan.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocates a fresh marker.
    pub fn marker(&mut self) -> Marker {
        let marker = Marker(self.next_marker);
        self.next_marker += 1;

        marker
    }

    /// Allocates several fresh markers at once, as in `let [x, y, z] = plan.markers();`.
    pub fn markers<const N: usize>(&mut self) -> [Marker; N] {
        [(); N].map(|()| self.marker())
    }

    /// Adds a node reading a range of revisions of a stored table.
    ///
    /// Marker `i` names the table's column `i`.
    /// When the plan executes, the table must have one column per marker,
    /// and the range must align with the table's blocks.
    ///
    /// # Panics
    ///
    /// Panics if a marker is not allocated from this plan or repeats.
    pub fn load(&mut self, table: TableId, range: Range<Revision>, markers: Vec<Marker>) -> NodeId {
        let markers = self.checked_order(markers);

        self.push(Node::Load {
            table,
            range,
            markers,
        })
    }

    /// Adds a node renaming the columns of a relation.
    ///
    /// Marker `i` renames the input's column `i`.
    /// An alias costs nothing. It lets a join read one relation under two sets of markers.
    ///
    /// # Panics
    ///
    /// Panics if the input is not a node of this plan,
    /// if a marker is not allocated from this plan or repeats,
    /// or if the markers do not rename every column of the input.
    pub fn alias(&mut self, input: NodeId, markers: Vec<Marker>) -> NodeId {
        let markers = self.checked_order(markers);
        assert_eq!(
            markers.len(),
            self.checked_markers(input).len(),
            "an alias must rename every column"
        );

        self.push(Node::Alias { input, markers })
    }

    /// Adds a node uniting relations that have the same set of markers, each in any order.
    ///
    /// # Panics
    ///
    /// Panics if there is no input, if an input is not a node of this plan,
    /// or if the inputs have different sets of markers.
    pub fn union(&mut self, inputs: Vec<NodeId>) -> NodeId {
        let [first, rest @ ..] = inputs.as_slice() else {
            panic!("a union needs at least one input");
        };
        let markers = self.checked_markers(*first).clone();

        let mut sorted = markers.to_vec();
        sorted.sort_unstable();
        for &input in rest {
            let mut other = self.checked_markers(input).to_vec();
            other.sort_unstable();
            assert_eq!(other, sorted, "united relations must share one marker set");
        }

        self.push(Node::Union { inputs, markers })
    }

    /// Adds a node joining relations on their shared markers.
    ///
    /// `order` lists the markers of the result, in the order the join binds them.
    /// A relation without columns holds the empty row or no row,
    /// so the join keeps the rows of the other inputs if it holds the empty row, and none
    /// otherwise.
    ///
    /// # Panics
    ///
    /// Panics if there is no input, if an input is not a node of this plan,
    /// if a marker of the order is not allocated from this plan or repeats,
    /// or if the order does not list exactly the markers of the inputs.
    pub fn join(&mut self, inputs: Vec<NodeId>, order: Vec<Marker>) -> NodeId {
        let order = self.checked_order(order);
        self.check_join_inputs(&inputs, &order);

        self.push(Node::Join { inputs, order })
    }

    /// Adds a node keeping the rows of a relation for which a condition holds.
    ///
    /// # Panics
    ///
    /// Panics if the input is not a node of this plan,
    /// or if the condition reads a marker the input lacks.
    pub fn filter(&mut self, input: NodeId, spec: FilterSpec) -> NodeId {
        let markers = self.checked_markers(input).clone();
        assert!(
            spec.inputs.iter().all(|marker| markers.contains(marker)),
            "a filter can only read its input's markers"
        );

        self.push(Node::Filter {
            input,
            spec,
            markers,
        })
    }

    /// Adds a node extending every row of a relation by a [computed column](ComputedColumnSpec).
    ///
    /// # Panics
    ///
    /// Panics if the input is not a node of this plan,
    /// if the column reads a marker the input lacks,
    /// or if its result marker is not allocated from this plan or is one of the input's markers.
    pub fn compute(&mut self, input: NodeId, spec: ComputedColumnSpec) -> NodeId {
        let available = self.checked_markers(input);
        assert!(
            spec.inputs.iter().all(|marker| available.contains(marker)),
            "a computed column can only read its input's markers"
        );
        assert!(
            !available.contains(&spec.result),
            "a computed result needs a fresh marker"
        );

        let mut markers = available.to_vec();
        markers.push(spec.result);
        let markers = self.checked_order(markers);

        self.push(Node::Compute {
            input,
            spec,
            markers,
        })
    }

    /// Adds a node dropping the rows of a relation that another relation matches.
    ///
    /// A row is dropped if the subtracted relation has a row
    /// that agrees with it on their shared markers.
    /// The other markers of the subtracted relation are not compared.
    /// If the relations share no marker, every row agrees,
    /// so a subtracted relation with a row drops every row of the input.
    ///
    /// # Panics
    ///
    /// Panics if either relation is not a node of this plan.
    pub fn subtract(&mut self, input: NodeId, relation: NodeId) -> NodeId {
        let markers = self.checked_markers(input).clone();
        self.check_node(relation);

        self.push(Node::Subtract {
            input,
            relation,
            markers,
        })
    }

    /// Adds a node keeping some markers of a relation, in the given order.
    ///
    /// Rows that agree on the kept markers become one.
    ///
    /// # Panics
    ///
    /// Panics if the input is not a node of this plan,
    /// or if a marker is not allocated from this plan, repeats, or is missing from the input.
    pub fn project(&mut self, input: NodeId, markers: Vec<Marker>) -> NodeId {
        let markers = self.checked_order(markers);
        let available = self.checked_markers(input);
        assert!(
            markers.iter().all(|marker| available.contains(marker)),
            "a projection can only keep the input's markers"
        );

        self.push(Node::Project { input, markers })
    }

    /// Adds a node aggregating the rows of a relation by group.
    ///
    /// The `group` markers of the input form the group key,
    /// and each aggregate adds one result column after them.
    ///
    /// # Panics
    ///
    /// Panics if the input is not a node of this plan,
    /// if the group or an aggregate reads a marker the input lacks,
    /// if there is no aggregate,
    /// or if a result marker is not allocated from this plan, repeats,
    /// or is one of the input's markers.
    pub fn aggregate(
        &mut self,
        input: NodeId,
        group: Vec<Marker>,
        aggregates: Vec<AggregateColumnSpec>,
    ) -> NodeId {
        let available = self.checked_markers(input);
        assert!(
            group.iter().all(|marker| available.contains(marker)),
            "a group key can only name the input's markers"
        );
        assert!(
            !aggregates.is_empty(),
            "an aggregation must compute at least one aggregate"
        );

        let mut markers = group;
        for aggregate in &aggregates {
            assert!(
                aggregate.read().all(|marker| available.contains(&marker)),
                "an aggregate must read only the input's markers"
            );
            assert!(
                !available.contains(&aggregate.result),
                "an aggregate result needs a fresh marker"
            );
            markers.push(aggregate.result);
        }
        let markers = self.checked_order(markers);

        self.push(Node::Aggregate(AggregateNode {
            input,
            aggregates,
            markers,
        }))
    }

    /// Declares a node an output of the plan,
    /// and returns its index among the [outputs](PlanResults::outputs) of an execution.
    ///
    /// # Panics
    ///
    /// Panics if the node is not a node of this plan, or is declared an output already.
    pub fn output(&mut self, node: NodeId) -> usize {
        self.check_node(node);
        assert!(
            !self.outputs.contains(&node),
            "a node can be declared an output only once"
        );

        self.outputs.push(node);

        self.outputs.len() - 1
    }

    /// Executes the plan and returns the rows of its outputs.
    ///
    /// The results are not stored.
    /// [add_trie](TableManager::add_trie) stores a returned trie as it is.
    ///
    /// # Panics
    ///
    /// Panics if a load reads a table that is unknown or dropped,
    /// that has another number of columns than the load has markers,
    /// or whose blocks the load's range does not align with.
    pub fn execute(
        &self,
        manager: &mut TableManager,
        dictionary: &mut Dictionary,
        context: EvaluationContext,
    ) -> PlanResults {
        let layout = PlanLayout::new(self);
        let arena = JoinArena::new();

        Execution::run(self, &layout, manager, dictionary, context, &arena)
    }

    /// Executes the plan until its only output has a row that `judge` accepts,
    /// and returns that row, or `None` if `judge` accepts no row of the output.
    ///
    /// `judge` is given a row with its columns in the order of the output node's markers,
    /// and the table manager, which the execution borrows.
    /// It may be given a row more than once, and a row that an earlier rejection included,
    /// since several joins can find the rows of one output.
    ///
    /// The nodes that the output reads are computed in full,
    /// as [execute](Self::execute) computes them.
    /// The output itself is not computed in full where one join finds its rows
    /// in the order of the output's markers.
    /// This is the case for a join, and for the filters, subtractions and projections
    /// above a join or above a single relation, unless the output has a computed column,
    /// or a projection reorders the join's markers or drops one before another that it keeps.
    /// Each row is then given to `judge` as the join finds it.
    /// The join ends with the first row accepted,
    /// and finds none of the rows that a [rejection](Verdict::Reject) passes over.
    /// Any other output is computed first, and its rows are given to `judge` in storage order.
    ///
    /// Evaluation errors are not reported.
    ///
    /// # Panics
    ///
    /// Panics if the plan does not have exactly one output,
    /// and in the cases that [execute](Self::execute) lists.
    ///
    /// # Example
    ///
    /// ```
    /// use nemo_physical::{
    ///     dictionary::Dictionary,
    ///     function::EvaluationContext,
    ///     management::{ColumnOrder, Revision, TableManager},
    ///     plan::{OperationPlan, Verdict},
    ///     storage::StorageValue,
    ///     trie::RowBuffer,
    /// };
    ///
    /// // The numbers 1 to 100, of which the even ones are stored at revision 2.
    /// let mut manager = TableManager::new();
    /// let number = manager.register("number", 1);
    /// for (revision, first) in [(1, 1), (2, 2)] {
    ///     let mut rows = RowBuffer::new(1);
    ///     for value in (first..=100).step_by(2) {
    ///         rows.push_row(&[StorageValue::integer(value)]);
    ///     }
    ///     manager.add_rows(number, Revision(revision), ColumnOrder::identity(1), rows);
    /// }
    ///
    /// // The pairs of numbers, a hundred times a hundred.
    /// let mut plan = OperationPlan::new();
    /// let [x, y] = plan.markers();
    /// let first = plan.load(number, Revision(0)..Revision(3), vec![x]);
    /// let second = plan.load(number, Revision(0)..Revision(3), vec![y]);
    /// let pairs = plan.join(vec![first, second], vec![x, y]);
    /// plan.output(pairs);
    ///
    /// // The first pair of even numbers is wanted.
    /// // A pair with an odd first number rules out every pair that starts with that number.
    /// let even = |manager: &TableManager, value: StorageValue| {
    ///     manager.revision_of(number, &[value]) == Some(Revision(2))
    /// };
    /// let mut offered = 0;
    /// let mut dictionary = Dictionary::new();
    /// let context = EvaluationContext::now();
    /// let found = plan.find_row(&mut manager, &mut dictionary, context, |manager, row| {
    ///     offered += 1;
    ///     if !even(manager, row[0]) {
    ///         Verdict::Reject { prefix: 1 }
    ///     } else {
    ///         Verdict::for_row(row, even(manager, row[1]))
    ///     }
    /// });
    /// assert_eq!(found, Some(vec![StorageValue::integer(2); 2]));
    ///
    /// // The join found the pairs (1, 1), (2, 1) and (2, 2), and none of the other 9,997.
    /// assert_eq!(offered, 3);
    /// ```
    pub fn find_row(
        &self,
        manager: &mut TableManager,
        dictionary: &mut Dictionary,
        context: EvaluationContext,
        judge: impl FnMut(&TableManager, &[StorageValue]) -> Verdict,
    ) -> Option<Vec<StorageValue>> {
        let layout = PlanLayout::new(self);
        let arena = JoinArena::new();

        Execution::find(self, &layout, manager, dictionary, context, &arena, judge)
    }

    /// Lays out the plan, for reuse across executions.
    ///
    /// The layout reads the plan's structure but not the ranges of its loads,
    /// so it stays valid when [set_load_range](Self::set_load_range) changes them.
    /// Adding a node changes the structure.
    pub fn layout(&self) -> PlanLayout {
        PlanLayout::new(self)
    }

    /// Executes the plan with a layout of its current structure,
    /// as [execute](Self::execute) does with a new layout and arena.
    ///
    /// `arena` is where the joins allocate their working memory.
    /// The execution does not reset it, so its allocations accumulate
    /// until [reset](JoinArena::reset), which keeps the memory for the next executions.
    ///
    /// # Panics
    ///
    /// Panics in the cases that [execute](Self::execute) lists.
    pub fn execute_with_layout(
        &self,
        layout: &PlanLayout,
        manager: &mut TableManager,
        dictionary: &mut Dictionary,
        context: EvaluationContext,
        arena: &JoinArena,
    ) -> PlanResults {
        Execution::run(self, layout, manager, dictionary, context, arena)
    }

    /// Changes the range of revisions a load node reads.
    ///
    /// The plan's structure and its layout stay valid,
    /// so a plan built once can read other revisions on every execution.
    ///
    /// # Panics
    ///
    /// Panics if the node is not a load node of this plan.
    pub fn set_load_range(&mut self, node: NodeId, range: Range<Revision>) {
        match self.nodes.get_mut(node.index()) {
            Some(Node::Load { range: slot, .. }) => *slot = range,
            _ => panic!("only load nodes carry revision ranges"),
        }
    }

    /// Returns the markers of a node's columns, in its canonical order.
    pub(crate) fn node_markers(&self, node: NodeId) -> &MarkerOrder {
        self.nodes[node.index()].markers()
    }

    /// Returns the node with the given id.
    pub(crate) fn node(&self, node: NodeId) -> &Node {
        &self.nodes[node.index()]
    }

    /// Returns the nodes in the order they were added, which is a topological order.
    pub(crate) fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Returns the declared outputs.
    pub(crate) fn declared_outputs(&self) -> &[NodeId] {
        &self.outputs
    }

    /// Adds a validated node and returns its id.
    fn push(&mut self, node: Node) -> NodeId {
        let id = NodeId::new(self.nodes.len());
        self.nodes.push(node);

        id
    }

    /// Checks that a node id belongs to this plan, and panics if it does not.
    fn check_node(&self, node: NodeId) {
        assert!(
            node.index() < self.nodes.len(),
            "a plan node refers to an unknown node"
        );
    }

    /// Returns the markers of a node, checking that the node belongs to this plan.
    fn checked_markers(&self, node: NodeId) -> &MarkerOrder {
        self.check_node(node);

        self.node_markers(node)
    }

    /// Returns markers as an order,
    /// and panics unless they are allocated from this plan and pairwise distinct.
    fn checked_order(&self, markers: Vec<Marker>) -> MarkerOrder {
        assert!(
            markers.iter().all(|marker| marker.0 < self.next_marker),
            "a marker must be allocated from its plan"
        );

        let mut sorted = markers.to_vec();
        sorted.sort_unstable();
        assert!(
            sorted.windows(2).all(|pair| pair[0] != pair[1]),
            "a relation's markers must be distinct"
        );

        MarkerOrder::new(markers)
    }

    /// Checks the inputs of a join against its order, and panics on a mismatch.
    ///
    /// The order must list exactly the inputs' markers.
    fn check_join_inputs(&self, inputs: &[NodeId], order: &[Marker]) {
        assert!(!inputs.is_empty(), "a join needs at least one input");
        for &input in inputs {
            let markers = self.checked_markers(input);
            assert!(
                markers.iter().all(|marker| order.contains(marker)),
                "a join's order must list every marker of its inputs"
            );
        }
        for marker in order {
            assert!(
                inputs
                    .iter()
                    .any(|&input| self.node_markers(input).contains(marker)),
                "a join's order can only list markers of its inputs"
            );
        }
    }
}

#[cfg(test)]
mod tests;
