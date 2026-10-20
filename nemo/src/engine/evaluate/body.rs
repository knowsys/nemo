//! The body plan: a rule's positive body as a join,
//! with its bindings, computed columns and filters above it and its negated atoms subtracted.
//!
//! A body is built the same way wherever it is evaluated.
//! A rule application, the proof search, the derivation query,
//! and a fetch rule's binding rows each choose what every atom reads and the order in which the
//! join binds the variables.
//! The operators above the join and the subtraction of the negated atoms are built here for all
//! of them.

use std::ops::Range;

use nemo_physical::{
    dictionary::Dictionary,
    function::{EvaluationContext, OpCode, StackProgram},
    management::{Revision, TableId, TableManager},
    plan::{ComputedColumnSpec, FilterSpec, Marker, NodeId, OperationPlan},
    value::DataValue,
};

use crate::engine::compile::compiled::{
    BodyAtom, CompiledRule, FilterOrigin, NegatedAtom, NegatedColumn, RuleFilter,
};

/// Returns the placeholder range of a load whose range a skeleton sets before every execution.
pub(super) fn unpatched() -> Range<Revision> {
    Revision(0)..Revision(0)
}

/// Returns a filter program that keeps the rows whose input is the same term as a constant.
pub(crate) fn same_constant(value: DataValue) -> StackProgram {
    StackProgram::new(
        vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Same],
        vec![value],
        1,
    )
}

/// Filters a node by each condition in turn.
pub(crate) fn filtered(plan: &mut OperationPlan, node: NodeId, filters: Vec<FilterSpec>) -> NodeId {
    filters
        .into_iter()
        .fold(node, |node, filter| plan.filter(node, filter))
}

/// Returns the position of each of a rule's columns in a body result row:
/// the variables at their positions in the rule's order, the computed columns after them.
pub(crate) fn combined_positions(rule: &CompiledRule) -> Vec<usize> {
    let mut positions: Vec<usize> = (0..rule.variables + rule.computed.len()).collect();
    for (position, &variable) in rule.order.iter().enumerate() {
        positions[variable] = position;
    }
    positions
}

/// A rule's columns as plan markers.
///
/// The rule's columns are its variables, then its computed columns.
/// Besides one marker per column, it holds the join order (the variables' markers in the rule's
/// order) and the order of a body result (the join order, then the computed columns).
#[derive(Debug)]
pub(crate) struct BodySpace<'r> {
    /// The rule.
    rule: &'r CompiledRule,
    /// One marker per column of the rule.
    pub(crate) markers: Vec<Marker>,
    /// The join order: the variables' markers in the rule's order.
    pub(crate) order: Vec<Marker>,
    /// The order of a body result: the join order, then the computed columns.
    pub(crate) result_columns: Vec<Marker>,
}

impl<'r> BodySpace<'r> {
    /// Allocates the rule's markers in a plan.
    pub(crate) fn new(plan: &mut OperationPlan, rule: &'r CompiledRule) -> Self {
        let markers: Vec<Marker> = (0..rule.variables + rule.computed.len())
            .map(|_| plan.marker())
            .collect();
        let order: Vec<Marker> = rule.order.iter().map(|&id| markers[id]).collect();
        let result_columns: Vec<Marker> = order
            .iter()
            .copied()
            .chain(markers[rule.variables..].iter().copied())
            .collect();
        Self {
            rule,
            markers,
            order,
            result_columns,
        }
    }

    /// Returns the markers of the given columns of the rule.
    pub(crate) fn columns(&self, columns: &[usize]) -> Vec<Marker> {
        columns.iter().map(|&column| self.markers[column]).collect()
    }

    /// Returns the markers of one body atom's columns.
    pub(crate) fn atom_markers(&self, atom: &BodyAtom) -> Vec<Marker> {
        self.columns(&atom.variables)
    }

    /// Returns one join of the body:
    /// the inputs joined in the given order, with the body's operators above it
    /// (see [operators](Self::operators)).
    pub(crate) fn join(
        &self,
        plan: &mut OperationPlan,
        inputs: Vec<NodeId>,
        order: Vec<Marker>,
        grounding: &[(usize, DataValue)],
    ) -> NodeId {
        let node = plan.join(inputs, order.clone());
        self.operators(plan, node, &order, grounding)
    }

    /// Returns the body's operators above a node binding the given markers.
    ///
    /// These are every binding, computed column and filter whose inputs the node binds,
    /// and a filter per entry of `grounding` that keeps the rows whose column is that value.
    /// Above a whole body, these are all of the rule's operators.
    /// Above a single atom, these are the operators over that atom's columns.
    /// A binding is the condition that its column is the term its expression computes,
    /// which the plan can check by a lookup instead of enumerating the column.
    /// Filters come after the computed columns they may read.
    pub(crate) fn operators(
        &self,
        plan: &mut OperationPlan,
        mut node: NodeId,
        bound_markers: &[Marker],
        grounding: &[(usize, DataValue)],
    ) -> NodeId {
        let rule = self.rule;
        let mut bound: Vec<bool> = self
            .markers
            .iter()
            .map(|marker| bound_markers.contains(marker))
            .collect();

        for binding in &rule.bindings {
            if bound[binding.target] && binding.inputs.iter().all(|&id| bound[id]) {
                let mut inputs = vec![self.markers[binding.target]];
                inputs.extend(self.columns(&binding.inputs));
                let program = binding.program.shifted_equality();
                node = plan.filter(node, FilterSpec::from_program(inputs, program));
            }
        }
        for (slot, entry) in rule.computed.iter().enumerate() {
            if entry.inputs.iter().all(|&id| bound[id]) {
                let spec = ComputedColumnSpec::from_program(
                    self.columns(&entry.inputs),
                    entry.program.clone(),
                    self.markers[rule.variables + slot],
                );
                node = plan.compute(node, spec);
                bound[rule.variables + slot] = true;
            }
        }

        let grounded: Vec<RuleFilter> = grounding
            .iter()
            .map(|(column, value)| RuleFilter {
                inputs: vec![*column],
                program: same_constant(value.clone()),
                origin: FilterOrigin::Constant(None),
            })
            .collect();
        for filter in rule.filters.iter().chain(&grounded) {
            if filter.inputs.iter().all(|&id| bound[id]) {
                let spec =
                    FilterSpec::from_program(self.columns(&filter.inputs), filter.program.clone());
                node = plan.filter(node, spec);
            }
        }

        node
    }

    /// Subtracts the rule's negated atoms from a body result,
    /// each matched on the columns it shares with the rule.
    ///
    /// Guards, the negated atoms sharing no column with the rule,
    /// are checked once when their stratum begins, not here.
    /// Without any other negated atom, the result is the input node itself.
    /// The negated atoms read the given range,
    /// and their loads are returned with the node, for a caller that sets their ranges per
    /// application.
    pub(crate) fn subtract_negations(
        &self,
        plan: &mut OperationPlan,
        mut node: NodeId,
        range: Range<Revision>,
    ) -> (NodeId, Vec<(NodeId, TableId)>) {
        let mut loads = Vec::new();
        for atom in self.rule.negated.iter().filter(|atom| !atom.is_guard()) {
            let relation = self.negated_node(plan, atom, range.clone(), &mut loads);
            node = plan.subtract(node, relation);
        }
        (node, loads)
    }

    /// Returns the plan node subtracted for a negated atom:
    /// its load, filtered by the atom's constants and repetitions.
    ///
    /// Columns the atom shares with no other literal stay existentially quantified inside the
    /// negation.
    fn negated_node(
        &self,
        plan: &mut OperationPlan,
        negated: &NegatedAtom,
        range: Range<Revision>,
        loads: &mut Vec<(NodeId, TableId)>,
    ) -> NodeId {
        // Per column: the shared marker, or a fresh private one.
        let column_markers: Vec<Marker> = negated
            .columns
            .iter()
            .map(|column| match column {
                NegatedColumn::Shared(id) => self.markers[*id],
                NegatedColumn::Private => plan.marker(),
            })
            .collect();
        let load = plan.load(negated.table, range, column_markers.clone());
        loads.push((load, negated.table));
        filtered(plan, load, negated_filters(negated, &column_markers))
    }
}

/// Returns the filters for a negated atom's constants and repetitions, over its column markers.
fn negated_filters(negated: &NegatedAtom, column_markers: &[Marker]) -> Vec<FilterSpec> {
    negated
        .filters
        .iter()
        .map(|filter| {
            let inputs = filter
                .inputs
                .iter()
                .map(|&position| column_markers[position])
                .collect();
            FilterSpec::from_program(inputs, filter.program.clone())
        })
        .collect()
}

/// Returns whether the table of a guard atom holds a matching row.
///
/// A guard is a negated atom sharing no variable with its rule.
/// It is checked once per stratum, since its predicate does not change within the stratum.
pub(crate) fn guard_matches(
    negated: &NegatedAtom,
    step: u64,
    manager: &mut TableManager,
    dictionary: &mut Dictionary,
    context: EvaluationContext,
) -> bool {
    let mut plan = OperationPlan::new();
    let column_markers: Vec<Marker> = negated.columns.iter().map(|_| plan.marker()).collect();
    let load = plan.load(
        negated.table,
        Revision(0)..Revision(step),
        column_markers.clone(),
    );
    let matching = filtered(&mut plan, load, negated_filters(negated, &column_markers));
    let exists = plan.project(matching, Vec::new());
    let output = plan.output(exists);
    let results = plan.execute(manager, dictionary, context);
    !results.outputs[output].is_empty()
}
