//! This module defines [GeneratorRestrictedHead].

use std::collections::HashSet;

use nemo_physical::management::execution_plan::ExecutionNodeRef;

use crate::{
    chase_model::analysis::variable_order::VariableOrder,
    execution::planning_new::{
        RuntimeInformation,
        normalization::{
            atom::{body::BodyAtom, head::HeadAtom},
            generator::VariableGenerator,
            operation::Operation,
        },
        operations::{
            duplicates::GeneratorDuplicates, filter::GeneratorFilter,
            join_seminaive::GeneratorJoinSeminaive,
            restricted_frontier::GeneratorRestrictedFrontier,
        },
    },
    rule_model::components::{
        tag::Tag,
        term::primitive::{Primitive, variable::Variable},
    },
    table_manager::SubtableExecutionPlan,
};

/// Generator of a execution plan node
/// that represents a seminaive evaluation of
/// existential head atoms
#[derive(Debug)]
pub struct GeneratorRestrictedHead {
    /// Join of the head atoms
    join: GeneratorJoinSeminaive,
    /// Additional filters that need to be applied
    filter: Option<GeneratorFilter>,
    /// Projection to frontier variables
    frontier: GeneratorRestrictedFrontier,
    /// Generates the duplicate elimination
    duplicates: GeneratorDuplicates,

    /// Predicate that identifies this table
    predicate: Tag,
    /// Output variables
    variables: Vec<Variable>,
}

impl GeneratorRestrictedHead {
    /// Create a new [GeneratorRestrictedHead].
    pub fn new(
        head: &[HeadAtom],
        frontier: HashSet<Variable>,
        order: &VariableOrder,
        rule_id: usize,
    ) -> Self {
        let head_variables = head
            .iter()
            .flat_map(|atom| atom.variables().cloned())
            .collect::<HashSet<_>>();
        let mut order = order.restrict_to(&head_variables);

        let (atoms, mut operations) = Self::normalize_head(head, &mut order);

        let join = GeneratorJoinSeminaive::new_exclusive(atoms, &order);
        let filter = GeneratorFilter::new(join.output_variables(), &mut operations);

        order = order.restrict_to(&frontier);
        let projection = GeneratorRestrictedFrontier::new(order.as_ordered_list());

        let predicate = Self::generate_predicate(rule_id);
        let duplicates = GeneratorDuplicates::new(predicate.clone());

        Self {
            join,
            filter: filter.or_none(),
            frontier: projection,
            duplicates,
            predicate,
            variables: order.as_ordered_list(),
        }
    }

    /// Generate the predicate name used to store the result of this generator.
    fn generate_predicate(rule_id: usize) -> Tag {
        let name = format!("_SATISFIED_{rule_id}");
        Tag::new(name)
    }

    /// Translate [HeadAtom]s into a list of [BodyAtom]s
    /// and additional filter [Operation]s.
    fn normalize_head(
        head: &[HeadAtom],
        order: &mut VariableOrder,
    ) -> (Vec<BodyAtom>, Vec<Operation>) {
        let mut generator = VariableGenerator::default();

        let mut atoms = Vec::<BodyAtom>::default();
        let mut operations = Vec::<Operation>::default();

        for head_atom in head {
            let mut used_variables = HashSet::<Variable>::default();
            let mut variables = Vec::<Variable>::default();

            for term in head_atom.terms() {
                match term {
                    Primitive::Variable(variable) => {
                        if !used_variables.insert(variable.clone()) {
                            let new_variable = generator.universal("RESTRICTED_HEAD");
                            let new_operation = Operation::new_assignment(
                                new_variable.clone(),
                                Operation::new_variable(variable.clone()),
                            );

                            variables.push(new_variable.clone());
                            order.push(new_variable);
                            operations.push(new_operation);
                        } else {
                            variables.push(variable.clone());
                        }
                    }
                    Primitive::Ground(ground_term) => {
                        let new_variable = generator.universal("RESTRICTED_HEAD");
                        let new_operation = Operation::new_assignment(
                            new_variable.clone(),
                            Operation::new_ground(ground_term.clone()),
                        );

                        variables.push(new_variable.clone());
                        order.push(new_variable);
                        operations.push(new_operation);
                    }
                }
            }

            atoms.push(BodyAtom::new(head_atom.predicate(), variables));
        }

        (atoms, operations)
    }

    /// Append this operation to the plan.
    pub fn create_plan(
        &self,
        plan: &mut SubtableExecutionPlan,
        runtime: &RuntimeInformation,
    ) -> ExecutionNodeRef {
        let node_join = self.join.create_plan(plan, runtime);

        let mut current_node = node_join;

        if let Some(generator) = self.filter.as_ref() {
            current_node = generator.create_plan(plan, current_node, runtime);
        }

        current_node = self.frontier.create_plan(plan, current_node, runtime);

        self.duplicates.create_plan(plan, current_node, runtime)
    }

    /// Return the predicate name that contains satisfied matches.
    pub fn predicate(&self) -> Tag {
        self.predicate.clone()
    }

    /// Return the variables marking the column of the node
    /// created by `create_plan`.
    pub fn output_variables(&self) -> Vec<Variable> {
        self.variables.clone()
    }
}
