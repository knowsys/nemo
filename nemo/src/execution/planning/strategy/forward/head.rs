//! This module defines [StrategyHead].

use std::collections::{HashMap, HashSet};

use nemo_physical::management::execution_plan::{ColumnOrder, ExecutionNodeRef};

use crate::{
    execution::planning::{
        RuntimeInformation,
        analysis::variable_order::VariableOrder,
        normalization::rule::NormalizedRule,
        operations::{duplicates::GeneratorDuplicates, projection_head::GeneratorProjectionHead},
        strategy::forward::restricted::StrategyRestricted,
    },
    rule_model::components::{tag::Tag, term::primitive::variable::Variable},
    table_manager::{SubtableExecutionPlan, SubtableIdentifier},
};

/// Type of head atom
#[derive(Debug)]
enum HeadAtomType {
    /// Contains only universal variables and no aggregation
    Datalog(GeneratorProjectionHead),
    /// Contains existential variables
    Existential(GeneratorProjectionHead),
    /// Contains aggregration
    Aggregation(GeneratorProjectionHead),
}

impl HeadAtomType {
    /// Return the predicate.
    pub fn predicate(&self) -> Tag {
        match self {
            HeadAtomType::Datalog(generator) => generator.predicate(),
            HeadAtomType::Existential(generator) => generator.predicate(),
            HeadAtomType::Aggregation(generator) => generator.predicate(),
        }
    }

    /// Returns whether an atom of this type required duplicate elimination
    pub fn duplicate_elimination(&self) -> bool {
        match self {
            HeadAtomType::Datalog(_) => true,
            HeadAtomType::Existential(_) => false,
            HeadAtomType::Aggregation(_) => true,
        }
    }
}

/// Generator of an execution plan that evaluates the head of a rule
#[derive(Debug)]
pub struct StrategyHead {
    /// Strategy for computing the restricted chase
    restricted: Option<StrategyRestricted>,

    /// Head atoms
    atoms: Vec<HeadAtomType>,
}

impl StrategyHead {
    /// Create a new [StrategyHead].
    pub fn new(
        rule: &NormalizedRule,
        order: &VariableOrder,
        frontier: HashSet<Variable>,
        aggregation_index: Option<usize>,
        rule_id: usize,
        is_existential: bool,
    ) -> Self {
        let restricted = if is_existential {
            Some(StrategyRestricted::new(rule, frontier, order, rule_id))
        } else {
            None
        };

        let mut atoms = Vec::<HeadAtomType>::default();
        for (atom_index, atom) in rule.head().iter().enumerate() {
            let generator = GeneratorProjectionHead::new(atom.clone());

            if Some(atom_index) == aggregation_index {
                atoms.push(HeadAtomType::Aggregation(generator))
            } else if atom.variables_existential().next().is_some() {
                atoms.push(HeadAtomType::Existential(generator))
            } else {
                atoms.push(HeadAtomType::Datalog(generator))
            }
        }

        Self { restricted, atoms }
    }

    /// Return an iterator over all special predicates needed to execute this strategy.
    pub fn special_predicates(&self) -> impl Iterator<Item = (Tag, usize)> {
        self.restricted
            .as_ref()
            .map(|restricted| restricted.special_predicates())
            .into_iter()
            .flatten()
    }

    /// Append this operation to the plan.
    pub fn create_plan<'a>(
        &self,
        plan: &mut SubtableExecutionPlan,
        node_body: ExecutionNodeRef,
        node_aggregation: Option<ExecutionNodeRef>,
        runtime: &RuntimeInformation<'a>,
    ) {
        let node_restricted = self
            .restricted
            .as_ref()
            .map(|strategy| strategy.create_plan(plan, node_body.clone(), runtime));

        let mut atom_map = HashMap::<Tag, Vec<ExecutionNodeRef>>::default();

        for atom in &self.atoms {
            let mut node = match atom {
                HeadAtomType::Datalog(generator) => {
                    generator.create_plan(plan, node_body.clone(), runtime)
                }
                HeadAtomType::Existential(generator) => generator.create_plan(
                    plan,
                    node_restricted.clone().expect("rule is existential"),
                    runtime,
                ),
                HeadAtomType::Aggregation(generator) => generator.create_plan(
                    plan,
                    node_aggregation.clone().expect("rule contains aggregation"),
                    runtime,
                ),
            };

            if atom.duplicate_elimination() {
                node = GeneratorDuplicates::new(atom.predicate()).create_plan(plan, node, runtime);
            }

            atom_map.entry(atom.predicate()).or_default().push(node);
        }

        for (predicate, nodes) in atom_map {
            let markers = nodes[0].markers_cloned();
            let node_final = plan.plan_mut().union(markers, nodes);

            let table_name = runtime.table_manager.generate_table_name(
                &predicate,
                &ColumnOrder::default(),
                runtime.step_current,
            );

            plan.add_permanent_table(
                node_final,
                "Duplicate Elimination (Datalog)",
                &table_name,
                SubtableIdentifier::new(predicate.clone(), runtime.step_current),
            );
        }
    }
}
