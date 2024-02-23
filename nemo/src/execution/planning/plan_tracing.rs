//! Module defining the strategy for calculating all body matches for a rule application.

use std::collections::HashMap;

use nemo_physical::{datavalues::AnyDataValue, management::execution_plan::ExecutionNodeRef};

use crate::{
    execution::rule_execution::VariableTranslation,
    model::{
        chase_model::{ChaseRule, VariableAtom},
        Constraint, Identifier, PrimitiveTerm, Term, Variable,
    },
    program_analysis::variable_order::VariableOrder,
    table_manager::{SubtableExecutionPlan, SubtableIdentifier, TableManager},
};

use super::operations::{filter::node_filter, join::node_join, negation::node_negation};

/// Implementation of the semi-naive existential rule evaluation strategy.
#[derive(Debug)]
pub(crate) struct TracingStrategy {
    positive_atoms: Vec<VariableAtom>,
    positive_constraints: Vec<Constraint>,

    negative_atoms: Vec<VariableAtom>,
    negatie_constraints: Vec<Constraint>,

    variable_translation: VariableTranslation,
}

impl TracingStrategy {
    /// Create new [`SeminaiveStrategy`] object.
    pub(crate) fn initialize(rule: &ChaseRule, grounding: HashMap<Variable, AnyDataValue>) -> Self {
        let mut variable_translation = VariableTranslation::new();
        for variable in rule.all_variables() {
            variable_translation.add_marker(variable);
        }

        let mut positive_constraints = rule.positive_constraints().clone();

        let constructors = rule
            .constructors()
            .iter()
            .map(|constructor| (constructor.variable().clone(), constructor.term().clone()))
            .collect::<HashMap<Variable, Term>>();

        for (variable, value) in grounding {
            if let Some(term) = constructors.get(&variable) {
                positive_constraints.push(Constraint::Equals(
                    term.clone(),
                    Term::Primitive(PrimitiveTerm::GroundTerm(value)),
                ));
            } else {
                positive_constraints.push(Constraint::Equals(
                    Term::Primitive(PrimitiveTerm::Variable(variable)),
                    Term::Primitive(PrimitiveTerm::GroundTerm(value)),
                ));
            }
        }

        Self {
            positive_atoms: rule.positive_body().clone(),
            positive_constraints,
            negative_atoms: rule.negative_body().clone(),
            negatie_constraints: rule.negative_constraints().clone(),
            variable_translation,
        }
    }

    pub(crate) fn add_plan(
        &self,
        table_manager: &TableManager,
        current_plan: &mut SubtableExecutionPlan,
        variable_order: &mut VariableOrder,
        step_number: usize,
    ) -> ExecutionNodeRef {
        let join_output_markers = self
            .variable_translation
            .operation_table(variable_order.iter());
        let node_join = node_join(
            current_plan.plan_mut(),
            table_manager,
            &self.variable_translation,
            0,
            step_number,
            &self.positive_atoms,
            join_output_markers,
        );

        let node_filter = node_filter(
            current_plan.plan_mut(),
            &self.variable_translation,
            node_join,
            &self.positive_constraints,
        );

        let node_negation = node_negation(
            current_plan.plan_mut(),
            table_manager,
            &self.variable_translation,
            node_filter,
            step_number,
            &self.negative_atoms,
            &self.negatie_constraints,
        );

        current_plan.add_permanent_table(
            node_negation.clone(),
            "Tracing Query",
            "Tracing Query",
            SubtableIdentifier::new(Identifier::new(String::from("_TRACING")), step_number),
        );

        node_negation
    }
}
