//! This implements the simple, fact-based tracing.

use std::collections::{HashMap, hash_map::Entry};

use nemo_physical::datavalues::AnyDataValue;

use crate::{
    chase_model::{ChaseAtom, GroundAtom, translation::ProgramChaseTranslation},
    error::Error,
    execution::{
        ExecutionEngine,
        planning::plan_tracing::TracingStrategy,
        selection_strategy::strategy::RuleSelectionStrategy,
        tracing::{
            error::TracingError,
            trace::{
                ExecutionTrace, TraceDerivation, TraceFactHandle, TraceRuleApplication, TraceStatus,
            },
        },
    },
    rule_model::{
        components::{
            ComponentBehavior,
            fact::Fact,
            term::primitive::{Primitive, ground::GroundTerm, variable::Variable},
        },
        substitution::Substitution,
    },
    table_manager::SubtableExecutionPlan,
};

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    /// Recursive part of `trace`.
    fn trace_recursive(
        &mut self,
        trace: &mut ExecutionTrace,
        fact: GroundAtom,
    ) -> Result<TraceFactHandle, TracingError> {
        let trace_handle = trace.register_fact(fact.clone());

        if trace.status(trace_handle).is_known() {
            return Ok(trace_handle);
        }

        // Find the origin of the given fact
        let step = match self
            .table_manager
            .find_table_row(&fact.predicate(), &fact.datavalues().collect::<Vec<_>>())
        {
            Some(s) => s,
            None => {
                // If the table manager does not know the predicate of the fact
                // then it could not have been derived
                trace.update_status(trace_handle, TraceStatus::Fail);
                return Ok(trace_handle);
            }
        };

        if step == 0 {
            // If a fact was derived in step 0 it must have been given as an EDB fact
            trace.update_status(trace_handle, TraceStatus::Success(TraceDerivation::Input));
            return Ok(trace_handle);
        }

        // Rule index of the rule that was applied to derive the given fact
        let rule_index = self.rule_history[step];
        let rule = self.program.rules()[rule_index].clone();

        // Iterate over all head atoms which could have derived the given fact
        for (head_index, head_atom) in rule.head().iter().enumerate() {
            if head_atom.predicate() != fact.predicate() {
                continue;
            }

            // Unify the head atom with the given fact

            // If unification is possible `compatible` remains true
            let mut compatible = true;
            // Contains the head variable and the ground term it aligns with.
            let mut grounding = HashMap::<Variable, AnyDataValue>::new();

            for (head_term, fact_term) in head_atom.terms().zip(fact.terms()) {
                match head_term {
                    Primitive::Ground(ground) => {
                        if ground != fact_term {
                            compatible = false;
                            break;
                        }
                    }
                    Primitive::Variable(variable) => {
                        // Matching with existential variables should not produce any restrictions,
                        // so we just consider universal variables here
                        if variable.is_existential() {
                            continue;
                        }

                        match grounding.entry(variable.clone()) {
                            Entry::Occupied(entry) => {
                                if *entry.get() != fact_term.value() {
                                    compatible = false;
                                    break;
                                }
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(fact_term.value());
                            }
                        }
                    }
                }
            }

            if !compatible {
                // Fact could not have been unified
                continue;
            }

            let rule = self.program.rules()[rule_index].clone();
            let analysis = &self.analysis.rule_analysis[rule_index];
            let mut variable_order = analysis.promising_variable_orders[0].clone(); // TODO: This selection is arbitrary
            let trace_strategy = TracingStrategy::initialize(&rule, grounding)?;

            let mut execution_plan = SubtableExecutionPlan::default();

            trace_strategy.add_plan(
                &self.table_manager,
                &mut execution_plan,
                &mut variable_order,
                step,
            );

            if let Some(query_result) = self.table_manager.execute_plan_first_match(execution_plan)
            {
                let variable_assignment: HashMap<Variable, AnyDataValue> = variable_order
                    .as_ordered_list()
                    .into_iter()
                    .zip(query_result.iter().cloned())
                    .collect();

                let mut fully_derived = true;
                let mut subtraces = Vec::<TraceFactHandle>::new();
                for body_atom in rule.positive_body() {
                    let next_fact_predicate = body_atom.predicate();
                    let next_fact_terms = body_atom
                        .terms()
                        .map(|variable| {
                            GroundTerm::from(
                                variable_assignment
                                    .get(variable)
                                    .expect("Query must assign value to each variable.")
                                    .clone(),
                            )
                        })
                        .collect::<Vec<_>>();

                    let next_fact = GroundAtom::new(next_fact_predicate, next_fact_terms);

                    let next_handle = self.trace_recursive(trace, next_fact)?;

                    if trace.status(next_handle).is_success() {
                        subtraces.push(next_handle);
                    } else {
                        fully_derived = false;
                        break;
                    }
                }

                if !fully_derived {
                    continue;
                }

                let rule_application = TraceRuleApplication::new(
                    rule_index,
                    Substitution::new(variable_assignment.into_iter().map(|(variable, value)| {
                        (Primitive::from(variable), Primitive::from(value))
                    })),
                    head_index,
                );

                let derivation = TraceDerivation::Derived(rule_application, subtraces);
                trace.update_status(trace_handle, TraceStatus::Success(derivation));

                return Ok(trace_handle);
            } else {
                continue;
            }
        }

        trace.update_status(trace_handle, TraceStatus::Fail);
        Ok(trace_handle)
    }

    /// Build an [ExecutionTrace] for a list of facts.
    /// Also returns a list containing a [TraceFactHandle] for each fact.
    ///
    /// TODO: Verify that Fact is ground
    pub fn trace(
        &mut self,
        facts: Vec<Fact>,
    ) -> Result<(ExecutionTrace, Vec<TraceFactHandle>), Error> {
        for fact in &facts {
            if fact.validate().is_err() {
                return Err(TracingError::InvalidFact {
                    fact: fact.to_string(),
                }
                .into());
            }
        }

        let chase_facts: Vec<_> = facts
            .into_iter()
            .filter_map(|fact| ProgramChaseTranslation::new().build_fact(&fact))
            .collect();

        let mut trace = ExecutionTrace::new(self.nemo_program.clone());
        let mut handles = Vec::new();

        let num_chase_facts = chase_facts.len();

        for (i, chase_fact) in chase_facts.into_iter().enumerate() {
            if i > 0 && i.is_multiple_of(500) {
                log::info!(
                    "{i}/{num_chase_facts} facts traced. ({}%)",
                    i * 100 / num_chase_facts
                );
            }

            handles.push(self.trace_recursive(&mut trace, chase_fact)?);
        }

        log::info!("{num_chase_facts}/{num_chase_facts} facts traced. (100%)");

        Ok((trace, handles))
    }
}
