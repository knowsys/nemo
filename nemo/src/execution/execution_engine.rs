//! Functionality which handles the execution of a program

use std::{
    borrow::BorrowMut,
    collections::{hash_map::Entry, HashMap},
};

use nemo_physical::{
    datatypes::{DataValueT, StorageValueT},
    management::database::TableSource,
    meta::TimedCode,
};

use crate::{
    error::Error,
    execution::{
        planning::{plan_body_seminaive::SeminaiveStrategy, BodyStrategy},
        tracing::trace::RuleApplication,
    },
    io::{input_manager::InputManager, resource_providers::ResourceProviders},
    model::{
        chase_model::ChaseProgram,
        types::{
            primitive_logical_value::{PrimitiveLogicalValueIteratorT, PrimitiveLogicalValueT},
            primitive_types::PrimitiveType,
        },
        Atom, Fact, Filter, FilterOperation, Identifier, Program, Term, TermOperation, TermTree,
        VariableAssignment,
    },
    program_analysis::analysis::ProgramAnalysis,
    table_manager::{MemoryUsage, SubtableExecutionPlan, SubtableIdentifier, TableManager},
};

use super::{
    rule_execution::RuleExecution, selection_strategy::strategy::RuleSelectionStrategy,
    tracing::trace::ExecutionTrace,
};

// Number of tables that are periodically combined into one.
const MAX_FRAGMENTATION: usize = 8;

/// Stores useful information about a rule.
#[derive(Default, Debug, Copy, Clone)]
pub struct RuleInfo {
    /// The execution step this rule was last applied in.
    pub step_last_applied: usize,
}

impl RuleInfo {
    /// Create new [`RuleInfo`].
    pub fn new() -> Self {
        Self {
            step_last_applied: 0,
        }
    }
}

/// Object which handles the evaluation of the program.
#[derive(Debug)]
pub struct ExecutionEngine<RuleSelectionStrategy> {
    input_program: Program,

    program: ChaseProgram,
    analysis: ProgramAnalysis,

    rule_strategy: RuleSelectionStrategy,

    #[allow(dead_code)]
    input_manager: InputManager,
    table_manager: TableManager,

    predicate_fragmentation: HashMap<Identifier, usize>,
    predicate_last_union: HashMap<Identifier, usize>,

    rule_infos: Vec<RuleInfo>,
    rule_history: Vec<usize>,
    current_step: usize,
}

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    /// Initialize [`ExecutionEngine`].
    pub fn initialize(
        program: Program,
        resource_providers: ResourceProviders,
    ) -> Result<Self, Error> {
        let chase_program: ChaseProgram = program.clone().try_into()?;

        chase_program.check_for_unsupported_features()?;
        let analysis = chase_program.analyze()?;

        let input_manager = InputManager::new(resource_providers);

        let mut table_manager = TableManager::new();
        Self::register_all_predicates(&mut table_manager, &analysis);
        Self::add_sources(
            &mut table_manager,
            &input_manager,
            &chase_program,
            &analysis,
        )?;

        let mut rule_infos = Vec::<RuleInfo>::new();
        chase_program
            .rules()
            .iter()
            .for_each(|_| rule_infos.push(RuleInfo::new()));

        let rule_strategy = Strategy::new(
            chase_program.rules().iter().collect(),
            analysis.rule_analysis.iter().collect(),
        )?;

        Ok(Self {
            input_program: program,
            program: chase_program,
            analysis,
            rule_strategy,
            input_manager,
            table_manager,
            predicate_fragmentation: HashMap::new(),
            predicate_last_union: HashMap::new(),
            rule_infos,
            rule_history: vec![usize::MAX], // Placeholder, Step counting starts at 1
            current_step: 1,
        })
    }

    fn register_all_predicates(table_manager: &mut TableManager, analysis: &ProgramAnalysis) {
        for (predicate, _) in &analysis.all_predicates {
            table_manager.register_predicate(
                predicate.clone(),
                analysis
                    .predicate_types
                    .get(predicate)
                    .cloned()
                    .expect("All predicates should have types by now."),
            );
        }
    }

    fn fact_to_vec(fact: &Fact, analysis: &ProgramAnalysis) -> Vec<DataValueT> {
        fact
        .0
        .term_trees()
        .iter()
        .enumerate()
        // TODO: get rid of unwrap
        .map(|(i, t)| {
            if let TermOperation::Term(ground_term) = t.operation() {
                analysis.predicate_types.get(&fact.0.predicate()).unwrap()[i]
                .ground_term_to_data_value_t(ground_term.clone()).expect("Trying to convert a ground type into an invalid logical type. Should have been prevented by the type checker.")
            } else {
                unreachable!(
                    "Its assumed that facts do not contain complicated expressions (for now?)"
                );
            }
        })
        .collect()
    }

    fn add_sources(
        table_manager: &mut TableManager,
        input_manager: &InputManager,
        program: &ChaseProgram,
        analysis: &ProgramAnalysis,
    ) -> Result<(), Error> {
        let mut predicate_to_sources = HashMap::<Identifier, Vec<TableSource>>::new();

        // Add all the data source declarations
        for source_declaration in program.sources() {
            let logical_types = analysis
                .predicate_types
                .get(&source_declaration.predicate)
                .cloned()
                .expect("All predicates should have types by now.");

            let table_source = input_manager
                .load_native_table_source(source_declaration.source.clone(), logical_types)?;

            predicate_to_sources
                .entry(source_declaration.predicate.clone())
                .or_default()
                .push(table_source)
        }

        // Add all the facts contained in the rule file as a source
        let mut predicate_to_rows = HashMap::<Identifier, Vec<Vec<DataValueT>>>::new();

        for fact in program.facts() {
            let rows = predicate_to_rows.entry(fact.0.predicate()).or_default();
            rows.push(Self::fact_to_vec(fact, analysis));
        }

        for (predicate, rows) in predicate_to_rows.into_iter() {
            predicate_to_sources
                .entry(predicate)
                .or_default()
                .push(TableSource::RLS(rows));
        }

        // Add all the sources to the table mananager
        for (predicate, sources) in predicate_to_sources {
            table_manager.add_edb(predicate, sources);
        }

        Ok(())
    }

    /// Executes the program.
    pub fn execute(&mut self) -> Result<(), Error> {
        TimedCode::instance().sub("Reasoning/Rules").start();
        TimedCode::instance().sub("Reasoning/Execution").start();

        let rule_execution: Vec<RuleExecution> = self
            .program
            .rules()
            .iter()
            .zip(self.analysis.rule_analysis.iter())
            .map(|(r, a)| RuleExecution::initialize(r, a))
            .collect();

        let mut new_derivations: Option<bool> = None;

        while let Some(current_rule_index) = self.rule_strategy.next_rule(new_derivations) {
            let timing_string = format!("Reasoning/Rules/Rule {current_rule_index}");

            TimedCode::instance().sub(&timing_string).start();
            log::info!(
                "<<< {0}: APPLYING RULE {current_rule_index} >>>",
                self.current_step
            );

            self.rule_history.push(current_rule_index);

            let current_info = &mut self.rule_infos[current_rule_index];
            let current_execution = &rule_execution[current_rule_index];

            let updated_predicates = current_execution.execute(
                &mut self.table_manager,
                current_info,
                self.current_step,
            )?;

            new_derivations = Some(!updated_predicates.is_empty());

            current_info.step_last_applied = self.current_step;

            let rule_duration = TimedCode::instance().sub(&timing_string).stop();
            log::info!("Rule duration: {} ms", rule_duration.as_millis());

            // We prevent fragmentation by periodically collecting single-step tables into larger ones
            for updated_pred in updated_predicates {
                let counter = self
                    .predicate_fragmentation
                    .entry(updated_pred.clone())
                    .or_insert(0);
                *counter += 1;

                if *counter == MAX_FRAGMENTATION {
                    let start =
                        if let Some(last_union) = self.predicate_last_union.get(&updated_pred) {
                            last_union + 1
                        } else {
                            0
                        };

                    let range = start..(self.current_step + 1);

                    self.table_manager
                        .combine_tables(updated_pred.clone(), range)?;

                    self.predicate_last_union
                        .insert(updated_pred, self.current_step);

                    *counter = 0;
                }
            }

            self.current_step += 1;
        }

        TimedCode::instance().sub("Reasoning/Rules").stop();
        TimedCode::instance().sub("Reasoning/Execution").stop();
        Ok(())
    }

    /// Get a reference to the loaded program.
    pub fn program(&self) -> &ChaseProgram {
        &self.program
    }

    /// Get a list of column iterators for the predicate
    pub fn get_predicate_column_iterators(
        &mut self,
        predicate: Identifier,
    ) -> Result<Option<Vec<PrimitiveLogicalValueIteratorT>>, Error> {
        let Some(table_id) = self.table_manager.combine_predicate(predicate.clone())? else {
            return Ok(None);
        };

        let predicate_types: &Vec<PrimitiveType> = self
            .analysis
            .predicate_types
            .get(&predicate)
            .expect("All predicates should have types by now.");

        let iterators = self.table_manager.table_column_iters(table_id)?;

        let logically_mapped_iters: Vec<PrimitiveLogicalValueIteratorT> = iterators
            .into_iter()
            .zip(predicate_types.iter())
            .map(|(iter, lt)| lt.primitive_logical_value_iterator(iter))
            .collect();

        Ok(Some(logically_mapped_iters))
    }

    /// Creates an [`Iterator`] over the resulting facts of a predicate.
    // TODO: we probably want to return a list of column iterators over logical values
    pub fn table_scan(
        &mut self,
        predicate: Identifier,
    ) -> Result<Option<impl Iterator<Item = Vec<PrimitiveLogicalValueT>> + '_>, Error> {
        let Some(logically_mapped_iters) = self.get_predicate_column_iterators(predicate)? else {
            return Ok(None);
        };

        struct CombinedIters<'a>(Vec<PrimitiveLogicalValueIteratorT<'a>>);

        impl<'a> Iterator for CombinedIters<'a> {
            type Item = Vec<PrimitiveLogicalValueT>;

            fn next(&mut self) -> Option<Self::Item> {
                let res: Self::Item = self.0.iter_mut().filter_map(|iter| iter.next()).collect();
                (!res.is_empty()).then_some(res)
            }
        }

        let combined_iters = CombinedIters(logically_mapped_iters);

        Ok(Some(combined_iters))
    }

    /// Creates an [`Iterator`] over the resulting facts of a predicate.
    pub fn output_serialization(
        &mut self,
        predicate: Identifier,
    ) -> Result<Option<impl Iterator<Item = Vec<String>> + '_>, Error> {
        let Some(table_id) = self.table_manager.combine_predicate(predicate.clone())? else {
            return Ok(None);
        };

        let predicate_types: &Vec<PrimitiveType> = self
            .analysis
            .predicate_types
            .get(&predicate)
            .expect("All predicates should have types by now.");

        let iterators = self.table_manager.table_column_iters(table_id)?;

        let logically_mapped_iters: Vec<Box<dyn Iterator<Item = String>>> = iterators
            .into_iter()
            .zip(predicate_types.iter())
            .map(|(iter, lt)| lt.serialize_output(iter))
            .collect();

        struct CombinedIters<'a>(Vec<Box<dyn Iterator<Item = String> + 'a>>);

        impl<'a> Iterator for CombinedIters<'a> {
            type Item = Vec<String>;

            fn next(&mut self) -> Option<Self::Item> {
                let res: Self::Item = self.0.iter_mut().filter_map(|iter| iter.next()).collect();
                (!res.is_empty()).then_some(res)
            }
        }

        let combined_iters = CombinedIters(logically_mapped_iters);

        Ok(Some(combined_iters))
    }

    /// Counts the facts of a single predicate.
    ///
    /// TODO: Currently only counting of in-memory facts is supported, see <https://github.com/knowsys/nemo/issues/335>
    pub fn count_facts_of_predicate(&self, predicate: &Identifier) -> Option<usize> {
        self.table_manager.predicate_count_rows(predicate)
    }

    /// Count the number of facts of derived predicates.
    ///
    /// TODO: Currently only counting of in-memory facts is supported, see <https://github.com/knowsys/nemo/issues/335>
    pub fn count_facts_of_derived_predicates(&self) -> usize {
        let mut result = 0;

        for predicate in &self.analysis.derived_predicates {
            if let Some(count) = self.count_facts_of_predicate(predicate) {
                result += count;
            }
        }

        result
    }

    /// Return the amount of consumed memory for the tables used by the chase.
    pub fn memory_usage(&self) -> MemoryUsage {
        self.table_manager.memory_usage()
    }

    /// Recursive part of the function `trace`.
    /// Takes as input the fact of which the trace should be computed.
    /// The fact is passed as a predicate and its terms given as [`Term`]s and their physical values.
    fn trace_recursive(
        &self,
        fact_predicate: Identifier,
        fact_terms: Vec<Term>,
        fact_values: Vec<StorageValueT>,
    ) -> Result<Option<ExecutionTrace>, Error> {
        // Find the origin of the given fact
        let step = match self
            .table_manager
            .find_table_row(&fact_predicate, &fact_values)
        {
            Some(s) => s,
            None => return Ok(None),
        };

        if step == 0 {
            // If fact has no predecesor rule it must have been given as an EDB fact

            return Ok(Some(ExecutionTrace::Fact(Atom::new(
                fact_predicate,
                fact_terms.into_iter().map(TermTree::leaf).collect(),
            ))));
        }

        // Rule index of the rule that was applied to derive the given fact
        let rule_index = self.rule_history[step];
        let rule = &self.program.rules()[rule_index];

        let predicate_types: &Vec<PrimitiveType> = self
            .analysis
            .predicate_types
            .get(&fact_predicate)
            .expect("Every predicate should be associated with a type.");

        // Iterate over all head atoms which could have derived the given fact
        for (head_index, head_atom) in rule.head().iter().enumerate() {
            if head_atom.predicate() != fact_predicate {
                continue;
            }

            // Unify the head atom with the given fact
            // If unification is possible `compatible` remain true
            // and `assignment` will contain the match which is responsible for the fact
            let mut compatible = true;
            let mut assignment = VariableAssignment::new();

            for (ty, (head_term, fact_term)) in predicate_types
                .iter()
                .zip(head_atom.terms().iter().zip(fact_terms.iter()))
            {
                if head_term.is_ground() {
                    if ty.ground_term_to_data_value_t(head_term.clone())
                        != ty.ground_term_to_data_value_t(fact_term.clone())
                    {
                        compatible = false;
                        break;
                    }
                } else if let Term::Variable(variable) = head_term {
                    // Matching with existential variables should not produce any restrictions,
                    // so we just consider universal variables here
                    if variable.is_existential() {
                        continue;
                    }

                    if rule.constructors().contains_key(variable) {
                        // TODO: Support arbitrary operations in the head
                        return Err(Error::TraceUnsupportedFeature());
                    }

                    match assignment.entry(variable.clone()) {
                        Entry::Occupied(entry) => {
                            if ty.ground_term_to_data_value_t(entry.get().clone())
                                != ty.ground_term_to_data_value_t(fact_term.clone())
                            {
                                compatible = false;
                                break;
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(fact_term.clone());
                        }
                    }
                } else {
                    unreachable!("Variables are the only non-ground terms");
                }
            }

            if !compatible {
                // Fact could not haven unified
                continue;
            }

            // The goal of this part of the code is to apply the rule which led to the given fact
            // but with the variable binding derived from the unification above

            let new_filters: Vec<Filter> = assignment
                .iter()
                .map(|(variable, term)| {
                    Filter::new(FilterOperation::Equals, variable.clone(), term.clone())
                })
                .collect();

            let mut rule = self.program.rules()[rule_index].clone();
            rule.positive_filters_mut().extend(new_filters.into_iter());
            let analysis = &self.analysis.rule_analysis[rule_index];

            let body_execution = SeminaiveStrategy::initialize(&rule, analysis);
            let mut current_plan = SubtableExecutionPlan::default();
            let mut variable_order = analysis.promising_variable_orders[0].clone(); // TODO: This selection is arbitrary

            // When going backwards we disable semi-naive evaluation
            let rule_info = RuleInfo {
                step_last_applied: 0,
            };

            let node = body_execution.add_plan_body(
                &self.table_manager,
                &mut current_plan,
                &rule_info,
                &mut variable_order,
                step,
            );

            // `execute_plan_first_match` requires one output node marked as permanent
            current_plan.plan_mut().clear_write_nodes();
            current_plan.add_permanent_table(
                node,
                "Tracing Query",
                "Tracing Query",
                SubtableIdentifier::new(fact_predicate.clone(), step),
            );

            if let Ok(Some(query_result)) =
                self.table_manager.execute_plan_first_match(current_plan)
            {
                // Convert query answer from the phyical representation into logical Term-representation
                let query_types: Vec<&PrimitiveType> = variable_order
                    .as_ordered_list()
                    .iter()
                    .map(|v| {
                        analysis
                            .variable_types
                            .get(v)
                            .expect("Every variable must have been assigned a type")
                    })
                    .collect();

                let query_terms: Vec<Term> = query_result
                    .iter()
                    .zip(query_types.iter())
                    .map(|(entry, ty)| {
                        let mut iterator = ty.primitive_logical_value_iterator(
                            self.table_manager
                                .database()
                                .storage_to_data_iterator(ty.datatype_name(), entry.iter_once()),
                        );

                        iterator
                            .next()
                            .expect("Iterator must contain at least one value")
                            .into()
                    })
                    .collect();

                let rule_assignment: VariableAssignment = variable_order
                    .as_ordered_list()
                    .into_iter()
                    .zip(query_terms.iter().cloned())
                    .collect();

                let mut fully_derived = true;
                let mut subtraces = Vec::<ExecutionTrace>::new();
                for body_atom in rule.positive_body() {
                    let next_fact_predicate = body_atom.predicate();
                    let mut next_fact_values = Vec::<StorageValueT>::new();
                    let mut next_fact_terms = Vec::<Term>::new();

                    for body_term in body_atom.terms() {
                        // Use the query result to generate the next fact which should be traced
                        if let Term::Variable(variable) = body_term {
                            let query_index = *variable_order
                                .get(variable)
                                .expect("Variable order must contain every variable");

                            next_fact_values.push(query_result[query_index]);
                            next_fact_terms.push(query_terms[query_index].clone());
                        } else {
                            unreachable!(
                                "Normalized body atoms should only contain universal variables."
                            );
                        }
                    }

                    if let Some(trace) = self.trace_recursive(
                        next_fact_predicate,
                        next_fact_terms,
                        next_fact_values,
                    )? {
                        subtraces.push(trace);
                    } else {
                        fully_derived = false;
                        break;
                    }
                }

                if !fully_derived {
                    continue;
                }

                return Ok(Some(ExecutionTrace::Rule(
                    RuleApplication::new(
                        self.input_program.rules()[rule_index].clone(),
                        rule_assignment,
                        head_index,
                    ),
                    subtraces,
                )));
            } else {
                continue;
            }
        }

        Ok(None)
    }

    /// Return a [`ExecutionTrace`] for a given fact
    pub fn trace(&self, fact: Fact) -> Result<Option<ExecutionTrace>, Error> {
        let predicate = fact.0.predicate();

        if !self.table_manager.predicate_exists(&predicate) {
            return Ok(None);
        }

        // Convert fact into physical representation
        let mut fact_values = Vec::<StorageValueT>::new();
        for entry in Self::fact_to_vec(&fact, &self.analysis) {
            if let Some(value) = entry.to_storage_value(self.table_manager.get_dict().borrow_mut())
            {
                fact_values.push(value);
            } else {
                // Dictionary does not contain term
                return Ok(None);
            }
        }

        self.trace_recursive(predicate, fact.0.terms().cloned().collect(), fact_values)
    }
}
