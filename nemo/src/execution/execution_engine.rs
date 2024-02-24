//! Functionality which handles the execution of a program

use std::collections::{hash_map::Entry, HashMap};

use nemo_physical::{
    datavalues::AnyDataValue,
    dictionary::DvDict,
    management::database::sources::{SimpleTable, TableSource},
    meta::timing::TimedCode,
};

use crate::{
    error::Error,
    execution::{planning::plan_tracing::TracingStrategy, tracing::trace::TraceDerivation},
    io::import_manager::ImportManager,
    model::{
        chase_model::{ChaseAtom, ChaseFact, ChaseProgram},
        Fact, Identifier, PrimitiveTerm, Program, Variable,
    },
    program_analysis::analysis::ProgramAnalysis,
    table_manager::{MemoryUsage, SubtableExecutionPlan, TableManager},
};

use super::{
    rule_execution::RuleExecution,
    selection_strategy::strategy::RuleSelectionStrategy,
    tracing::trace::{ExecutionTrace, TraceFactHandle, TraceRuleApplication, TraceStatus},
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
    program: ChaseProgram,
    analysis: ProgramAnalysis,

    rule_strategy: RuleSelectionStrategy,

    #[allow(dead_code)]
    input_manager: ImportManager,
    table_manager: TableManager,

    predicate_fragmentation: HashMap<Identifier, usize>,
    predicate_last_union: HashMap<Identifier, usize>,

    rule_infos: Vec<RuleInfo>,
    rule_history: Vec<usize>,
    current_step: usize,
}

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    /// Initialize [`ExecutionEngine`].
    pub fn initialize(program: &Program, input_manager: ImportManager) -> Result<Self, Error> {
        let chase_program: ChaseProgram = program.clone().try_into()?;

        let analysis = chase_program.analyze()?;

        let mut table_manager = TableManager::new();
        Self::register_all_predicates(&mut table_manager, &analysis);
        Self::add_all_constants(&mut table_manager, &chase_program);
        Self::add_imports(&mut table_manager, &input_manager, &chase_program)?;

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

    /// Register all predicates found in a rule program to the [`TableManager`].
    fn register_all_predicates(table_manager: &mut TableManager, analysis: &ProgramAnalysis) {
        for (predicate, arity) in &analysis.all_predicates {
            table_manager.register_predicate(predicate.clone(), *arity);
        }
    }

    /// Add all constants appearing in the rules of the program to the dictionary.
    fn add_all_constants(table_manager: &mut TableManager, program: &ChaseProgram) {
        for dv in program.all_datavalues() {
            table_manager.dictionary_mut().add_datavalue(dv.clone());
        }
    }

    /// Add edb tables to the [`TableManager`]
    /// based on the import declaration of the given progam.
    fn add_imports(
        table_manager: &mut TableManager,
        input_manager: &ImportManager,
        program: &ChaseProgram,
    ) -> Result<(), Error> {
        let mut predicate_to_sources = HashMap::<Identifier, Vec<TableSource>>::new();

        // Add all the import specifications
        for (import_predicate, import_handler) in program.imports() {
            let import_arity = table_manager.arity(import_predicate);
            let table_source = TableSource::new(
                input_manager.table_provider_from_handler(import_handler, import_arity)?,
                import_arity,
            );

            predicate_to_sources
                .entry(import_predicate.clone())
                .or_default()
                .push(table_source);
        }

        // Add all the facts contained in the rule file as a source
        let mut predicate_to_rows = HashMap::<Identifier, SimpleTable>::new();

        for fact in program.facts() {
            let table = predicate_to_rows
                .entry(fact.predicate())
                .or_insert(SimpleTable::new(fact.arity()));
            table.add_row(fact.terms().to_vec());
        }

        for (predicate, table) in predicate_to_rows.into_iter() {
            predicate_to_sources
                .entry(predicate)
                .or_default()
                .push(TableSource::from_simple_table(table));
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

                    self.table_manager.combine_tables(&updated_pred, range)?;

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
    pub(crate) fn program(&self) -> &ChaseProgram {
        &self.program
    }

    /// Creates an [Iterator] over all facts of a predicate.
    pub fn predicate_rows(
        &mut self,
        predicate: &Identifier,
    ) -> Result<Option<impl Iterator<Item = Vec<AnyDataValue>> + '_>, Error> {
        let Some(table_id) = self.table_manager.combine_predicate(predicate)? else {
            return Ok(None);
        };

        Ok(Some(self.table_manager.table_row_iterator(table_id)?))
    }

    /// Returns the arity of the predicate if the predicate is known to the engine,
    /// and `None` otherwise.
    pub fn predicate_arity(&self, predicate: &Identifier) -> Option<usize> {
        self.analysis.all_predicates.get(predicate).copied()
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

    fn trace_recursive(
        &mut self,
        program: &ChaseProgram,
        trace: &mut ExecutionTrace,
        fact: ChaseFact,
    ) -> TraceFactHandle {
        let trace_handle = trace.register_fact(fact.clone());

        if trace.status(trace_handle).is_known() {
            return trace_handle;
        }

        // Find the origin of the given fact
        let step = match self
            .table_manager
            .find_table_row(&fact.predicate(), fact.terms())
        {
            Some(s) => s,
            None => {
                // If the table manager does not know the predicate of the fact
                // then it could not have been derived
                trace.update_status(trace_handle, TraceStatus::Fail);
                return trace_handle;
            }
        };

        if step == 0 {
            // If a fact was derived in step 0 it must have been given as an EDB fact
            trace.update_status(trace_handle, TraceStatus::Success(TraceDerivation::Input));
            return trace_handle;
        }

        // Rule index of the rule that was applied to derive the given fact
        let rule_index = self.rule_history[step];
        let rule = &program.rules()[rule_index];

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

            for (head_term, fact_term) in head_atom.terms().iter().zip(fact.terms().iter()) {
                match head_term {
                    PrimitiveTerm::GroundTerm(ground) => {
                        if ground != fact_term {
                            compatible = false;
                            break;
                        }
                    }
                    PrimitiveTerm::Variable(variable) => {
                        // Matching with existential variables should not produce any restrictions,
                        // so we just consider universal variables here
                        if variable.is_existential() {
                            continue;
                        }

                        match grounding.entry(variable.clone()) {
                            Entry::Occupied(entry) => {
                                if entry.get() != fact_term {
                                    compatible = false;
                                    break;
                                }
                            }
                            Entry::Vacant(entry) => {
                                entry.insert(fact_term.clone());
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
            let trace_strategy = TracingStrategy::initialize(&rule, grounding);

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
                        .iter()
                        .map(|variable| {
                            variable_assignment
                                .get(variable)
                                .expect("Query must assign value to each variable.")
                                .clone()
                        })
                        .collect::<Vec<_>>();

                    let next_fact = ChaseFact::new(next_fact_predicate, next_fact_terms);

                    let next_handle = self.trace_recursive(program, trace, next_fact);

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

                let rule_application =
                    TraceRuleApplication::new(rule_index, variable_assignment, head_index);

                let derivation = TraceDerivation::Derived(rule_application, subtraces);
                trace.update_status(trace_handle, TraceStatus::Success(derivation));

                return trace_handle;
            } else {
                continue;
            }
        }

        trace.update_status(trace_handle, TraceStatus::Fail);
        trace_handle
    }

    /// Build an [`ExecutionTrace`] for a list of facts.
    /// Also returns a list containing a [`TraceFactHandle`] for each fact.
    pub fn trace(
        &mut self,
        program: Program,
        facts: Vec<Fact>,
    ) -> (ExecutionTrace, Vec<TraceFactHandle>) {
        let mut trace = ExecutionTrace::new(program);

        let mut handles = Vec::new();

        for fact in facts {
            let chase_fact = ChaseFact::from_flat_atom(&fact.0);

            let program = self.program().clone();

            handles.push(self.trace_recursive(&program, &mut trace, chase_fact));
        }

        (trace, handles)
    }
}
