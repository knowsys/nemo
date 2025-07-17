//! Functionality which handles the execution of a program

use std::collections::{hash_map::Entry, HashMap};

use itertools::Itertools;

use nemo_physical::{
    datavalues::AnyDataValue,
    dictionary::DvDict,
    management::database::sources::{SimpleTable, TableSource},
    meta::timing::TimedCode,
};

use crate::{
    chase_model::{
        analysis::{program_analysis::ProgramAnalysis, variable_order::VariableOrder},
        components::{
            atom::{ground_atom::GroundAtom, ChaseAtom},
            export::ChaseExport,
            program::ChaseProgram,
            rule::ChaseRule,
        },
        translation::ProgramChaseTranslation,
    },
    error::{report::ProgramReport, warned::Warned, Error},
    execution::{
        planning::plan_tracing::TracingStrategy,
        tracing::{
            shared::{Rule as TraceRule, TableEntryQuery},
            trace::TraceDerivation,
        },
    },
    io::{formats::Export, import_manager::ImportManager},
    rule_file::RuleFile,
    rule_model::{
        components::{
            atom::Atom,
            fact::Fact,
            tag::Tag,
            term::{
                primitive::{ground::GroundTerm, variable::Variable, Primitive},
                Term,
            },
        },
        pipeline::transformations::default::TransformationDefault,
        programs::{handle::ProgramHandle, program::Program},
        substitution::Substitution,
    },
    table_manager::{MemoryUsage, SubtableExecutionPlan, TableManager},
};

use super::{
    execution_parameters::ExecutionParameters,
    rule_execution::RuleExecution,
    selection_strategy::strategy::RuleSelectionStrategy,
    tracing::{
        error::TracingError,
        node_query::{
            TableEntriesForTreeNodesQuery, TableEntriesForTreeNodesQueryInner,
            TableEntriesForTreeNodesResponse, TableEntriesForTreeNodesResponseElement, TreeAddress,
        },
        shared::{PaginationQuery, PaginationResponse, TableEntryResponse},
        trace::{ExecutionTrace, TraceFactHandle, TraceRuleApplication, TraceStatus},
        tree_query::{TreeForTableQuery, TreeForTableResponse, TreeForTableResponseSuccessor},
    },
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
    /// Create new [RuleInfo].
    pub fn new() -> Self {
        Self {
            step_last_applied: 0,
        }
    }
}

/// Object which handles the evaluation of the program.
#[derive(Debug)]
pub struct ExecutionEngine<RuleSelectionStrategy> {
    /// Logical program
    nemo_program: Program,

    /// Normalized program
    program: ChaseProgram,
    /// Auxillary information for `program`
    analysis: ProgramAnalysis,

    /// The picked selection strategy for rules
    rule_strategy: RuleSelectionStrategy,

    /// Management of tables that represent predicates
    table_manager: TableManager,

    /// Stores for each predicate the number of subtables
    predicate_fragmentation: HashMap<Tag, usize>,
    /// Stores for each predicate the step up until subtables have been combined
    predicate_last_union: HashMap<Tag, usize>,

    /// For each rule in `program` additional information
    rule_infos: Vec<RuleInfo>,
    /// For each step the rule index of the applied rule
    rule_history: Vec<usize>,
    /// Current step
    current_step: usize,
}

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    /// Initialize a [ExecutionEngine] by parsing and translating
    /// the contents of the given file.
    pub fn from_file(
        file: RuleFile,
        parameters: ExecutionParameters,
    ) -> Result<Warned<Self, ProgramReport>, Error> {
        let handle = ProgramHandle::from_file(&file);
        let report = ProgramReport::new(file);

        let (program, report) = report.merge_program_parser_report(handle)?;
        let (program, report) = report.merge_validation_report(
            &program,
            program.transform(TransformationDefault::new(&parameters)),
        )?;

        let engine = Self::initialize(program.materialize(), parameters.import_manager)?;

        report.warned(engine)
    }

    /// Initialize [ExecutionEngine].
    pub fn initialize(program: Program, import_manager: ImportManager) -> Result<Self, Error> {
        let chase_program = ProgramChaseTranslation::new().translate(&program);
        let analysis = chase_program.analyze();

        let mut table_manager = TableManager::new();
        Self::register_all_predicates(&mut table_manager, &analysis);
        Self::add_all_constants(&mut table_manager, &chase_program);
        Self::add_imports(&mut table_manager, &import_manager, &chase_program)?;

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
            nemo_program: program,
            program: chase_program,
            analysis,
            rule_strategy,
            table_manager,
            predicate_fragmentation: HashMap::new(),
            predicate_last_union: HashMap::new(),
            rule_infos,
            rule_history: vec![usize::MAX], // Placeholder, Step counting starts at 1
            current_step: 1,
        })
    }

    /// Register all predicates found in a rule program to the [TableManager].
    fn register_all_predicates(table_manager: &mut TableManager, analysis: &ProgramAnalysis) {
        for (predicate, arity) in &analysis.all_predicates {
            table_manager.register_predicate(predicate.clone(), *arity);
        }
    }

    /// Add all constants appearing in the rules of the program to the dictionary.
    fn add_all_constants(table_manager: &mut TableManager, program: &ChaseProgram) {
        for value in program.datavalues() {
            table_manager.dictionary_mut().add_datavalue(value);
        }
    }

    /// Add edb tables to the [TableManager]
    /// based on the import declaration of the given progam.
    fn add_imports(
        table_manager: &mut TableManager,
        import_manager: &ImportManager,
        program: &ChaseProgram,
    ) -> Result<(), Error> {
        let mut predicate_to_sources = HashMap::<Tag, Vec<TableSource>>::new();

        // Add all the import specifications
        for import in program.imports() {
            let table_source = import_manager.table_provider_from_handler(import.handler())?;

            predicate_to_sources
                .entry(import.predicate().clone())
                .or_default()
                .push(table_source);
        }

        // Add all the facts contained in the rule file as a source
        let mut predicate_to_rows = HashMap::<Tag, SimpleTable>::new();

        for fact in program.facts() {
            let table = predicate_to_rows
                .entry(fact.predicate())
                .or_insert(SimpleTable::new(fact.arity()));
            table.add_row(fact.datavalues().collect());
        }

        for (predicate, table) in predicate_to_rows.into_iter() {
            predicate_to_sources
                .entry(predicate)
                .or_default()
                .push(Box::new(table));
        }

        // Add all the sources to the table manager
        for (predicate, sources) in predicate_to_sources {
            table_manager.add_edb(predicate, sources);
        }

        Ok(())
    }

    fn step(&mut self, rule_index: usize, execution: &RuleExecution) -> Result<Vec<Tag>, Error> {
        let timing_string = format!("Reasoning/Rules/Rule {rule_index}");

        TimedCode::instance().sub(&timing_string).start();
        log::info!("<<< {0}: APPLYING RULE {rule_index} >>>", self.current_step);

        self.rule_history.push(rule_index);

        let current_info = &mut self.rule_infos[rule_index];

        let updated_predicates =
            execution.execute(&mut self.table_manager, current_info, self.current_step)?;

        current_info.step_last_applied = self.current_step;

        let rule_duration = TimedCode::instance().sub(&timing_string).stop();
        log::info!("Rule duration: {} ms", rule_duration.as_millis());

        self.current_step += 1;
        Ok(updated_predicates)
    }

    fn defrag(&mut self, updated_predicates: Vec<Tag>) -> Result<(), Error> {
        for updated_pred in updated_predicates {
            let counter = self
                .predicate_fragmentation
                .entry(updated_pred.clone())
                .or_insert(0);
            *counter += 1;

            if *counter == MAX_FRAGMENTATION {
                let start = if let Some(last_union) = self.predicate_last_union.get(&updated_pred) {
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

        while let Some(index) = self.rule_strategy.next_rule(new_derivations) {
            let updated_predicates = self.step(index, &rule_execution[index])?;
            new_derivations = Some(!updated_predicates.is_empty());

            self.defrag(updated_predicates)?;
        }

        TimedCode::instance().sub("Reasoning/Rules").stop();
        TimedCode::instance().sub("Reasoning/Execution").stop();
        Ok(())
    }

    /// Return a reference to the current [Program].
    pub fn program(&self) -> &Program {
        &self.nemo_program
    }

    /// Get a reference to the loaded program.
    pub(crate) fn chase_program(&self) -> &ChaseProgram {
        &self.program
    }

    /// Creates an [Iterator] over all facts of a predicate.
    pub fn predicate_rows(
        &mut self,
        predicate: &Tag,
    ) -> Result<Option<impl Iterator<Item = Vec<AnyDataValue>> + '_>, Error> {
        let Some(table_id) = self.table_manager.combine_predicate(predicate)? else {
            return Ok(None);
        };

        Ok(Some(self.table_manager.table_row_iterator(table_id)?))
    }

    /// Returns the arity of the predicate if the predicate is known to the engine,
    /// and `None` otherwise.
    pub fn predicate_arity(&self, predicate: &Tag) -> Option<usize> {
        self.analysis.all_predicates.get(predicate).copied()
    }

    /// Return a list of all all export predicates and their respective [`Export`]s,
    /// which can be used for exporting into files.
    pub fn exports(&self) -> Vec<(Tag, Export)> {
        self.program
            .exports()
            .iter()
            .cloned()
            .map(ChaseExport::into_predicate_and_handler)
            .collect()
    }

    /// Counts the facts of a single predicate that are currently in memory.
    pub fn count_facts_in_memory_for_predicate(&self, predicate: &Tag) -> Option<usize> {
        self.table_manager
            .count_rows_in_memory_for_predicate(predicate)
    }

    /// Count the number of facts of derived predicates that are currently in memory.
    pub fn count_facts_in_memory_for_derived_predicates(&self) -> usize {
        self.analysis
            .derived_predicates
            .iter()
            .map(|p| self.count_facts_in_memory_for_predicate(p).unwrap_or(0))
            .sum()
    }

    /// Return the amount of consumed memory for the tables used by the chase.
    pub fn memory_usage(&self) -> MemoryUsage {
        self.table_manager.memory_usage()
    }

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

    fn trace_next_facts(
        grounding: &[AnyDataValue],
        next_facts: &mut [Vec<GroundAtom>],
        rule: &ChaseRule,
        variable_order: &VariableOrder,
    ) {
        let variable_assignment: HashMap<Variable, AnyDataValue> = variable_order
            .as_ordered_list()
            .into_iter()
            .zip(grounding.iter().cloned())
            .collect();

        for (body_index, body_atom) in rule.positive_body().iter().enumerate() {
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
            next_facts[body_index].push(next_fact);
        }
    }

    fn trace_tree_recursive(&mut self, facts: Vec<GroundAtom>) -> Option<TreeForTableResponse> {
        let predicate = if let Some(first_fact) = facts.first() {
            first_fact.predicate() // We assume that all facts have the same predicate
        } else {
            return None;
        };

        // Prepare the result, which contains some information independant of tracing results below
        let entries =
            facts
                .iter()
                .map(|fact| {
                    Some(TableEntryResponse {
                        entry_id: self.predicate_rows(&predicate).ok().flatten()?.position(
                            |row| fact.terms().map(|t| t.value()).collect::<Vec<_>>() == row,
                        )?,
                        terms: fact.terms().map(|term| term.value()).collect(),
                    })
                })
                .collect::<Option<Vec<_>>>()?;

        let possible_rules_above = self
            .analysis
            .predicate_to_rule_body
            .get(&predicate)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .flat_map(|idx| {
                TraceRule::all_possible_single_head_rules(idx, self.program().rule(idx))
            })
            .collect::<Vec<_>>();

        let possible_rules_below = self
            .analysis
            .predicate_to_rule_head
            .get(&predicate)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .flat_map(|idx| {
                TraceRule::possible_rules_for_head_predicate(
                    idx,
                    self.program().rule(idx),
                    &predicate,
                )
            })
            .collect::<Vec<_>>();

        let mut result = TreeForTableResponse {
            predicate: predicate.to_string(),
            entries,
            pagination: PaginationResponse {
                start: 0,
                more: false,
            },
            possible_rules_above,
            possible_rules_below,
            next: None,
        };

        // Get the steps
        let steps = facts
            .iter()
            .map(|fact| {
                self.table_manager
                    .find_table_row(&predicate, &fact.datavalues().collect::<Vec<_>>())
            })
            .collect::<Option<Vec<usize>>>()?;

        // Some of the traced facts come from the input database
        if steps.iter().contains(&0) {
            return Some(result);
        }

        // Get the rule that has been applied in each step
        // We can only proceed if every fact has been derived by the same rule
        let rule_index = self.rule_history[steps[0]];
        if steps
            .iter()
            .any(|&step| self.rule_history[step] != rule_index)
        {
            return Some(result);
        }

        let chase_rule = &self.chase_program().rules()[rule_index].clone();
        let rule = &self.program().rule(rule_index).clone();

        for (head_index, _) in rule.head().iter().enumerate() {
            let combination = facts.iter().cloned().map(|fact| {
                partial_grounding_for_rule_head_and_fact(chase_rule.clone(), head_index, fact)
            });

            let mut query_results = Vec::new();

            for (partial_grounding, &step) in combination.into_iter().zip(steps.iter()) {
                let rule = &self.program.rules()[rule_index];
                let analysis = &self.analysis.rule_analysis[rule_index];

                let trace_strategy = TracingStrategy::initialize(rule, partial_grounding?).ok()?;

                let mut execution_plan = SubtableExecutionPlan::default();
                let mut variable_order = analysis.promising_variable_orders[0].clone(); // TODO: This selection is arbitrary

                trace_strategy.add_plan(
                    &self.table_manager,
                    &mut execution_plan,
                    &mut variable_order,
                    step,
                );

                // Here, we simply compute all results to iterate over each
                // possible grounding (as different groundings may lead to different results)
                let query_result = self
                    .table_manager
                    .execute_plan_trie(execution_plan)
                    .ok()?
                    .pop()
                    .unwrap();

                query_results.push(query_result);
            }

            let results = query_results
                .iter()
                .map(|query_result| {
                    self.table_manager
                        .trie_row_iterator(query_result)
                        .map(|res| res.collect::<Vec<_>>())
                })
                .collect::<Result<Vec<_>, Error>>()
                .ok()?;

            if self.program.rules()[rule_index].aggregate().is_some() {
                // If rule is an aggregate rule then we continue with all matches

                let mut next_facts =
                    vec![Vec::new(); self.program.rules()[rule_index].positive_body().len()];

                for groundings in results {
                    for grounding in groundings {
                        let rule = &self.program.rules()[rule_index];
                        let analysis = &self.analysis.rule_analysis[rule_index];

                        let variable_order = analysis.promising_variable_orders[0].clone(); // TODO: This selection is arbitrary

                        Self::trace_next_facts(&grounding, &mut next_facts, rule, &variable_order);
                    }
                }

                let children_option = next_facts
                    .into_iter()
                    .map(|facts| self.trace_tree_recursive(facts))
                    .collect::<Option<Vec<TreeForTableResponse>>>();

                if let Some(children) = children_option {
                    result.next = Some(TreeForTableResponseSuccessor {
                        rule: TraceRule::from_rule_and_head(
                            rule_index,
                            rule,
                            head_index,
                            &rule.head()[head_index],
                        ),
                        children,
                    });

                    return Some(result);
                }
            } else {
                // If rule is not an aggregate rule then we try all combination of matches
                // to see which one works

                for groundings in results.into_iter().multi_cartesian_product() {
                    let mut next_facts =
                        vec![Vec::new(); self.program.rules()[rule_index].positive_body().len()];

                    for grounding in groundings {
                        let rule = &self.program.rules()[rule_index];
                        let analysis = &self.analysis.rule_analysis[rule_index];

                        let variable_order = analysis.promising_variable_orders[0].clone(); // TODO: This selection is arbitrary

                        Self::trace_next_facts(&grounding, &mut next_facts, rule, &variable_order);
                    }

                    let children_option = next_facts
                        .into_iter()
                        .map(|facts| self.trace_tree_recursive(facts))
                        .collect::<Option<Vec<TreeForTableResponse>>>();

                    if let Some(children) = children_option {
                        result.next = Some(TreeForTableResponseSuccessor {
                            rule: TraceRule::from_rule_and_head(
                                rule_index,
                                rule,
                                head_index,
                                &rule.head()[head_index],
                            ),
                            children,
                        });

                        return Some(result);
                    }
                }
            }
        }

        None
    }

    /// Evaluate a [TreeForTableQuery].
    pub fn trace_tree(&mut self, query: TreeForTableQuery) -> Result<TreeForTableResponse, Error> {
        let mut facts = Vec::new();

        for fact_query in query.queries {
            let fact = match fact_query {
                TableEntryQuery::Entry(row_index) => {
                    let terms_to_trace: Vec<AnyDataValue> = self
                        .predicate_rows(&Tag::new(query.predicate.clone()))?
                        .into_iter()
                        .flatten()
                        .nth(row_index)
                        .ok_or(Error::TracingError(TracingError::InvalidFactId {
                            predicate: query.predicate.clone(),
                            id: row_index,
                        }))?;

                    GroundAtom::new(
                        Tag::new(query.predicate.clone()),
                        terms_to_trace
                            .into_iter()
                            .map(GroundTerm::from)
                            .collect(),
                    )
                }
                TableEntryQuery::Query(query_string) => {
                    // TODO: Support patterns
                    let atom = Atom::parse(&format!("{}({})", query.predicate, query_string))
                        .map_err(|_| {
                            Error::TracingError(TracingError::InvalidFactQuery {
                                predicate: query.predicate.clone(),
                                query: query_string.clone(),
                            })
                        })?;
                    GroundAtom::try_from(atom).map_err(|_| {
                        Error::TracingError(TracingError::InvalidFactQuery {
                            predicate: query.predicate.clone(),
                            query: query_string,
                        })
                    })?
                }
            };

            facts.push(fact);
        }

        if let Some(result) = self.trace_tree_recursive(facts) {
            Ok(result)
        } else {
            let predicate = Tag::new(query.predicate.clone());

            let possible_rules_above = self
                .analysis
                .predicate_to_rule_body
                .get(&predicate)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .flat_map(|idx| {
                    TraceRule::all_possible_single_head_rules(idx, self.program().rule(idx))
                })
                .collect::<Vec<_>>();

            let possible_rules_below = self
                .analysis
                .predicate_to_rule_head
                .get(&predicate)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .flat_map(|idx| {
                    TraceRule::possible_rules_for_head_predicate(
                        idx,
                        self.program().rule(idx),
                        &predicate,
                    )
                })
                .collect::<Vec<_>>();

            Ok(TreeForTableResponse {
                predicate: query.predicate,
                entries: vec![],
                pagination: PaginationResponse {
                    start: query
                        .pagination
                        .map(|pagination| pagination.start)
                        .unwrap_or(0),
                    more: false,
                },
                possible_rules_above,
                possible_rules_below,
                next: None,
            })
        }
    }

    fn check_fact(fact: &GroundAtom, query_string: &str) -> bool {
        let atom = if let Ok(atom) = Atom::parse(&format!("{}({})", fact.predicate(), query_string))
        {
            atom
        } else {
            return false;
        };

        let mut assignment = HashMap::<Variable, AnyDataValue>::new();

        if fact.terms().count() != atom.len() {
            return false;
        }

        for (fact_term, atom_term) in fact.terms().zip(atom.terms()) {
            match atom_term {
                Term::Primitive(primitive) => match primitive {
                    Primitive::Variable(variable) => match assignment.entry(variable.clone()) {
                        Entry::Occupied(entry) => {
                            if &fact_term.value() != entry.get() {
                                return false;
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(fact_term.value());
                        }
                    },
                    Primitive::Ground(ground) => {
                        if fact_term.value() != ground.value() {
                            return false;
                        }
                    }
                },
                Term::Aggregate(_)
                | Term::FunctionTerm(_)
                | Term::Map(_)
                | Term::Operation(_)
                | Term::Tuple(_) => return false,
            }
        }

        true
    }

    fn trace_node_recursive(
        &mut self,
        inner: &TableEntriesForTreeNodesQueryInner,
        fact: GroundAtom,
        address: TreeAddress,
    ) -> TraceNodeResult {
        // Check if fact passes the query filter
        if !inner.queries.is_empty()
            && inner.queries.iter().all(|query| match query {
                TableEntryQuery::Entry(row_index) => {
                    let terms_to_trace_opt: Option<Vec<AnyDataValue>> = self
                        .predicate_rows(&fact.predicate())
                        .ok()
                        .flatten()
                        .into_iter()
                        .flatten()
                        .nth(*row_index);

                    let fact_at_index_opt = terms_to_trace_opt.map(|terms_to_trace| {
                        GroundAtom::new(
                            fact.predicate(),
                            terms_to_trace
                                .into_iter()
                                .map(GroundTerm::from)
                                .collect(),
                        )
                    });

                    fact_at_index_opt.map(|f| f != fact).unwrap_or(true)
                }
                TableEntryQuery::Query(query_string) => !Self::check_fact(&fact, query_string),
            })
        {
            return TraceNodeResult::default();
        }

        let step = if let Some(step) = self
            .table_manager
            .find_table_row(&fact.predicate(), &fact.datavalues().collect::<Vec<_>>())
        {
            step
        } else {
            return TraceNodeResult::default();
        };

        let no_restriction = if let Some(successor) = &inner.next {
            successor.children.is_empty()
        } else {
            true
        };

        if step == 0 && !no_restriction {
            return TraceNodeResult::default();
        }

        if no_restriction {
            return TraceNodeResult::single(address, inner.pagination, fact);
        }

        let rule_index = self.rule_history[step];
        let rule = self.program.rules()[rule_index].clone();

        if let Some(successor) = &inner.next {
            if rule_index != successor.rule {
                return TraceNodeResult::default();
            }

            let partial_grounding = partial_grounding_for_rule_head_and_fact(
                rule.clone(),
                successor.head_index,
                fact.clone(),
            );

            if partial_grounding.is_none() {
                return TraceNodeResult::default();
            }
        }

        let children = if let Some(successor) = &inner.next {
            &successor.children
        } else {
            unreachable!("no_restriction is false")
        };

        // Iterate over all head atoms which could have derived the given fact
        for (head_index, head_atom) in rule.head().iter().enumerate() {
            let is_aggregate_atom = Some(head_index) == rule.aggregate_head_index();
            let aggregate_variable = rule
                .aggregate()
                .map(|aggregate| aggregate.output_variable());

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

                        // Aggregate variables are not grounded
                        if is_aggregate_atom && Some(variable) == aggregate_variable {
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
            let trace_strategy = if let Ok(strategy) = TracingStrategy::initialize(&rule, grounding)
            {
                strategy
            } else {
                return TraceNodeResult::default();
            };

            let mut execution_plan = SubtableExecutionPlan::default();

            trace_strategy.add_plan(
                &self.table_manager,
                &mut execution_plan,
                &mut variable_order,
                step,
            );

            let query_trie =
                if let Ok(mut trie) = self.table_manager.execute_plan_trie(execution_plan) {
                    trie.pop().unwrap()
                } else {
                    return TraceNodeResult::default();
                };

            let query_result = if let Ok(result) = self.table_manager.trie_row_iterator(&query_trie)
            {
                result.collect::<Vec<_>>()
            } else {
                return TraceNodeResult::default();
            };

            let mut result =
                TraceNodeResult::single(address.clone(), inner.pagination, fact.clone());
            let mut accept = is_aggregate_atom;

            for grounding in query_result {
                let variable_assignment: HashMap<Variable, AnyDataValue> = variable_order
                    .as_ordered_list()
                    .into_iter()
                    .zip(grounding.iter().cloned())
                    .collect();

                let mut fully_derived = true;
                for (body_index, (body_atom, child)) in
                    rule.positive_body().iter().zip(children.iter()).enumerate()
                {
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
                    let mut next_address = address.clone();
                    next_address.push(body_index);

                    let derived_next = self.trace_node_recursive(child, next_fact, next_address);

                    if derived_next.is_empty() {
                        fully_derived = false;

                        break;
                    }

                    result.combine(derived_next);
                }

                if is_aggregate_atom && !fully_derived {
                    accept = false;
                    break;
                }

                if !is_aggregate_atom && fully_derived {
                    accept = true;
                    break;
                }
            }

            if accept {
                return result;
            } else {
                return TraceNodeResult::default();
            }
        }

        TraceNodeResult::default()
    }

    /// Evauate a [TableEntriesForTreeNodesQuery].
    pub fn trace_node(
        &mut self,
        query: TableEntriesForTreeNodesQuery,
    ) -> TableEntriesForTreeNodesResponse {
        let query_predicate = Tag::new(query.predicate.clone());

        let initial_facts = self
            .predicate_rows(&query_predicate)
            .ok()
            .flatten()
            .map(|iter| {
                iter.map(|row| {
                    GroundAtom::new(
                        query_predicate.clone(),
                        row.into_iter().map(GroundTerm::from).collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let mut result = TraceNodeResult::default();

        for fact in initial_facts {
            result.combine(self.trace_node_recursive(&query.inner, fact, Vec::new()));
        }

        let mut elements = Vec::new();

        for (address, pagination, facts) in result.iter() {
            let predicate = if let Some(fact) = facts.first() {
                fact.predicate()
            } else {
                continue;
            };

            let len_facts = facts.len();
            let (start, count) = if let Some(pagination) = pagination {
                (pagination.start, pagination.count)
            } else {
                (0, len_facts)
            };

            let entries = facts
                .iter()
                .map(|fact| {
                    Some(TableEntryResponse {
                        entry_id: self.predicate_rows(&predicate).ok().flatten()?.position(
                            |row| fact.terms().map(|t| t.value()).collect::<Vec<_>>() == row,
                        )?,
                        terms: fact.terms().map(|term| term.value()).collect(),
                    })
                })
                .skip(start)
                .take(count)
                .collect::<Option<Vec<_>>>()
                .unwrap_or(vec![]);

            let more = entries.len() + start < len_facts;

            let possible_rules_above = self
                .analysis
                .predicate_to_rule_body
                .get(&predicate)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .flat_map(|idx| {
                    TraceRule::all_possible_single_head_rules(idx, self.program().rule(idx))
                })
                .collect::<Vec<_>>();

            let possible_rules_below = self
                .analysis
                .predicate_to_rule_head
                .get(&predicate)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .flat_map(|idx| {
                    TraceRule::possible_rules_for_head_predicate(
                        idx,
                        self.program().rule(idx),
                        &predicate,
                    )
                })
                .collect::<Vec<_>>();

            let element = TableEntriesForTreeNodesResponseElement {
                predicate: predicate.to_string(),
                entries,
                pagination: PaginationResponse { start, more },
                possible_rules_above,
                possible_rules_below,
                address: address.clone(),
            };

            elements.push(element);
        }

        TableEntriesForTreeNodesResponse { elements }
    }
}

fn partial_grounding_for_rule_head_and_fact(
    rule: ChaseRule,
    head_index: usize,
    fact: GroundAtom,
) -> Option<HashMap<Variable, AnyDataValue>> {
    let head_atom = &rule.head()[head_index];

    let is_aggregate_atom = Some(head_index) == rule.aggregate_head_index();
    let aggregate_variable = rule
        .aggregate()
        .map(|aggregate| aggregate.output_variable());

    if head_atom.predicate() != fact.predicate() {
        return None;
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

                // Aggregate variables are not grounded
                if is_aggregate_atom && Some(variable) == aggregate_variable {
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
        return None;
    }

    Some(grounding)
}

#[derive(Debug, Default)]
struct TraceNodeResult {
    map: HashMap<TreeAddress, Vec<GroundAtom>>,
    pagination: HashMap<TreeAddress, Option<PaginationQuery>>,
}

impl TraceNodeResult {
    pub fn single(
        address: TreeAddress,
        pagination_query: Option<PaginationQuery>,
        fact: GroundAtom,
    ) -> Self {
        let mut map = HashMap::new();
        let mut pagination = HashMap::new();

        map.insert(address.clone(), vec![fact]);
        pagination.insert(address, pagination_query);

        Self { map, pagination }
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn combine(&mut self, other: Self) {
        for (address, atoms) in other.map {
            match self.map.entry(address) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().extend(atoms);
                }
                Entry::Vacant(entry) => {
                    entry.insert(atoms);
                }
            }
        }

        for (address, pagination) in other.pagination {
            match self.pagination.entry(address) {
                Entry::Occupied(_) => {}
                Entry::Vacant(entry) => {
                    entry.insert(pagination);
                }
            }
        }
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (&TreeAddress, Option<&PaginationQuery>, &Vec<GroundAtom>)> {
        self.map.iter().map(move |(addr, atoms)| {
            let pagination_opt = self.pagination.get(addr).and_then(|x| x.as_ref());
            (addr, pagination_opt, atoms)
        })
    }
}
