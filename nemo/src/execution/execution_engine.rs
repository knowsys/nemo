//! Functionality which handles the execution of a program

use std::collections::HashMap;

use nemo_physical::{
    datatypes::{data_value::DataValueIteratorT, DataValueT},
    management::database::TableSource,
    meta::TimedCode,
};

use crate::{
    error::Error,
    io::input_manager::{InputManager, ResourceProviders},
    model::{chase_model::ChaseProgram, Identifier, Program, TermOperation},
    program_analysis::analysis::ProgramAnalysis,
    table_manager::TableManager,
    types::LogicalTypeEnum,
};

use super::{rule_execution::RuleExecution, selection_strategy::strategy::RuleSelectionStrategy};

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
    input_manager: InputManager,
    table_manager: TableManager,

    predicate_fragmentation: HashMap<Identifier, usize>,
    predicate_last_union: HashMap<Identifier, usize>,

    rule_infos: Vec<RuleInfo>,
    current_step: usize,
}

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    /// Initialize [`ExecutionEngine`].
    pub fn initialize(
        program: Program,
        resource_providers: ResourceProviders,
    ) -> Result<Self, Error> {
        let mut program: ChaseProgram = program.try_into()?;

        program.check_for_unsupported_features()?;
        program.normalize();

        let analysis = program.analyze()?;

        let input_manager = InputManager::new(resource_providers);

        let mut table_manager = TableManager::new();
        Self::register_all_predicates(&mut table_manager, &analysis);
        Self::add_sources(&mut table_manager, &input_manager, &program, &analysis)?;

        let mut rule_infos = Vec::<RuleInfo>::new();
        program
            .rules()
            .iter()
            .for_each(|_| rule_infos.push(RuleInfo::new()));

        let rule_strategy = Strategy::new(
            program.rules().iter().collect(),
            analysis.rule_analysis.iter().collect(),
        )?;

        Ok(Self {
            program,
            analysis,
            rule_strategy,
            input_manager,
            table_manager,
            predicate_fragmentation: HashMap::new(),
            predicate_last_union: HashMap::new(),
            rule_infos,
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

    fn add_sources(
        table_manager: &mut TableManager,
        input_manager: &InputManager,
        program: &ChaseProgram,
        analysis: &ProgramAnalysis,
    ) -> Result<(), Error> {
        let mut predicate_to_sources = HashMap::<Identifier, Vec<TableSource>>::new();

        // Add all the data source declarations
        for (predicate, _, input_types, data_source) in program.sources() {
            let logical_types = analysis
                .predicate_types
                .get(predicate)
                .cloned()
                .expect("All predicates should have types by now.");

            let reader_types = logical_types
                .iter()
                .zip(input_types)
                .map(|(lt, it)| {
                    if *lt == LogicalTypeEnum::Any && *it == LogicalTypeEnum::String {
                        LogicalTypeEnum::String
                    } else {
                        *lt
                    }
                })
                .collect::<Vec<_>>();

            let table_source = input_manager.load_table_source(data_source, reader_types)?;

            predicate_to_sources
                .entry(predicate.clone())
                .or_default()
                .push(table_source)
        }

        // Add all the facts contained in the rule file as a source
        let mut predicate_to_rows = HashMap::<Identifier, Vec<Vec<DataValueT>>>::new();

        for fact in program.facts() {
            let new_row: Vec<DataValueT> = fact
                .0
                .terms()
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
                .collect();

            let rows = predicate_to_rows
                .entry(fact.0.predicate())
                .or_insert(Vec::new());
            rows.push(new_row);
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

    /// Creates an [`Iterator`] over the resulting facts of a predicate.
    // TODO: we probably want to return a list of column iterators over logical values
    pub fn table_scan(
        &mut self,
        predicate: Identifier,
    ) -> Result<Option<impl Iterator<Item = Vec<DataValueT>> + '_>, Error> {
        let Some(table_id) = self.table_manager.combine_predicate(predicate.clone())? else {
            return Ok(None);
        };

        let predicate_types: &Vec<LogicalTypeEnum> = self
            .analysis
            .predicate_types
            .get(&predicate)
            .expect("All predicates should have types by now.");

        let iterators = self.table_manager.table_column_iters(table_id)?;

        let logically_mapped_iters = iterators.into_iter().zip(predicate_types.iter()).map(|(iter, lt)| {
            if *lt == LogicalTypeEnum::String {
                match iter {
                    DataValueIteratorT::String(string_iter) => Box::new(string_iter.map(|s| {
                        DataValueT::String(s.get(1..(s.len() - 1))
                            .expect("The physical string is wrapped in quotes.")
                            .to_string())
                    })),
                _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch."),
                }
            } else {
                let boxed_iter: Box<dyn Iterator<Item = DataValueT>> = match iter {
                    DataValueIteratorT::String(iter) => Box::new(iter.map(DataValueT::String)),
                    DataValueIteratorT::U32(iter) => Box::new(iter.map(DataValueT::U32)),
                    DataValueIteratorT::U64(iter) => Box::new(iter.map(DataValueT::U64)),
                    DataValueIteratorT::I64(iter) => Box::new(iter.map(DataValueT::I64)),
                    DataValueIteratorT::Float(iter) => Box::new(iter.map(DataValueT::Float)),
                    DataValueIteratorT::Double(iter) => Box::new(iter.map(DataValueT::Double)),
                };

                boxed_iter
            }
        });

        struct CombinedIters<'a>(Vec<Box<dyn Iterator<Item = DataValueT> + 'a>>);

        impl<'a> Iterator for CombinedIters<'a> {
            type Item = Vec<DataValueT>;

            fn next(&mut self) -> Option<Self::Item> {
                let res: Self::Item = self.0.iter_mut().filter_map(|iter| iter.next()).collect();
                (!res.is_empty()).then_some(res)
            }
        }

        let combined_iters = CombinedIters(logically_mapped_iters.collect());

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

        let predicate_types: &Vec<LogicalTypeEnum> = self
            .analysis
            .predicate_types
            .get(&predicate)
            .expect("All predicates should have types by now.");

        let iterators = self.table_manager.table_column_iters(table_id)?;

        let logically_mapped_iters = iterators.into_iter().zip(predicate_types.iter()).map(|(iter, lt)| {
            if *lt == LogicalTypeEnum::String {
                match iter {
                    DataValueIteratorT::String(string_iter) => Box::new(string_iter.map(|s| {
                        s.get(1..(s.len() - 1))
                            .expect("The physical string is wrapped in quotes.")
                            .to_string()
                    })),
                _ => unreachable!("If the database representation of the logical types is correct, we never reach this branch."),
                }
            } else {
                match iter {
                    DataValueIteratorT::String(iter) => iter,
                    DataValueIteratorT::U32(iter) => Box::new(iter.map(|v| v.to_string())),
                    DataValueIteratorT::U64(iter) => Box::new(iter.map(|v| v.to_string())),
                    DataValueIteratorT::I64(iter) => Box::new(iter.map(|v| v.to_string())),
                    DataValueIteratorT::Float(iter) => Box::new(iter.map(|v| v.to_string())),
                    DataValueIteratorT::Double(iter) => Box::new(iter.map(|v| v.to_string())),
                }
            }
        });

        struct CombinedIters<'a>(Vec<Box<dyn Iterator<Item = String> + 'a>>);

        impl<'a> Iterator for CombinedIters<'a> {
            type Item = Vec<String>;

            fn next(&mut self) -> Option<Self::Item> {
                let res: Self::Item = self.0.iter_mut().filter_map(|iter| iter.next()).collect();
                (!res.is_empty()).then_some(res)
            }
        }

        let combined_iters = CombinedIters(logically_mapped_iters.collect());

        Ok(Some(combined_iters))
    }

    /// Count the number of derived facts during the computation.
    pub fn count_derived_facts(&self) -> usize {
        let mut result = 0;

        for predicate in &self.analysis.derived_predicates {
            if let Some(count) = self.table_manager.predicate_count_rows(predicate) {
                result += count;
            }
        }

        result
    }
}
