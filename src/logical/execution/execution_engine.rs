//! Functionality which handles the execution of a program

use std::collections::{HashMap, HashSet};

use crate::{
    error::Error,
    io::dsv::DSVReader,
    logical::{
        model::{chase_model::ChaseProgram, DataSource, Identifier, Program},
        program_analysis::analysis::ProgramAnalysis,
        types::LogicalTypeEnum,
        TableManager,
    },
    meta::TimedCode,
    physical::{
        datatypes::DataValueT,
        dictionary::value_serializer::TrieSerializer,
        management::database::{TableId, TableSource},
    },
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

    table_manager: TableManager,

    predicate_fragmentation: HashMap<Identifier, usize>,
    predicate_last_union: HashMap<Identifier, usize>,

    rule_infos: Vec<RuleInfo>,
    current_step: usize,
}

/// Refers to a single computed relation.
#[derive(Debug, Clone)]
pub struct IdbPredicate {
    /// The identifier corresponding to the predicate.
    identifier: Identifier,
    /// The table id corresponding to the predicate.
    table_id: Option<TableId>,
}

impl IdbPredicate {
    /// Get the identifier of the predicate
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }
}

impl std::fmt::Display for IdbPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.identifier)
    }
}

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    /// Initialize [`ExecutionEngine`].
    pub fn initialize(program: Program) -> Result<Self, Error> {
        let mut program: ChaseProgram = program.into();

        program.check_for_unsupported_features()?;
        program.normalize();

        let analysis = program.analyze()?;

        let mut table_manager = TableManager::new();
        Self::register_all_predicates(&mut table_manager, &analysis);
        Self::add_sources(&mut table_manager, &program, &analysis);

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
            table_manager,
            predicate_fragmentation: HashMap::new(),
            predicate_last_union: HashMap::new(),
            rule_infos,
            current_step: 1,
        })
    }

    fn register_all_predicates(table_manager: &mut TableManager, analysis: &ProgramAnalysis) {
        for (predicate, arity) in &analysis.all_predicates {
            table_manager.register_predicate(
                predicate.clone(),
                analysis
                    .predicate_types
                    .get(predicate)
                    .cloned()
                    .unwrap_or_else(|| (0..*arity).map(|_| LogicalTypeEnum::Any).collect()),
            );
        }
    }

    fn add_sources(
        table_manager: &mut TableManager,
        program: &ChaseProgram,
        analysis: &ProgramAnalysis,
    ) {
        let mut predicate_to_sources = HashMap::<Identifier, Vec<TableSource>>::new();

        // Add all the data source declarations
        for ((predicate, arity), source) in program.sources() {
            let new_source = match source {
                DataSource::DsvFile { file, delimiter } => {
                    let logical_types = analysis
                        .predicate_types
                        .get(predicate)
                        .cloned()
                        .unwrap_or_else(|| vec![LogicalTypeEnum::Any; arity]);
                    let reader = DSVReader::dsv(*file.clone(), *delimiter, logical_types);

                    TableSource::FileReader(Box::new(reader))
                }
                DataSource::RdfFile(_) => todo!("RDF data sources are not yet implemented"),
                DataSource::SparqlQuery(_) => {
                    todo!("SPARQL query data sources are not yet implemented")
                }
            };

            predicate_to_sources
                .entry(predicate.clone())
                .or_default()
                .push(new_source)
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
                    analysis.predicate_types.get(&fact.0.predicate()).unwrap()[i]
                        .ground_term_to_data_value_t(t.clone()).expect("Trying to convert a ground type into an invalid logical type. Should have been prevented by the type checker.")
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

    /// Combine the output tries that resulted form the execution.
    /// Returns a Vector of [`IdbPredicate`], which can be turned into tables by calling
    /// [table_serializer](ExecutionEngine#method.table_serializer)
    ///
    /// Uses the default [`crate::physical::management::database::ColumnOrder`]
    pub fn combine_results(&mut self) -> Result<Vec<IdbPredicate>, Error> {
        let output_predicates = self.program.output_predicates().collect::<HashSet<_>>();
        let mut result_ids = Vec::new();

        for predicate in &self.analysis.derived_predicates {
            if !output_predicates.contains(predicate) {
                continue;
            }

            let table_id = self.table_manager.combine_predicate(predicate.clone())?;
            result_ids.push((predicate.clone(), table_id));
        }

        Ok(result_ids
            .into_iter()
            .map(|(identifier, table_id)| IdbPredicate {
                identifier,
                table_id,
            })
            .collect())
    }

    /// Get the table for the specified predicate if there is some.
    pub fn table_serializer(&self, predicate: IdbPredicate) -> Option<impl TrieSerializer + '_> {
        let table_id = predicate.table_id?;
        Some(self.table_manager.table_serializer(table_id))
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
