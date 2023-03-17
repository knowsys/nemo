//! Functionality which handles the execution of a program

use std::collections::HashMap;

use crate::{
    error::Error,
    logical::{
        model::{DataSource, Identifier, NumericLiteral, Program, Term},
        program_analysis::analysis::ProgramAnalysis,
        TableManager,
    },
    meta::TimedCode,
    physical::{
        datatypes::DataValueT,
        dictionary::Dictionary,
        management::database::{TableId, TableSource},
        tabular::table_types::trie::Trie,
    },
};

use super::rule_execution::RuleExecution;

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
pub struct ExecutionEngine<Dict: Dictionary> {
    program: Program<Dict>,
    analysis: ProgramAnalysis,

    table_manager: TableManager<Dict>,

    predicate_fragmentation: HashMap<Identifier, usize>,
    predicate_last_union: HashMap<Identifier, usize>,

    rule_infos: Vec<RuleInfo>,
    current_step: usize,
}

impl<Dict: Dictionary> ExecutionEngine<Dict> {
    /// Initialize [`ExecutionEngine`].
    pub fn initialize(mut program: Program<Dict>) -> Self {
        program.normalize();

        let analysis = program.analyze();

        let mut table_manager = TableManager::new(program.get_names().clone());
        Self::register_all_predicates(&mut table_manager, &analysis);
        Self::add_sources(&mut table_manager, &program);

        let mut rule_infos = Vec::<RuleInfo>::new();
        program
            .rules()
            .iter()
            .for_each(|_| rule_infos.push(RuleInfo::new()));

        Self {
            program,
            analysis,
            table_manager,
            predicate_fragmentation: HashMap::new(),
            predicate_last_union: HashMap::new(),
            rule_infos,
            current_step: 1,
        }
    }

    fn register_all_predicates(table_manager: &mut TableManager<Dict>, analysis: &ProgramAnalysis) {
        for (predicate, arity) in &analysis.all_predicates {
            table_manager.register_predicate(*predicate, *arity);
        }
    }

    fn add_sources(table_manager: &mut TableManager<Dict>, program: &Program<Dict>) {
        let mut predicate_to_sources = HashMap::<Identifier, Vec<TableSource>>::new();

        // Add all the data source declarations
        for ((predicate, _), source) in program.sources() {
            let new_source = match source {
                DataSource::CsvFile(file) => TableSource::CSV(*file.clone()),
                DataSource::RdfFile(_) => todo!("RDF data sources are not yet implemented"),
                DataSource::SparqlQuery(_) => {
                    todo!("SPARQL query data sources are not yet implemented")
                }
            };

            predicate_to_sources
                .entry(predicate)
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
                .map(|t| match t {
                    Term::NumericLiteral(nl) => match nl {
                        NumericLiteral::Integer(i) => DataValueT::U64((*i).try_into().unwrap()),
                        _ => unimplemented!(),
                    },
                    Term::Constant(identifier) => DataValueT::U64(identifier.to_constant_u64()),
                    _ => unimplemented!(),
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

        let rule_execution: Vec<RuleExecution<Dict>> = self
            .program
            .rules()
            .iter()
            .zip(self.analysis.rule_analysis.iter())
            .map(|(r, a)| RuleExecution::initialize(r, a))
            .collect();

        let mut without_derivation: usize = 0;
        let mut current_rule_index: usize = 0;

        while without_derivation < self.program.rules().len() {
            let timing_string = format!("Reasoning/Rules/Rule {current_rule_index}");
            TimedCode::instance().sub(&timing_string).start();
            log::info!(
                "<<< {0}: APPLYING RULE {current_rule_index} >>>",
                self.current_step
            );

            let current_info = &mut self.rule_infos[current_rule_index];
            let current_analysis = &self.analysis.rule_analysis[current_rule_index];
            let current_execution = &rule_execution[current_rule_index];

            let updated_predicates = current_execution.execute(
                &self.program,
                &mut self.table_manager,
                current_info,
                self.current_step,
            )?;

            let no_derivation = updated_predicates.is_empty();

            if no_derivation {
                without_derivation += 1;
            } else {
                without_derivation = 0;
            }

            // If we have a derivation and the rule is recursive we want to stay on the same rule
            let update_rule_index = no_derivation || !current_analysis.is_recursive;

            if update_rule_index {
                current_rule_index = (current_rule_index + 1) % self.program.rules().len();
            }

            current_info.step_last_applied = self.current_step;

            let rule_duration = TimedCode::instance().sub(&timing_string).stop();
            log::info!("Rule duration: {} ms", rule_duration.as_millis());

            // We prevent fragmentation by periodically collecting single-step tables into larger ones
            for updated_pred in updated_predicates {
                let counter = self
                    .predicate_fragmentation
                    .entry(updated_pred)
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

                    self.table_manager.combine_tables(updated_pred, range)?;

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

    /// Return the output tries that resulted form the execution.
    pub fn get_results(&mut self) -> Result<Vec<(Identifier, &Trie)>, Error> {
        let mut result_ids = Vec::<(Identifier, TableId)>::new();
        for &predicate in &self.analysis.derived_predicates {
            if let Some(combined_id) = self.table_manager.combine_predicate(predicate)? {
                result_ids.push((predicate, combined_id));
            }
        }

        let result = result_ids
            .into_iter()
            .map(|(p, id)| (p, self.table_manager.table_from_id(id)))
            .collect();

        Ok(result)
    }

    /// Return the dictionary used in the database instance.
    /// TODO: Remove this once proper Dictionary support is implemented on the physical layer.
    pub fn get_dict(&self) -> &Dict {
        self.table_manager.get_dict()
    }

    /// Iterator over all IDB predicates, with Tries if present.
    pub fn idb_predicates(
        &mut self,
    ) -> Result<impl Iterator<Item = (Identifier, Option<&Trie>)>, Error> {
        let idbs = self.program.idb_predicates();
        let tables = self.get_results()?.into_iter().collect::<HashMap<_, _>>();
        let mut result = Vec::new();

        for predicate in idbs {
            let table = tables.get(&predicate).copied();
            result.push((predicate, table));
        }

        Ok(result.into_iter())
    }
}
