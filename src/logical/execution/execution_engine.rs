//! Functionality which handles the execution of a program

use std::collections::HashMap;

use crate::{
    logical::{
        model::{Identifier, NumericLiteral, Program, Term},
        program_analysis::analysis::ProgramAnalysis,
        table_manager::{ColumnOrder, TableKey},
        TableManager,
    },
    meta::{
        logging::{log_apply_rule, log_fragmentation_combine, log_rule_duration},
        TimedCode,
    },
    physical::{
        datatypes::DataValueT,
        dictionary::PrefixedStringDictionary,
        tabular::{
            table_types::trie::Trie,
            traits::{table::Table, table_schema::TableSchema},
        },
    },
};

use super::rule_execution::RuleExecution;

// Number of tables that are periodically combined into one.
const MAX_FRAGMENTATION: usize = 8;

/// Stores useful information about a rule.
#[derive(Debug, Copy, Clone)]
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
pub struct ExecutionEngine {
    program: Program,
    analysis: ProgramAnalysis,

    table_manager: TableManager,

    predicate_fragmentation: HashMap<Identifier, usize>,
    predicate_last_union: HashMap<Identifier, usize>,

    rule_infos: Vec<RuleInfo>,
    current_step: usize,
}

impl ExecutionEngine {
    /// Initialize [`ExecutionEngine`].
    pub fn initialize(mut program: Program) -> Self {
        program.normalize();
        let analysis = program.analyze();

        let mut table_manager = TableManager::new(program.get_dict_constants().clone());

        Self::add_input_sources(&mut table_manager, &program);
        Self::add_input_facts(&mut table_manager, &program);

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

    /// Add all the data source decliarations from the program into the table manager.
    fn add_input_sources(table_manager: &mut TableManager, program: &Program) {
        for ((predicate, arity), source) in program.sources() {
            table_manager.add_source(predicate, arity, source.clone());
        }
    }

    /// Add all input facts as tables into the table manager.
    /// TODO: This function has to be revised when the new type system for the logicla layer is introduced.
    fn add_input_facts(table_manager: &mut TableManager, program: &Program) {
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
            let trie = Trie::from_rows(rows);
            let arity = trie.get_types().len();

            let mut schema = TableSchema::new();
            for &type_name in trie.get_types() {
                schema.add_entry(type_name, false, false);
            }

            table_manager.add_table(predicate, 0..1, ColumnOrder::default(arity), schema, trie);
        }
    }

    /// Executes the program.
    pub fn execute(&mut self) {
        TimedCode::instance().sub("Reasoning/Rules").start();
        TimedCode::instance().sub("Reasoning/Execution").start();

        let rule_execution: Vec<RuleExecution> = self
            .program
            .rules()
            .iter()
            .enumerate()
            .map(|(i, r)| RuleExecution::initialize(r, &self.analysis.rule_analysis[i]))
            .collect();

        let mut without_derivation: usize = 0;
        let mut current_rule_index: usize = 0;

        while without_derivation < self.program.rules().len() {
            let timing_string = format!("Reasoning/Rules/Rule {current_rule_index}");
            TimedCode::instance().sub(&timing_string).start();
            log_apply_rule(&self.program, self.current_step, current_rule_index);

            let current_info = &mut self.rule_infos[current_rule_index];
            let current_analysis = &self.analysis.rule_analysis[current_rule_index];
            let current_execution = &rule_execution[current_rule_index];

            let updated_predicates = current_execution.execute(
                &self.program,
                &mut self.table_manager,
                current_info,
                self.current_step,
            );

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
            log_rule_duration(rule_duration);

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

                    let new_key = self.table_manager.add_union_table(
                        updated_pred,
                        range.clone(),
                        updated_pred,
                        range.clone(),
                        None,
                    );

                    let new_table_opt = new_key.map(|key| self.table_manager.get_trie(&key));
                    log_fragmentation_combine(updated_pred, new_table_opt);

                    self.predicate_last_union
                        .insert(updated_pred, self.current_step);
                    *counter = 0;
                }
            }

            self.current_step += 1;
        }

        TimedCode::instance().sub("Reasoning/Rules").stop();
        TimedCode::instance().sub("Reasoning/Execution").stop();
    }

    /// Return the output tries that resulted form the exection.
    pub fn get_results(&mut self) -> Vec<(Identifier, &Trie)> {
        let mut result = Vec::<(Identifier, &Trie)>::new();
        let result_keys: Vec<Option<TableKey>> = self
            .analysis
            .derived_predicates
            .iter()
            .map(|&p| self.table_manager.combine_predicate(p, self.current_step))
            .collect();

        for key_opt in result_keys {
            if let Some(key) = &key_opt {
                result.push((key.name.predicate, self.table_manager.get_trie(key)));
            }
        }

        result
    }

    /// Return the dictionary used in the database instance.
    /// TODO: Remove this once proper Dictionary support is implemented on the physical layer.
    pub fn get_dict(&self) -> &PrefixedStringDictionary {
        self.table_manager.get_dict()
    }
}
