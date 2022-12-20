//! Functionality which handles the execution of a program

use std::collections::HashMap;

use crate::{
    logical::{
        model::{Identifier, NumericLiteral, Program, Term},
        program_analysis::analysis::{ProgramAnalysis, RuleAnalysis},
        table_manager::{ColumnOrder, TableKey},
        TableManager,
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

    rule_infos: Vec<RuleInfo>,
    current_step: usize,
}

impl ExecutionEngine {
    /// Initialize [`ExecutionEngine`].
    pub fn initialize(mut program: Program) -> Self {
        program.normalize();
        let analysis = program.analyze();

        let mut table_manager = TableManager::new();

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
            rule_infos,
            table_manager,
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
            let _current_rule = &self.program.rules()[current_rule_index];

            let current_info = &mut self.rule_infos[current_rule_index];
            let current_analysis = &self.analysis.rule_analysis[current_rule_index];
            let current_execution = &rule_execution[current_rule_index];

            let no_derivation = !current_execution.execute(
                &mut self.table_manager,
                current_info,
                self.current_step,
            );

            if no_derivation {
                without_derivation += 1;
            } else {
                without_derivation = 0;
            }

            // If we have a derivation and the rule is recursive we want to stay on the same rule
            let update_rule_index = if let RuleAnalysis::Normal(analysis) = current_analysis {
                no_derivation || !analysis.is_recursive
            } else {
                true
            };

            if update_rule_index {
                current_rule_index = (current_rule_index + 1) % self.program.rules().len();
            }

            current_info.step_last_applied = self.current_step;
            self.current_step += 1;
        }
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
