//! Functionality which handles the execution of a program

use std::collections::{HashMap, HashSet};

use nemo_physical::{
    datavalues::AnyDataValue,
    dictionary::DvDict,
    management::database::sources::{SimpleTable, TableSource},
    meta::timing::TimedCode,
};

use crate::{
    error::{Error, report::ProgramReport, warned::Warned},
    execution::planning::{
        normalization::program::NormalizedProgram, strategy::forward::StrategyForward,
    },
    io::{formats::Export, import_manager::ImportManager},
    rule_file::RuleFile,
    rule_model::{
        components::tag::Tag,
        pipeline::transformations::default::TransformationDefault,
        programs::{handle::ProgramHandle, program::Program},
    },
    table_manager::{MemoryUsage, TableManager},
};

use super::{
    execution_parameters::ExecutionParameters, selection_strategy::strategy::RuleSelectionStrategy,
};

pub mod tracing;

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
    program: NormalizedProgram,

    /// The picked selection strategy for rules
    selection_strategy: RuleSelectionStrategy,

    /// Management of tables that represent predicates
    table_manager: TableManager,
    /// Managermet of imports
    import_manager: ImportManager,

    /// Stores for each predicate the number of subtables
    predicate_fragmentation: HashMap<Tag, usize>,
    /// Stores for each predicate the step up until subtables have been combined
    predicate_last_union: HashMap<Tag, usize>,

    /// For each rule in `program` additional information
    rule_infos: Vec<RuleInfo>,
    /// For each step the rule index of the applied rule
    rule_history: Vec<usize>,
    /// For each step, the execution time in milliseconds
    step_times_ms: Vec<u128>,
    /// Current step
    current_step: usize,
}

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    /// Initialize a [ExecutionEngine] by parsing and translating
    /// the contents of the given file.
    pub async fn from_file(
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

        let engine = Self::initialize(program.materialize(), parameters.import_manager).await?;

        report.warned(engine)
    }

    /// Initialize [ExecutionEngine].
    pub async fn initialize(
        program: Program,
        import_manager: ImportManager,
    ) -> Result<Self, Error> {
        let normalized_program = NormalizedProgram::normalize_program(&program);

        let mut table_manager = TableManager::new();
        Self::register_all_predicates(&mut table_manager, &normalized_program);
        Self::add_all_constants(&mut table_manager, &normalized_program);
        Self::add_imports(&mut table_manager, &import_manager, &normalized_program).await?;

        let mut rule_infos = Vec::<RuleInfo>::new();
        normalized_program
            .rules()
            .iter()
            .for_each(|_| rule_infos.push(RuleInfo::new()));

        let selection_strategy = Strategy::new(normalized_program.rules().iter().collect())?;

        Ok(Self {
            nemo_program: program,
            program: normalized_program,
            selection_strategy,
            table_manager,
            import_manager,
            predicate_fragmentation: HashMap::new(),
            predicate_last_union: HashMap::new(),
            rule_infos,
            rule_history: vec![usize::MAX], // Placeholder, Step counting starts at 1
            step_times_ms: vec![0],         // Placeholder, Step counting starts at 1
            current_step: 1,
        })
    }

    /// Register all predicates found in a rule program to the [TableManager].
    fn register_all_predicates(table_manager: &mut TableManager, program: &NormalizedProgram) {
        for (predicate, arity) in program.predicates() {
            table_manager.register_predicate(predicate, arity);
        }
    }

    /// Add all constants appearing in the rules of the program to the dictionary.
    fn add_all_constants(table_manager: &mut TableManager, program: &NormalizedProgram) {
        for value in program.datavalues() {
            table_manager.dictionary_mut().add_datavalue(value.clone());
        }
    }

    /// Add edb tables to the [TableManager]
    /// based on the import declaration of the given progam.
    async fn add_imports(
        table_manager: &mut TableManager,
        import_manager: &ImportManager,
        program: &NormalizedProgram,
    ) -> Result<(), Error> {
        let mut predicate_to_sources = HashMap::<Tag, Vec<TableSource>>::new();

        // Add all the import specifications
        for import in program.imports() {
            let table_source = import_manager
                .table_provider_from_handler(&import.handler())
                .await?;

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
            table_manager.add_edb(predicate.clone(), sources);
        }

        Ok(())
    }

    async fn step(
        &mut self,
        rule_index: usize,
        execution: &StrategyForward,
    ) -> Result<Vec<Tag>, Error> {
        let timing_string = format!("Reasoning/Rules/Rule {rule_index}");

        TimedCode::instance().sub(&timing_string).start();
        log::info!(
            "<<< STEP {}: APPLYING RULE {} {} >>>",
            self.current_step,
            rule_index,
            self.program.rules()[rule_index]
        );

        self.rule_history.push(rule_index);

        let current_info = &mut self.rule_infos[rule_index];

        let updated_predicates = execution
            .execute(
                &mut self.table_manager,
                &self.import_manager,
                self.current_step,
                current_info.step_last_applied,
            )
            .await?;

        current_info.step_last_applied = self.current_step;

        let rule_duration = TimedCode::instance().sub(&timing_string).stop();

        self.step_times_ms.push(rule_duration.as_millis());

        log::info!("Rule duration: {} ms", rule_duration.as_millis());

        self.current_step += 1;

        Ok(updated_predicates)
    }

    async fn defrag(&mut self, updated_predicates: Vec<Tag>) -> Result<(), Error> {
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

                self.table_manager
                    .combine_tables(&updated_pred, range)
                    .await?;

                self.predicate_last_union
                    .insert(updated_pred, self.current_step);

                *counter = 0;
            }
        }

        Ok(())
    }

    /// Executes the program.
    pub async fn execute(&mut self) -> Result<(), Error> {
        TimedCode::instance().sub("Reasoning/Rules").start();
        TimedCode::instance().sub("Reasoning/Execution").start();

        let execution_strategy = self
            .program
            .rules()
            .iter()
            .map(StrategyForward::new)
            .collect::<Vec<_>>();

        for (predicate, arity) in execution_strategy
            .iter()
            .flat_map(|strategy| strategy.special_predicates())
        {
            self.table_manager.register_predicate(predicate, arity);
        }

        for predicate in self.program.output_predicates().iter().cloned().chain(
            self.program
                .exports()
                .iter()
                .map(|export| export.predicate()),
        ) {
            self.table_manager.combine_predicate(&predicate).await?;
        }

        let mut new_derivations: Option<bool> = None;

        while let Some(index) = self.selection_strategy.next_rule(new_derivations) {
            let updated_predicates = self.step(index, &execution_strategy[index]).await?;
            new_derivations = Some(!updated_predicates.is_empty());

            self.defrag(updated_predicates).await?;
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
    pub(crate) fn chase_program(&self) -> &NormalizedProgram {
        &self.program
    }

    /// Creates an [Iterator] over all facts of a predicate.
    pub async fn predicate_rows(
        &mut self,
        predicate: &Tag,
    ) -> Result<Option<impl Iterator<Item = Vec<AnyDataValue>> + '_>, Error> {
        let Some(table_id) = self.table_manager.combine_predicate(predicate).await? else {
            return Ok(None);
        };

        Ok(Some(self.table_manager.table_row_iterator(table_id).await?))
    }

    /// Creates an [Iterator] over all facts of a predicate with their corresponding id.
    pub async fn predicate_rows_ids(
        &mut self,
        predicate: &Tag,
    ) -> Result<Option<impl Iterator<Item = (Vec<AnyDataValue>, usize)>>, Error> {
        let Some(table_id) = self.table_manager.combine_predicate(predicate).await? else {
            return Ok(None);
        };

        // Once `combine_predicate` is called all subtables should be inmemory

        let iterator = self
            .table_manager
            .table_row_iterator_inmemory(table_id)?
            .map(|row| {
                let (id, _step) = self
                    .table_manager
                    .table_row_id_inmemory(predicate, &row)
                    .expect("row must exist since it comes from calling `combine_predicate`");
                (row, id)
            });

        Ok(Some(iterator))
    }

    /// Returns the arity of the predicate if the predicate is known to the engine,
    /// and `None` otherwise.
    pub fn predicate_arity(&self, predicate: &Tag) -> Option<usize> {
        self.program.predicate_arity(predicate)
    }

    /// Return a list of all all export predicates and their respective [`Export`]s,
    /// which can be used for exporting into files.
    pub fn exports(&self) -> Vec<(Tag, Export)> {
        self.program
            .exports()
            .iter()
            .map(|export| (export.predicate(), export.handler()))
            .collect::<Vec<_>>()
    }

    /// Counts the facts of a single predicate that are currently in memory.
    pub fn count_facts_in_memory_for_predicate(&self, predicate: &Tag) -> Option<usize> {
        self.table_manager
            .count_rows_in_memory_for_predicate(predicate)
    }

    /// Count the number of facts of derived predicates that are currently in memory.
    pub fn count_facts_in_memory_for_derived_predicates(&self) -> usize {
        let output_predicates = self.program.output_predicates().iter().cloned();
        let export_predicates = self
            .program
            .exports()
            .iter()
            .map(|export| export.predicate());
        let derived_predicates = self.program.derived_predicates().iter().cloned();

        let predicates = output_predicates
            .chain(export_predicates)
            .chain(derived_predicates)
            .collect::<HashSet<_>>();

        predicates
            .iter()
            .map(|predicate| {
                self.count_facts_in_memory_for_predicate(predicate)
                    .unwrap_or(0)
            })
            .sum()
    }

    /// Return the amount of consumed memory for the tables used by the chase.
    pub fn memory_usage(&self) -> MemoryUsage {
        self.table_manager.memory_usage()
    }

    /// For a given iterator over rule execution steps
    /// returns the sum of execution time for those steps.
    pub fn steps_time_ms<Iter: Iterator<Item = usize>>(&self, steps: Iter) -> u128 {
        steps
            .map(|step| self.step_times_ms.get(step).cloned().unwrap_or_default())
            .sum()
    }
}
