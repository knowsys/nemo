//! Functionality which handles the execution of a program

use std::collections::HashMap;

use nemo_physical::{
    datavalues::AnyDataValue,
    dictionary::DvDict,
    management::database::sources::{SimpleTable, TableSource},
    meta::timing::TimedCode,
};

use crate::{
    chase_model::{
        analysis::program_analysis::ProgramAnalysis,
        components::{atom::ChaseAtom, export::ChaseExport, program::ChaseProgram},
        translation::ProgramChaseTranslation,
    },
    error::{Error, report::ProgramReport, warned::Warned},
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
    execution_parameters::ExecutionParameters, rule_execution::RuleExecution,
    selection_strategy::strategy::RuleSelectionStrategy,
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
    program: ChaseProgram,
    /// Auxillary information for `program`
    analysis: ProgramAnalysis,

    /// The picked selection strategy for rules
    rule_strategy: RuleSelectionStrategy,

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
            import_manager,
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
            table_manager.add_edb(predicate.clone(), sources);
        }

        Ok(())
    }

    fn step(&mut self, rule_index: usize, execution: &RuleExecution) -> Result<Vec<Tag>, Error> {
        let timing_string = format!("Reasoning/Rules/Rule {rule_index}");

        TimedCode::instance().sub(&timing_string).start();
        log::info!("<<< {0}: APPLYING RULE {rule_index} >>>", self.current_step);

        self.rule_history.push(rule_index);

        let current_info = &mut self.rule_infos[rule_index];

        let updated_predicates = execution.execute(
            &mut self.table_manager,
            &self.import_manager,
            current_info,
            self.current_step,
        )?;

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
            .enumerate()
            .zip(self.analysis.rule_analysis.iter())
            .map(|((index, rule), analysis)| RuleExecution::initialize(rule, index, analysis))
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
}
