//! Translation: model programs into compiled, normalized rules.
//!
//! The [Translator] resolves names, registers tables, loads facts, and compiles rules.
//! Its concerns are split by statement kind into the submodules,
//! each an `impl` block over the same translator.

mod compiler;
mod declarations;
pub(crate) mod exports;
mod facts;
pub(crate) mod imports;
mod options;
mod program_builder;
mod rules;
mod state;
mod terms;

use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use nemo_io::file_format::FileFormat;
use nemo_physical::{
    diagnostics::{Diagnostic, Severity},
    dictionary::Dictionary,
    function::StackProgram,
    management::{TableId, TableManager},
    trie::RowBuffer,
    value::{DataValue, ErrorCounts, LabelTypes, Type},
};

pub(crate) use state::TranslationState;

use crate::{
    diagnostics::{EngineError, TraceError, TranslationError},
    engine::{
        Engine, ExportSpec,
        compile::compiled::{CompiledProgram, CompiledRule, VariableId},
        compile::import_plan::ImportPlan,
        compile::optimize::{ImportSurvey, OptimizationPasses},
        compile::strata,
        compile::translation::{
            imports::PendingImport,
            program_builder::{StackProgramBuilder, VariableScope},
        },
        compile::types,
        registered::Registered,
        remote::{RemoteResponse, ResourceRequest},
        written::WrittenProgram,
    },
    model::{Directive, Fact, Identifier, Operation, Span, Statement, TypeExpression, Walk},
    pipeline::{NameScope, Pipeline, StatementId},
    telemetry::SinkSlot,
};

/// The parts of the engine one translation writes into.
pub(crate) struct TranslationStores<'a> {
    /// The table store facts load into.
    pub(crate) manager: &'a mut TableManager,
    /// The dictionary fact values intern into.
    pub(crate) dictionary: &'a mut Dictionary,
    /// The registered predicates by resolved name, each with its table
    /// and its number of columns.
    pub(crate) predicates: &'a mut HashMap<String, Registered>,
    /// The pipeline the program's reports go to, with the files its statements come from.
    pub(crate) pipeline: &'a mut Pipeline,
    /// Evaluation errors of pushed-down import conditions, counted per kind.
    ///
    /// A row whose condition fails to evaluate is dropped.
    pub(crate) errors: &'a mut ErrorCounts,
}

/// The settings one translation runs with.
pub(crate) struct TranslationSetup<'a> {
    /// The directory relative import resources resolve against, if the engine sets one.
    pub(crate) import_directory: Option<&'a Path>,
    /// The in-memory resources registered on the engine.
    ///
    /// A resource name found here reads its bytes instead of the filesystem.
    pub(crate) resources: &'a HashMap<String, Arc<[u8]>>,
    /// The responses passed to `respond` for the remote requests of an earlier,
    /// suspended translation, by resource name, or `None` if there are none.
    pub(crate) responses: Option<&'a HashMap<String, RemoteResponse>>,
    /// The slot of the installed telemetry sink, to which imports report their loads.
    pub(crate) telemetry: &'a SinkSlot,
    /// The optimization passes.
    ///
    /// The import passes run where data is read.
    pub(crate) passes: &'a OptimizationPasses,
    /// What the translation is for.
    pub(crate) purpose: Purpose<'a>,
    /// The written program of the statements translated,
    /// whose derived predicates the import passes keep whole,
    /// or `None` if no import pass runs.
    pub(crate) written: Option<&'a WrittenProgram>,
    /// The column types the loaded program was analyzed with,
    /// if a program is loaded, so that an error of a change can say what it widened.
    pub(crate) previous_types: Option<&'a HashMap<TableId, Vec<Type>>>,
}

/// What a translation is for:
/// whether it reads data, and what becomes of the problems it finds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Purpose<'a> {
    /// A load of a program the pipeline stages left as written:
    /// data is read, and the first error ends the translation.
    Load,
    /// A load of the program the pipeline stages made of a written program
    /// that passed its [validation](Self::Validate): data is read.
    ///
    /// A problem reading finds ends the translation.
    /// Any other error is a bug of the stages, and panics;
    /// any other warning was reported for the written program already, and is dropped.
    LoadRewritten {
        /// The stages that made the program.
        stages: &'a [&'static str],
    },
    /// The translation of a written program before the program its stages made loads:
    /// no data is read, and the first error ends the translation.
    Validate,
    /// A check: no data is read, and every problem is reported.
    Check,
}

impl<'a> Purpose<'a> {
    /// Returns the purpose of a load of the program the given pipeline stages made,
    /// a plain load if no stage changed it.
    pub(crate) fn of_load(stages: &'a [&'static str]) -> Self {
        if stages.is_empty() {
            Purpose::Load
        } else {
            Purpose::LoadRewritten { stages }
        }
    }

    /// Returns whether the translation reads data.
    fn reads(self) -> bool {
        matches!(self, Purpose::Load | Purpose::LoadRewritten { .. })
    }
}

/// The pseudo-column an aggregate result reads as while a head expression compiles.
///
/// It is remapped into the aggregation's output space.
const AGGREGATE_RESULT: usize = usize::MAX;

/// Returns a diagnostic for an unsupported feature, labeled if a span is given.
fn unsupported(what: &str, span: Option<Span>) -> Diagnostic {
    TranslationError::Unsupported {
        what: what.to_owned(),
    }
    .at(span)
}

/// Resolves an identifier against a base IRI and declared prefixes.
///
/// Returns `None` if the prefix is undeclared or the identifier names a module.
fn resolve_identifier(
    base_iri: Option<&str>,
    prefixes: &HashMap<String, String>,
    identifier: &Identifier,
) -> Option<String> {
    match identifier {
        Identifier::Bare(name) => Some(match base_iri {
            Some(base) => format!("{base}{name}"),
            None => name.clone(),
        }),
        Identifier::Iri(iri) => Some(iri.clone()),
        Identifier::Prefixed { prefix, name } => {
            prefixes.get(prefix).map(|iri| format!("{iri}{name}"))
        }
        Identifier::Qualified { .. } => None,
    }
}

/// Collects the column pseudo-variables of a condition as variable ids equal to their positions.
///
/// The import passes name them `column N` for delivered column `N`.
fn collect_columns(operation: &Operation, names: &mut HashMap<String, VariableId>) {
    for variable in operation.variables() {
        if let Some(position) = variable
            .name
            .strip_prefix("column ")
            .and_then(|position| position.parse().ok())
        {
            names.insert(variable.name.clone(), position);
        }
    }
}

/// The rules of one translation as they compile,
/// with what reporting and the analysis need of each, aligned by index:
/// its rendered text, its statement, its variables' source names, and its span.
#[derive(Default)]
struct CompiledRules {
    /// The rules, in statement order.
    rules: Vec<CompiledRule>,
    /// The rules' rendered texts.
    texts: Vec<String>,
    /// The rules' originating statements.
    statements: Vec<StatementId>,
    /// Per rule, the source name of each variable by variable id.
    names: Vec<Vec<Option<String>>>,
    /// The rules' source positions.
    spans: Vec<Option<Span>>,
}

impl CompiledRules {
    /// Adds a rule.
    fn push(
        &mut self,
        rule: CompiledRule,
        text: String,
        statement: StatementId,
        names: Vec<Option<String>>,
        span: Option<Span>,
    ) {
        self.rules.push(rule);
        self.texts.push(text);
        self.statements.push(statement);
        self.names.push(names);
        self.spans.push(span);
    }

    /// Returns what the type analysis needs of each rule.
    fn source_info(&self) -> Vec<types::RuleSourceInfo<'_>> {
        self.names
            .iter()
            .zip(&self.spans)
            .map(|(names, &span)| types::RuleSourceInfo { span, names })
            .collect()
    }
}

/// The translator: resolves names, registers tables, loads facts, and
/// compiles rules.
pub(crate) struct Translator<'a> {
    /// The table store facts load into.
    pub(crate) manager: &'a mut TableManager,
    /// The dictionary fact values intern into.
    pub(crate) dictionary: &'a mut Dictionary,
    /// The registered predicates by resolved name, each with its table
    /// and its number of columns.
    pub(crate) predicates: &'a mut HashMap<String, Registered>,
    /// The pipeline the program's reports go to, with the files its statements come from.
    pub(crate) pipeline: &'a mut Pipeline,
    /// Evaluation errors of pushed-down import conditions, counted per kind.
    ///
    /// A row whose condition fails to evaluate is dropped.
    pub(crate) errors: &'a mut ErrorCounts,
    /// The directory relative import resources resolve against, if the engine sets one.
    import_directory: Option<&'a Path>,
    /// The in-memory resources registered on the engine.
    ///
    /// A resource name found here reads its bytes instead of the filesystem.
    resources: &'a HashMap<String, Arc<[u8]>>,
    /// The declared base IRI bare names resolve against.
    base_iri: Option<String>,
    /// The declared prefixes.
    prefixes: HashMap<String, String>,
    /// The resolved global parameters.
    globals: HashMap<String, DataValue>,
    /// The fact rows collected per table, dictionary-encoded.
    facts: HashMap<TableId, RowBuffer>,
    /// The import directives, executed after every arity is known.
    imports: Vec<PendingImport>,
    /// The export directives, resolved.
    exports: Vec<ExportSpec>,
    /// The export directives' source positions, aligned with [exports](Self::exports),
    /// for the checks that need every predicate's arity
    /// and so run after all statements are translated.
    export_spans: Vec<Option<Span>>,
    /// The declared column types, by resolved predicate name.
    declarations: HashMap<String, types::PredicateDeclaration>,
    /// The `@type` definitions, by resolved label.
    type_definitions: HashMap<String, (TypeExpression, Option<Span>)>,
    /// The types the `@type` definitions declare, by resolved label,
    /// known once every definition is recorded.
    label_types: LabelTypes,
    /// The column type seeds per table, from the facts and import formats.
    seeds: HashMap<TableId, Vec<Type>>,
    /// The import-fed predicates whose columns are not statically known, and the declared
    /// inputs.
    ///
    /// The analysis takes their columns as anything their declarations allow.
    open: HashSet<String>,
    /// The `@input` predicates, resolved.
    ///
    /// Their rows come from a using program or from
    /// [import_rows](crate::engine::Engine::import_rows),
    /// so they cannot be the target of an `@import`.
    inputs: HashSet<String>,
    /// What the loaders of import-fed predicates leave out (see [ImportPlan]).
    pub(super) plan: ImportPlan,
    /// The program's blank nodes: per label, the null it stands for,
    /// created before any term is evaluated and kept across translations.
    blanks: HashMap<String, u64>,
    /// Per predicate of an import with a `where` clause, the scratch table of its fetches.
    scratch: HashMap<String, TableId>,
    /// The fetch rules of imports with a `where` clause, compiled when the
    /// imports execute, each with the rule index reserved for it and its
    /// variables' names.
    fetch_rules: Vec<(usize, CompiledRule, Vec<Option<String>>)>,
    /// The slot of the installed telemetry sink, to which imports report their loads.
    telemetry: &'a SinkSlot,
    /// The responses passed to `respond`, by resource name.
    responses: Option<&'a HashMap<String, RemoteResponse>>,
    /// The optimization passes.
    passes: &'a OptimizationPasses,
    /// What the translation is for.
    purpose: Purpose<'a>,
    /// The written program of the statements translated.
    pub(super) written: Option<&'a WrittenProgram>,
    /// The column types the loaded program was analyzed with, if one is loaded.
    previous_types: Option<&'a HashMap<TableId, Vec<Type>>>,
    /// The remote requests found without a response while the imports ran,
    /// which suspend the translation.
    requests: Vec<ResourceRequest>,
    /// Per statement, whether its data is already in the tables, or empty for a first load.
    ///
    /// A preloaded fact contributes no rows and a preloaded import reads nothing,
    /// while everything they declare still compiles.
    preloaded: Vec<bool>,
}

impl<'a> Translator<'a> {
    /// Creates a translator over the engine's stores, run with the given setup.
    ///
    /// The translation state carries what earlier translations established
    /// (empty on a first load),
    /// and `preloaded` marks the statements whose data is already stored.
    pub(crate) fn new(
        stores: TranslationStores<'a>,
        setup: TranslationSetup<'a>,
        state: TranslationState,
        preloaded: Vec<bool>,
    ) -> Self {
        Self {
            manager: stores.manager,
            dictionary: stores.dictionary,
            predicates: stores.predicates,
            pipeline: stores.pipeline,
            errors: stores.errors,
            import_directory: setup.import_directory,
            resources: setup.resources,
            base_iri: state.base_iri,
            prefixes: state.prefixes,
            globals: state.globals,
            facts: HashMap::new(),
            imports: Vec::new(),
            exports: Vec::new(),
            export_spans: Vec::new(),
            declarations: state.declarations,
            type_definitions: state.type_definitions,
            label_types: state.label_types,
            seeds: state.seeds,
            open: state.open,
            inputs: state.inputs,
            plan: state.plan,
            blanks: state.blanks,
            scratch: state.scratch,
            fetch_rules: Vec::new(),
            telemetry: setup.telemetry,
            responses: setup.responses,
            passes: setup.passes,
            purpose: setup.purpose,
            written: setup.written,
            previous_types: setup.previous_types,
            requests: Vec::new(),
            preloaded,
        }
    }

    /// Returns the remote requests the translation suspended for,
    /// empty if it did not suspend.
    pub(crate) fn take_requests(&mut self) -> Vec<ResourceRequest> {
        std::mem::take(&mut self.requests)
    }

    /// Returns whether a statement's data is already stored.
    fn is_preloaded(&self, index: usize) -> bool {
        self.preloaded.get(index).copied().unwrap_or(false)
    }

    /// Returns whether an import validates its resolved options without reading data:
    /// in a translation that reads no data, or for a directive whose data is already stored.
    fn options_only(&self, import: &PendingImport) -> bool {
        !self.purpose.reads() || import.preloaded
    }

    /// Handles a problem with the program's statements according to the purpose:
    /// a check reports it and continues, and every other translation returns it as the error.
    ///
    /// # Panics
    ///
    /// Panics in the translation of a program the pipeline stages made,
    /// whose written program passed: the stages wrote what translation refuses.
    fn recover(&mut self, problem: Diagnostic) -> Result<(), Diagnostic> {
        match self.purpose {
            Purpose::Check => {
                self.pipeline.report(problem);
                Ok(())
            }
            Purpose::Load | Purpose::Validate => Err(problem),
            Purpose::LoadRewritten { stages } => {
                let names: Vec<String> = stages.iter().map(|stage| format!("`{stage}`")).collect();
                panic!(
                    "the pipeline stages {} wrote a program that translation refuses, \
                     though its written program passed: {problem}",
                    names.join(", ")
                )
            }
        }
    }

    /// Reports a warning about the program's statements,
    /// except in the translation of a program the pipeline stages made,
    /// whose written program was warned about already.
    fn warn(&mut self, warning: Diagnostic) {
        if !matches!(self.purpose, Purpose::LoadRewritten { .. }) {
            self.pipeline.report(warning);
        }
    }

    /// Translates the statements of a program: facts become rows (to be
    /// loaded by the caller), rules compile, directives configure.
    pub(crate) fn program(
        &mut self,
        statements: &[Statement],
        ids: &[StatementId],
    ) -> Result<CompiledProgram, Diagnostic> {
        assert_eq!(
            statements.len(),
            ids.len(),
            "statements and their pipeline ids must align"
        );
        // Blank nodes get their nulls first, so that every occurrence of
        // a label maps to the same null.
        self.create_blank_nodes(statements);
        self.namespace(statements)?;
        self.declaration_directives(statements)?;
        self.plan_imports(statements)?;
        let compiled = self.translate_statements(statements, ids)?;
        // Imports run last, when every predicate's arity is known.
        self.execute_imports()?;
        // So do the export checks that need an arity.
        self.validate_exports()?;
        self.finish_program(compiled)
    }

    /// Reads the prefixes, the base, the parameters, and the `@type` definitions
    /// before anything is resolved, typed, or evaluated.
    ///
    /// A name may thus be used before its declaration, whatever the statement order,
    /// and `@type` definitions may refer to each other in any order.
    fn namespace(&mut self, statements: &[Statement]) -> Result<(), Diagnostic> {
        if let Err(problem) = NameScope::check_base(statements) {
            self.recover(problem)?;
        }
        let mut parameters = Vec::new();
        for statement in statements {
            match statement {
                Statement::Directive(Directive::Prefix(prefix)) => {
                    self.prefixes
                        .insert(prefix.prefix.clone(), prefix.iri.clone());
                }
                Statement::Directive(Directive::Base(base)) => {
                    self.base_iri.get_or_insert_with(|| base.iri.clone());
                }
                Statement::Directive(Directive::Parameter(parameter)) => {
                    parameters.push(parameter);
                }
                _ => {}
            }
        }
        // `@type` definitions come before anything typed or evaluated,
        // since parameters, declarations, and facts are all checked
        // against them.
        for statement in statements {
            if let Statement::Directive(Directive::Type(directive)) = statement
                && let Err(problem) = self.type_directive(directive)
            {
                self.recover(problem)?;
            }
        }
        // A second loop translates every definition once, so a bad field
        // is reported at its own directive however the definitions are
        // ordered.
        for statement in statements {
            if let Statement::Directive(Directive::Type(directive)) = statement {
                match self.definition_type(&directive.definition, directive.span) {
                    Ok(declared) => {
                        if let Some(label) = self.defined_label(directive) {
                            self.label_types.declare(&label, declared);
                        }
                    }
                    Err(problem) => self.recover(problem)?,
                }
            }
        }
        if let Err(problem) = self.resolve_parameters(parameters) {
            self.recover(problem)?;
        }
        Ok(())
    }

    /// Reads the column declarations and the inputs before the statements are translated.
    ///
    /// A declaration arriving after data (through `extend`) is checked against what the tables
    /// already hold, since the facts it would have rejected are already stored.
    fn declaration_directives(&mut self, statements: &[Statement]) -> Result<(), Diagnostic> {
        let mut fresh = Vec::new();
        for (index, statement) in statements.iter().enumerate() {
            let preloaded = self.is_preloaded(index);
            let outcome = match statement {
                Statement::Directive(Directive::Declare(declare)) => {
                    self.declare_directive(declare).map(|name| {
                        if !preloaded {
                            fresh.push((name, declare.span));
                        }
                    })
                }
                Statement::Directive(Directive::Input(input)) => {
                    self.input_directive(input, preloaded).map(|name| {
                        if let Some(name) = name
                            && !preloaded
                        {
                            fresh.push((name, input.span));
                        }
                    })
                }
                _ => Ok(()),
            };
            if let Err(problem) = outcome {
                self.recover(problem)?;
            }
        }
        for (name, span) in fresh {
            if let Err(problem) = self.check_declaration_against_store(&name, span) {
                self.recover(problem)?;
            }
        }
        Ok(())
    }

    /// Runs the import passes, which decide what loaders leave out.
    ///
    /// The plan determines the stored tables, so a later translation,
    /// after `extend` or `reload`, must not expect what an earlier plan
    /// left out. A translation that reads no import plans nothing,
    /// and keeps the plan of what is stored.
    fn plan_imports(&mut self, statements: &[Statement]) -> Result<(), Diagnostic> {
        if !self.purpose.reads() {
            return Ok(());
        }
        let stored = std::mem::take(&mut self.plan);
        let survey = ImportSurvey::new(self, statements)?;
        self.plan = self.passes.plan_imports(&survey, self)?;
        self.plan.check_stability(stored, self.predicates.keys())
    }

    /// Translates the statements one by one, returning the compiled rules:
    /// facts become rows, rules compile, directives configure.
    fn translate_statements(
        &mut self,
        statements: &[Statement],
        ids: &[StatementId],
    ) -> Result<CompiledRules, Diagnostic> {
        let mut compiled = CompiledRules::default();
        for (index, statement) in statements.iter().enumerate() {
            let already = self.pipeline.reports().len();
            let outcome = match statement {
                // A preloaded fact's rows and seeds are already stored.
                Statement::Fact(_) if self.is_preloaded(index) => Ok(()),
                Statement::Fact(fact) => self.fact(fact),
                Statement::Rule(rule) => self.rule(rule).map(|(rule_compiled, names)| {
                    compiled.push(
                        rule_compiled,
                        rule.to_string(),
                        ids[index],
                        names,
                        rule.span,
                    );
                }),
                Statement::Directive(directive) => {
                    // An import with a `where` clause is a rule of the
                    // program (its fetch rule, compiled when the imports
                    // execute), and keeps its position among the rules, so
                    // rule indices follow statement order.
                    #[cfg(feature = "sparql")]
                    if let Directive::Import(import) = directive
                        && !import.bindings.is_empty()
                    {
                        compiled.push(
                            CompiledRule::inert(),
                            import.to_string(),
                            ids[index],
                            Vec::new(),
                            import.span,
                        );
                    }
                    self.directive(directive, self.is_preloaded(index), compiled.rules.len())
                }
            };
            // What the statement's translation reported is about it: in a
            // module instance, about that instance.
            self.pipeline
                .label_instances_in_reports(already, ids[index]);
            if let Err(problem) = outcome {
                let problem = self.pipeline.label_instances(problem, ids[index]);
                self.recover(problem)?;
            }
        }
        Ok(compiled)
    }

    /// Completes the compiled program after the imports ran.
    ///
    /// This chooses the join orders, runs the program passes and the type analysis,
    /// puts the fetch rules at their reserved indices, and computes the strata.
    fn finish_program(
        &mut self,
        mut compiled: CompiledRules,
    ) -> Result<CompiledProgram, Diagnostic> {
        super::order::choose(&mut compiled.rules);
        for report in self.passes.rewrite(&mut compiled.rules, &compiled.spans) {
            self.warn(report);
        }

        // The type analysis: column types to a fixpoint, then its
        // proofs, warnings for rules that cannot fire and errors for
        // contradicted declarations. Fetch rules take no part: an
        // import's columns are seeded with whatever an endpoint may return.
        let inputs = types::AnalysisInputs {
            predicates: self.predicates,
            seeds: &self.seeds,
            declared: types::Declared {
                predicates: &self.declarations,
                labels: &self.label_types,
            },
            open: &self.open,
            plan: &self.plan,
            previous: self.previous_types,
        };
        let types::Analysis {
            column_types,
            rule_columns,
            findings,
        } = types::analyze(&compiled.rules, &compiled.source_info(), &inputs);
        for types::TypeFinding { rule, diagnostic } in findings {
            let problem = match rule {
                Some(rule) => self
                    .pipeline
                    .label_instances(diagnostic, compiled.statements[rule]),
                None => diagnostic,
            };
            if problem.severity() == Severity::Error {
                self.recover(problem)?;
            } else {
                self.warn(problem);
            }
        }

        // The fetch rules take their reserved indices before the strata
        // are computed: an import whose `where` clause reads a derived
        // predicate depends on that predicate's rules like any consumer.
        for (index, rule, names) in std::mem::take(&mut self.fetch_rules) {
            compiled.rules[index] = rule;
            compiled.names[index] = names;
        }
        let strata = match strata::stratify(&compiled.rules, &compiled.spans) {
            Ok(strata) => strata,
            Err(problem) => {
                self.recover(problem)?;
                Vec::new()
            }
        };

        Ok(CompiledProgram {
            rules: compiled.rules,
            texts: compiled.texts,
            statements: compiled.statements,
            names: compiled.names,
            spans: compiled.spans,
            strata,
            exports: std::mem::take(&mut self.exports),
            column_types,
            rule_columns,
        })
    }

    /// Resolves a fact for tracing: the predicate's table and the ground
    /// values, without storing anything, seeding anything, or
    /// registering an unknown predicate.
    pub(crate) fn trace_fact(
        &mut self,
        fact: &Fact,
    ) -> Result<(TableId, Vec<DataValue>), Diagnostic> {
        let positional = self.positional_fact(fact)?;
        let fact = positional.as_ref().unwrap_or(fact);
        let name = self.resolve(&fact.predicate, fact.span)?;
        let Some(&Registered { table, arity }) = self.predicates.get(&name) else {
            return Err(EngineError::UnknownPredicate { predicate: name }.diagnostic());
        };
        if fact.terms.len() != arity {
            let written = fact.to_string();
            return Err(TraceError::FactArity {
                predicate: name,
                arity: arity.into(),
                fact: written.strip_suffix(" .").unwrap_or(&written).to_owned(),
                found: fact.terms.len().into(),
            }
            .diagnostic());
        }
        // A label the program never mentioned has no stored null.
        let mut labels = Vec::new();
        terms::collect_blank_nodes(fact, &mut labels);
        if let Some(label) = labels
            .into_iter()
            .find(|label| !self.blanks.contains_key(*label))
        {
            return Err(TraceError::UnknownBlankNode {
                label: label.to_owned(),
            }
            .diagnostic());
        }
        let values = fact
            .terms
            .iter()
            .map(|term| self.evaluate(term))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((table, values))
    }

    /// Dissolves the translator into the persistent translation state and the collected fact
    /// rows, by table.
    ///
    /// The dissolution is infallible, so the caller recovers its state on the error path too.
    pub(crate) fn finish(self) -> (TranslationState, HashMap<TableId, RowBuffer>) {
        (
            TranslationState {
                base_iri: self.base_iri,
                prefixes: self.prefixes,
                globals: self.globals,
                declarations: self.declarations,
                type_definitions: self.type_definitions,
                label_types: self.label_types,
                seeds: self.seeds,
                open: self.open,
                inputs: self.inputs,
                plan: self.plan,
                blanks: self.blanks,
                scratch: self.scratch,
            },
            self.facts,
        )
    }

    /// Handles one directive, reporting an unsupported one rather than failing.
    ///
    /// `rule_count` is the number of rules compiled so far, a bound import's placeholder
    /// included.
    fn directive(
        &mut self,
        directive: &Directive,
        preloaded: bool,
        rule_count: usize,
    ) -> Result<(), Diagnostic> {
        match directive {
            Directive::Prefix(_) | Directive::Parameter(_) => Ok(()),
            Directive::Base(_) => Ok(()),
            // `@output` names outputs, which the written program records;
            // their names must resolve.
            Directive::Output(output) => {
                for predicate in &output.predicates {
                    self.resolve(predicate, output.span)?;
                }
                Ok(())
            }
            Directive::Import(import) => {
                let predicate = self.resolve(&import.predicate, import.span)?;
                // The format is a keyword, not a name, so the base does
                // not apply.
                let Identifier::Bare(name) = &import.format else {
                    return Err(TranslationError::UnknownFormat {
                        name: import.format.to_string(),
                    }
                    .at(import.span));
                };
                let Some(format) = FileFormat::from_name(name) else {
                    return Err(
                        TranslationError::UnknownFormat { name: name.clone() }.at(import.span)
                    );
                };
                self.imports.push(PendingImport {
                    predicate,
                    variables: import.variables.clone(),
                    format,
                    options: import.options.clone(),
                    guards: import.guards.clone(),
                    bindings: import.bindings.clone(),
                    conditions: import.conditions.clone(),
                    span: import.span,
                    preloaded,
                    // The rule index of the fetch rule of an import with a
                    // `where` clause: the placeholder pushed just before
                    // this call.
                    rule_index: rule_count.saturating_sub(1),
                });
                Ok(())
            }
            // Declarations, `@type` definitions, and inputs were read
            // before the statements were translated. See
            // [declare_directive](Self::declare_directive),
            // [type_directive](Self::type_directive), and
            // [input_directive](Self::input_directive).
            Directive::Declare(_) | Directive::Type(_) | Directive::Input(_) => Ok(()),
            Directive::Export(export) => self.export_directive(export),
            Directive::Use(_) => {
                unreachable!(
                    "linking expands every `@use`, and loading rejects an unlinked program"
                )
            }
            Directive::Define(_) | Directive::Extern(_) => {
                let keyword = match directive {
                    Directive::Define(_) => "@define",
                    _ => "@extern",
                };
                let warning = TranslationError::IgnoredDirective {
                    directive: keyword.to_owned(),
                }
                .diagnostic();
                self.warn(match directive.span() {
                    Some(span) => warning.with_label(span, "this directive has no effect"),
                    None => warning,
                });
                Ok(())
            }
        }
    }

    /// Creates a null for every blank-node label the statements mention
    /// that has none yet.
    fn create_blank_nodes(&mut self, statements: &[Statement]) {
        let mut labels: Vec<&str> = Vec::new();
        for statement in statements {
            terms::collect_blank_nodes(statement, &mut labels);
        }
        for label in labels {
            if !self.blanks.contains_key(label) {
                let id = self.dictionary.fresh_null();
                self.blanks.insert(label.to_owned(), id);
            }
        }
    }

    /// Compiles a condition over pseudo-variables into a program,
    /// returning the variables it reads, in input order, and the program,
    /// from which an import pass builds a row filter.
    pub(crate) fn condition(
        &self,
        operation: &Operation,
    ) -> Result<(Vec<VariableId>, StackProgram), Diagnostic> {
        let mut names: HashMap<String, VariableId> = HashMap::new();
        collect_columns(operation, &mut names);
        let mut builder = StackProgramBuilder::default();
        builder.operation(self, VariableScope::Rule(&names), operation)?;
        let inputs = builder.inputs.clone();
        Ok((inputs, builder.finish(operation.span)?))
    }

    /// Resolves a name against the declared prefixes and base.
    pub(crate) fn resolve(
        &self,
        identifier: &Identifier,
        span: Option<Span>,
    ) -> Result<String, Diagnostic> {
        resolve_identifier(self.base_iri.as_deref(), &self.prefixes, identifier).ok_or_else(|| {
            match identifier {
                Identifier::Prefixed { prefix, .. } => TranslationError::UndeclaredPrefix {
                    prefix: prefix.clone(),
                }
                .at(span),
                // A qualified name that reaches translation belongs to no
                // instantiation: a program that uses modules has had them
                // resolved long before this.
                Identifier::Qualified { module, name } => {
                    TranslationError::QualifiedWithoutModules {
                        module: module.clone(),
                        name: name.clone(),
                    }
                    .at(span)
                }
                Identifier::Bare(_) | Identifier::Iri(_) => {
                    unreachable!("bare and absolute names always resolve")
                }
            }
        })
    }

    /// Returns the table of a predicate, registered on first sight.
    fn table(
        &mut self,
        predicate: &Identifier,
        arity: usize,
        span: Option<Span>,
    ) -> Result<TableId, Diagnostic> {
        let name = self.resolve(predicate, span)?;
        self.register_table(name, arity, span, |_| predicate.to_string())
    }

    /// Returns the table of a resolved predicate name, registered on first
    /// sight. A name is one predicate: a use whose number of columns
    /// disagrees with an earlier one is an error.
    fn table_named(
        &mut self,
        name: String,
        arity: usize,
        span: Option<Span>,
    ) -> Result<TableId, Diagnostic> {
        self.register_table(name, arity, span, |name| Identifier::bare(name).to_string())
    }

    /// Returns the table of a resolved predicate name, registered on first sight,
    /// with `shown` giving the name the arity error presents.
    fn register_table(
        &mut self,
        name: String,
        arity: usize,
        span: Option<Span>,
        shown: impl FnOnce(&str) -> String,
    ) -> Result<TableId, Diagnostic> {
        if let Some(&Registered {
            table,
            arity: registered,
        }) = self.predicates.get(&name)
        {
            if registered != arity {
                return Err(TranslationError::ArityMismatch {
                    name: shown(name.as_str()),
                    arity: arity.into(),
                    registered: registered.into(),
                }
                .at(span));
            }
            return Ok(table);
        }
        let table = self.manager.register(name.clone(), arity);
        self.predicates.insert(name, Registered { table, arity });
        Ok(table)
    }
}

/// Stores a translation writes into in place of the engine's, which it leaves as they were:
/// the engine's tables without their rows, a dictionary, registered predicates,
/// and error counts.
pub(in crate::engine) struct ScratchStores {
    /// The engine's tables, under their ids, without their rows.
    manager: TableManager,
    /// A dictionary of its own.
    dictionary: Dictionary,
    /// The registered predicates the translation starts from.
    predicates: HashMap<String, Registered>,
    /// Error counts of its own.
    errors: ErrorCounts,
}

impl ScratchStores {
    /// Returns the predicates registered in these stores.
    pub(in crate::engine) fn into_predicates(self) -> HashMap<String, Registered> {
        self.predicates
    }
}

impl Engine {
    /// Returns the stores and setup one translation runs with,
    /// borrowed separately so that the translation writes the stores while reading the rest.
    pub(in crate::engine) fn translation_parts<'a>(
        &'a mut self,
        purpose: Purpose<'a>,
    ) -> (TranslationStores<'a>, TranslationSetup<'a>) {
        self.parts(purpose, None)
    }

    /// Returns scratch stores beside the engine's tables, starting from the given predicates.
    pub(in crate::engine) fn scratch_stores(
        &self,
        predicates: HashMap<String, Registered>,
    ) -> ScratchStores {
        ScratchStores {
            manager: self.manager.without_rows(),
            dictionary: Dictionary::new(),
            predicates,
            errors: ErrorCounts::default(),
        }
    }

    /// Returns the scratch stores and the setup of a translation that leaves the engine's
    /// stores as they were, and reports to the engine's pipeline.
    pub(in crate::engine) fn scratch_translation_parts<'a>(
        &'a mut self,
        purpose: Purpose<'a>,
        scratch: &'a mut ScratchStores,
    ) -> (TranslationStores<'a>, TranslationSetup<'a>) {
        self.parts(purpose, Some(scratch))
    }

    /// Returns the stores and setup of a translation, the stores being the scratch ones if given
    /// and the engine's otherwise.
    fn parts<'a>(
        &'a mut self,
        purpose: Purpose<'a>,
        scratch: Option<&'a mut ScratchStores>,
    ) -> (TranslationStores<'a>, TranslationSetup<'a>) {
        let (manager, dictionary, predicates, errors) = match scratch {
            Some(scratch) => (
                &mut scratch.manager,
                &mut scratch.dictionary,
                &mut scratch.predicates,
                &mut scratch.errors,
            ),
            None => (
                &mut self.manager,
                &mut self.dictionary,
                &mut self.predicates,
                &mut self.errors,
            ),
        };
        (
            TranslationStores {
                manager,
                dictionary,
                predicates,
                pipeline: &mut self.pipeline,
                errors,
            },
            TranslationSetup {
                import_directory: self.import_directory.as_deref(),
                resources: &self.resources,
                responses: None,
                telemetry: &self.telemetry,
                passes: &self.passes,
                purpose,
                written: None,
                previous_types: self.program.as_ref().map(|program| &program.column_types),
            },
        )
    }
}
