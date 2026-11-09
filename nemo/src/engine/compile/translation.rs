//! Translation: turning a program's statements into stored rows and compiled rules.
//!
//! The [Translator] resolves names, registers tables, loads facts and imports,
//! and compiles rules.
//! Its submodules split this work by statement kind,
//! and each of them adds an `impl` block to the same translator.

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
    dictionary::{Dictionary, LabelScope},
    function::{EvaluationContext, StackProgram},
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
        remote::{FetchedResource, Progress, ResourceRequest, Suspension},
        written::WrittenProgram,
    },
    model::{
        Directive, Fact, Identifier, ImportDirective, Operation, Span, Statement, TypeExpression,
        Walk,
    },
    pipeline::{NameScope, Pipeline, StatementId},
    telemetry::SinkSlot,
};

/// The parts of the engine that one translation writes to.
pub(crate) struct TranslationStores<'a> {
    /// The table store that facts are loaded into.
    pub(crate) manager: &'a mut TableManager,
    /// The dictionary in which fact values are interned.
    pub(crate) dictionary: &'a mut Dictionary,
    /// The registered predicates by resolved name, each with its table
    /// and its number of columns.
    pub(crate) predicates: &'a mut HashMap<String, Registered>,
    /// The pipeline that receives the program's reports,
    /// and knows the files that its statements come from.
    pub(crate) pipeline: &'a mut Pipeline,
    /// The failed evaluations of import conditions, counted per error kind.
    ///
    /// A row whose condition fails to evaluate is dropped.
    pub(crate) errors: &'a mut ErrorCounts,
}

/// The settings of one translation.
pub(crate) struct TranslationSetup<'a> {
    /// The directory against which relative import resources are resolved,
    /// if the engine sets one.
    pub(crate) import_directory: Option<&'a Path>,
    /// The in-memory resources registered on the engine, by name.
    ///
    /// An import of a resource with one of these names reads its bytes from here
    /// instead of the file system.
    pub(crate) resources: &'a HashMap<String, Arc<[u8]>>,
    /// The slot of the installed telemetry sink, which receives a report of each import's load.
    pub(crate) telemetry: &'a SinkSlot,
    /// The optimization passes.
    ///
    /// The import passes run only in a translation that reads data.
    pub(crate) passes: &'a OptimizationPasses,
    /// What the translation is for.
    pub(crate) purpose: Purpose<'a>,
    /// The written program of the translated statements, or `None` if no import pass runs.
    ///
    /// The import passes store the predicates that it derives whole.
    pub(crate) written: Option<&'a WrittenProgram>,
    /// The column types with which the loaded program was analyzed, if a program is loaded.
    ///
    /// An error about a change can then say which types the change widened.
    pub(crate) previous_types: Option<&'a HashMap<TableId, Vec<Type>>>,
    /// The evaluation context of ground expressions, such as the instant that their `NOW()`
    /// returns.
    pub(crate) context: EvaluationContext,
}

/// What a translation is for,
/// which decides whether it reads data and what happens to the problems it finds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Purpose<'a> {
    /// A load of a program that the pipeline stages left as written.
    ///
    /// Data is read, and the first error ends the translation.
    Load,
    /// A load of the program that the pipeline stages made from a written program,
    /// after the written program passed its [validation](Self::Validate).
    ///
    /// Data is read, and a problem found while reading data ends the translation.
    /// Any other error is a bug of the stages, and panics.
    /// Any other warning was already reported for the written program, and is dropped.
    LoadRewritten {
        /// The names of the stages that changed the program.
        stages: &'a [&'static str],
    },
    /// The translation of a written program before the program that its stages made is
    /// loaded.
    ///
    /// No data is read, and the first error ends the translation.
    Validate,
    /// A check.
    ///
    /// No data is read, and every problem is reported.
    Check,
}

impl<'a> Purpose<'a> {
    /// Returns the purpose of a load of the program that the given pipeline stages made.
    ///
    /// It is [Load](Self::Load) if no stage changed the program.
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

/// The column that stands for an aggregate's result while the head expression around the
/// aggregate is compiled.
///
/// It is replaced by the result's position in a row of the aggregation's output.
const AGGREGATE_RESULT: usize = usize::MAX;

/// The name of the label scope in which a program's blank nodes are nulls.
///
/// In this scope, `_:a` names the same null in every statement and revision of the program.
const PROGRAM_BLANK_NODES: &str = "program";

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

/// Collects the variables of an import's condition that stand for delivered columns,
/// each with its column's position as its variable id.
///
/// Delivered column `N` is the variable `column N`
/// (see [column_variable](crate::engine::compile::optimize::column_variable)).
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

/// The rules of one translation as they are compiled,
/// with what the reports and the type analysis need of each rule.
///
/// The other fields match [rules](Self::rules) by index.
#[derive(Default)]
struct CompiledRules {
    /// The rules, in statement order.
    rules: Vec<CompiledRule>,
    /// The text of each rule, rendered from the rule.
    texts: Vec<String>,
    /// The statement that each rule was compiled from.
    statements: Vec<StatementId>,
    /// Per rule, the written name of each rule column, or `None` for a column without one.
    names: Vec<Vec<Option<String>>>,
    /// The source position of each rule.
    spans: Vec<Option<Span>>,
}

impl CompiledRules {
    /// Adds a rule with its text, statement, column names and source position.
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

/// The translator of a program's statements, which resolves names, registers tables,
/// loads facts and imports, and compiles rules.
pub(crate) struct Translator<'a> {
    /// The table store that facts are loaded into.
    pub(crate) manager: &'a mut TableManager,
    /// The dictionary in which fact values are interned.
    pub(crate) dictionary: &'a mut Dictionary,
    /// The registered predicates by resolved name, each with its table
    /// and its number of columns.
    pub(crate) predicates: &'a mut HashMap<String, Registered>,
    /// The pipeline that receives the program's reports,
    /// and knows the files that its statements come from.
    pub(crate) pipeline: &'a mut Pipeline,
    /// The failed evaluations of import conditions, counted per error kind.
    ///
    /// A row whose condition fails to evaluate is dropped.
    pub(crate) errors: &'a mut ErrorCounts,
    /// The directory against which relative import resources are resolved,
    /// if the engine sets one.
    import_directory: Option<&'a Path>,
    /// The in-memory resources registered on the engine, by name.
    ///
    /// An import of a resource with one of these names reads its bytes from here
    /// instead of the file system.
    resources: &'a HashMap<String, Arc<[u8]>>,
    /// The declared base IRI, against which bare names are resolved.
    base_iri: Option<String>,
    /// The declared prefixes.
    prefixes: HashMap<String, String>,
    /// The values of the global parameters, by name.
    globals: HashMap<String, DataValue>,
    /// The rows of the facts per table, encoded by the dictionary.
    facts: HashMap<TableId, RowBuffer>,
    /// The import directives, which are executed once every arity is known.
    imports: Vec<PendingImport>,
    /// The resolved export directives.
    exports: Vec<ExportSpec>,
    /// The source positions of the export directives, matching [exports](Self::exports) by
    /// index.
    ///
    /// The checks that need every predicate's arity run after all statements are translated,
    /// and report at these positions.
    export_spans: Vec<Option<Span>>,
    /// The declared column types, by resolved predicate name.
    declarations: HashMap<String, types::PredicateDeclaration>,
    /// The `@type` definitions, by resolved label.
    type_definitions: HashMap<String, (TypeExpression, Option<Span>)>,
    /// The types that the `@type` definitions declare, by resolved label.
    ///
    /// They are known once every definition is recorded.
    label_types: LabelTypes,
    /// Per table, the initial column types from the facts and import formats,
    /// from which the type analysis starts.
    seeds: HashMap<TableId, Vec<Type>>,
    /// The imported predicates whose column types are not known before loading,
    /// and the `@input` predicates.
    ///
    /// The type analysis assumes that their columns can hold anything that their declarations
    /// allow.
    open: HashSet<String>,
    /// The resolved `@input` predicates.
    ///
    /// Their rows come from a program that uses this one as a module, or from
    /// [import_rows](crate::engine::Engine::import_rows),
    /// so an `@import` cannot fill them.
    inputs: HashSet<String>,
    /// What the loads of imported predicates leave out (see [ImportPlan]).
    pub(super) plan: ImportPlan,
    /// The label scope of the program's blank nodes, in which each label names one null.
    ///
    /// It is created before any term is evaluated, and kept across translations.
    program_blanks: LabelScope,
    /// Per resource that an RDF import reads, the label scope of its blank nodes.
    ///
    /// So the imports of one resource within one translation agree on its blank nodes.
    /// The scopes close when the translation [finishes](Self::finish).
    documents: HashMap<String, LabelScope>,
    /// Per predicate of an import with atoms in its `where` clause,
    /// the scratch table of its fetches.
    scratch: HashMap<String, TableId>,
    /// The fetch rules of the imports with atoms in their `where` clauses,
    /// each with the rule index reserved for it and the names of its rule columns.
    ///
    /// They are compiled when the imports are executed.
    fetch_rules: Vec<(usize, CompiledRule, Vec<Option<String>>)>,
    /// The slot of the installed telemetry sink, which receives a report of each import's load.
    telemetry: &'a SinkSlot,
    /// The remote resources fetched for the imports,
    /// by the position of each import's directive among the statements.
    ///
    /// [commit](Self::commit) receives them.
    fetched: HashMap<usize, FetchedResource>,
    /// The optimization passes.
    passes: &'a OptimizationPasses,
    /// What the translation is for.
    purpose: Purpose<'a>,
    /// The written program of the translated statements, or `None` if no import pass runs.
    pub(super) written: Option<&'a WrittenProgram>,
    /// The column types with which the loaded program was analyzed, if a program is loaded.
    previous_types: Option<&'a HashMap<TableId, Vec<Type>>>,
    /// The evaluation context of ground expressions, such as the instant that their `NOW()`
    /// returns.
    context: EvaluationContext,
    /// Per statement, whether its data is already stored, or empty for a first load.
    ///
    /// A *preloaded* fact adds no rows, and a preloaded import reads nothing,
    /// but everything that they declare is still compiled.
    preloaded: Vec<bool>,
}

impl<'a> Translator<'a> {
    /// Creates a translator over the engine's stores, with the given setup.
    ///
    /// `state` contains what earlier translations established, and is empty for a first load.
    /// `preloaded` marks the statements whose data is already stored.
    pub(crate) fn new(
        stores: TranslationStores<'a>,
        setup: TranslationSetup<'a>,
        state: TranslationState,
        preloaded: Vec<bool>,
    ) -> Self {
        let program_blanks = stores.dictionary.shared_label_scope(PROGRAM_BLANK_NODES);
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
            program_blanks,
            documents: HashMap::new(),
            scratch: state.scratch,
            fetch_rules: Vec::new(),
            telemetry: setup.telemetry,
            fetched: HashMap::new(),
            passes: setup.passes,
            purpose: setup.purpose,
            written: setup.written,
            previous_types: setup.previous_types,
            context: setup.context,
            preloaded,
        }
    }

    /// Returns whether a statement's data is already stored.
    fn is_preloaded(&self, index: usize) -> bool {
        self.preloaded.get(index).copied().unwrap_or(false)
    }

    /// Returns whether an import only validates its resolved options, without reading data.
    ///
    /// This is the case in a translation that reads no data,
    /// and for a directive whose data is already stored.
    fn options_only(&self, import: &PendingImport) -> bool {
        !self.purpose.reads() || import.preloaded
    }

    /// Handles a problem with the program's statements according to the purpose.
    ///
    /// A check reports the problem and continues,
    /// and every other translation returns it as the error.
    ///
    /// # Panics
    ///
    /// Panics in the translation of a program that the pipeline stages made from a written
    /// program that passed, since the stages then wrote something that translation refuses.
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
    /// except in the translation of a program that the pipeline stages made,
    /// since the warnings about its written program were already reported.
    fn warn(&mut self, warning: Diagnostic) {
        if !matches!(self.purpose, Purpose::LoadRewritten { .. }) {
            self.pipeline.report(warning);
        }
    }

    /// Translates the statements of a program whose imports read no remote resource,
    /// and returns the compiled program.
    ///
    /// The facts are collected as rows, which [finish](Self::finish) returns,
    /// the rules are compiled, and the directives take effect.
    /// A program whose imports do read one is translated in two steps instead,
    /// [prepare](Self::prepare) and [commit](Self::commit), with the fetching in between.
    pub(crate) fn program(
        &mut self,
        statements: &[Statement],
        ids: &[StatementId],
    ) -> Result<CompiledProgram, Diagnostic> {
        self.prepare(statements, ids)?;
        self.commit(statements, ids, HashMap::new())
    }

    /// Prepares the translation of a program, and returns the requests for the remote
    /// resources that its imports read, each tagged with the position of its directive.
    ///
    /// Preparing creates the nulls of blank nodes, reads the names, parameters and
    /// declarations, and computes the import plan.
    /// It registers, reads and stores nothing else.
    /// So a load can be suspended for the responses,
    /// and then [commit](Self::commit) from the translation state that preparing leaves,
    /// without preparing again.
    /// [finish](Self::finish) takes that state out.
    /// A translation that reads no data requests no remote resource.
    pub(crate) fn prepare(
        &mut self,
        statements: &[Statement],
        ids: &[StatementId],
    ) -> Result<Vec<ResourceRequest>, Diagnostic> {
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
        self.remote_requests(statements, ids)
    }

    /// Completes a [prepared](Self::prepare) translation, and returns the compiled program.
    ///
    /// The facts are collected as rows, the rules are compiled, the directives take effect,
    /// and the imports are executed.
    /// `fetched` contains the remote resources of the imports,
    /// by the position of each import's directive among the statements.
    pub(crate) fn commit(
        &mut self,
        statements: &[Statement],
        ids: &[StatementId],
        fetched: HashMap<usize, FetchedResource>,
    ) -> Result<CompiledProgram, Diagnostic> {
        self.fetched = fetched;
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
    /// So a name may be used before its declaration,
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
    /// A declaration that arrives after data, through [extend](crate::engine::Engine::extend),
    /// is checked against what the tables already contain,
    /// since the facts that it would have rejected are already stored.
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

    /// Runs the import passes, which decide what the loads leave out.
    ///
    /// The new plan is checked against the plan that the stored tables were loaded under
    /// (see [check_stability](ImportPlan::check_stability)).
    /// A translation that reads no data plans nothing, and keeps the stored plan.
    fn plan_imports(&mut self, statements: &[Statement]) -> Result<(), Diagnostic> {
        if !self.purpose.reads() {
            return Ok(());
        }
        let stored = std::mem::take(&mut self.plan);
        let survey = ImportSurvey::new(self, statements)?;
        self.plan = self.passes.plan_imports(&survey, self)?;
        self.plan.check_stability(stored, self.predicates.keys())
    }

    /// Translates the statements one by one, and returns the compiled rules.
    ///
    /// The facts are collected as rows, the rules are compiled, and the directives take effect.
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
                    // An import with atoms in its `where` clause is a rule of the program,
                    // its fetch rule, which is compiled when the imports are executed.
                    // A placeholder keeps its position among the rules,
                    // so that rule indices follow statement order.
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
                    self.directive(directive, index, compiled.rules.len())
                }
            };
            // What the statement's translation reported is about the statement,
            // and so, in a module instance, about that instance.
            self.pipeline
                .label_instances_in_reports(already, ids[index]);
            if let Err(problem) = outcome {
                let problem = self.pipeline.label_instances(problem, ids[index]);
                self.recover(problem)?;
            }
        }
        Ok(compiled)
    }

    /// Completes the compiled program after the imports were executed.
    ///
    /// It chooses the join orders, runs the program passes and the type analysis,
    /// puts the fetch rules at their reserved indices, and computes the strata.
    fn finish_program(
        &mut self,
        mut compiled: CompiledRules,
    ) -> Result<CompiledProgram, Diagnostic> {
        super::order::choose(&mut compiled.rules);
        for report in self.passes.rewrite(&mut compiled.rules, &compiled.spans) {
            self.warn(report);
        }

        // The type analysis computes the column types to a fixpoint, and then its proofs:
        // warnings for rules that cannot fire, and errors for contradicted declarations.
        // The fetch rules take no part, since an import's columns start with every type
        // that an endpoint may return.
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

        // The fetch rules take their reserved indices before the strata are computed,
        // since an import whose `where` clause reads a derived predicate
        // depends on that predicate's rules, like any rule that reads it.
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

    /// Resolves a fact for tracing into its predicate's table and its values.
    ///
    /// Nothing is stored, no column type is recorded, and an unknown predicate is not
    /// registered.
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
        if let Some(label) = labels.into_iter().find(|label| {
            self.dictionary
                .find_labeled_null(self.program_blanks, label)
                .is_none()
        }) {
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

    /// Dissolves the translator into the translation state that persists,
    /// and the collected fact rows by table.
    ///
    /// It cannot fail, so the translation state can be recovered after a failed translation
    /// too.
    pub(crate) fn finish(self) -> (TranslationState, HashMap<TableId, RowBuffer>) {
        for scope in self.documents.values() {
            self.dictionary.close_label_scope(*scope);
        }
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
                scratch: self.scratch,
            },
            self.facts,
        )
    }

    /// Handles one directive, the statement at position `index`.
    ///
    /// A directive without effect is reported as a warning rather than failing.
    /// `rule_count` is the number of rules compiled so far,
    /// including the placeholder of an import with atoms in its `where` clause.
    fn directive(
        &mut self,
        directive: &Directive,
        index: usize,
        rule_count: usize,
    ) -> Result<(), Diagnostic> {
        match directive {
            Directive::Prefix(_) | Directive::Parameter(_) => Ok(()),
            Directive::Base(_) => Ok(()),
            // The written program records the predicates that `@output` names,
            // and their names must resolve.
            Directive::Output(output) => {
                for predicate in &output.predicates {
                    self.resolve(predicate, output.span)?;
                }
                Ok(())
            }
            Directive::Import(import) => {
                // The rule index of the fetch rule of an import with atoms in its `where`
                // clause is the index of the placeholder pushed just before this call.
                let pending = self.pending_import(import, index, rule_count.saturating_sub(1))?;
                self.imports.push(pending);
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

    /// Returns the pending import that an import directive describes,
    /// with its predicate resolved and its format known.
    ///
    /// The import is executed once every arity is known.
    /// `index` is the directive's position among the statements, and `rule_index` is the rule
    /// index reserved for the fetch rule of an import with atoms in its `where` clause.
    fn pending_import(
        &self,
        import: &ImportDirective,
        index: usize,
        rule_index: usize,
    ) -> Result<PendingImport, Diagnostic> {
        let predicate = self.resolve(&import.predicate, import.span)?;
        // The format is a keyword, not a name, so the base does not apply.
        let Identifier::Bare(name) = &import.format else {
            return Err(TranslationError::UnknownFormat {
                name: import.format.to_string(),
            }
            .at(import.span));
        };
        let Some(format) = FileFormat::from_name(name) else {
            return Err(TranslationError::UnknownFormat { name: name.clone() }.at(import.span));
        };
        Ok(PendingImport {
            predicate,
            variables: import.variables.clone(),
            format,
            options: import.options.clone(),
            guards: import.guards.clone(),
            bindings: import.bindings.clone(),
            conditions: import.conditions.clone(),
            span: import.span,
            statement: index,
            preloaded: self.is_preloaded(index),
            rule_index,
        })
    }

    /// Creates a null for every blank node label that the statements mention
    /// and that has no null yet.
    fn create_blank_nodes(&mut self, statements: &[Statement]) {
        let mut labels: Vec<&str> = Vec::new();
        for statement in statements {
            terms::collect_blank_nodes(statement, &mut labels);
        }
        for label in labels {
            self.dictionary.labeled_null(self.program_blanks, label);
        }
    }

    /// Compiles an import's condition into a program,
    /// and returns the columns that the program reads, in input order, with the program.
    ///
    /// The condition refers to delivered column `N` by the variable `column N`,
    /// whose id is `N`.
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
                // A qualified name that reaches translation belongs to no module instance,
                // since linking resolves the names of a program that uses modules.
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

    /// Returns the table of a predicate, and registers it on first use,
    /// as [register_table](Self::register_table) does.
    fn table(
        &mut self,
        predicate: &Identifier,
        arity: usize,
        span: Option<Span>,
    ) -> Result<TableId, Diagnostic> {
        let name = self.resolve(predicate, span)?;
        self.register_table(name, arity, span, |_| predicate.to_string())
    }

    /// Returns the table of a resolved predicate name, and registers it on first use,
    /// as [register_table](Self::register_table) does.
    fn table_named(
        &mut self,
        name: String,
        arity: usize,
        span: Option<Span>,
    ) -> Result<TableId, Diagnostic> {
        self.register_table(name, arity, span, |name| Identifier::bare(name).to_string())
    }

    /// Returns the table of a resolved predicate name, and registers it on first use.
    ///
    /// A name is one predicate, so a use with another number of columns than an earlier one
    /// is an error.
    /// `shown` returns the name that this error shows.
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

/// Stores that a translation writes to in place of the engine's,
/// so that the engine's stores stay as they were.
///
/// They are the engine's tables without their rows, a dictionary, registered predicates
/// and error counts.
pub(in crate::engine) struct ScratchStores {
    /// The engine's tables, under their ids, without their rows.
    manager: TableManager,
    /// A dictionary separate from the engine's.
    dictionary: Dictionary,
    /// The registered predicates that the translation starts from.
    predicates: HashMap<String, Registered>,
    /// Error counts separate from the engine's.
    errors: ErrorCounts,
}

impl ScratchStores {
    /// Returns the predicates registered in these stores.
    pub(in crate::engine) fn into_predicates(self) -> HashMap<String, Registered> {
        self.predicates
    }
}

impl Engine {
    /// Returns the evaluation context in which a loading method translates.
    ///
    /// If no loading method has chosen a context since the last run began,
    /// it chooses one at the current instant.
    /// The next run evaluates in the same context.
    pub(in crate::engine) fn loading_context(&mut self) -> EvaluationContext {
        *self.next_context.get_or_insert_with(EvaluationContext::now)
    }

    /// Runs a loading method, and settles its choice of the context by how it ended
    /// (see [settle_context](Self::settle_context)).
    pub(in crate::engine) fn choosing_context(
        &mut self,
        call: impl FnOnce(&mut Self) -> Result<Progress, Diagnostic>,
    ) -> Result<Progress, Diagnostic> {
        let chose = self.next_context.is_none();
        let outcome = call(self);
        self.settle_context(chose, &outcome);
        outcome
    }

    /// Settles a loading method's choice of the context by how the method ended.
    ///
    /// `chose` says whether the method chose the context.
    /// A method suspended for data from the network keeps the choice until it is resumed
    /// or discarded.
    /// A method that failed undoes the choice, so that the next loading method chooses again.
    pub(in crate::engine) fn settle_context(
        &mut self,
        chose: bool,
        outcome: &Result<Progress, Diagnostic>,
    ) {
        if !chose {
            return;
        }
        match outcome {
            Ok(Progress::Done) => {}
            Ok(Progress::Suspended(_)) => {
                if let Some(Suspension::Load(load)) = &mut self.suspension {
                    load.chose_context = true;
                }
            }
            Err(_) => self.next_context = None,
        }
    }

    /// Returns the stores and the setup of one translation.
    ///
    /// They are borrowed separately,
    /// so that the translation can write the stores while it reads the rest of the engine.
    /// The translation evaluates its ground expressions in `context`.
    pub(in crate::engine) fn translation_parts<'a>(
        &'a mut self,
        purpose: Purpose<'a>,
        context: EvaluationContext,
    ) -> (TranslationStores<'a>, TranslationSetup<'a>) {
        self.parts(purpose, None, context)
    }

    /// Returns scratch stores beside the engine's tables,
    /// which start from the given registered predicates.
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

    /// Returns the stores and the setup of a translation that writes to `scratch`,
    /// so that the engine's stores stay as they were.
    ///
    /// The translation still reports to the engine's pipeline.
    /// Its ground expressions are evaluated at the instant chosen for the next run,
    /// if one is chosen, and otherwise at the current instant, which this does not choose.
    pub(in crate::engine) fn scratch_translation_parts<'a>(
        &'a mut self,
        purpose: Purpose<'a>,
        scratch: &'a mut ScratchStores,
    ) -> (TranslationStores<'a>, TranslationSetup<'a>) {
        let context = self.next_context.unwrap_or_else(EvaluationContext::now);
        self.parts(purpose, Some(scratch), context)
    }

    /// Returns the stores and the setup of a translation.
    ///
    /// The stores are `scratch` if it is given, and the engine's otherwise.
    fn parts<'a>(
        &'a mut self,
        purpose: Purpose<'a>,
        scratch: Option<&'a mut ScratchStores>,
        context: EvaluationContext,
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
                telemetry: &self.telemetry,
                passes: &self.passes,
                purpose,
                written: None,
                previous_types: self.program.as_ref().map(|program| &program.column_types),
                context,
            },
        )
    }
}
