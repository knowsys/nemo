//! A module file as one instantiation reads it: its statements, scope, inputs and bindings.

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::{AboutResource, ModuleError},
    engine::link::{
        InstanceRenaming, anchor_import_resources, directory_of, files::identity,
        location::Location,
    },
    model::{Directive, Identifier, Statement, UseDirective},
    pipeline::{NameScope, Pipeline},
};

/// A module file as one instantiation reads it.
pub(super) struct InstantiatedModule {
    /// Where the module lies, which its own `@use`s resolve against.
    pub(super) location: Location,
    /// The path of its file.
    path: PathBuf,
    /// The directory its own `@import`s resolve against.
    directory: PathBuf,
    /// Its statements, as parsed.
    pub(super) statements: Vec<Statement>,
    /// Its own name scope.
    pub(super) scope: NameScope,
    /// The input predicates it declares: each resolved in its scope, and as written.
    inputs: Vec<(String, Identifier)>,
    /// Its inputs' bindings: each resolved input, and the predicate the
    /// `@use` binds it to.
    pub(super) bindings: HashMap<String, Identifier>,
    /// The prefix of every name of the instantiation:
    /// the namespace of the using program or module, then the alias of the `@use`.
    pub(super) namespace: String,
}

impl InstantiatedModule {
    /// Creates a module as the given `@use` instantiates it,
    /// inside the `namespace` of the using program or module.
    pub(super) fn new(
        location: Location,
        path: PathBuf,
        statements: Vec<Statement>,
        directive: &UseDirective,
        namespace: &str,
    ) -> Result<Self, Diagnostic> {
        NameScope::check_base(&statements)?;
        let scope = NameScope::of(&statements);
        let inputs = inputs(&statements, &scope)?;
        let bindings = bindings(directive, &inputs, &scope)?;
        Ok(Self {
            location,
            directory: directory_of(&path),
            path,
            statements,
            scope,
            inputs,
            bindings,
            namespace: format!("{namespace}{}::", directive.alias),
        })
    }

    /// Returns the names the module offers with `@output`, by the unqualified name a
    /// reference uses, mapped to what each stands for in the linked program.
    pub(super) fn public(&self) -> Result<HashMap<String, Identifier>, Diagnostic> {
        let mut public = HashMap::new();
        for statement in &self.statements {
            let Statement::Directive(Directive::Output(directive)) = statement else {
                continue;
            };
            for name in &directive.predicates {
                // A reference is written `alias::name`, so an offered name
                // must be a bare name.
                let Identifier::Bare(plain) = name else {
                    return Err(ModuleError::InterfaceNameNotPlain {
                        name: name.to_string(),
                    }
                    .at(directive.span));
                };
                let resolved = self.scope.resolve(name, directive.span)?;
                // An offered input *is* what it was bound to: the interface
                // passes the predicate through rather than naming an empty
                // one of its own.
                let stands_for = match (self.is_input(&resolved), self.bindings.get(&resolved)) {
                    (true, Some(actual)) => actual.clone(),
                    _ => Identifier::Iri(format!("{}{resolved}", self.namespace)),
                };
                public.insert(plain.clone(), stands_for);
            }
        }
        Ok(public)
    }

    /// Returns the module's own statements renamed for the instantiation.
    ///
    /// Its `@input`, `@output`, `@use`, `@base`, and `@prefix` directives
    /// are consumed by the instantiation and not copied into the using program.
    pub(super) fn own_statements(
        &self,
        renaming: &mut InstanceRenaming<'_>,
    ) -> Result<Vec<Statement>, Diagnostic> {
        let mut renamed = Vec::new();
        for statement in &self.statements {
            if matches!(
                statement,
                Statement::Directive(
                    Directive::Input(_)
                        | Directive::Output(_)
                        | Directive::Use(_)
                        | Directive::Base(_)
                        | Directive::Prefix(_)
                )
            ) {
                continue;
            }
            let mut statement = statement.clone();
            statement.rename_identifiers(renaming)?;
            anchor_import_resources(&mut statement, &self.directory);
            renamed.push(statement);
        }
        Ok(renamed)
    }

    /// Sets the module's parameters to the values the `@use` supplies.
    ///
    /// The parameters in `expanded` are already renamed for this instantiation,
    /// and the values are already resolved in the scope of the using program.
    /// Returns an error for a value naming no parameter of the module,
    /// and for a parameter left without a value.
    pub(super) fn parameters(
        &self,
        expanded: &mut [Statement],
        directive: &UseDirective,
    ) -> Result<(), Diagnostic> {
        for (name, value) in &directive.values {
            let owned = format!("{}{name}", self.namespace);
            let mut found = false;
            for statement in expanded.iter_mut() {
                if let Statement::Directive(Directive::Parameter(parameter)) = statement
                    && parameter.variable.name == owned
                {
                    parameter.value = Some(value.clone());
                    found = true;
                }
            }
            if !found {
                return Err(ModuleError::NoSuchParameter {
                    module: directive.module.to_string(),
                    name: name.to_owned(),
                }
                .at(directive.span));
            }
        }
        for statement in expanded.iter() {
            if let Statement::Directive(Directive::Parameter(parameter)) = statement
                && parameter.value.is_none()
                && let Some(name) = parameter.variable.name.strip_prefix(&self.namespace)
            {
                return Err(ModuleError::ParameterWithoutValue {
                    module: directive.module.to_string(),
                    name: name.to_owned(),
                }
                .at(parameter.span.or(directive.span))
                .with_hint(format!(
                    "give it a default value, or set it in the `@use`, as `{{ ${name} = value }}`"
                )));
            }
        }
        Ok(())
    }

    /// Reports the names the module offers but never writes,
    /// which give the using program an always empty predicate without explanation.
    pub(super) fn report_unwritten(
        &self,
        pipeline: &mut Pipeline,
        directive: &UseDirective,
        public: &HashMap<String, Identifier>,
    ) {
        let written = self.written();
        let mut missing: Vec<&str> = public
            .keys()
            .map(String::as_str)
            .filter(|name| {
                let resolved = self
                    .scope
                    .resolve(&Identifier::bare(*name), None)
                    .unwrap_or_default();
                !written.contains(&resolved) && !self.is_input(&resolved)
            })
            .collect();
        if missing.is_empty() {
            return;
        }
        missing.sort_unstable();
        let names: Vec<String> = missing.iter().map(|name| format!("`{name}`")).collect();
        let mut problem = ModuleError::OffersUnwritten {
            module: directive.module.to_string(),
            count: missing.len().into(),
            names: names.join(", "),
        }
        .diagnostic();
        // The `@output` directives offering a name that is never written.
        let mut offered = false;
        for statement in &self.statements {
            let Statement::Directive(Directive::Output(output)) = statement else {
                continue;
            };
            let offers_missing = output.predicates.iter().any(
                |name| matches!(name, Identifier::Bare(plain) if missing.contains(&plain.as_str())),
            );
            if let (true, Some(span)) = (offers_missing, output.span) {
                problem = problem.with_label(span, "offered here");
                offered = true;
            }
        }
        if !offered {
            problem = problem.with_resource(identity(&self.path));
        }
        pipeline.report(problem);
    }

    /// Returns whether a resolved name is one of the module's inputs.
    fn is_input(&self, resolved: &str) -> bool {
        self.inputs.iter().any(|(input, _)| input == resolved)
    }

    /// Returns the predicates the module's facts, rule heads, and imports write,
    /// resolved in its own scope.
    fn written(&self) -> HashSet<String> {
        let mut written = HashSet::new();
        let mut write = |predicate: &Identifier| {
            if let Ok(name) = self.scope.resolve(predicate, None) {
                written.insert(name);
            }
        };
        for statement in &self.statements {
            match statement {
                Statement::Fact(fact) => write(&fact.predicate),
                Statement::Rule(rule) => rule.head.iter().for_each(|atom| write(&atom.predicate)),
                Statement::Directive(Directive::Import(import)) => write(&import.predicate),
                _ => {}
            }
        }
        written
    }
}

/// Returns the input predicates a module declares: each resolved in its own scope,
/// and as written, which is how a `@use` binds it.
///
/// A file without `@input` directives is a module without inputs,
/// so any file is usable.
fn inputs(
    statements: &[Statement],
    scope: &NameScope,
) -> Result<Vec<(String, Identifier)>, Diagnostic> {
    let mut inputs: Vec<(String, Identifier)> = Vec::new();
    for statement in statements {
        let Statement::Directive(Directive::Input(input)) = statement else {
            continue;
        };
        let resolved = scope.resolve(&input.predicate, input.span)?;
        if inputs.iter().any(|(known, _)| *known == resolved) {
            return Err(ModuleError::DuplicateInput {
                predicate: input.predicate.to_string(),
            }
            .at(input.span));
        }
        inputs.push((resolved, input.predicate.clone()));
    }
    Ok(inputs)
}

/// Returns the input bindings of a `@use`, from each input's resolved name to
/// the predicate it is bound to.
fn bindings(
    directive: &UseDirective,
    inputs: &[(String, Identifier)],
    scope: &NameScope,
) -> Result<HashMap<String, Identifier>, Diagnostic> {
    let mut bindings = HashMap::new();
    for (input, actual) in &directive.bindings {
        let resolved = scope.resolve(input, directive.span)?;
        if !inputs.iter().any(|(known, _)| *known == resolved) {
            let mut problem = ModuleError::NoSuchInput {
                input: input.to_string(),
            }
            .at(directive.span);
            if !inputs.is_empty() {
                problem = problem.with_note(format!(
                    "the module's inputs are: {}",
                    listed(inputs.iter())
                ));
            }
            return Err(problem);
        }
        if bindings.insert(resolved, actual.clone()).is_some() {
            return Err(ModuleError::InputBoundTwice {
                input: input.to_string(),
            }
            .at(directive.span));
        }
    }

    let unbound = inputs
        .iter()
        .filter(|(input, _)| !bindings.contains_key(input))
        .count();
    if unbound > 0 {
        return Err(ModuleError::InputsUnbound {
            count: unbound.into(),
            inputs: listed(
                inputs
                    .iter()
                    .filter(|(input, _)| !bindings.contains_key(input)),
            ),
        }
        .at(directive.span));
    }
    Ok(bindings)
}

/// Returns inputs as a `@use` writes them, quoted for a message.
fn listed<'a>(inputs: impl Iterator<Item = &'a (String, Identifier)>) -> String {
    inputs
        .map(|(_, written)| format!("`{written}`"))
        .collect::<Vec<_>>()
        .join(", ")
}
