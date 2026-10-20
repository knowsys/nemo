//! Linking: a program's `@use`s instantiated into the program the engine loads.
//!
//! [load](Engine::load) takes a linked program: no `@use`, and no module-qualified name.
//! Linking is not a rewrite that tracing reverses:
//! diagnostics and traces show the linked program, instance names included.
//! Each instance of a module file is registered as a source of its own,
//! recording the `@use` that created it,
//! so diagnostics and traces can name the instance a statement belongs to.
//!
//! Any file is a module.
//! Its `@input` directives name the predicates the using program binds,
//! its `@parameter`s the values, and its `@output` directives the predicates it offers.
//! Instantiating it with `@use lib::tc { edge = link, $x = 5 } as alias .`
//! copies its statements into the using program,
//! with every name it owns rewritten to belong to *that instantiation*:
//! `alias::name` for the names it offers, and the same prefix for the ones it keeps to itself.
//! Inputs are the exception:
//! they become the predicates of the using program they are bound to (here `link`),
//! which is how data reaches a module.
//! A value binding gives the named parameter its value, overriding a default.
//!
//! Copying has two consequences.
//! A module used twice is two independent copies,
//! so `t::reach` and `u::reach` are different predicates over different data.
//! And a name a module does not make public cannot be referred to,
//! *provided* no other name resolves to what it was renamed to.
//! That is why a program may not write a predicate whose name begins with the `alias::`
//! of an instantiation in scope.
//! Privacy rests on this check alone,
//! since the renamed names live in the same namespace as everything else.
//!
//! Names are resolved *in the file they are written in*:
//! a module's `@base` and `@prefix` declarations apply to the module and are consumed here,
//! never leaking into the program that uses it.
//! The same holds for its global variables and for the files it reads:
//! a `@use` and a module's `@import` resolve against the directory of the module containing
//! them.
//!
//! A `@use` names its module rather than locating it:
//! `lib::tc` is the file `lib/tc.rls` next to the file containing the `@use`,
//! each leading `super` goes one directory up,
//! and a name beginning with a module root names a file inside the root's directory.

mod files;
mod instances;
mod location;
mod module;
mod renaming;
mod sources;
#[cfg(test)]
mod tests;

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use nemo_physical::diagnostics::Diagnostic;

use self::{
    files::anchor_import_resources,
    instances::{InstanceInterface, InstancesByAlias},
    location::Location,
    module::InstantiatedModule,
    renaming::InstanceRenaming,
};
pub(in crate::engine) use self::{files::directory_of, sources::ModuleSources};

use crate::{
    diagnostics::ModuleError,
    engine::Engine,
    model::{
        Directive, FileId, Identifier, IdentifierRenaming, IdentifierRole, Program, Span,
        Statement, UseDirective,
    },
    pipeline::{NameScope, Pipeline},
};

/// How deep a chain of modules may go.
///
/// Without a bound, a deep chain of files would overflow the stack
/// instead of producing a diagnostic.
const MAX_DEPTH: usize = 64;

/// How many instantiations one program may expand into.
///
/// Instantiating is copying, so a module used twice by each of a chain
/// of modules doubles per level. The bound turns an expansion that would
/// never finish into a diagnostic.
const MAX_INSTANTIATIONS: usize = 512;

impl Engine {
    /// Instantiates a program's modules and resolves its module-qualified names.
    pub(in crate::engine) fn link_program(
        &mut self,
        program: Program,
    ) -> Result<Program, Diagnostic> {
        Linker {
            pipeline: &mut self.pipeline,
            roots: &self.modules.roots,
            texts: self.modules.texts(),
            files: HashMap::new(),
            reported: HashSet::new(),
            count: 0,
        }
        .link(program)
    }
}

/// One run of linking: the pipeline module files are registered in,
/// and the state shared by its instantiations.
struct Linker<'a> {
    /// The registry of sources, which also collects warnings.
    pipeline: &'a mut Pipeline,
    /// The module roots by name, with their directories.
    roots: &'a HashMap<String, PathBuf>,
    /// The registered module texts by location, read instead of the files.
    texts: HashMap<Location, Arc<str>>,
    /// The module files read so far, by the name they are registered under.
    files: HashMap<String, FileId>,
    /// The module files whose warnings were reported: a module
    /// used twice is parsed and instantiated per instance but reported once.
    reported: HashSet<FileId>,
    /// How many instantiations have been expanded so far.
    count: usize,
}

impl Linker<'_> {
    /// Returns the directory of the program file containing a `@use` at the given span,
    /// or the working directory for a program without a file.
    fn directory(&self, span: Option<Span>) -> PathBuf {
        match span.and_then(|span| self.pipeline.file_name(span.file)) {
            Some(name) => directory_of(Path::new(name)),
            None => PathBuf::from("."),
        }
    }

    /// Links a program.
    fn link(mut self, program: Program) -> Result<Program, Diagnostic> {
        // A program without a `@use` instantiates nothing, and is not
        // walked at all: a qualified name in it belongs to no
        // instantiation, and translation says so when it reaches it.
        let is_use =
            |statement: &Statement| matches!(statement, Statement::Directive(Directive::Use(_)));
        if !program.statements.iter().any(is_use) {
            return Ok(program);
        }

        // The program's own scope: what its names resolve to, which is
        // what an ownership check has to be made against. A name is only
        // an instantiation's if it *resolves* to one, since a prefix or a
        // base can produce `alias::` out of a name that does not look
        // like one.
        let scope = NameScope::of(&program.statements);
        let aliases: Vec<String> = program
            .statements
            .iter()
            .filter_map(|statement| match statement {
                Statement::Directive(Directive::Use(directive)) => Some(directive.alias.clone()),
                _ => None,
            })
            .collect();

        // Instantiate first, so every alias is known before the
        // statements that reference it are rewritten. Each statement
        // notes whether the program wrote it itself.
        let mut instances = InstancesByAlias::default();
        let mut linked: Vec<(Statement, bool)> = Vec::with_capacity(program.statements.len());
        for statement in program.statements {
            let Statement::Directive(Directive::Use(directive)) = statement else {
                linked.push((statement, true));
                continue;
            };
            // A predicate a `@use` binds is a name of the using program,
            // and is checked like any other name it writes.
            for (_, actual) in &directive.bindings {
                if !matches!(actual, Identifier::Qualified { .. }) {
                    forbid_owned_name(actual, &scope, &aliases, directive.span)?;
                }
            }
            let program = self.directory(directive.span);
            let mut chain = Vec::new();
            let (statements, instance) = self.instantiate(
                &directive,
                &directive,
                &Location::program(),
                &program,
                "",
                &mut chain,
            )?;
            instances.add(&directive, instance)?;
            linked.extend(statements.into_iter().map(|statement| (statement, false)));
        }

        // Every remaining `alias::name` resolves here, including one an
        // instantiation carries because a `@use` *bound* an input to it,
        // whose meaning nothing before this point could have known. Only
        // the names the program wrote itself are checked against what
        // the instantiations own, since a module's own statements are
        // made of exactly those names.
        let mut statements = Vec::with_capacity(linked.len());
        for (mut statement, written_here) in linked {
            let mut resolve = |identifier: &Identifier,
                               role: IdentifierRole,
                               span: Option<Span>|
             -> Result<Option<Identifier>, Diagnostic> {
                match identifier {
                    Identifier::Qualified { module, name } => {
                        instances.resolve(module, name, span).map(Some)
                    }
                    other => {
                        if written_here && role == IdentifierRole::Predicate {
                            forbid_owned_name(other, &scope, &aliases, span)?;
                        }
                        Ok(None)
                    }
                }
            };
            statement.rename_identifiers(&mut resolve)?;
            statements.push(statement);
        }
        Ok(Program {
            statements,
            span: program.span,
        })
    }
}

/// Returns an error if a predicate *resolves* to a name an instantiation owns.
///
/// The check is made on the resolved name. A renamed module name is an
/// ordinary name in an ordinary namespace, and a program can write a name
/// in that namespace in more ways than the obvious one: `<t::secret>`, but
/// also a `@prefix t: <t::>` making `t:secret` the same name, or a
/// `@base <t::>` making every bare name in the file one. Checking only how
/// a name is written catches the first and none of the rest.
///
/// A name that does not resolve is left to translation, which reports
/// the undeclared prefix that made it unresolvable.
fn forbid_owned_name(
    predicate: &Identifier,
    scope: &NameScope,
    aliases: &[String],
    span: Option<Span>,
) -> Result<(), Diagnostic> {
    let Ok(resolved) = scope.resolve(predicate, span) else {
        return Ok(());
    };
    for alias in aliases {
        if resolved.starts_with(&format!("{alias}::")) {
            let mut problem = ModuleError::OwnedName {
                name: resolved.clone(),
                alias: alias.clone(),
            }
            .at(span);
            if !matches!(predicate, Identifier::Iri(iri) if *iri == resolved) {
                problem = problem.with_note(format!("`{predicate}` resolves to `{resolved}`"));
            }
            return Err(problem);
        }
    }
    Ok(())
}

impl Linker<'_> {
    /// Counts one more instantiation, of the module file at `canonical` by the given `@use`.
    ///
    /// Returns an error for a file already being instantiated (a cycle),
    /// a chain deeper than [MAX_DEPTH], and more instantiations than [MAX_INSTANTIATIONS].
    fn enter(
        &mut self,
        directive: &UseDirective,
        canonical: &Path,
        chain: &[PathBuf],
    ) -> Result<(), Diagnostic> {
        if chain.iter().any(|file| file == canonical) {
            return Err(ModuleError::UsedInCycle {
                module: directive.module.to_string(),
            }
            .at(directive.span));
        }
        if chain.len() >= MAX_DEPTH {
            return Err(ModuleError::TooDeep { limit: MAX_DEPTH }.at(directive.span));
        }
        self.count += 1;
        if self.count > MAX_INSTANTIATIONS {
            return Err(ModuleError::TooManyInstantiations {
                limit: MAX_INSTANTIATIONS,
            }
            .at(directive.span));
        }
        Ok(())
    }
}

impl Linker<'_> {
    /// Instantiates one `@use`, returning the statements it contributes
    /// and the instance a reference resolves against.
    ///
    /// `directive` is the `@use` with its bindings renamed for the using
    /// program or module, and `written` the same `@use` as written, which
    /// the instance's source records.
    /// `from` is the directory of the file containing the `@use`,
    /// and `program` the directory of the program containing the outermost one.
    /// `namespace` is the prefix of the using program's or module's own names,
    /// so a module used inside a module carries the whole path of aliases.
    /// `chain` is the files being instantiated, which is what makes a cycle
    /// an error rather than a hang.
    fn instantiate(
        &mut self,
        directive: &UseDirective,
        written: &UseDirective,
        from: &Location,
        program: &Path,
        namespace: &str,
        chain: &mut Vec<PathBuf>,
    ) -> Result<(Vec<Statement>, InstanceInterface), Diagnostic> {
        let location = self.locate(directive, from, program)?;
        let tree = location.tree(program, self.roots);
        let path = location.file(tree);
        let canonical = fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
        self.enter(directive, &canonical, chain)?;
        let file = self.read(&location, tree, directive)?;
        let instance = self.pipeline.add_instance(file, written.clone());
        // A module used twice reports what it offers but never writes once.
        let first = !self.reported.contains(&file);
        let statements = self.parse(file, instance)?;
        let module = InstantiatedModule::new(location, path, statements, directive, namespace)?;
        let public = module.public()?;

        // A module's own `@use`s resolve against *its* directory, and
        // their instances nest inside this one.
        chain.push(canonical);
        let nested = self.expand_nested(&module, program, chain);
        chain.pop();
        let (nested, mut expanded) = nested?;

        let mut renaming = InstanceRenaming::new(&module, &nested);
        expanded.extend(module.own_statements(&mut renaming)?);
        module.parameters(&mut expanded, directive)?;
        if first {
            module.report_unwritten(self.pipeline, directive, &public);
        }

        Ok((
            expanded,
            InstanceInterface {
                public,
                span: directive.span,
            },
        ))
    }

    /// Instantiates the `@use`s a module contains, nested inside this
    /// instantiation.
    fn expand_nested(
        &mut self,
        module: &InstantiatedModule,
        program: &Path,
        chain: &mut Vec<PathBuf>,
    ) -> Result<(InstancesByAlias, Vec<Statement>), Diagnostic> {
        // Every alias the module declares, so that a reference to an alias
        // of a later `@use` gets its own diagnostic.
        let declared: Vec<&str> = module
            .statements
            .iter()
            .filter_map(|statement| match statement {
                Statement::Directive(Directive::Use(inner)) => Some(inner.alias.as_str()),
                _ => None,
            })
            .collect();

        let mut nested = InstancesByAlias::default();
        let mut expanded = Vec::new();
        for statement in &module.statements {
            let Statement::Directive(Directive::Use(written)) = statement else {
                continue;
            };
            // The predicates this module binds are named in *its* scope:
            // one of its inputs, or one of its own predicates.
            // Renaming the bound predicates first is what carries data
            // through a chain of modules.
            let mut inner = written.clone();
            let mut renaming = InstanceRenaming::new(module, &nested);
            for (_, actual) in &mut inner.bindings {
                match renaming.identifier(actual, IdentifierRole::Predicate, inner.span) {
                    Ok(Some(renamed)) => *actual = renamed,
                    Ok(None) => {}
                    Err(problem) => {
                        return Err(later_alias(problem, actual, &declared, &nested, inner.span));
                    }
                }
            }
            // The values it passes on are its own too: its constants
            // resolve here, and a `$parameter` of this module is passed on
            // under its renamed name.
            for (_, value) in &mut inner.values {
                value.rename_identifiers(&mut renaming)?;
            }
            let (inner_statements, inner_instance) = self.instantiate(
                &inner,
                written,
                &module.location.directory(),
                program,
                &module.namespace,
                chain,
            )?;
            nested.add(&inner, inner_instance)?;
            expanded.extend(inner_statements);
        }
        Ok((nested, expanded))
    }
}

/// Returns the diagnostic for an unresolved `alias::name` bound in a `@use`:
/// the given problem, or, if a later `@use` of the module declares the alias,
/// an error saying so.
fn later_alias(
    problem: Diagnostic,
    actual: &Identifier,
    declared: &[&str],
    nested: &InstancesByAlias,
    span: Option<Span>,
) -> Diagnostic {
    let Identifier::Qualified { module, .. } = actual else {
        return problem;
    };
    if nested.by_alias.contains_key(module) || !declared.contains(&module.as_str()) {
        return problem;
    }
    ModuleError::UsedBelow {
        module: module.to_owned(),
    }
    .at(span)
}
