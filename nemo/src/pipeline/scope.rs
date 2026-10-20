//! The name scope of one file: what its `@base` and `@prefix`
//! declarations resolve its names against.
//!
//! Names resolve in the file they are written in, before translation
//! resolves them for the composed program: a module's declarations apply
//! to the module alone. A linked program has one scope, since instance
//! statements carry resolved names.

use std::collections::HashMap;

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::{ModuleError, TranslationError},
    model::{Directive, Identifier, Span, Statement},
};

/// The name scope of one file.
#[derive(Debug, Clone, Default)]
pub(crate) struct NameScope {
    /// The base IRI relative names extend, if declared.
    base: Option<String>,
    /// The declared prefixes.
    prefixes: HashMap<String, String>,
}

impl NameScope {
    /// Returns the scope a file's directives describe.
    ///
    /// The first `@base` counts.
    /// [check_base](Self::check_base) returns an error for a different second one.
    pub(crate) fn of(statements: &[Statement]) -> Self {
        let mut scope = Self::default();
        for statement in statements {
            match statement {
                Statement::Directive(Directive::Base(directive)) => {
                    scope.base.get_or_insert_with(|| directive.iri.clone());
                }
                Statement::Directive(Directive::Prefix(directive)) => {
                    scope
                        .prefixes
                        .insert(directive.prefix.clone(), directive.iri.clone());
                }
                _ => {}
            }
        }
        scope
    }

    /// Checks that the statements declare at most one base IRI.
    ///
    /// The same `@base` may appear again.
    ///
    /// # Errors
    ///
    /// Returns [ConflictingBase](TranslationError::ConflictingBase) at the first `@base`
    /// whose IRI differs from an earlier one, pointing at the first `@base` too.
    pub(crate) fn check_base(statements: &[Statement]) -> Result<(), Diagnostic> {
        let mut bases = statements.iter().filter_map(|statement| match statement {
            Statement::Directive(Directive::Base(directive)) => Some(directive),
            _ => None,
        });
        let Some(first) = bases.next() else {
            return Ok(());
        };
        let Some(other) = bases.find(|base| base.iri != first.iri) else {
            return Ok(());
        };
        let mut problem = TranslationError::ConflictingBase.at(other.span);
        if let Some(span) = first.span {
            problem = problem.with_label(span, "first declared here");
        }
        Err(problem)
    }

    /// Returns the base IRI, if declared.
    #[cfg(feature = "sparql")]
    pub(crate) fn base(&self) -> Option<&str> {
        self.base.as_deref()
    }

    /// Returns the declared prefixes.
    #[cfg(feature = "sparql")]
    pub(crate) fn prefixes(&self) -> &HashMap<String, String> {
        &self.prefixes
    }

    /// Returns the absolute name an identifier stands for in this scope.
    pub(crate) fn resolve(
        &self,
        identifier: &Identifier,
        span: Option<Span>,
    ) -> Result<String, Diagnostic> {
        match identifier {
            Identifier::Bare(name) => Ok(match &self.base {
                Some(base) => format!("{base}{name}"),
                None => name.clone(),
            }),
            Identifier::Iri(iri) => Ok(iri.clone()),
            Identifier::Prefixed { prefix, name } => match self.prefixes.get(prefix) {
                Some(iri) => Ok(format!("{iri}{name}")),
                None => Err(TranslationError::UndeclaredPrefix {
                    prefix: prefix.clone(),
                }
                .at(span)),
            },
            Identifier::Qualified { module, name } => Err(ModuleError::QualifiedUnresolvable {
                module: module.clone(),
                name: name.clone(),
            }
            .at(span)),
        }
    }
}
