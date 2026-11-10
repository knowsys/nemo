//! The name scope of one file, which its `@base` and `@prefix` directives declare.
//!
//! A name is resolved in the file that it is written in,
//! so the declarations of a module apply to that module alone.
//! A linked program has one scope,
//! since the statements of its module instances contain resolved names.

use std::collections::HashMap;

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::{ModuleError, TranslationError},
    model::{Directive, Identifier, Span, Statement},
};

/// The name scope of one file.
#[derive(Debug, Clone, Default)]
pub(crate) struct NameScope {
    /// The base IRI that bare names are appended to, if one is declared.
    base: Option<String>,
    /// The declared prefixes, mapped to their IRIs.
    prefixes: HashMap<String, String>,
}

impl NameScope {
    /// Returns the scope that the directives among `statements` declare.
    ///
    /// The first `@base` counts,
    /// and [check_base](Self::check_base) rejects a later one with a different IRI.
    /// A later `@prefix` for the same prefix replaces an earlier one.
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
    /// whose IRI differs from the first one, with a label at the first one.
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
    pub(crate) fn base(&self) -> Option<&str> {
        self.base.as_deref()
    }

    /// Returns the declared prefixes, mapped to their IRIs.
    pub(crate) fn prefixes(&self) -> &HashMap<String, String> {
        &self.prefixes
    }

    /// Returns the absolute name that an identifier stands for in this scope.
    ///
    /// A bare name is appended to the base IRI, if one is declared.
    /// Fails for a prefix that the scope does not declare, and for a module-qualified name,
    /// with a diagnostic at `span`.
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
