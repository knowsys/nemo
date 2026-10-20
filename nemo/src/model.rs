//! The components of Nemo programs.
//!
//! Components are plain owned data.
//! Their only positional information is an optional [Span],
//! which the parser sets and which is `None` for a component built programmatically.
//! Structural equality and hashing ignore spans.
//!
//! Every `Display` implementation produces valid Nemo syntax,
//! and statements print with their terminating ` .`.

mod atom;
mod attribute;
mod builder;
mod directive;
mod fact;
mod identifier;
mod literal;
pub mod macros;
mod program;
mod rule;
mod statement;
mod substitution;
mod term;
mod walk;

/// Source positions of components: a source text and a byte range in it.
pub use nemo_physical::diagnostics::{FileId, Span};

pub use self::{
    atom::Atom,
    attribute::Attribute,
    builder::{ProgramBuilder, RuleBuilder},
    directive::{
        BaseDirective, BaseType, ColumnDeclaration, DeclareDirective, DefineDirective, Directive,
        ExportDirective, ExternDirective, ImportDirective, InputDirective, ModuleName,
        OutputDirective, ParameterDirective, PrefixDirective, TypeDirective, TypeExpression,
        TypeLabel, UseDirective,
    },
    fact::Fact,
    identifier::Identifier,
    literal::Literal,
    program::Program,
    rule::Rule,
    statement::Statement,
    substitution::Substitution,
    term::{
        Aggregate, AggregateKind, BuiltinDocumentation, Operation, Operator, Term, TermKind,
        Variable, VariableKind,
    },
    walk::Walk,
};

pub(crate) use self::{
    directive::OnError,
    identifier::{IdentifierRenaming, IdentifierRole},
};

use std::fmt::{self, Display, Formatter};

/// Implements [PartialEq], [Eq], and [Hash](std::hash::Hash) for a component type
/// over the listed fields, leaving out its `span`.
///
/// Every component's equality comes from this macro,
/// so two components are equal exactly if their contents are,
/// whether they were parsed or built programmatically.
macro_rules! span_ignoring_eq {
    ($type:ty: $($field:ident),+ $(,)?) => {
        impl PartialEq for $type {
            fn eq(&self, other: &Self) -> bool {
                $(self.$field == other.$field)&&+
            }
        }

        impl Eq for $type {}

        impl std::hash::Hash for $type {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                $(self.$field.hash(state);)+
            }
        }
    };
}
pub(crate) use span_ignoring_eq;

/// Writes items separated by `", "`.
pub(crate) fn fmt_list<Item: Display>(f: &mut Formatter<'_>, items: &[Item]) -> fmt::Result {
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            f.write_str(", ")?;
        }
        write!(f, "{item}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
