//! Directives: the `@`-statements configuring a program.

mod declare;
mod define;
mod external;
mod import;
mod input;
mod module_name;
mod using;

pub use self::{
    declare::{
        BaseType, ColumnDeclaration, DeclareDirective, TypeDirective, TypeExpression, TypeLabel,
    },
    define::DefineDirective,
    external::ExternDirective,
    import::{ExportDirective, ImportDirective},
    input::InputDirective,
    module_name::ModuleName,
    using::UseDirective,
};

pub(crate) use self::import::OnError;

use std::fmt::{self, Display, Formatter};

use crate::model::{Identifier, Span, Variable, fmt_list, span_ignoring_eq};

/// A directive statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Directive {
    /// `@base <iri> .`, the base IRI relative names resolve against.
    Base(BaseDirective),
    /// `@prefix pre: <iri> .`, a name prefix declaration.
    Prefix(PrefixDirective),
    /// `@import p from format { … } .`, a source of a predicate's rows.
    Import(ImportDirective),
    /// `@export p to format { … } .`, a target for a predicate's rows.
    Export(ExportDirective),
    /// `@output p, q .`, the predicates the file makes available:
    /// the program's output when run, and what a `@use` of the file can refer to.
    Output(OutputDirective),
    /// `@input p(…) .`, a predicate whose rows come from the program that `@use`s the file,
    /// or from the API.
    Input(InputDirective),
    /// `@declare p(…) .`, a predicate's column types.
    Declare(DeclareDirective),
    /// `@type point(double, double) .`, the element types of every `point(…)` value.
    Type(TypeDirective),
    /// `@parameter $x: string = "a" .`, a global parameter, optionally typed and defaulted.
    Parameter(ParameterDirective),
    /// `@use lib::tc { input = actual, $x = 5 } as alias .`, a module instantiation.
    Use(UseDirective),
    /// `@define f(?x) = … .`, a derived builtin.
    Define(DefineDirective),
    /// `@extern f(any) -> double .`, an externally registered builtin.
    Extern(ExternDirective),
}

impl Directive {
    /// Returns the directive's source position.
    pub fn span(&self) -> Option<Span> {
        match self {
            Directive::Base(directive) => directive.span,
            Directive::Prefix(directive) => directive.span,
            Directive::Import(directive) => directive.span,
            Directive::Export(directive) => directive.span,
            Directive::Output(directive) => directive.span,
            Directive::Input(directive) => directive.span,
            Directive::Declare(directive) => directive.span,
            Directive::Type(directive) => directive.span,
            Directive::Parameter(directive) => directive.span,
            Directive::Use(directive) => directive.span,
            Directive::Define(directive) => directive.span,
            Directive::Extern(directive) => directive.span,
        }
    }

    /// Sets the directive's source position.
    pub fn set_span(&mut self, span: Option<Span>) {
        match self {
            Directive::Base(directive) => directive.span = span,
            Directive::Prefix(directive) => directive.span = span,
            Directive::Import(directive) => directive.span = span,
            Directive::Export(directive) => directive.span = span,
            Directive::Output(directive) => directive.span = span,
            Directive::Input(directive) => directive.span = span,
            Directive::Declare(directive) => directive.span = span,
            Directive::Type(directive) => directive.span = span,
            Directive::Parameter(directive) => directive.span = span,
            Directive::Use(directive) => directive.span = span,
            Directive::Define(directive) => directive.span = span,
            Directive::Extern(directive) => directive.span = span,
        }
    }
}

impl Display for Directive {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Directive::Base(directive) => write!(f, "{directive}"),
            Directive::Prefix(directive) => write!(f, "{directive}"),
            Directive::Import(directive) => write!(f, "{directive}"),
            Directive::Export(directive) => write!(f, "{directive}"),
            Directive::Output(directive) => write!(f, "{directive}"),
            Directive::Input(directive) => write!(f, "{directive}"),
            Directive::Type(directive) => write!(f, "{directive}"),
            Directive::Declare(directive) => write!(f, "{directive}"),
            Directive::Parameter(directive) => write!(f, "{directive}"),
            Directive::Use(directive) => write!(f, "{directive}"),
            Directive::Define(directive) => write!(f, "{directive}"),
            Directive::Extern(directive) => write!(f, "{directive}"),
        }
    }
}

/// The base-IRI declaration: `@base <iri> .`.
#[derive(Debug, Clone)]
pub struct BaseDirective {
    /// The base IRI.
    pub iri: String,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(BaseDirective: iri);

impl Display for BaseDirective {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@base <{}> .", self.iri)
    }
}

/// A prefix declaration: `@prefix pre: <iri> .`.
#[derive(Debug, Clone)]
pub struct PrefixDirective {
    /// The declared prefix, without the colon.
    pub prefix: String,
    /// The IRI it expands to.
    pub iri: String,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(PrefixDirective: prefix, iri);

impl Display for PrefixDirective {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@prefix {}: <{}> .", self.prefix, self.iri)
    }
}

/// An output declaration: `@output p, q .`.
///
/// It lists the predicates the file makes available:
/// - Run directly, the program outputs these predicates.
/// - Instantiated by a `@use`, the file makes them available as `alias::name`,
///   and keeps every other predicate private to the instantiation.
#[derive(Debug, Clone)]
pub struct OutputDirective {
    /// The predicates the file makes available.
    pub predicates: Vec<Identifier>,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(OutputDirective: predicates);

impl Display for OutputDirective {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("@output ")?;
        fmt_list(f, &self.predicates)?;
        f.write_str(" .")
    }
}

/// A global-parameter declaration: `@parameter $x: string = "a" .`.
#[derive(Debug, Clone)]
pub struct ParameterDirective {
    /// The declared global variable.
    pub variable: Variable,
    /// Its declared type, if one is stated.
    pub datatype: Option<TypeExpression>,
    /// Its default value, or `None` if the directive declares none.
    pub value: Option<crate::model::Term>,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(ParameterDirective: variable, datatype, value);

impl Display for ParameterDirective {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "@parameter {}", self.variable)?;
        if let Some(datatype) = &self.datatype {
            write!(f, ": {datatype}")?;
        }
        if let Some(value) = &self.value {
            write!(f, " = {value}")?;
        }
        f.write_str(" .")
    }
}
