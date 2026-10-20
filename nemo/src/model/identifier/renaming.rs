//! Renaming the identifiers of a statement, each visited in place.
//!
//! What a name means depends on its role:
//! - A predicate name belongs to the program's namespace.
//! - A constant, a structure label, or a datatype means the same in every module.
//! - A format name is matched by its spelling.
//!
//! Module instantiation renames names by their role.
//! One walk visits every name and passes its role along.
//!
//! Global variables and blank-node labels are visited too, each through a separate method.
//! Module instantiation renames them like predicates, but neither is an [Identifier].
//!
//! A renaming that returns `None` leaves the name unchanged,
//! so the same walk also answers read-only questions without allocating.

use nemo_physical::diagnostics::Diagnostic;

use crate::model::{
    Aggregate, Atom, Attribute, Directive, Identifier, Literal, Operation, Rule, Span, Statement,
    Term, TermKind, TypeExpression, TypeLabel, VariableKind,
};

/// The role of a name in a statement, which decides how module instantiation renames it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdentifierRole {
    /// A predicate, which module instantiation renames into the instance's namespace.
    Predicate,
    /// A value (a constant, a structure's label, a datatype),
    /// which means the same thing in every module.
    Value,
    /// A name the engine matches by spelling, like the `csv` of an
    /// import: not a name in the program's namespace at all.
    Reserved,
}

/// What a walk does with each name it finds.
pub(crate) trait IdentifierRenaming {
    /// Returns what an identifier becomes, or `None` to leave it.
    fn identifier(
        &mut self,
        identifier: &Identifier,
        role: IdentifierRole,
        span: Option<Span>,
    ) -> Result<Option<Identifier>, Diagnostic>;

    /// Returns what a global variable's name becomes, or `None` to leave it.
    fn global(&mut self, name: &str) -> Option<String> {
        let _ = name;
        None
    }

    /// Returns what a blank node's label becomes, or `None` to leave it.
    fn blank(&mut self, label: &str) -> Option<String> {
        let _ = label;
        None
    }
}

/// A renaming written as a closure, which renames identifiers only.
impl<F> IdentifierRenaming for F
where
    F: FnMut(&Identifier, IdentifierRole, Option<Span>) -> Result<Option<Identifier>, Diagnostic>,
{
    fn identifier(
        &mut self,
        identifier: &Identifier,
        role: IdentifierRole,
        span: Option<Span>,
    ) -> Result<Option<Identifier>, Diagnostic> {
        self(identifier, role, span)
    }
}

impl Statement {
    /// Rewrites every name of the statement.
    pub(crate) fn rename_identifiers(
        &mut self,
        rename: &mut impl IdentifierRenaming,
    ) -> Result<(), Diagnostic> {
        statement(self, rename)
    }
}

impl Term {
    /// Rewrites every name inside the term.
    pub(crate) fn rename_identifiers(
        &mut self,
        rename: &mut impl IdentifierRenaming,
    ) -> Result<(), Diagnostic> {
        term(self, rename)
    }
}

/// Rewrites every name of a statement.
fn statement(
    statement: &mut Statement,
    rename: &mut impl IdentifierRenaming,
) -> Result<(), Diagnostic> {
    match statement {
        Statement::Fact(fact) => {
            set(
                &mut fact.predicate,
                rename,
                IdentifierRole::Predicate,
                fact.span,
            )?;
            for argument in &mut fact.terms {
                term(argument, rename)?;
            }
            Ok(())
        }
        Statement::Rule(rule) => self::rule(rule, rename),
        Statement::Directive(directive) => self::directive(directive, rename),
    }
}

/// Rewrites one identifier in place, leaving it unchanged if the renaming returns `None`.
fn set(
    identifier: &mut Identifier,
    rename: &mut impl IdentifierRenaming,
    role: IdentifierRole,
    span: Option<Span>,
) -> Result<(), Diagnostic> {
    if let Some(renamed) = rename.identifier(identifier, role, span)? {
        *identifier = renamed;
    }
    Ok(())
}

/// Rewrites the names of a rule.
fn rule(rule: &mut Rule, rename: &mut impl IdentifierRenaming) -> Result<(), Diagnostic> {
    for attribute in &mut rule.attributes {
        self::attribute(attribute, rename)?;
    }
    for head in &mut rule.head {
        atom(head, rename)?;
    }
    for literal in &mut rule.body {
        match literal {
            Literal::Positive(body) | Literal::Negative(body) => atom(body, rename)?,
            Literal::Operation(constraint) => operation(constraint, rename)?,
        }
    }
    Ok(())
}

/// Rewrites the names of a rule attribute.
///
/// An attribute's name, like `name` in `#[name("…")]`, is a keyword of the language,
/// so it is [reserved](IdentifierRole::Reserved).
/// Its arguments are terms like any other.
fn attribute(
    attribute: &mut Attribute,
    rename: &mut impl IdentifierRenaming,
) -> Result<(), Diagnostic> {
    set(
        &mut attribute.name,
        rename,
        IdentifierRole::Reserved,
        attribute.span,
    )?;
    for argument in &mut attribute.arguments {
        term(argument, rename)?;
    }
    Ok(())
}

/// Rewrites the names of an atom: its predicate, then its arguments.
fn atom(atom: &mut Atom, rename: &mut impl IdentifierRenaming) -> Result<(), Diagnostic> {
    set(
        &mut atom.predicate,
        rename,
        IdentifierRole::Predicate,
        atom.span,
    )?;
    for argument in &mut atom.terms {
        term(argument, rename)?;
    }
    Ok(())
}

/// Rewrites the names inside a term.
fn term(term: &mut Term, rename: &mut impl IdentifierRenaming) -> Result<(), Diagnostic> {
    let span = term.span;
    match &mut term.kind {
        TermKind::Constant(name) => set(name, rename, IdentifierRole::Value, span)?,
        TermKind::RdfLiteral { datatype, .. } => {
            set(datatype, rename, IdentifierRole::Value, span)?
        }
        // A function term is a labeled tuple, which is a value.
        // A call to a `@define`d builtin is written the same way.
        // Telling the two apart needs the defines, which this walk does not know.
        // Column labels are not names to rewrite.
        TermKind::Function {
            name, arguments, ..
        } => {
            set(name, rename, IdentifierRole::Value, span)?;
            for argument in arguments {
                self::term(argument, rename)?;
            }
        }
        TermKind::Tuple(elements) => {
            for element in elements {
                self::term(element, rename)?;
            }
        }
        TermKind::Map { label, pairs } => {
            if let Some(label) = label {
                set(label, rename, IdentifierRole::Value, span)?;
            }
            for (key, value) in pairs {
                self::term(key, rename)?;
                self::term(value, rename)?;
            }
        }
        TermKind::Operation(applied) => operation(applied, rename)?,
        TermKind::Aggregate(applied) => aggregate(applied, rename)?,
        TermKind::Variable(variable) => {
            if variable.kind == VariableKind::Global
                && let Some(renamed) = rename.global(&variable.name)
            {
                variable.name = renamed;
            }
        }
        TermKind::BlankNode(label) => {
            if let Some(renamed) = rename.blank(label) {
                *label = renamed;
            }
        }
        TermKind::String(_)
        | TermKind::LanguageString { .. }
        | TermKind::Boolean(_)
        | TermKind::Number(_) => {}
    }
    Ok(())
}

/// Rewrites the names inside an operation's arguments.
fn operation(
    operation: &mut Operation,
    rename: &mut impl IdentifierRenaming,
) -> Result<(), Diagnostic> {
    for argument in &mut operation.arguments {
        term(argument, rename)?;
    }
    Ok(())
}

/// Rewrites the names inside an aggregate: its input, and the
/// variables scoping what it aggregates over.
fn aggregate(
    aggregate: &mut Aggregate,
    rename: &mut impl IdentifierRenaming,
) -> Result<(), Diagnostic> {
    if let Some(input) = &mut aggregate.input {
        term(input, rename)?;
    }
    if let Some(value) = &mut aggregate.value {
        term(value, rename)?;
    }
    for variable in &mut aggregate.scope {
        if variable.kind == VariableKind::Global
            && let Some(renamed) = rename.global(&variable.name)
        {
            variable.name = renamed;
        }
    }
    Ok(())
}

/// Rewrites the names of a directive.
///
/// A directive's format name and option keys are matched by spelling,
/// not resolved as program names: the format is visited as
/// [reserved](IdentifierRole::Reserved) and the keys are not visited at all.
fn directive(
    directive: &mut Directive,
    rename: &mut impl IdentifierRenaming,
) -> Result<(), Diagnostic> {
    match directive {
        Directive::Import(import) => {
            set(
                &mut import.predicate,
                rename,
                IdentifierRole::Predicate,
                import.span,
            )?;
            set(
                &mut import.format,
                rename,
                IdentifierRole::Reserved,
                import.span,
            )?;
            // An option's key is matched by its spelling and is not a program name.
            // Only its value can contain one.
            for (_, value) in &mut import.options {
                term(value, rename)?;
            }
            for (_, value) in &mut import.guards {
                term(value, rename)?;
            }
            for condition in &mut import.conditions {
                operation(condition, rename)?;
            }
        }
        Directive::Export(export) => {
            set(
                &mut export.predicate,
                rename,
                IdentifierRole::Predicate,
                export.span,
            )?;
            set(
                &mut export.format,
                rename,
                IdentifierRole::Reserved,
                export.span,
            )?;
            for (_, value) in &mut export.options {
                term(value, rename)?;
            }
            for (_, value) in &mut export.guards {
                term(value, rename)?;
            }
        }
        Directive::Output(output) => {
            for predicate in &mut output.predicates {
                set(predicate, rename, IdentifierRole::Predicate, output.span)?;
            }
        }
        Directive::Declare(declare) => {
            set(
                &mut declare.predicate,
                rename,
                IdentifierRole::Predicate,
                declare.span,
            )?;
            for column in &mut declare.columns {
                datatype(&mut column.datatype, rename)?;
            }
        }
        Directive::Type(declaration) => {
            // The declared type's label and every name inside it are
            // value names, rewritten by the datatype walk.
            datatype(&mut declaration.definition, rename)?;
        }
        Directive::Input(input) => {
            set(
                &mut input.predicate,
                rename,
                IdentifierRole::Predicate,
                input.span,
            )?;
            for column in input.columns.iter_mut().flatten() {
                datatype(&mut column.datatype, rename)?;
            }
        }
        Directive::Define(define) => {
            set(
                &mut define.name,
                rename,
                IdentifierRole::Predicate,
                define.span,
            )?;
            term(&mut define.body, rename)?;
        }
        Directive::Extern(external) => {
            set(
                &mut external.name,
                rename,
                IdentifierRole::Predicate,
                external.span,
            )?;
            for parameter in &mut external.parameters {
                datatype(parameter, rename)?;
            }
            datatype(&mut external.result, rename)?;
        }
        Directive::Parameter(parameter) => {
            if let Some(renamed) = rename.global(&parameter.variable.name) {
                parameter.variable.name = renamed;
            }
            if let Some(value) = &mut parameter.value {
                term(value, rename)?;
            }
        }
        // `@base`, `@prefix`, and `@use` name IRIs, prefixes, and files,
        // not program names.
        Directive::Base(_) | Directive::Prefix(_) | Directive::Use(_) => {}
    }
    Ok(())
}

/// Rewrites the names inside a declared type expression: a structured
/// type's label names a value's shape.
fn datatype(
    datatype: &mut TypeExpression,
    rename: &mut impl IdentifierRenaming,
) -> Result<(), Diagnostic> {
    match datatype {
        TypeExpression::Base(_) => {}
        TypeExpression::Named(name) => {
            set(name, rename, IdentifierRole::Value, None)?;
        }
        TypeExpression::Tuple { label, fields } => {
            type_label(label, rename)?;
            for field in fields {
                self::datatype(field, rename)?;
            }
        }
        TypeExpression::Map { label, fields } => {
            type_label(label, rename)?;
            for (_, value) in fields {
                self::datatype(value, rename)?;
            }
        }
        TypeExpression::Union(members) => {
            for member in members {
                self::datatype(member, rename)?;
            }
        }
    }
    Ok(())
}

/// Rewrites a structured type's label, if it names one.
fn type_label(
    label: &mut TypeLabel,
    rename: &mut impl IdentifierRenaming,
) -> Result<(), Diagnostic> {
    if let TypeLabel::Named(name) = label {
        set(name, rename, IdentifierRole::Value, None)?;
    }
    Ok(())
}
