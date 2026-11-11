//! Parsing import and export directives,
//! which share the specification of a predicate, a format and options.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::ParseError,
    model::{
        Atom, ExportDirective, Identifier, ImportDirective, Literal, Operation, Term, TermKind,
        VariableKind,
    },
    parser::{cursor::Parser, lexer::TokenKind},
};

/// The parsed parts of an import or export specification.
struct Specification {
    /// The predicate.
    predicate: Identifier,
    /// The variables that an import names its columns by, or empty if it names none.
    variables: Vec<String>,
    /// The format.
    format: Identifier,
    /// The options, as key-value pairs.
    options: Vec<(Term, Term)>,
    /// The guards, which bind variables that the option values may use.
    guards: Vec<(String, Term)>,
    /// An import's binding atoms, if it has a `where` clause.
    bindings: Vec<Atom>,
    /// An import's conditions, if it has a `where` clause.
    conditions: Vec<Operation>,
}

impl Parser<'_> {
    /// Parses `@import predicate from format { … }`, with its guards
    /// and its `where` clause.
    pub(super) fn import_directive(&mut self) -> Result<ImportDirective, Diagnostic> {
        let specification = self.specification("from", true)?;
        Ok(ImportDirective {
            predicate: specification.predicate,
            variables: specification.variables,
            format: specification.format,
            options: specification.options,
            guards: specification.guards,
            bindings: specification.bindings,
            conditions: specification.conditions,
            span: None,
        })
    }

    /// Parses `@export predicate to format { … }`, with its guards.
    pub(super) fn export_directive(&mut self) -> Result<ExportDirective, Diagnostic> {
        let specification = self.specification("to", false)?;
        Ok(ExportDirective {
            predicate: specification.predicate,
            format: specification.format,
            options: specification.options,
            guards: specification.guards,
            span: None,
        })
    }

    /// Parses an import or export specification,
    /// such as `predicate from format { key = value, … }`.
    ///
    /// `connective` is the word before the format,
    /// which is `from` for an import and `to` for an export.
    /// It is a keyword only in this position.
    /// `bindable` says whether the specification may name its columns after the predicate
    /// and end in a `where` clause, as an import may.
    fn specification(
        &mut self,
        connective: &str,
        bindable: bool,
    ) -> Result<Specification, Diagnostic> {
        let predicate = self.identifier()?;
        let mut variables = Vec::new();
        if bindable && self.eat(&TokenKind::ParenOpen).is_some() {
            variables = self.separated(|parser| {
                parser.expect_variable(VariableKind::Universal, "a column variable like `?x`")
            })?;
            self.expect(&TokenKind::ParenClose, "`)` closing the column variables")?;
        }
        self.expect_word(
            connective,
            &format!("`{connective}` before the format"),
            || ParseError::ExpectedConnective {
                connective: connective.to_owned(),
            },
        )?;
        let format = self.identifier()?;
        let options = self
            .after(&TokenKind::BraceOpen, |parser| {
                parser.list(
                    &TokenKind::BraceClose,
                    "`}` closing the format options",
                    |parser| {
                        let key = parser.plain_term()?;
                        parser.expect(&TokenKind::Equal, "`=` between option key and value")?;

                        // Only the names of column formats carry the marker `?`,
                        // as in `format = (int?, string)`.
                        parser.markers = matches!(
                            &key.kind,
                            TermKind::Constant(Identifier::Bare(name)) if name == "format"
                        );
                        let value = parser.plain_term();
                        parser.markers = false;

                        Ok((key, value?))
                    },
                )
            })?
            .unwrap_or_default();
        // Guards `, ?x = term` follow the options.
        let mut guards = Vec::new();
        while self.eat(&TokenKind::Comma).is_some() {
            let variable =
                self.expect_variable(VariableKind::Universal, "a guard variable like `?x`")?;
            self.expect(&TokenKind::Equal, "`=` after the guard variable")?;
            guards.push((variable, self.term()?));
        }
        // The `where` clause contains binding atoms and conditions, as in a rule body.
        // `where` is a keyword only in this position.
        let mut bindings = Vec::new();
        let mut conditions = Vec::new();
        if bindable && self.at_word("where") {
            self.advance();
            for literal in self.separated(Self::literal)? {
                match literal {
                    Literal::Positive(atom) => bindings.push(atom),
                    Literal::Operation(operation) => conditions.push(operation),
                    Literal::Negative(atom) => {
                        return Err(ParseError::NegationInWhere.at(atom.span));
                    }
                }
            }
        }
        Ok(Specification {
            predicate,
            variables,
            format,
            options,
            guards,
            bindings,
            conditions,
        })
    }
}
