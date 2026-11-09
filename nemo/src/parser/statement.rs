//! Parsing statements, and the atoms and literals of facts and rules.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::ParseError,
    model::{
        Atom, Attribute, Fact, Identifier, Literal, Operator, Rule, Span, Statement, Term, TermKind,
    },
    parser::{cursor::Parser, lexer::TokenKind},
};

impl Parser<'_> {
    /// Parses one statement, through its terminating ` .`.
    ///
    /// A statement is a directive, a rule or a fact.
    /// Attributes may come before a rule, and are an error before a directive or a fact.
    pub(super) fn statement(&mut self) -> Result<Statement, Diagnostic> {
        let attributes = self.attributes()?;

        if self.at(&TokenKind::At) {
            if let Some(attribute) = attributes.first() {
                return Err(ParseError::MisplacedAttribute.at(attribute.span));
            }
            return Ok(Statement::Directive(self.directive()?));
        }

        let start = self.peek_span();
        let head = self.separated(Self::atom)?;

        if self.eat(&TokenKind::ArrowLeft).is_some() {
            let body = self.separated(Self::literal)?;
            self.expect(&TokenKind::Dot, "`.` at the end of the rule")?;
            return Ok(Statement::Rule(Rule {
                attributes,
                head,
                body,
                span: Some(self.span_from(start)),
            }));
        }

        if let Some(attribute) = attributes.first() {
            return Err(ParseError::MisplacedAttribute.at(attribute.span));
        }
        let [atom] = <[Atom; 1]>::try_from(head)
            .map_err(|_| ParseError::FactWithSeveralHeads.at(Some(self.span_from(start))))?;
        self.expect(&TokenKind::Dot, "`.` at the end of the fact")?;
        Ok(Statement::Fact(Fact {
            predicate: atom.predicate,
            terms: atom.terms,
            labels: atom.labels,
            span: Some(self.span_from(start)),
        }))
    }

    /// Parses the attributes `#[name(…)]` before a statement, if there are any.
    fn attributes(&mut self) -> Result<Vec<Attribute>, Diagnostic> {
        let mut attributes = Vec::new();
        while self.at(&TokenKind::AttributeOpen) {
            let start = self.peek_span();
            self.advance();
            let name = self.identifier()?;
            let arguments = self
                .after(&TokenKind::ParenOpen, |parser| {
                    parser.list(
                        &TokenKind::ParenClose,
                        "the closing bracket of the argument list",
                        Self::term,
                    )
                })?
                .unwrap_or_default();
            self.expect(&TokenKind::BracketClose, "`]` closing the attribute")?;
            attributes.push(Attribute {
                name,
                arguments,
                span: Some(self.span_from(start)),
            });
        }
        Ok(attributes)
    }

    /// Parses an atom, such as `parent(?x, ?y)`.
    pub(super) fn atom(&mut self) -> Result<Atom, Diagnostic> {
        let start = self.peek_span();
        let term = self.term()?;
        self.atom_from(term, start)
    }

    /// Parses one literal of a rule's body,
    /// which is an atom, a negated atom `~atom`, or an operation, such as `?x > 1`.
    pub(super) fn literal(&mut self) -> Result<Literal, Diagnostic> {
        if self.eat(&TokenKind::Tilde).is_some() {
            return Ok(Literal::Negative(self.atom()?));
        }
        let start = self.peek_span();
        let term = self.term()?;
        match term.kind {
            TermKind::Operation(operation) => Ok(Literal::Operation(operation)),
            _ => Ok(Literal::Positive(self.atom_from(term, start)?)),
        }
    }

    /// Converts a parsed term into an atom, or returns an error if the term is not an atom.
    ///
    /// An atom is parsed as a term first, since it looks like a function term.
    /// `start` is the span of the term's first token.
    fn atom_from(&self, term: Term, start: Span) -> Result<Atom, Diagnostic> {
        match term.kind {
            TermKind::Function {
                name,
                arguments,
                labels,
            } => Ok(Atom {
                predicate: name,
                terms: arguments,
                labels,
                span: Some(self.span_from(start)),
            }),
            TermKind::Operation(operation) => {
                let span = self.span_from(start);
                let written = self.span_text(start);
                // Builtin names are reserved, so a predicate with the name of a builtin
                // is parsed as a call of the builtin. The error points that out.
                if Operator::from_name(written) == Some(operation.operator) {
                    return Err(ParseError::ExpectedAtom
                        .diagnostic()
                        .with_label(
                            span,
                            format!("`{written}` is a builtin, so this is a call, not an atom"),
                        )
                        .with_hint(
                            "rename the predicate, since builtin names are reserved \
                             (case-insensitively)",
                        ));
                }
                Err(ParseError::ExpectedAtom
                    .diagnostic()
                    .with_label(span, "this is an expression, not an atom"))
            }
            // A name alone is a constant, and an atom without terms is written with
            // empty parentheses.
            TermKind::Constant(Identifier::Bare(name)) => Err(ParseError::ExpectedAtom
                .at(Some(self.span_from(start)))
                .with_hint(format!("write `{name}()` for an atom without terms"))),
            _ => Err(ParseError::ExpectedAtom.at(Some(self.span_from(start)))),
        }
    }
}
