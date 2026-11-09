//! Parsing literal terms: numbers, strings and format strings.

use nemo_physical::{diagnostics::Diagnostic, value::Number};

use crate::{
    diagnostics::ParseError,
    model::{Operation, Operator, Span, Term, TermKind},
    parser::{
        cursor::Parser,
        lexer::{FStringPart, Token, TokenKind},
    },
};

impl Parser<'_> {
    /// Returns the term of a format string with the given parts, which starts at `start`.
    ///
    /// The term is a [Concat](Operator::Concat) of the parts.
    /// In it, a text part is a string,
    /// and an embedded expression is converted to a string with [Str](Operator::Str).
    pub(super) fn format_string_term(
        &self,
        parts: Vec<FStringPart>,
        start: Span,
    ) -> Result<Term, Diagnostic> {
        let mut arguments = Vec::with_capacity(parts.len());
        for part in parts {
            arguments.push(match part {
                FStringPart::Text(text) => Term {
                    kind: TermKind::String(text),
                    span: Some(start),
                },
                FStringPart::Expression(region) => Term::from(Operation {
                    operator: Operator::Str,
                    arguments: vec![self.embedded(region)?],
                    span: Some(region),
                }),
            });
        }
        Ok(Term::from(Operation {
            operator: Operator::Concat,
            arguments,
            span: Some(start),
        }))
    }

    /// Returns the term of a number literal token, which [Number::from_term_syntax] parses,
    /// or an error if the literal is malformed.
    pub(super) fn number(&self, token: &Token) -> Result<Term, Diagnostic> {
        let number = Number::from_term_syntax(self.text(token))
            .ok_or_else(|| ParseError::MalformedNumber.at(Some(token.span)))?;
        Ok(Term {
            kind: TermKind::Number(number),
            span: Some(token.span),
        })
    }

    /// Returns the negation of `operand`, whose prefix minus is at `start`.
    ///
    /// The negation of a number is the negated number,
    /// and the negation of any other term is a [Negate](Operator::Negate) operation.
    pub(super) fn negated(&self, operand: Term, start: Span) -> Term {
        match operand.kind {
            TermKind::Number(value) => self.spanned(
                start,
                Term::value(&-value).expect("a number has a term").kind,
            ),
            kind => Term::from(Operation {
                operator: Operator::Negate,
                arguments: vec![Term {
                    kind,
                    span: operand.span,
                }],
                span: Some(self.span_from(start)),
            }),
        }
    }

    /// Parses the rest of a string literal with the given value, which starts at `start`.
    ///
    /// The literal is a plain string, a language string such as `"chat"@fr`,
    /// or a literal with a datatype, such as `"5"^^xsd:integer`.
    /// The language tag is converted to lowercase.
    pub(super) fn string_term(&mut self, value: String, start: Span) -> Result<Term, Diagnostic> {
        if self.at(&TokenKind::LanguageTag) {
            let token = self.advance();
            let language = self.marked_text(&token).to_lowercase();
            return Ok(self.spanned(
                start,
                TermKind::LanguageString {
                    text: value,
                    language,
                },
            ));
        }
        if self.eat(&TokenKind::Carets).is_some() {
            let datatype = self.identifier()?;
            return Ok(self.spanned(
                start,
                TermKind::RdfLiteral {
                    lexical: value,
                    datatype,
                },
            ));
        }
        Ok(self.spanned(start, TermKind::String(value)))
    }
}
