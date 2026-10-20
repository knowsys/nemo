//! Literal terms: numbers, strings, and format strings.

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
    /// Builds the term of a format string: a concatenation whose text
    /// pieces are strings and whose embedded expressions wrap in STR.
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

    /// Returns the term of a number literal token, read with [Number::from_term_syntax].
    pub(super) fn number(&self, token: &Token) -> Result<Term, Diagnostic> {
        let number = Number::from_term_syntax(self.text(token))
            .ok_or_else(|| ParseError::MalformedNumber.at(Some(token.span)))?;
        Ok(Term {
            kind: TermKind::Number(number),
            span: Some(token.span),
        })
    }

    /// Folds a prefix minus: numeric literals negate in place, anything
    /// else becomes a negation operation.
    pub(super) fn negated(&self, operand: Term, start: Span) -> Term {
        match operand.kind {
            // The negation of a number is a number.
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

    /// Finishes a string literal: plain, language-tagged, or with an
    /// explicit datatype.
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
