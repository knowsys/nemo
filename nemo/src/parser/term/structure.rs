//! Structured terms: tuples and grouping parentheses, and maps.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::{Identifier, Span, Term, TermKind},
    parser::{cursor::Parser, lexer::TokenKind},
};

impl Parser<'_> {
    /// Finishes a parenthesized term: grouping,
    /// or a tuple if a comma follows the first element, or nothing does (the empty tuple).
    pub(super) fn parenthesized(&mut self, start: Span) -> Result<Term, Diagnostic> {
        if self.eat(&TokenKind::ParenClose).is_some() {
            return Ok(self.spanned(start, TermKind::Tuple(Vec::new())));
        }
        let first = self.term()?;
        if self.eat(&TokenKind::ParenClose).is_some() {
            return Ok(first);
        }
        self.expect(&TokenKind::Comma, "`,` or `)` in the parentheses")?;
        let mut elements = vec![first];
        elements.extend(self.list(&TokenKind::ParenClose, "`)` closing the tuple", Self::term)?);
        Ok(self.spanned(start, TermKind::Tuple(elements)))
    }

    /// Finishes a map term `{key = value, …}`, labeled if a name touched the opening brace.
    pub(super) fn map(
        &mut self,
        start: Span,
        label: Option<Identifier>,
    ) -> Result<Term, Diagnostic> {
        let pairs = self.list(&TokenKind::BraceClose, "`}` closing the map", |parser| {
            let key = parser.plain_term()?;
            parser.expect(&TokenKind::Equal, "`=` between the map key and its value")?;
            let value = parser.plain_term()?;
            Ok((key, value))
        })?;
        Ok(self.spanned(start, TermKind::Map { label, pairs }))
    }
}
