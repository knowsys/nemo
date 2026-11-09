//! Parsing tuples, maps, and terms in parentheses.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::{Identifier, Span, Term, TermKind},
    parser::{cursor::Parser, lexer::TokenKind},
};

impl Parser<'_> {
    /// Parses the rest of a term in parentheses, after its `(` at `start`.
    ///
    /// `()` is the empty tuple, and a single term in parentheses is that term.
    /// A comma after the first element makes a tuple, so `(1,)` is a tuple of one element.
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

    /// Parses the rest of a map `{key = value, …}`, after its `{` at `start`.
    ///
    /// `label` is the name directly before the `{`, if there is one.
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
