//! Combinators that parse lists, optional items, variables and names,
//! built on the basic operations of the parser's cursor.
//!
//! Each decides what to parse by looking at the next token, and never backtracks.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::{Variable, VariableKind},
    parser::{cursor::Parser, lexer::TokenKind},
};

impl Parser<'_> {
    /// Parses a list of items separated by commas, possibly empty, through the token `close`.
    ///
    /// `closing` describes the `close` token in the error if it is missing.
    /// A comma after the last item is an error.
    pub(super) fn list<T>(
        &mut self,
        close: &TokenKind,
        closing: &str,
        item: impl FnMut(&mut Self) -> Result<T, Diagnostic>,
    ) -> Result<Vec<T>, Diagnostic> {
        let items = if self.at(close) {
            Vec::new()
        } else {
            self.separated(item)?
        };
        self.expect(close, closing)?;
        Ok(items)
    }

    /// Parses one or more items separated by commas: `item (, item)*`.
    pub(super) fn separated<T>(
        &mut self,
        mut item: impl FnMut(&mut Self) -> Result<T, Diagnostic>,
    ) -> Result<Vec<T>, Diagnostic> {
        let mut items = vec![item(self)?];
        while self.eat(&TokenKind::Comma).is_some() {
            items.push(item(self)?);
        }
        Ok(items)
    }

    /// Parses an item after the token `kind` if that token is next, and returns `None`
    /// otherwise.
    pub(super) fn after<T>(
        &mut self,
        kind: &TokenKind,
        item: impl FnOnce(&mut Self) -> Result<T, Diagnostic>,
    ) -> Result<Option<T>, Diagnostic> {
        self.eat(kind).map(|_| item(self)).transpose()
    }

    /// Consumes a variable of the given kind if one is next, and returns its name.
    pub(super) fn eat_variable(&mut self, kind: VariableKind) -> Option<String> {
        let token = self.eat(&TokenKind::Variable(kind))?;
        Some(self.marked_text(&token).to_owned())
    }

    /// Consumes a variable of the given kind and returns its name,
    /// or returns an error if no such variable is next.
    ///
    /// `what` describes the expected variable in the error.
    pub(super) fn expect_variable(
        &mut self,
        kind: VariableKind,
        what: &str,
    ) -> Result<String, Diagnostic> {
        self.eat_variable(kind).ok_or_else(|| self.unexpected(what))
    }

    /// Consumes and returns a variable of any kind,
    /// or returns an error if no variable is next.
    pub(super) fn variable(&mut self, what: &str) -> Result<Variable, Diagnostic> {
        let &TokenKind::Variable(kind) = self.peek() else {
            return Err(self.unexpected(what));
        };
        let token = self.advance();
        Ok(Variable {
            kind,
            name: self.marked_text(&token).to_owned(),
        })
    }

    /// Consumes a bare name and returns its text, or returns an error if no bare name is next.
    pub(super) fn name(&mut self, what: &str) -> Result<String, Diagnostic> {
        let token = self.expect(&TokenKind::Ident, what)?;
        Ok(self.text(&token).to_owned())
    }
}
