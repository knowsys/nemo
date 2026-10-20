//! The combinators: lists, options, and token classes, built from the cursor's primitives.
//!
//! Each decides by peeking at the next token and never backtracks.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::{Variable, VariableKind},
    parser::{cursor::Parser, lexer::TokenKind},
};

impl Parser<'_> {
    /// Parses a comma-separated list of items, possibly empty,
    /// up to and including the closing token, which `closing` names for the message.
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

    /// Parses one or more comma-separated items: `item (, item)*`.
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

    /// Parses an item introduced by the given token, if that token is next: `[kind item]`.
    pub(super) fn after<T>(
        &mut self,
        kind: &TokenKind,
        item: impl FnOnce(&mut Self) -> Result<T, Diagnostic>,
    ) -> Result<Option<T>, Diagnostic> {
        self.eat(kind).map(|_| item(self)).transpose()
    }

    /// Consumes a variable of the given kind if one is next, returning its name.
    pub(super) fn eat_variable(&mut self, kind: VariableKind) -> Option<String> {
        let token = self.eat(&TokenKind::Variable(kind))?;
        Some(self.marked_text(&token).to_owned())
    }

    /// Consumes a variable of the given kind, requiring it, and returns its name.
    pub(super) fn expect_variable(
        &mut self,
        kind: VariableKind,
        what: &str,
    ) -> Result<String, Diagnostic> {
        self.eat_variable(kind).ok_or_else(|| self.unexpected(what))
    }

    /// Consumes a variable of any kind, requiring it.
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

    /// Consumes a bare name, requiring it, and returns its text.
    pub(super) fn name(&mut self, what: &str) -> Result<String, Diagnostic> {
        let token = self.expect(&TokenKind::Ident, what)?;
        Ok(self.text(&token).to_owned())
    }
}
