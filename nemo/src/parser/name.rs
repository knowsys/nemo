//! The name grammar: bare, prefixed, and module-qualified names, IRIs, and module names.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::ParseError,
    model::{Identifier, ModuleName},
    parser::{cursor::Parser, lexer::TokenKind},
};

impl Parser<'_> {
    /// Parses a name: bare, prefixed (`pre:name`, adjacent), qualified
    /// (`alias::name`), or a full IRI.
    pub(super) fn identifier(&mut self) -> Result<Identifier, Diagnostic> {
        if self.at(&TokenKind::Iri) {
            let token = self.advance();
            return Ok(Identifier::Iri(self.iri_text(&token).to_owned()));
        }
        // `:name` resolves against the empty prefix.
        if self.at(&TokenKind::Colon) && self.peek_second() == &TokenKind::Ident {
            let colon = self.advance();
            if Self::adjacent(colon.span, self.peek_span()) {
                return Ok(Identifier::Prefixed {
                    prefix: String::new(),
                    name: self.name("a name")?,
                });
            }
            return Err(self.unexpected("a name directly after the `:`"));
        }
        let first = self.expect(&TokenKind::Ident, "a name")?;

        if self.at(&TokenKind::Colon)
            && Self::adjacent(first.span, self.peek_span())
            && self.peek_second() == &TokenKind::Ident
        {
            let colon = self.advance();
            if Self::adjacent(colon.span, self.peek_span()) {
                return Ok(Identifier::Prefixed {
                    prefix: self.text(&first).to_owned(),
                    name: self.name("a name")?,
                });
            }
            return Err(self.unexpected("a name directly after the `:`"));
        }

        if self.eat(&TokenKind::DoubleColon).is_some() {
            return Ok(Identifier::Qualified {
                module: self.text(&first).to_owned(),
                name: self.name("a name after `::`")?,
            });
        }

        Ok(Identifier::Bare(self.text(&first).to_owned()))
    }

    /// Parses a module name: `super` steps, then names, all separated by `::`.
    pub(super) fn module_name(&mut self) -> Result<ModuleName, Diagnostic> {
        let mut name = ModuleName {
            parents: 0,
            segments: Vec::new(),
        };
        loop {
            let what = if name.parents == 0 && name.segments.is_empty() {
                "the module's name, as `lib::tc` for `lib/tc.rls`"
            } else {
                "a name after `::`"
            };
            let token = self.expect(&TokenKind::Ident, what)?;
            let part = self.text(&token);
            if part == "super" {
                if !name.segments.is_empty() {
                    return Err(ParseError::SuperInsideModuleName.at(Some(token.span)));
                }
                name.parents += 1;
                self.expect(&TokenKind::DoubleColon, "`::` after `super`")?;
                continue;
            }
            name.segments.push(part.to_owned());
            if self.eat(&TokenKind::DoubleColon).is_none() {
                return Ok(name);
            }
        }
    }
}
