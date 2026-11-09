//! Parsing names: bare, prefixed and module-qualified names, IRIs, and module names.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::ParseError,
    model::{Identifier, ModuleName},
    parser::{cursor::Parser, lexer::TokenKind},
};

impl Parser<'_> {
    /// Parses a name.
    ///
    /// A name is one of:
    /// - a bare name, such as `parent`;
    /// - a prefixed name `pre:name` without spaces, `:name` with the empty prefix,
    ///   or `pre:` alone at the end of a term;
    /// - a qualified name `alias::name`;
    /// - a full IRI in angle brackets.
    pub(super) fn identifier(&mut self) -> Result<Identifier, Diagnostic> {
        if self.at(&TokenKind::Iri) {
            let token = self.advance();
            return Ok(Identifier::Iri(self.iri_text(&token).into_owned()));
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

        if self.at(&TokenKind::Colon) && Self::adjacent(first.span, self.peek_span()) {
            if self.peek_second() == &TokenKind::Ident {
                let colon = self.advance();
                if Self::adjacent(colon.span, self.peek_span()) {
                    return Ok(Identifier::Prefixed {
                        prefix: self.text(&first).to_owned(),
                        name: self.name("a name")?,
                    });
                }
                return Err(self.unexpected("a name directly after the `:`"));
            }
            // A `:` at the end of a term leaves the local name empty,
            // so that `ex:` names the IRI of the prefix.
            // In an argument list, `ex: +1` is a named argument,
            // whose label is consumed before this point.
            // Anywhere else, a `+` or `-` after the `:` is an operator.
            let second = self.peek_second();
            if second.ends_term() || matches!(second, TokenKind::Plus | TokenKind::Minus) {
                self.advance();
                return Ok(Identifier::Prefixed {
                    prefix: self.text(&first).to_owned(),
                    name: String::new(),
                });
            }
        }

        if self.eat(&TokenKind::DoubleColon).is_some() {
            return Ok(Identifier::Qualified {
                module: self.text(&first).to_owned(),
                name: self.name("a name after `::`")?,
            });
        }

        Ok(Identifier::Bare(self.text(&first).to_owned()))
    }

    /// Parses a module name, such as `super::util::pairs`.
    ///
    /// A module name is any number of `super`, followed by one or more names,
    /// all separated by `::`.
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
