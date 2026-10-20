//! Names in term position: builtin calls, function terms, constants, and their argument lists.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::ParseError,
    model::{Identifier, Operation, Operator, Span, Term, TermKind},
    parser::{cursor::Parser, lexer::TokenKind},
};

impl Parser<'_> {
    /// Finishes a name in term position: a call (builtin or function
    /// term), a boolean keyword, or a plain constant.
    pub(super) fn applied(
        &mut self,
        identifier: Identifier,
        start: Span,
    ) -> Result<Term, Diagnostic> {
        if let Identifier::Bare(name) = &identifier
            && !self.at(&TokenKind::ParenOpen)
        {
            match name.as_str() {
                "true" => return Ok(self.spanned(start, TermKind::Boolean(true))),
                "false" => return Ok(self.spanned(start, TermKind::Boolean(false))),
                _ => {}
            }
        }

        // A name touching `{` labels a map. A space between them,
        // as in a directive's `csv { … }`, makes it an option block.
        if self.at(&TokenKind::BraceOpen) && Self::adjacent(self.previous_span(), self.peek_span())
        {
            if let Identifier::Bare(name) = &identifier
                && Operator::from_name(name).is_some()
            {
                return Err(ParseError::BuiltinLabel {
                    name: name.to_owned(),
                }
                .at(Some(self.span_from(start))));
            }
            self.advance();
            return self.map(start, Some(identifier));
        }

        if self.eat(&TokenKind::ParenOpen).is_none() {
            return Ok(self.spanned(start, TermKind::Constant(identifier)));
        }
        let (arguments, labels) = self.argument_list()?;

        // A bare name from the operator catalogue is a builtin call;
        // everything else is an uninterpreted function term.
        if let Identifier::Bare(name) = &identifier
            && let Some(operator) = Operator::from_name(name)
        {
            if labels.is_some() {
                return Err(ParseError::BuiltinNamedArguments {
                    name: name.to_owned(),
                }
                .at(Some(self.span_from(start))));
            }
            return Ok(Term::from(Operation {
                operator,
                arguments,
                span: Some(self.span_from(start)),
            }));
        }
        Ok(self.spanned(
            start,
            TermKind::Function {
                name: identifier,
                arguments,
                labels,
            },
        ))
    }

    /// Parses a call's arguments up to the closing `)`:
    /// terms, each optionally introduced by a `label:` naming the column it fills.
    ///
    /// Naming is all or nothing,
    /// since a half-named atom would leave the reader guessing which columns the rest mean.
    fn argument_list(&mut self) -> Result<(Vec<Term>, Option<Vec<String>>), Diagnostic> {
        let start = self.peek_span();
        let mut labels = Vec::new();
        let terms = self.list(
            &TokenKind::ParenClose,
            "the closing bracket of the argument list",
            |parser| {
                labels.push(parser.argument_label());
                parser.term()
            },
        )?;

        let named = labels.iter().flatten().count();
        if named == 0 {
            return Ok((terms, None));
        }
        if named < labels.len() {
            return Err(ParseError::MixedArguments.at(Some(self.span_from(start))));
        }
        Ok((terms, Some(labels.into_iter().flatten().collect())))
    }

    /// Takes an argument's `label:` if the next tokens are one:
    /// a name and a `:` that are not the start of a prefixed name.
    ///
    /// The parts of a prefixed name all touch, as in `xsd:integer`.
    fn argument_label(&mut self) -> Option<String> {
        let (name, colon, second) = (self.lookahead(0), self.lookahead(1), self.lookahead(2));
        if name.kind != TokenKind::Ident || colon.kind != TokenKind::Colon {
            return None;
        }
        if second.kind == TokenKind::Ident
            && Self::adjacent(name.span, colon.span)
            && Self::adjacent(colon.span, second.span)
        {
            return None;
        }
        let name = self.advance();
        self.advance();
        Some(self.text(&name).to_owned())
    }
}
