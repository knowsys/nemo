//! Parsing the terms that start with a name: builtin calls, function terms, labeled maps
//! and constants, with the argument lists of calls and function terms.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::ParseError,
    model::{Identifier, Operation, Operator, Span, Term, TermKind},
    parser::{cursor::Parser, lexer::TokenKind},
};

impl Parser<'_> {
    /// Parses the rest of a term that starts with the name `identifier` at `start`.
    ///
    /// The term is a call of a builtin, a function term, a map labeled with the name,
    /// a boolean `true` or `false`, or the name alone as a constant.
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

        // A name directly before `{` labels a map.
        // With a space between them, as in the `csv { … }` of a directive,
        // the braces enclose options instead, and the name is a constant.
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

        // A bare name of a builtin makes a call of the builtin.
        // Any other name makes a function term.
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

    /// Parses the arguments of a call through the closing `)`, and returns them with their
    /// labels.
    ///
    /// An argument is a term, optionally preceded by a label `name:`,
    /// which names the column that the argument fills.
    /// Either every argument has a label or none has, and the labels are `None` if none has.
    /// In an atom with only some labels,
    /// the reader would have to guess which columns the other arguments fill.
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
            let mut problem = ParseError::MixedArguments.at(Some(self.span_from(start)));
            // Among named arguments, an argument that is a prefixed name most likely lacks
            // the space that would make it a named argument too.
            let prefixed = labels
                .iter()
                .zip(&terms)
                .find_map(|(label, term)| match &term.kind {
                    TermKind::Constant(Identifier::Prefixed { prefix, name })
                        if label.is_none() && !name.is_empty() =>
                    {
                        Some((prefix, name))
                    }
                    _ => None,
                });
            if let Some((prefix, name)) = prefixed {
                problem = problem.with_hint(format!(
                    "write `{prefix}: {name}` with a space for a named argument"
                ));
            }
            return Err(problem);
        }
        Ok((terms, Some(labels.into_iter().flatten().collect())))
    }

    /// Consumes and returns the label of an argument,
    /// if the next tokens are a name and a `:` that do not start a prefixed name.
    ///
    /// In a prefixed name, the prefix directly touches its `:`.
    /// The `:` directly touches the local name, as in `xsd:integer` or `wd:42`,
    /// or ends the term, as in `ex:` or `ex: = ?x`.
    /// A `+` or `-` after the `:` starts the value of a named argument, as in `ex: +1`.
    fn argument_label(&mut self) -> Option<String> {
        let (name, colon, second) = (self.lookahead(0), self.lookahead(1), self.lookahead(2));
        if name.kind != TokenKind::Ident || colon.kind != TokenKind::Colon {
            return None;
        }
        let local_name = second.kind == TokenKind::Ident && Self::adjacent(colon.span, second.span);
        if Self::adjacent(name.span, colon.span) && (local_name || second.kind.ends_term()) {
            return None;
        }
        let name = self.advance();
        self.advance();
        Some(self.text(&name).to_owned())
    }
}
