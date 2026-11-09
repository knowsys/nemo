//! Parsing aggregates and their scopes.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::ParseError,
    model::{Aggregate, AggregateKind, Span, Term, Variable},
    parser::{
        cursor::Parser,
        lexer::{Token, TokenKind},
    },
};

impl Parser<'_> {
    /// Parses an aggregate whose `#name` is `token` and which starts at `start`.
    ///
    /// The forms are `#count()`, `#count(per ?w, …)`, `#kind(?v)` and `#kind(?v per ?w, …)`.
    /// The deprecated form `#kind(?v, ?w, …)` means the same as `#kind(?v per ?w, …)`.
    pub(super) fn aggregate(&mut self, token: &Token, start: Span) -> Result<Term, Diagnostic> {
        let name = self.marked_text(token);
        let Some(kind) = AggregateKind::from_name(name) else {
            let known: Vec<String> = AggregateKind::ALL
                .iter()
                .map(|kind| format!("`{kind}`"))
                .collect();
            return Err(ParseError::UnknownAggregate {
                name: name.to_owned(),
                known: known.join(", "),
            }
            .at(Some(token.span)));
        };
        self.expect(&TokenKind::ParenOpen, "`(` after the aggregate name")?;

        // A count needs no value, so it may have a scope alone.
        // `per` is a keyword only if a variable follows it.
        let alone = self.at(&TokenKind::ParenClose)
            || (self.at_word("per") && matches!(self.peek_second(), TokenKind::Variable(_)));
        let (input, value, (scope, legacy_list)) = if alone {
            (None, None, (self.scope_alone(kind, start)?, false))
        } else {
            let (input, value) = self.aggregated_terms(kind)?;
            (Some(Box::new(input)), value, self.scope()?)
        };

        self.expect(&TokenKind::ParenClose, "`)` closing the aggregate")?;
        Ok(Term::from(Aggregate {
            kind,
            input,
            value,
            scope,
            legacy_list,
            span: Some(self.span_from(start)),
        }))
    }

    /// Parses the scope of an aggregate without a value.
    ///
    /// The scope is empty for `#count()`, which counts the body matches,
    /// and `?x, ?y` for `#count(per ?x, ?y)`, which counts their distinct combinations.
    /// Every aggregate other than a count needs a value, so for any other `kind`
    /// this returns an error, which spans the aggregate from `start`.
    fn scope_alone(
        &mut self,
        kind: AggregateKind,
        start: Span,
    ) -> Result<Vec<Variable>, Diagnostic> {
        if self.at(&TokenKind::ParenClose) {
            if kind != AggregateKind::Count {
                return Err(ParseError::EmptyAggregate {
                    kind: kind.to_string(),
                }
                .at(Some(self.span_from(start))));
            }
            return Ok(Vec::new());
        }
        if kind != AggregateKind::Count {
            return Err(ParseError::ScopeWithoutValue {
                kind: kind.to_string(),
            }
            .at(Some(self.span_from(start))));
        }
        self.advance();
        self.scope_variables()
    }

    /// Parses the aggregated terms of an aggregate, which are `?v`, or `?k = ?v` for a `#map`.
    ///
    /// Returns the aggregate's input and, for a `#map`, its value.
    fn aggregated_terms(
        &mut self,
        kind: AggregateKind,
    ) -> Result<(Term, Option<Box<Term>>), Diagnostic> {
        if kind == AggregateKind::Map {
            // `#map(?k = ?v)` is written like a pair of a map,
            // so its `=` separates the key from the value and does not compare them.
            let key = self.plain_term()?;
            self.expect(&TokenKind::Equal, "`=` between the map key and its value")?;
            Ok((key, Some(Box::new(self.plain_term()?))))
        } else {
            Ok((self.term()?, None))
        }
    }

    /// Parses the scope after an aggregate's value,
    /// which is `per ?w, …`, the deprecated `, ?w, …`, or nothing.
    ///
    /// Returns the scope variables, and whether they were written as a comma list.
    fn scope(&mut self) -> Result<(Vec<Variable>, bool), Diagnostic> {
        let keyword = self.at_word("per");
        if !keyword && !self.at(&TokenKind::Comma) {
            return Ok((Vec::new(), false));
        }
        // Both `per` and the comma are one token before the scope variables.
        self.advance();
        Ok((self.scope_variables()?, !keyword))
    }

    /// Parses the variables of an aggregate's scope, separated by commas.
    fn scope_variables(&mut self) -> Result<Vec<Variable>, Diagnostic> {
        self.separated(|parser| parser.variable("a scope variable"))
    }
}
