//! Aggregates and their scopes.

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
    /// Parses an aggregate: `#count()`, `#count(per ?w, …)`, `#kind(?v)`, or `#kind(?v per ?w, …)`,
    /// and the legacy comma list `#kind(?v, ?w…)`, which means the `per` form.
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

        // A count has no value apart from its scope, so it may name the
        // scope alone. The word is a keyword only when a variable follows it.
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

    /// Parses the scope of an aggregate without a value:
    /// none for `#count()`, which counts the body matches,
    /// and `?x, ?y` for `#count(per ?x, ?y)`, which counts their distinct combinations.
    ///
    /// Every aggregate other than a count needs a value.
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

    /// Parses the terms an aggregate aggregates: `?v`, or a map's `?k = ?v`.
    ///
    /// Returns the input and a map's value.
    fn aggregated_terms(
        &mut self,
        kind: AggregateKind,
    ) -> Result<(Term, Option<Box<Term>>), Diagnostic> {
        if kind == AggregateKind::Map {
            // `#map(?k = ?v)` mirrors the map value syntax, so its `=`
            // separates rather than compares.
            let key = self.plain_term()?;
            self.expect(&TokenKind::Equal, "`=` between the map key and its value")?;
            Ok((key, Some(Box::new(self.plain_term()?))))
        } else {
            Ok((self.term()?, None))
        }
    }

    /// Parses the scope after an aggregate's value: `per ?w, …`, the legacy `, ?w…`, or nothing.
    ///
    /// Returns the scope variables and whether they were written as the legacy list.
    fn scope(&mut self) -> Result<(Vec<Variable>, bool), Diagnostic> {
        let keyword = self.at_word("per");
        if !keyword && !self.at(&TokenKind::Comma) {
            return Ok((Vec::new(), false));
        }
        // The `per` keyword or the legacy comma, either way one token
        // before the scope list.
        self.advance();
        Ok((self.scope_variables()?, !keyword))
    }

    /// Parses the variable list of an aggregate's `per` scope.
    fn scope_variables(&mut self) -> Result<Vec<Variable>, Diagnostic> {
        self.separated(|parser| parser.variable("a scope variable"))
    }
}
