//! The term grammar: Pratt parsing for expressions.

mod aggregate;
mod call;
mod literal;
mod structure;

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::{Operation, Operator, Span, Term, TermKind, Variable},
    parser::{cursor::Parser, lexer::TokenKind},
};

/// The binding power a plain term starts from: arithmetic, but no comparison,
/// whose `=` separates map pairs and options instead.
const PLAIN: u8 = power(Operator::Add);

/// The binding power of a prefix minus, tighter than every infix operator.
const PREFIX: u8 = power(Operator::Multiply) + 1;

/// Returns an infix operator's binding power, shared with the display
/// through [Operator::precedence].
const fn power(operator: Operator) -> u8 {
    match operator.precedence() {
        Some(power) => power,
        None => panic!("only infix operators have a binding power"),
    }
}

/// Returns the infix operator a token starts, with its binding power.
fn binding_power(kind: &TokenKind) -> Option<(Operator, u8)> {
    let operator = match kind {
        TokenKind::Equal => Operator::Equal,
        TokenKind::Unequal => Operator::Unequal,
        TokenKind::ValueEqual => Operator::ValueEqual,
        TokenKind::ValueUnequal => Operator::ValueUnequal,
        TokenKind::Less => Operator::Less,
        TokenKind::LessEq => Operator::LessEq,
        TokenKind::Greater => Operator::Greater,
        TokenKind::GreaterEq => Operator::GreaterEq,
        TokenKind::Plus => Operator::Add,
        TokenKind::Minus => Operator::Subtract,
        TokenKind::Star => Operator::Multiply,
        TokenKind::Slash => Operator::Divide,
        _ => return None,
    };
    Some((operator, power(operator)))
}

impl Parser<'_> {
    /// Parses a full term (expression).
    pub(super) fn term(&mut self) -> Result<Term, Diagnostic> {
        self.term_with_power(0)
    }

    /// Parses a term without comparison operators,
    /// the form map pairs and option lists use, whose `=` separates rather than compares.
    pub(super) fn plain_term(&mut self) -> Result<Term, Diagnostic> {
        self.term_with_power(PLAIN)
    }

    /// Parses a term whose operators bind at least as tightly as `minimum`.
    ///
    /// This is the Pratt loop: every token is consumed exactly once.
    fn term_with_power(&mut self, minimum: u8) -> Result<Term, Diagnostic> {
        self.nested(|parser| parser.term_at(minimum))
    }

    /// Runs the Pratt loop one level of nesting in, returning the term it parses.
    fn term_at(&mut self, minimum: u8) -> Result<Term, Diagnostic> {
        let start = self.peek_span();
        let mut left = self.primary()?;

        // Postfix component access binds tighter than every infix
        // operator (`?t[0] + 1` indexes first) and chains left to
        // right: `?t[1][0]` reads element 0 of element 1.
        while self.eat(&TokenKind::BracketOpen).is_some() {
            let index = self.term()?;
            self.expect(&TokenKind::BracketClose, "`]` closing the component access")?;
            left = Term::from(Operation {
                operator: Operator::Get,
                arguments: vec![left, index],
                span: Some(self.span_from(start)),
            });
        }

        while let Some((operator, power)) = binding_power(self.peek()) {
            if power < minimum {
                break;
            }
            self.advance();
            let right = self.term_with_power(power + 1)?;
            left = Term::from(Operation {
                operator,
                arguments: vec![left, right],
                span: Some(self.span_from(start)),
            });
        }
        Ok(left)
    }

    /// Parses one primary term: a constant, variable, call, aggregate,
    /// tuple, map, parenthesized term, prefixed negation, or signed
    /// number.
    fn primary(&mut self) -> Result<Term, Diagnostic> {
        let start = self.peek_span();
        match self.peek().clone() {
            TokenKind::Variable(_) => {
                let variable = self.variable("a variable")?;
                Ok(self.spanned(start, TermKind::Variable(variable)))
            }
            TokenKind::Number => {
                let token = self.advance();
                self.number(&token)
            }
            TokenKind::Minus => {
                self.advance();
                let operand = self.term_with_power(PREFIX)?;
                Ok(self.negated(operand, start))
            }
            // A plus is only ever the sign of a number literal.
            TokenKind::Plus => {
                self.advance();
                let token = self.expect(&TokenKind::Number, "a number after the `+` sign")?;
                let number = self.number(&token)?;
                Ok(self.spanned(start, number.kind))
            }
            TokenKind::String(value) => {
                self.advance();
                self.string_term(value, start)
            }
            TokenKind::FString(parts) => {
                self.advance();
                self.format_string_term(parts, start)
            }
            TokenKind::BlankNode => {
                let token = self.advance();
                let label = self.blank_label(&token).to_owned();
                Ok(self.spanned(start, TermKind::BlankNode(label)))
            }
            TokenKind::Iri | TokenKind::Ident | TokenKind::Colon => {
                // A lone `_` is an anonymous variable, never a name.
                if self.at_word("_") {
                    self.advance();
                    return Ok(self.spanned(start, TermKind::Variable(Variable::anonymous())));
                }
                let identifier = self.identifier()?;
                self.applied(identifier, start)
            }
            TokenKind::Hash => {
                let token = self.advance();
                self.aggregate(&token, start)
            }
            TokenKind::ParenOpen => {
                self.advance();
                self.parenthesized(start)
            }
            TokenKind::BraceOpen => {
                self.advance();
                self.map(start, None)
            }
            _ => Err(self.unexpected("a term")),
        }
    }

    /// Returns a term of the given kind spanning from `start` through the last consumed token.
    fn spanned(&self, start: Span, kind: TermKind) -> Term {
        Term {
            kind,
            span: Some(self.span_from(start)),
        }
    }
}
