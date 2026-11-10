//! Programs: sequences of statements.

use std::fmt::{self, Display, Formatter};

use crate::model::{Literal, Span, Statement, span_ignoring_eq};

/// A Nemo program, which is a sequence of statements.
///
/// # Example
///
/// A program parsed from text equals the same program built from components,
/// since equality ignores spans:
///
/// ```
/// use nemo::{
///     atom,
///     model::{Fact, Program, Rule, Term},
/// };
///
/// let text = "parent(alice, bob) . ancestor(?x, ?y) :- parent(?x, ?y) .";
/// let (parsed, diagnostics) = Program::parse(text);
/// assert!(diagnostics.is_empty());
///
/// let built = Program::builder()
///     .fact(Fact::new(
///         "parent",
///         vec![Term::constant("alice"), Term::constant("bob")],
///     ))
///     .rule(
///         Rule::builder()
///             .head(atom!(ancestor(?x, ?y)))
///             .body(atom!(parent(?x, ?y)))
///             .build(),
///     )
///     .build();
/// assert_eq!(parsed, built);
/// assert_eq!(built.to_string(), text.replace(" . ", " .\n"));
/// ```
#[derive(Debug, Clone, Default)]
pub struct Program {
    /// The statements, in source order.
    pub statements: Vec<Statement>,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(Program: statements);

impl Program {
    /// Creates a program from its statements, without a source position.
    pub fn new(statements: Vec<Statement>) -> Self {
        Self {
            statements,
            span: None,
        }
    }

    /// Returns the predicates of the program's facts and of the atoms of its rules,
    /// each with its arity, sorted and without repeats.
    ///
    /// Each predicate is named as it is written, without resolving prefixes.
    /// The predicates of directives are left out.
    /// A predicate written with two different arities appears once for each.
    pub fn predicates(&self) -> Vec<(String, usize)> {
        let mut predicates = Vec::new();
        for statement in &self.statements {
            match statement {
                Statement::Fact(fact) => {
                    predicates.push((fact.predicate.to_string(), fact.terms.len()));
                }
                Statement::Rule(rule) => {
                    let body = rule.body.iter().filter_map(|literal| match literal {
                        Literal::Positive(atom) | Literal::Negative(atom) => Some(atom),
                        Literal::Operation(_) => None,
                    });
                    for atom in rule.head.iter().chain(body) {
                        predicates.push((atom.predicate.to_string(), atom.terms.len()));
                    }
                }
                Statement::Directive(_) => {}
            }
        }
        predicates.sort();
        predicates.dedup();
        predicates
    }
}

impl Display for Program {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for (index, statement) in self.statements.iter().enumerate() {
            if index > 0 {
                f.write_str("\n")?;
            }
            write!(f, "{statement}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The predicates are those of facts and of the atoms of rules, negated ones included,
    /// and the predicates of directives are left out.
    #[test]
    fn predicates_are_those_the_atoms_name() {
        let (program, diagnostics) = Program::parse(
            "@output reach .\nedge(1, 2) .\n\
             reach(?x, ?y) :- edge(?x, ?y), ~blocked(?x), ?x < ?y .\n",
        );
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        assert_eq!(
            program.predicates(),
            vec![
                (String::from("blocked"), 1),
                (String::from("edge"), 2),
                (String::from("reach"), 2),
            ]
        );
    }
}
