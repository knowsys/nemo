//! This module contains basic data structures for tracing the origins of derived facts.

use ascii_tree::write_tree;

use crate::model::{Atom, Rule};

/// Identifies an atom within the head of a rule
#[derive(Debug)]
pub struct RulePosition {
    rule: Rule,
    _position: usize,
}

impl RulePosition {
    /// Create new [`RulePosition`].
    pub fn new(rule: Rule, _position: usize) -> Self {
        debug_assert!(_position < rule.head().len());

        Self { rule, _position }
    }
}

impl std::fmt::Display for RulePosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.rule.fmt(f)
    }
}

/// Represents the derivation tree for one derived fact
#[derive(Debug)]
pub enum ExecutionTrace {
    /// Fact was given as input
    Fact(Atom),
    /// Fact was derived from the given rule
    Rule(RulePosition, Vec<ExecutionTrace>),
}

impl ExecutionTrace {
    /// Create a new leaf node in an [`ExecutionTree`].
    pub fn leaf(fact: Atom) -> Self {
        ExecutionTrace::Fact(fact)
    }

    /// Create a new node in an [`ExecutionTree`].
    pub fn node(rule_position: RulePosition, subtraces: Vec<ExecutionTrace>) -> Self {
        ExecutionTrace::Rule(rule_position, subtraces)
    }

    /// Create an [`acii_tree::Tree`] representation.
    pub fn ascii_tree(&self) -> ascii_tree::Tree {
        match self {
            ExecutionTrace::Fact(fact) => ascii_tree::Tree::Leaf(vec![fact.to_string()]),
            ExecutionTrace::Rule(rule, subtraces) => {
                let subtrees = subtraces.iter().map(|t| t.ascii_tree()).collect();
                ascii_tree::Tree::Node(rule.to_string(), subtrees)
            }
        }
    }
}

impl std::fmt::Display for ExecutionTrace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write_tree(f, &self.ascii_tree())
    }
}

#[cfg(test)]
mod test {
    use crate::model::{Atom, Identifier, Literal, Rule, Term, TermTree};

    use super::{ExecutionTrace, RulePosition};

    #[test]
    fn print_trace() {
        // P(b, a) :- Q(a, b) .
        let rule_1 = Rule::new(
            vec![Atom::new(
                Identifier("P".to_string()),
                vec![
                    TermTree::leaf(Term::Constant(Identifier("b".to_string()))),
                    TermTree::leaf(Term::Constant(Identifier("a".to_string()))),
                ],
            )],
            vec![Literal::Positive(Atom::new(
                Identifier("Q".to_string()),
                vec![
                    TermTree::leaf(Term::Constant(Identifier("a".to_string()))),
                    TermTree::leaf(Term::Constant(Identifier("b".to_string()))),
                ],
            ))],
            vec![],
        );
        // S(a) :- T(a) .
        let rule_2 = Rule::new(
            vec![Atom::new(
                Identifier("S".to_string()),
                vec![TermTree::leaf(Term::Constant(Identifier("a".to_string())))],
            )],
            vec![Literal::Positive(Atom::new(
                Identifier("T".to_string()),
                vec![TermTree::leaf(Term::Constant(Identifier("a".to_string())))],
            ))],
            vec![],
        );
        // R(b, a) :- P(b, a), S(a) .
        let rule_3 = Rule::new(
            vec![Atom::new(
                Identifier("R".to_string()),
                vec![
                    TermTree::leaf(Term::Constant(Identifier("b".to_string()))),
                    TermTree::leaf(Term::Constant(Identifier("a".to_string()))),
                ],
            )],
            vec![
                Literal::Positive(Atom::new(
                    Identifier("P".to_string()),
                    vec![
                        TermTree::leaf(Term::Constant(Identifier("b".to_string()))),
                        TermTree::leaf(Term::Constant(Identifier("a".to_string()))),
                    ],
                )),
                Literal::Positive(Atom::new(
                    Identifier("S".to_string()),
                    vec![TermTree::leaf(Term::Constant(Identifier("a".to_string())))],
                )),
            ],
            vec![],
        );

        let q_ab = Atom::new(
            Identifier("Q".to_string()),
            vec![
                TermTree::leaf(Term::Constant(Identifier("a".to_string()))),
                TermTree::leaf(Term::Constant(Identifier("b".to_string()))),
            ],
        );

        let s_a = Atom::new(
            Identifier("S".to_string()),
            vec![TermTree::leaf(Term::Constant(Identifier("a".to_string())))],
        );

        let trace = ExecutionTrace::node(
            RulePosition::new(rule_3, 0),
            vec![
                ExecutionTrace::node(
                    RulePosition::new(rule_1, 0),
                    vec![ExecutionTrace::leaf(q_ab)],
                ),
                ExecutionTrace::node(
                    RulePosition::new(rule_2, 0),
                    vec![ExecutionTrace::leaf(s_a)],
                ),
            ],
        );

        let trace_string = r#" R(b, a) :- P(b, a), S(a) .
 ├─ P(b, a) :- Q(a, b) .
 │  └─ Q(a, b)
 └─ S(a) :- T(a) .
    └─ S(a)
"#;

        assert_eq!(trace.to_string(), trace_string.to_string())
    }
}
