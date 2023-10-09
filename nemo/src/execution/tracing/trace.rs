//! This module contains basic data structures for tracing the origins of derived facts.

use ascii_tree::write_tree;

use crate::model::{chase_model::ChaseFact, Rule, VariableAssignment};

/// Identifies an atom within the head of a rule
#[derive(Debug)]
pub struct RuleApplication {
    rule: Rule,
    assignment: VariableAssignment,
    _position: usize,
}

impl RuleApplication {
    /// Create new [`RuleApplication`].
    pub fn new(rule: Rule, assignment: VariableAssignment, _position: usize) -> Self {
        debug_assert!(_position < rule.head().len());

        Self {
            rule,
            assignment,
            _position,
        }
    }
}

impl std::fmt::Display for RuleApplication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut rule_applied = self.rule.clone();
        rule_applied.apply_assignment(&self.assignment);

        rule_applied.fmt(f)
    }
}

/// Represents the derivation tree for one derived fact
#[derive(Debug)]
pub enum ExecutionTrace {
    /// Fact was given as input
    Fact(ChaseFact),
    /// Fact was derived from the given rule
    Rule(RuleApplication, Vec<ExecutionTrace>),
}

impl ExecutionTrace {
    /// Create a new leaf node in an [`ExecutionTrace`].
    pub fn leaf(fact: ChaseFact) -> Self {
        ExecutionTrace::Fact(fact)
    }

    /// Create a new node in an [`ExecutionTrace`].
    pub fn node(rule_position: RuleApplication, subtraces: Vec<ExecutionTrace>) -> Self {
        ExecutionTrace::Rule(rule_position, subtraces)
    }

    /// Create an ascii tree representation.
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
    use std::collections::HashMap;

    use crate::model::{
        chase_model::ChaseFact, Atom, Constant, Identifier, Literal, PrimitiveTerm, Rule, Term,
        Variable,
    };

    use super::{ExecutionTrace, RuleApplication};

    #[test]
    fn print_trace() {
        // P(?x, ?y) :- Q(?y, ?x) .
        let rule_1 = Rule::new(
            vec![Atom::new(
                Identifier("P".to_string()),
                vec![
                    Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(Identifier(
                        "x".to_string(),
                    )))),
                    Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(Identifier(
                        "y".to_string(),
                    )))),
                ],
            )],
            vec![Literal::Positive(Atom::new(
                Identifier("Q".to_string()),
                vec![
                    Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(Identifier(
                        "y".to_string(),
                    )))),
                    Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(Identifier(
                        "x".to_string(),
                    )))),
                ],
            ))],
            vec![],
        );
        let mut rule_1_assignment = HashMap::default();
        rule_1_assignment.insert(
            Variable::Universal(Identifier("x".to_string())),
            Term::Primitive(PrimitiveTerm::Constant(Constant::Abstract(Identifier(
                "b".to_string(),
            )))),
        );
        rule_1_assignment.insert(
            Variable::Universal(Identifier("y".to_string())),
            Term::Primitive(PrimitiveTerm::Constant(Constant::Abstract(Identifier(
                "a".to_string(),
            )))),
        );

        // S(?x) :- T(?x) .
        let rule_2 = Rule::new(
            vec![Atom::new(
                Identifier("S".to_string()),
                vec![Term::Primitive(PrimitiveTerm::Variable(
                    Variable::Universal(Identifier("x".to_string())),
                ))],
            )],
            vec![Literal::Positive(Atom::new(
                Identifier("T".to_string()),
                vec![Term::Primitive(PrimitiveTerm::Variable(
                    Variable::Universal(Identifier("x".to_string())),
                ))],
            ))],
            vec![],
        );
        let mut rule_2_assignment = HashMap::default();
        rule_2_assignment.insert(
            Variable::Universal(Identifier("x".to_string())),
            Term::Primitive(PrimitiveTerm::Constant(Constant::Abstract(Identifier(
                "a".to_string(),
            )))),
        );

        // R(?x, ?y) :- P(?x, ?y), S(?y) .
        let rule_3 = Rule::new(
            vec![Atom::new(
                Identifier("R".to_string()),
                vec![
                    Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(Identifier(
                        "x".to_string(),
                    )))),
                    Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(Identifier(
                        "y".to_string(),
                    )))),
                ],
            )],
            vec![
                Literal::Positive(Atom::new(
                    Identifier("P".to_string()),
                    vec![
                        Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(Identifier(
                            "x".to_string(),
                        )))),
                        Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(Identifier(
                            "y".to_string(),
                        )))),
                    ],
                )),
                Literal::Positive(Atom::new(
                    Identifier("S".to_string()),
                    vec![Term::Primitive(PrimitiveTerm::Variable(
                        Variable::Universal(Identifier("y".to_string())),
                    ))],
                )),
            ],
            vec![],
        );
        let mut rule_3_assignment = HashMap::default();
        rule_3_assignment.insert(
            Variable::Universal(Identifier("x".to_string())),
            Term::Primitive(PrimitiveTerm::Constant(Constant::Abstract(Identifier(
                "b".to_string(),
            )))),
        );
        rule_3_assignment.insert(
            Variable::Universal(Identifier("y".to_string())),
            Term::Primitive(PrimitiveTerm::Constant(Constant::Abstract(Identifier(
                "a".to_string(),
            )))),
        );

        let q_ab = ChaseFact::new(
            Identifier("Q".to_string()),
            vec![
                Constant::Abstract(Identifier("a".to_string())),
                Constant::Abstract(Identifier("b".to_string())),
            ],
        );

        let s_a = ChaseFact::new(
            Identifier("S".to_string()),
            vec![Constant::Abstract(Identifier("a".to_string()))],
        );

        let trace = ExecutionTrace::node(
            RuleApplication::new(rule_3, rule_3_assignment, 0),
            vec![
                ExecutionTrace::node(
                    RuleApplication::new(rule_1, rule_1_assignment, 0),
                    vec![ExecutionTrace::leaf(q_ab)],
                ),
                ExecutionTrace::node(
                    RuleApplication::new(rule_2, rule_2_assignment, 0),
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
