//! This module contains basic data structures for tracing the origins of derived facts.

use std::collections::HashMap;

use ascii_tree::write_tree;

use crate::model::{chase_model::ChaseFact, Constant, PrimitiveTerm, Rule, Term, Variable};

/// Identifies an atom within the head of a rule
#[derive(Debug, Clone)]
pub struct RuleApplication {
    /// The rule, that was applied.
    pub rule: Rule,
    /// The assignement, that was found.
    pub assignment: HashMap<Variable, Constant>,
    _position: usize,
}

impl RuleApplication {
    /// Create new [`RuleApplication`].
    pub fn new(rule: Rule, assignment: HashMap<Variable, Constant>, _position: usize) -> Self {
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
        rule_applied.apply_assignment(
            &self
                .assignment
                .iter()
                .map(|(variable, constant)| {
                    (
                        variable.clone(),
                        Term::Primitive(PrimitiveTerm::Constant(constant.clone())),
                    )
                })
                .collect(),
        );

        rule_applied.fmt(f)
    }
}

/// Represents the derivation tree for one derived fact
#[derive(Debug, Clone)]
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

    macro_rules! variable_assignment {
        ($($k:expr => $v:expr),*) => {
            [$(($k, $v)),*]
                .into_iter()
                .map(|(k, v)| {
                    (
                        Variable::Universal(Identifier(k.to_string())),
                        Constant::Abstract(Identifier(v.to_string())),
                    )
                })
                .collect()
        };
    }

    macro_rules! atom_term {
        (? $var:expr ) => {
            Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(Identifier(
                $var.to_string(),
            ))))
        };
    }

    macro_rules! atom {
        ( $pred:expr; $( $marker:tt $t:tt ),* ) => {
            Atom::new(Identifier($pred.to_string()), vec![ $( atom_term!( $marker $t ) ),* ])
        };
    }

    #[test]
    fn print_trace() {
        // P(?x, ?y) :- Q(?y, ?x) .
        let rule_1 = Rule::new(
            vec![atom!("P"; ?"x", ?"y")],
            vec![Literal::Positive(atom!("Q"; ?"y", ?"x"))],
            vec![],
        );
        let rule_1_assignment = variable_assignment!("x" => "b", "y" => "a");

        // S(?x) :- T(?x) .
        let rule_2 = Rule::new(
            vec![atom!("S"; ?"x")],
            vec![Literal::Positive(atom!("T"; ?"x"))],
            vec![],
        );

        let rule_2_assignment = variable_assignment!("x" => "a");

        // R(?x, ?y) :- P(?x, ?y), S(?y) .
        let rule_3 = Rule::new(
            vec![atom!("R"; ?"x", ?"y")],
            vec![
                Literal::Positive(atom!("P"; ?"x", ?"y")),
                Literal::Positive(atom!("S"; ?"y")),
            ],
            vec![],
        );
        let rule_3_assignment: HashMap<_, _> = variable_assignment!("x" => "b", "y" => "a");

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
