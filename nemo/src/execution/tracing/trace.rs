//! This module contains basic data structures for tracing the origins of derived facts.

use std::collections::HashMap;

use crate::model::{Atom, Rule, Term, Variable};

/// Identifies an atom within the head of a rule
#[derive(Debug)]
struct RulePosition {
    rule: Rule,
    position: usize,
}

impl RulePosition {
    /// Create new [`RulePosition`].
    pub fn new(rule: Rule, position: usize) -> Self {
        debug_assert!(position < rule.head().len());

        Self { rule, position }
    }
}

impl std::fmt::Display for RulePosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.rule.fmt(f)
    }
}

#[derive(Debug)]
enum FactOrigin {
    /// Fact was derived from the given rule
    Rule(RulePosition),
    /// Fact was given as input
    Fact(Atom),
}

impl std::fmt::Display for FactOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FactOrigin::Rule(rule) => rule.fmt(f),
            FactOrigin::Fact(fact) => fact.fmt(f),
        }
    }
}

/// Represents the derivation tree for one derived fact
#[derive(Debug)]
pub struct ExecutionTrace {
    origin: FactOrigin,

    subtraces: Vec<ExecutionTrace>,
}

impl ExecutionTrace {
    /// Create a new leaf node in an [`ExecutionTree`].
    pub fn leaf(origin: FactOrigin) -> Self {
        ExecutionTrace {
            origin,
            subtraces: Vec::default(),
        }
    }

    /// Create a new node in an [`ExecutionTree`].
    pub fn node(origin: FactOrigin, subtraces: Vec<ExecutionTrace>) -> Self {
        ExecutionTrace { origin, subtraces }
    }

    fn assign_variables(rule: &Rule, assignment: &VariableAssignment) -> Rule {
        todo!()
    }

    // fn ascii_tree_recursive()

    pub fn ascii_tree(&self) -> ascii_tree::Tree {
        if self.subtraces.is_empty() {
            ascii_tree::Tree::Leaf(vec![])
        }
    }
}

#[derive(Debug)]
struct VariableAssignment(HashMap<Variable, Term>);
