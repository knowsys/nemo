use nemo_physical::util::TaggedTree;

use super::{Aggregate, Identifier, PrimitiveValue};

/// Variable occuring in a rule
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub enum Variable {
    /// A universally quantified variable.
    Universal(Identifier),
    /// An existentially quantified variable.
    Existential(Identifier),
}

impl Variable {
    /// Return the name of the variable.
    pub fn name(&self) -> String {
        match self {
            Self::Universal(identifier) | Self::Existential(identifier) => identifier.name(),
        }
    }
}

impl std::fmt::Display for Variable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.name())
    }
}
/// Supported operations between terms.
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub enum TermOperation {
    /// Add two terms.
    Addition,
    /// Subtract one term from another.
    Subtraction,
    /// Multiply two terms
    Multiplication,
    /// Dividing terms.
    Division,
    /// Function term (e.g. operation or constructor).
    Function(Identifier),
}

/// A nested Term.
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct Term(pub(crate) TaggedTree<TermOperation, LeafTerm>);

/// A constant [`PrimitiveValue`], [`Variable`] or [`Aggregate`] operation.
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub enum LeafTerm {
    /// A (universal or existential) variable
    Variable(Variable),
    /// A constant of type primitive
    Constant(PrimitiveValue),
    /// An aggregate operation
    Aggregate(Aggregate),
}

impl LeafTerm {
    pub fn substitute_variable(&mut self, var: &Variable, subst: &Variable) {
        match self {
            LeafTerm::Variable(v) => {
                if v == var {
                    *v = subst.clone()
                }
            }
            LeafTerm::Constant(_) => {}
            LeafTerm::Aggregate(a) => a.substitute_variable(var, subst),
        }
    }
}

impl Term {
    pub fn variables(&self) -> impl Iterator<Item = &Variable> {
        self.0.leaves().filter_map(|t| match t {
            LeafTerm::Variable(v) => Some(v),
            // TODO: should Aggregates be considered as well?
            _ => None,
        })
    }

    pub fn variables_mut(&mut self) -> impl Iterator<Item = &mut Variable> {
        self.0.leaves_mut().filter_map(|t| match t {
            LeafTerm::Variable(v) => Some(v),
            // TODO: should Aggregates be considered as well?
            _ => None,
        })
    }

    pub fn aggregates(&self) -> impl Iterator<Item = &Aggregate> {
        self.0.leaves().filter_map(|t| match t {
            LeafTerm::Aggregate(a) => Some(a),
            _ => None,
        })
    }

    /// Substitutes all occurrences of `var` for `subst`.
    pub fn substitute_variable(&mut self, var: &Variable, subst: &Variable) {
        debug_assert!(matches!(var, Variable::Universal(_)));
        fn substitute(
            tree: &mut TaggedTree<TermOperation, LeafTerm>,
            var: &Variable,
            subst: &Variable,
        ) {
            match tree {
                TaggedTree::Node { tag, subtrees } => {
                    for tree in subtrees {
                        substitute(tree, var, subst)
                    }
                }
                TaggedTree::Leaf(l) => l.substitute_variable(var, subst),
            }
        }

        substitute(&mut self.0, var, subst);
    }
}
