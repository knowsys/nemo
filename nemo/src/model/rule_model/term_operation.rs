use nemo_physical::util::TaggedTree;

use super::{Identifier, Term, Variable};

/// Supported operations between terms.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TermOperation {
    /// Leaf node of the tree.
    Term(Term),
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

impl std::fmt::Display for TermOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TermOperation::Term(term) => term.fmt(f),
            TermOperation::Addition => f.write_str("+"),
            TermOperation::Subtraction => f.write_str("-"),
            TermOperation::Multiplication => f.write_str("*"),
            TermOperation::Division => f.write_str("/"),
            TermOperation::Function(name) => f.write_fmt(format_args!("<Function {}>", name)),
        }
    }
}

/// [`TaggedTree`] with [`TermOperation`] as tags.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TermTree(pub TaggedTree<TermOperation>);

impl From<TaggedTree<TermOperation>> for TermTree {
    fn from(tree: TaggedTree<TermOperation>) -> Self {
        TermTree(tree)
    }
}

impl TermTree {
    /// Create a new leaf node of a [`TermTree`].
    pub fn leaf(term: Term) -> Self {
        Self(TaggedTree::<TermOperation>::leaf(TermOperation::Term(term)))
    }

    /// Create a new [`TermTree`].
    pub fn tree(operation: TermOperation, subtrees: Vec<TermTree>) -> Self {
        Self(TaggedTree::<TermOperation>::tree(
            operation,
            subtrees.into_iter().map(|t| t.0).collect(),
        ))
    }

    /// Return the [`TermOperation`] performed at this node.
    pub fn operation(&self) -> &TermOperation {
        &self.0.tag
    }

    /// Return a list of all the [`Term`]s contained in this tree.
    pub fn terms(&self) -> Vec<&Term> {
        self.0
            .leaves()
            .into_iter()
            .map(|l| {
                if let TermOperation::Term(term) = l {
                    term
                } else {
                    unreachable!("This is the only Leaf type");
                }
            })
            .collect()
    }

    /// Substitutes all occurrences of `variable` for `subst`.
    pub fn substitute_variable(&mut self, variable: &Variable, subst: &Variable) {
        for leaf in self.0.leaves_mut() {
            match leaf {
                TermOperation::Term(t) => t.substitute_variable(variable, subst),
                _ => continue,
            }
        }
    }

    /// Replace one [`Term`] with another.
    pub fn replace_term(&mut self, old: &Term, new: &Term) {
        for leaf_node in self.0.leaves_mut() {
            if let TermOperation::Term(term) = leaf_node {
                if term == old {
                    *term = new.clone();
                }
            } else {
                unreachable!("Leaf nodes must be terms");
            }
        }
    }
}

impl std::fmt::Display for TermTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0.tag {
            TermOperation::Term(term) => term.fmt(f),
            TermOperation::Addition
            | TermOperation::Subtraction
            | TermOperation::Multiplication
            | TermOperation::Division => {
                f.write_str("(")?;
                for (index, subtree) in self.0.subtrees.iter().enumerate() {
                    TermTree(subtree.clone()).fmt(f)?;
                    if index < self.0.subtrees.len() - 1 {
                        self.0.tag.fmt(f)?;
                    }
                }

                f.write_str(")")
            }
            TermOperation::Function(name) => {
                name.fmt(f)?;
                f.write_str("(")?;
                for (index, subtree) in self.0.subtrees.iter().enumerate() {
                    TermTree(subtree.clone()).fmt(f)?;
                    if index < self.0.subtrees.len() - 1 {
                        f.write_str(", ")?;
                    }
                }
                f.write_str(")")
            }
        }
    }
}
