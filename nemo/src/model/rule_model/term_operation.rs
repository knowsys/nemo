use nemo_physical::util::TaggedTree;

use super::Term;

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
}
