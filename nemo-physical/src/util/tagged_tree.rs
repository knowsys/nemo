//! Module containing a data structure for representing trees with node labels.

use std::fmt::Debug;

/// A tree structure such that every node in the tree has a tag of the given tag type.
#[derive(Clone, Eq, PartialEq, PartialOrd, Ord)]
pub enum TaggedTree<Node, Leaf> {
    /// A node with 0 or more children
    Node {
        /// The tag of the node.
        tag: Node,
        /// The subtrees below this node.
        subtrees: Vec<TaggedTree<Node, Leaf>>,
    },
    /// A terminal node in the tree
    Leaf(Leaf),
}

macro_rules! dfs_leaf_iterator {
    ($stack:expr) => {
        loop {
            // Loops through node layers until
            // the first leaf node is found.
            match $stack.pop()? {
                TaggedTree::Node { subtrees, .. } => {
                    // Found a non-leaf, add all subtrees to the stack.
                    for subtree in subtrees {
                        $stack.push(subtree)
                    }
                }
                // Found a leaf
                TaggedTree::Leaf(t) => break Some(t),
            }
        }
    };
}
struct LeafIter<'a, Node, Leaf> {
    stack: Vec<&'a TaggedTree<Node, Leaf>>,
}

impl<'a, Node, Leaf> Iterator for LeafIter<'a, Node, Leaf> {
    type Item = &'a Leaf;

    fn next(&mut self) -> Option<Self::Item> {
        dfs_leaf_iterator!(self.stack)
    }
}

struct LeafIterMut<'a, Node, Leaf> {
    stack: Vec<&'a mut TaggedTree<Node, Leaf>>,
}

impl<'a, Node, Leaf> Iterator for LeafIterMut<'a, Node, Leaf> {
    type Item = &'a mut Leaf;

    fn next(&mut self) -> Option<Self::Item> {
        dfs_leaf_iterator!(self.stack)
    }
}

impl<Node, Leaf> TaggedTree<Node, Leaf> {
    /// Construct a new [`TaggedTree`].
    pub fn tree(tag: Node, subtrees: Vec<Self>) -> Self {
        Self::Node { tag, subtrees }
    }

    /// Construct a new [`TaggedTree`] consisting of a single leaf node.
    pub fn leaf(leaf: Leaf) -> Self {
        Self::Leaf(leaf)
    }

    /// Return whether this node is a leaf node.
    pub fn is_leaf(&self) -> bool {
        matches!(self, Self::Leaf(_))
    }

    /// Return a list of references to the tags of the leaf nodes in the tree.
    pub fn leaves(&self) -> impl Iterator<Item = &Leaf> {
        LeafIter { stack: vec![self] }
    }

    /// Return a list of mutable references to the tags of the leaf nodes in the tree.    
    pub fn leaves_mut(&mut self) -> impl Iterator<Item = &mut Leaf> {
        LeafIterMut { stack: vec![self] }
    }
}

impl<Node, Leaf> Debug for TaggedTree<Node, Leaf>
where
    Node: Debug,
    Leaf: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaggedTree::Node { tag, subtrees } => {
                write!(f, "{:?}(", tag)?;

                for (index, subtree) in subtrees.iter().enumerate() {
                    write!(f, "{subtree:?}")?;
                    if index < subtrees.len() - 1 {
                        write!(f, ", ")?;
                    }
                }

                write!(f, ")")?;
                Ok(())
            }
            TaggedTree::Leaf(tag) => write!(f, "{tag:?}"),
        }
    }
}
