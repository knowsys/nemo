//! Module containing a data structure for representing trees with node labels.

use std::fmt::{Debug, Display};

/// A tree structure such that every node in the tree has a tag of the given tag type.
#[derive(Clone, Eq, PartialEq)]
pub struct TaggedTree<Tag> {
    /// The tag of the node.
    pub tag: Tag,
    /// The subtrees below this node.
    pub subtrees: Vec<TaggedTree<Tag>>,
}

impl<Tag> TaggedTree<Tag> {
    /// Construct a new [`TaggedTree`].
    pub fn tree(tag: Tag, subtrees: Vec<TaggedTree<Tag>>) -> Self {
        Self { tag, subtrees }
    }

    /// Construct a new [`TaggedTree`] consisting of a single leaf node.
    pub fn leaf(tag: Tag) -> Self {
        Self::tree(tag, vec![])
    }

    /// Return whether this node is a leaf node.
    pub fn is_leaf(&self) -> bool {
        self.subtrees.is_empty()
    }

    /// Recursive implementation of the function `leaves`.
    fn leaves_recursive(tree: &TaggedTree<Tag>) -> Vec<&Tag> {
        let mut result = Vec::<&Tag>::new();

        if tree.is_leaf() {
            result.push(&tree.tag);
        } else {
            for subtree in &tree.subtrees {
                result.extend(Self::leaves_recursive(subtree));
            }
        }

        result
    }

    /// Return a list of references to the tags of the leaf nodes in the tree.
    pub fn leaves(&self) -> Vec<&Tag> {
        Self::leaves_recursive(self)
    }

    /// Recursive implementation of the function `leaves_mut`.
    fn leaves_mut_recursive(tree: &mut TaggedTree<Tag>) -> Vec<&mut Tag> {
        let mut result = Vec::<&mut Tag>::new();

        if tree.is_leaf() {
            result.push(&mut tree.tag);
        } else {
            for subtree in &mut tree.subtrees {
                result.extend(Self::leaves_mut_recursive(subtree));
            }
        }

        result
    }

    /// Return a list of mutable references to the tags of the leaf nodes in the tree.    
    pub fn leaves_mut(&mut self) -> Vec<&mut Tag> {
        Self::leaves_mut_recursive(self)
    }
}

impl<Tag> Debug for TaggedTree<Tag>
where
    Tag: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_leaf() {
            return write!(f, "{:?}", self.tag);
        }

        write!(f, "{:?}(", self.tag)?;

        for (index, subtree) in self.subtrees.iter().enumerate() {
            write!(f, "{subtree:?}")?;
            if index < self.subtrees.len() - 1 {
                write!(f, ", ")?;
            }
        }

        write!(f, ")")?;
        Ok(())
    }
}

impl<Tag> Display for TaggedTree<Tag>
where
    Tag: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_leaf() {
            return write!(f, "{}", self.tag);
        }

        write!(f, "{}(", self.tag)?;

        for (index, subtree) in self.subtrees.iter().enumerate() {
            write!(f, "{subtree}")?;
            if index < self.subtrees.len() - 1 {
                write!(f, ", ")?;
            }
        }

        write!(f, ")")?;
        Ok(())
    }
}
