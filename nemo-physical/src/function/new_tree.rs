//! This module defines a tree representation
//! of complex functions: [FunctionTree].

use std::fmt::Debug;

use super::new_definitions::Functions;

/// Leaf of a [FunctionTree]
#[derive(Debug, Clone)]
pub enum FunctionLeaf<Value, Reference>
where
    Value: Debug + Clone,
    Reference: Debug + Clone,
{
    /// Constant value
    Constant(Value),
    /// Referenced value supplied "at runtime"
    Reference(Reference),
}

/// Tree structure representing a complex function
#[derive(Debug, Clone)]
pub enum FunctionTree<Value, Reference>
where
    Value: Debug + Clone,
    Reference: Debug + Clone,
{
    /// Leaf of the tree
    Leaf(FunctionLeaf<Value, Reference>),
    /// Operation with its sub trees
    Function(Functions, Vec<FunctionTree<Value, Reference>>),
}

// Constructors
impl<Value, Reference> FunctionTree<Value, Reference>
where
    Value: Debug + Clone,
    Reference: Debug + Clone,
{
    /// Create a leaf node with a constant.
    pub fn constant(constant: Value) -> Self {
        Self::Leaf(FunctionLeaf::Constant(constant))
    }

    /// Create a leaf node with a reference.
    pub fn reference(reference: Reference) -> Self {
        Self::Leaf(FunctionLeaf::Reference(reference))
    }

    generate_tree_constructors!();
}

#[cfg(test)]
mod test {
    use super::FunctionTree;

    #[test]
    fn tree_constructor() {
        let x = FunctionTree::<bool, usize>::boolean_negation(FunctionTree::constant(false));
    }
}
