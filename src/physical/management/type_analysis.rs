use crate::physical::tabular::traits::table_schema::TableSchema;

// use super::{database::TableKeyType, execution_plan::ExecutionTree, ExecutionPlan};

/// A [`TypeTree`] is just a [`TypeTreeNode`]
pub type TypeTree = TypeTreeNode;

/// Tree that represents the types of an TypeTreeNodeoperator tree.
#[derive(Debug, Default)]
pub struct TypeTreeNode {
    /// Datatypes of the columns of the table represented by this node
    pub schema: TableSchema,
    /// Subnodes of this node
    pub subnodes: Vec<TypeTreeNode>,
}

impl TypeTreeNode {
    /// Create new [`TypeTreeNode`].
    pub fn new(schema: TableSchema, subnodes: Vec<TypeTreeNode>) -> Self {
        Self { schema, subnodes }
    }
}

impl TypeTree {
    // /// Create a [`TypeTree`] from an [`ExecutionPlan`]
    // pub fn from_execution_tree<TableKey: TableKeyType>(tree: &ExecutionTree<TableKey>) -> Self {
    //     if let Some(tree_root) = tree.root() {
    //         Self::from_execution_node(tree_root)
    //     } else {
    //         TypeTree::default()
    //     }
    // }

    // /// Implements the functionality of `from_plan` by recursively traversing the [`ExecutionPlan`]
    // fn from_execution_node(node: ) -> TypeTreeNode {

    // }
}
