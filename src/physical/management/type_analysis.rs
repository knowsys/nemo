use std::{cmp::Ordering, collections::HashMap};

use crate::{
    error::Error,
    physical::tabular::traits::table_schema::{TableSchema, TableSchemaEntry},
};

use super::{
    database::{TableId, TableKeyType},
    execution_plan::{ExecutionNode, ExecutionNodeRef, ExecutionTree},
    DatabaseInstance,
};

/// A [`TypeTree`] is just a [`TypeTreeNode`]
pub(super) type TypeTree = TypeTreeNode;

/// Tree that represents the types of an TypeTreeNodeoperator tree.
#[derive(Debug, Default)]
pub(super) struct TypeTreeNode {
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
    /// Create a [`TypeTree`] from an [`ExecutionPlan`]
    pub(super) fn from_execution_tree<TableKey: TableKeyType>(
        instance: &DatabaseInstance<TableKey>,
        temp_schemas: &HashMap<TableId, TableSchema>,
        tree: &ExecutionTree<TableKey>,
    ) -> Result<Self, Error> {
        if let Some(tree_root) = tree.root() {
            Self::from_execution_node(instance, temp_schemas, tree_root)
        } else {
            Ok(TypeTree::default())
        }
    }

    /// Implements the functionality of `from_execution_tree` by recursively traversing the [`ExecutionTree`].
    fn from_execution_node<TableKey: TableKeyType>(
        instance: &DatabaseInstance<TableKey>,
        temp_schemas: &HashMap<TableId, TableSchema>,
        node: ExecutionNodeRef<TableKey>,
    ) -> Result<TypeTreeNode, Error> {
        if let Some(node_rc) = node.0.upgrade() {
            let node_ref = &*node_rc.as_ref().borrow();

            match node_ref {
                ExecutionNode::FetchTable(key) => {
                    let schema = instance.get_schema(key).clone();
                    Ok(TypeTreeNode::new(schema, vec![]))
                }
                ExecutionNode::FetchTemp(id) => {
                    let schema = temp_schemas
                        .get(id)
                        .expect("Function assumes that referenced trie exists.");

                    Ok(TypeTreeNode::new(schema.clone(), vec![]))
                }
                ExecutionNode::Join(subtrees, bindings) => {
                    let mut subtype_nodes = Vec::<TypeTreeNode>::with_capacity(subtrees.len());
                    for subtree in subtrees {
                        let subtype_node =
                            Self::from_execution_node(instance, temp_schemas, subtree.clone())?;

                        subtype_nodes.push(subtype_node);
                    }

                    Ok(TypeTreeNode::default())
                }
                ExecutionNode::Union(subtrees) => Ok(TypeTreeNode::default()),
                ExecutionNode::Minus(left, right) => Ok(TypeTreeNode::default()),
                ExecutionNode::Project(subtree, reordering) => Ok(TypeTreeNode::default()),
                ExecutionNode::SelectValue(subtree, assignments) => Ok(TypeTreeNode::default()),
                ExecutionNode::SelectEqual(subtree, classes) => Ok(TypeTreeNode::default()),
            }
        } else {
            unreachable!()
        }
    }

    /// Returns whether the given [`TableSchemaEntry`]s are compatbile with each other.
    /// I.e. if it would make sense to have values from both columns in one column.
    /// The rules for this are as following:
    ///     * The underlying data types must be compatible (e.g. `U32` is compatible with `U64` but not with `Float`)
    ///     * Either both columns contain keys to a dictionary or both do not
    /// TODO: Consider nulls
    fn compatible(entry_a: &TableSchemaEntry, entry_b: &TableSchemaEntry) -> bool {
        entry_a.type_name.partial_cmp(&entry_b.type_name).is_some() && entry_a.dict == entry_b.dict
    }

    /// Of the given [`TableSchemaEntry`] returns the one which contains the greater [`DataTypeName`]
    /// or `None` if the entries are not compatible (see function `compatible`).
    fn entry_max<'a>(
        entry_a: &'a TableSchemaEntry,
        entry_b: &'a TableSchemaEntry,
    ) -> Option<&'a TableSchemaEntry> {
        if !Self::compatible(entry_a, entry_b) {
            return None;
        }

        if let Some(Ordering::Greater) = entry_a.type_name.partial_cmp(&entry_b.type_name) {
            return Some(entry_a);
        }

        if let Some(Ordering::Greater) = entry_b.type_name.partial_cmp(&entry_a.type_name) {
            return Some(entry_b);
        }

        None
    }

    /// Of the given [`TableSchemaEntry`] returns the one which contains the smaller [`DataTypeName`]
    /// or `None` if the entries are not compatible (see function `compatible`).
    fn entry_min<'a>(
        entry_a: &'a TableSchemaEntry,
        entry_b: &'a TableSchemaEntry,
    ) -> Option<&'a TableSchemaEntry> {
        if !Self::compatible(entry_a, entry_b) {
            return None;
        }

        if let Some(Ordering::Less) = entry_a.type_name.partial_cmp(&entry_b.type_name) {
            return Some(entry_a);
        }

        if let Some(Ordering::Less) = entry_b.type_name.partial_cmp(&entry_a.type_name) {
            return Some(entry_b);
        }

        None
    }
}
