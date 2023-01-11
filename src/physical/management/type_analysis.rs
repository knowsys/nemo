use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
};

use crate::{
    error::Error,
    physical::{
        datatypes::DataTypeName,
        tabular::traits::table_schema::{TableSchema, TableSchemaEntry},
    },
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
    pub(super) fn new(schema: TableSchema, subnodes: Vec<TypeTreeNode>) -> Self {
        Self { schema, subnodes }
    }

    /// String representation of a [`TypeTree`].
    fn as_string(&self, layer: usize) -> String {
        let mut result = String::from("  ").repeat(layer);

        result += "[";
        for (entry_index, entry) in self.schema.get_entries().iter().enumerate() {
            result += &format!("{}", entry.type_name);

            if entry_index < self.schema.arity() - 1 {
                result += ", ";
            }
        }
        result += "]\n";

        self.subnodes
            .iter()
            .for_each(|s| result += &s.as_string(layer + 1));

        result
    }
}

impl PartialEq for TypeTreeNode {
    fn eq(&self, other: &Self) -> bool {
        let arity = self.schema.arity();
        if other.schema.arity() != arity {
            return false;
        }

        if (0..arity).any(|i| self.schema.get_entry(i) != other.schema.get_entry(i)) {
            return false;
        }

        let subnode_len = if self.subnodes.len() == other.subnodes.len() {
            self.subnodes.len()
        } else {
            return false;
        };

        (0..subnode_len).all(|i| self.subnodes[i] == other.subnodes[i])
    }
}
impl Eq for TypeTreeNode {}

impl Display for TypeTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_string(0))
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
            let mut tree = Self::propagate_up(instance, temp_schemas, tree_root.clone())?;
            Self::propagate_down(&mut tree, None, tree_root);

            Ok(tree)
        } else {
            Ok(TypeTree::default())
        }
    }

    /// Types are propagated from bottom to top through the [`ExecutionTree`].
    fn propagate_up<TableKey: TableKeyType>(
        instance: &DatabaseInstance<TableKey>,
        temp_schemas: &HashMap<TableId, TableSchema>,
        node: ExecutionNodeRef<TableKey>,
    ) -> Result<TypeTreeNode, Error> {
        let node_rc = node
            .0
            .upgrade()
            .expect("Referenced execution node has been deleted");
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
                    let subtype_node = Self::propagate_up(instance, temp_schemas, subtree.clone())?;

                    subtype_nodes.push(subtype_node);
                }

                // The following will take the minimum type of all joined positions
                // E.g. bindings = [[0, 1], [1, 0]]; subtype_nodes =  [[U32, U32], [U64, U64]
                // result -> [U32, U32, U64]
                let mut position_type_map = HashMap::<usize, &TableSchemaEntry>::new();
                for (subnode_index, binding) in bindings.iter().enumerate() {
                    for (column_index, value) in binding.iter().enumerate() {
                        let current_entry =
                            subtype_nodes[subnode_index].schema.get_entry(column_index);

                        match position_type_map.entry(*value) {
                            Entry::Occupied(mut entry) => {
                                *entry.get_mut() = if let Some(min_entry) =
                                    Self::entry_min(entry.get(), current_entry)
                                {
                                    min_entry
                                } else {
                                    return Err(Error::InvalidExecutionPlan);
                                }
                            }
                            Entry::Vacant(vacant) => {
                                vacant.insert(current_entry);
                            }
                        }
                    }
                }
                let placeholder_entry = TableSchemaEntry {
                    type_name: DataTypeName::U64,
                    dict: false,
                    nullable: false,
                };
                let mut result_schema_entries = vec![placeholder_entry; position_type_map.len()];
                for (postion, entry) in position_type_map {
                    result_schema_entries[postion] = *entry;
                }

                Ok(TypeTreeNode::new(
                    TableSchema::from_vec(result_schema_entries),
                    subtype_nodes,
                ))
            }
            ExecutionNode::Union(subtrees) => {
                let mut subtype_nodes = Vec::<TypeTreeNode>::with_capacity(subtrees.len());
                for subtree in subtrees {
                    let subtype_node = Self::propagate_up(instance, temp_schemas, subtree.clone())?;

                    subtype_nodes.push(subtype_node);
                }

                let arity = if !subtype_nodes.is_empty() {
                    subtype_nodes[0].schema.arity()
                } else {
                    return Ok(TypeTreeNode::default());
                };

                // The following will take the maximum type of all columns
                // E.g. subtype_nodes = [[U32, U32], [U32, U64]
                // result -> [U32, U64]
                let mut result_schema_entries =
                    Vec::<TableSchemaEntry>::with_capacity(subtrees.len());
                for column_index in 0..arity {
                    for subtype_node in &subtype_nodes {
                        let current_entry = subtype_node.schema.get_entry(column_index);

                        if result_schema_entries.len() <= column_index {
                            result_schema_entries.push(*current_entry);
                        } else {
                            result_schema_entries[column_index] = if let Some(max_entry) =
                                Self::entry_max(&result_schema_entries[column_index], current_entry)
                            {
                                *max_entry
                            } else {
                                return Err(Error::InvalidExecutionPlan);
                            };
                        }
                    }
                }

                Ok(TypeTreeNode::new(
                    TableSchema::from_vec(result_schema_entries),
                    subtype_nodes,
                ))
            }
            ExecutionNode::Minus(left, right) => {
                let subtypenode_left = Self::propagate_up(instance, temp_schemas, left.clone())?;
                let subtypenode_right = Self::propagate_up(instance, temp_schemas, right.clone())?;

                let arity = subtypenode_left.schema.arity();

                // Will copy the type of the left subtree as long
                // as they are compatbile with the types of the right subtree
                let mut result_schema_entries = Vec::<TableSchemaEntry>::new();
                for column_index in 0..arity {
                    let current_left = subtypenode_left.schema.get_entry(column_index);
                    let current_right = subtypenode_right.schema.get_entry(column_index);

                    if Self::compatible(current_left, current_right) {
                        result_schema_entries.push(*current_left);
                    } else {
                        return Err(Error::InvalidExecutionPlan);
                    }
                }

                let subtype_nodes = vec![subtypenode_left, subtypenode_right];

                Ok(TypeTreeNode::new(
                    TableSchema::from_vec(result_schema_entries),
                    subtype_nodes,
                ))
            }
            ExecutionNode::Project(subtree, reordering) => {
                let subtype_node = Self::propagate_up(instance, temp_schemas, subtree.clone())?;

                let result_schema_enries = reordering.apply_to(subtype_node.schema.get_entries());
                Ok(TypeTreeNode::new(
                    TableSchema::from_vec(result_schema_enries),
                    vec![subtype_node],
                ))
            }
            ExecutionNode::SelectValue(subtree, _assignments) => {
                let subtype_node = Self::propagate_up(instance, temp_schemas, subtree.clone())?;
                Ok(TypeTreeNode::new(
                    subtype_node.schema.clone(),
                    vec![subtype_node],
                ))
            }
            ExecutionNode::SelectEqual(subtree, _classes) => {
                let subtype_node = Self::propagate_up(instance, temp_schemas, subtree.clone())?;
                Ok(TypeTreeNode::new(
                    subtype_node.schema.clone(),
                    vec![subtype_node],
                ))
            }
        }
    }

    // Propagates types from the top of a [`TypeTree`] to the bottom.
    fn propagate_down<TableKey: TableKeyType>(
        type_node: &mut TypeTreeNode,
        schema_map_opt: Option<HashMap<usize, TableSchemaEntry>>,
        execution_node: ExecutionNodeRef<TableKey>,
    ) {
        if let Some(schema_map) = schema_map_opt {
            for (index, schema_entry) in schema_map {
                *type_node.schema.get_entry_mut(index) = schema_entry;
            }
        }

        let node_rc = execution_node
            .0
            .upgrade()
            .expect("Referenced execution node has been deleted");
        let node_ref = &*node_rc.as_ref().borrow();

        match node_ref {
            ExecutionNode::FetchTable(_) => {}
            ExecutionNode::FetchTemp(_) => {}
            ExecutionNode::Join(subtrees, bindings) => {
                for (tree_index, binding) in bindings.iter().enumerate() {
                    let mut schema_map = HashMap::<usize, TableSchemaEntry>::new();
                    for (column_index, value) in binding.iter().enumerate() {
                        schema_map.insert(column_index, *type_node.schema.get_entry(*value));
                    }

                    Self::propagate_down(
                        &mut type_node.subnodes[tree_index],
                        Some(schema_map),
                        subtrees[tree_index].clone(),
                    );
                }
            }
            ExecutionNode::Union(subtrees) => {
                let mut schema_map = HashMap::<usize, TableSchemaEntry>::new();
                for (column_index, schema_entry) in
                    type_node.schema.get_entries().iter().enumerate()
                {
                    schema_map.insert(column_index, *schema_entry);
                }

                for (tree_index, subtree) in subtrees.iter().enumerate() {
                    Self::propagate_down(
                        &mut type_node.subnodes[tree_index],
                        Some(schema_map.clone()),
                        subtree.clone(),
                    );
                }
            }
            ExecutionNode::Minus(left, right) => {
                let mut schema_map = HashMap::<usize, TableSchemaEntry>::new();
                for (column_index, schema_entry) in
                    type_node.schema.get_entries().iter().enumerate()
                {
                    schema_map.insert(column_index, *schema_entry);
                }

                Self::propagate_down(
                    &mut type_node.subnodes[0],
                    Some(schema_map.clone()),
                    left.clone(),
                );

                Self::propagate_down(
                    &mut type_node.subnodes[1],
                    Some(schema_map.clone()),
                    right.clone(),
                );
            }
            ExecutionNode::Project(subtree, reordering) => {
                let mut schema_map = HashMap::<usize, TableSchemaEntry>::new();
                for (index, value) in reordering.iter().enumerate() {
                    schema_map.insert(*value, *type_node.schema.get_entry(index));
                }

                Self::propagate_down(
                    &mut type_node.subnodes[0],
                    Some(schema_map.clone()),
                    subtree.clone(),
                );
            }
            ExecutionNode::SelectValue(subtree, _assignments) => {
                let mut schema_map = HashMap::<usize, TableSchemaEntry>::new();
                for (column_index, schema_entry) in
                    type_node.schema.get_entries().iter().enumerate()
                {
                    schema_map.insert(column_index, *schema_entry);
                }

                Self::propagate_down(
                    &mut type_node.subnodes[0],
                    Some(schema_map.clone()),
                    subtree.clone(),
                );
            }
            ExecutionNode::SelectEqual(subtree, _classes) => {
                let mut schema_map = HashMap::<usize, TableSchemaEntry>::new();
                for (column_index, schema_entry) in
                    type_node.schema.get_entries().iter().enumerate()
                {
                    schema_map.insert(column_index, *schema_entry);
                }

                Self::propagate_down(
                    &mut type_node.subnodes[0],
                    Some(schema_map.clone()),
                    subtree.clone(),
                );
            }
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
            Some(entry_a)
        } else {
            Some(entry_b)
        }
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
            Some(entry_a)
        } else {
            Some(entry_b)
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::physical::{
        datatypes::{DataTypeName, DataValueT},
        dictionary::PrefixedStringDictionary,
        management::{
            database::TableId,
            execution_plan::{ExecutionResult, ExecutionTree},
            DatabaseInstance,
        },
        tabular::{
            table_types::trie::Trie,
            traits::{
                table::Table,
                table_schema::{TableSchema, TableSchemaEntry},
            },
        },
        util::Reordering,
    };

    use super::{TypeTree, TypeTreeNode};

    type StringKeyType = String;

    fn schema_entry(type_name: DataTypeName) -> TableSchemaEntry {
        TableSchemaEntry {
            type_name,
            dict: false,
            nullable: false,
        }
    }

    fn build_execution_tree() -> ExecutionTree<StringKeyType> {
        // ExecutionPlan:
        // Union
        //  -> Minus
        //      -> Trie_a [U64, U32]
        //      -> Trie_b [U32, U32]
        //  -> Project [0, 2]
        //      -> Join [[0, 1], [1, 2]]
        //          -> Union
        //              -> Trie_b [U32, U32]
        //              -> Trie_c [U32, U64]
        //          -> Union
        //              -> Trie_c [U32, U64]
        //              -> Temp(0) [U32, U32]

        let mut execution_tree =
            ExecutionTree::<StringKeyType>::new(String::from("Test"), ExecutionResult::Temp(1));

        let node_load_a = execution_tree.fetch_table(String::from("TableA"));
        let node_load_b_1 = execution_tree.fetch_table(String::from("TableB"));
        let node_load_b_2 = execution_tree.fetch_table(String::from("TableB"));
        let node_load_c_1 = execution_tree.fetch_table(String::from("TableC"));
        let node_load_c_2 = execution_tree.fetch_table(String::from("TableC"));
        let node_load_temp = execution_tree.fetch_temp(0);

        let node_minus = execution_tree.minus(node_load_a, node_load_b_1);

        let node_left_union = execution_tree.union(vec![node_load_b_2, node_load_c_1]);
        let node_right_union = execution_tree.union(vec![node_load_c_2, node_load_temp]);

        let node_join = execution_tree.join(
            vec![node_left_union, node_right_union],
            vec![vec![0, 1], vec![1, 2]],
        );

        let node_project = execution_tree.project(node_join, Reordering::new(vec![0, 2], 3));

        let node_root = execution_tree.union(vec![node_project, node_minus]);
        execution_tree.set_root(node_root);

        execution_tree
    }

    fn build_expected_type_tree_up() -> TypeTree {
        let schema_a = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U32),
        ]);
        let schema_b = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U32),
        ]);
        let schema_c = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
        ]);
        let schema_temp = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U32),
        ]);

        let schema_union_left = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
        ]);
        let schema_union_right = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
        ]);

        let schema_join = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
        ]);

        let schema_project = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
        ]);

        let schema_minus = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U32),
        ]);

        let schema_root = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U64),
        ]);

        TypeTreeNode::new(
            schema_root,
            vec![
                TypeTreeNode::new(
                    schema_project,
                    vec![TypeTreeNode::new(
                        schema_join,
                        vec![
                            TypeTreeNode::new(
                                schema_union_left,
                                vec![
                                    TypeTreeNode::new(schema_b.clone(), vec![]),
                                    TypeTreeNode::new(schema_c.clone(), vec![]),
                                ],
                            ),
                            TypeTreeNode::new(
                                schema_union_right,
                                vec![
                                    TypeTreeNode::new(schema_c, vec![]),
                                    TypeTreeNode::new(schema_temp, vec![]),
                                ],
                            ),
                        ],
                    )],
                ),
                TypeTreeNode::new(
                    schema_minus,
                    vec![
                        TypeTreeNode::new(schema_a, vec![]),
                        TypeTreeNode::new(schema_b, vec![]),
                    ],
                ),
            ],
        )
    }

    fn build_expected_type_tree_down() -> TypeTree {
        let schema_a = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U64),
        ]);
        let schema_b_minus = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U64),
        ]);
        let schema_b_union = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U32),
        ]);
        let schema_c_left = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U32),
        ]);
        let schema_c_right = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
        ]);
        let schema_temp = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
        ]);

        let schema_union_left = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U32),
        ]);
        let schema_union_right = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
        ]);

        let schema_join = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
        ]);

        let schema_project = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U64),
        ]);

        let schema_minus = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U64),
        ]);

        let schema_root = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U64),
        ]);

        TypeTreeNode::new(
            schema_root,
            vec![
                TypeTreeNode::new(
                    schema_project,
                    vec![TypeTreeNode::new(
                        schema_join,
                        vec![
                            TypeTreeNode::new(
                                schema_union_left,
                                vec![
                                    TypeTreeNode::new(schema_b_union, vec![]),
                                    TypeTreeNode::new(schema_c_left, vec![]),
                                ],
                            ),
                            TypeTreeNode::new(
                                schema_union_right,
                                vec![
                                    TypeTreeNode::new(schema_c_right, vec![]),
                                    TypeTreeNode::new(schema_temp, vec![]),
                                ],
                            ),
                        ],
                    )],
                ),
                TypeTreeNode::new(
                    schema_minus,
                    vec![
                        TypeTreeNode::new(schema_a, vec![]),
                        TypeTreeNode::new(schema_b_minus, vec![]),
                    ],
                ),
            ],
        )
    }

    #[test]
    fn test_from_execution_plan() {
        let trie_a = Trie::from_rows(vec![vec![DataValueT::U64(1), DataValueT::U32(2)]]);
        let trie_b = Trie::from_rows(vec![vec![DataValueT::U32(1), DataValueT::U32(2)]]);
        let trie_c = Trie::from_rows(vec![vec![DataValueT::U32(1), DataValueT::U64(2)]]);

        let schema_a = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U64),
            schema_entry(DataTypeName::U32),
        ]);
        let schema_b = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U32),
        ]);
        let schema_c = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U64),
        ]);
        let schema_temp = TableSchema::from_vec(vec![
            schema_entry(DataTypeName::U32),
            schema_entry(DataTypeName::U32),
        ]);

        let mut instance =
            DatabaseInstance::<StringKeyType>::new(PrefixedStringDictionary::default());
        instance.add(String::from("TableA"), trie_a, schema_a);
        instance.add(String::from("TableB"), trie_b, schema_b);
        instance.add(String::from("TableC"), trie_c, schema_c);

        let mut temp_schemas = HashMap::<TableId, TableSchema>::new();
        temp_schemas.insert(0, schema_temp);

        let execution_tree = build_execution_tree();

        let type_tree_up =
            TypeTree::propagate_up(&instance, &temp_schemas, execution_tree.root().unwrap());
        assert!(type_tree_up.is_ok());

        let type_tree_up = type_tree_up.unwrap();
        let expected_tree_up = build_expected_type_tree_up();
        assert_eq!(type_tree_up, expected_tree_up);

        let type_tree = TypeTree::from_execution_tree(&instance, &temp_schemas, &execution_tree);
        let type_tree = type_tree.unwrap();
        let expected_tree_down = build_expected_type_tree_down();
        assert_eq!(type_tree, expected_tree_down);
    }
}
