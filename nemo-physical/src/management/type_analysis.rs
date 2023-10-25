use std::{cmp::Ordering, collections::HashMap, fmt::Display};

use crate::{
    aggregates::operation::AggregateOperation,
    arithmetic::expression::{StackOperation, StackValue},
    datatypes::{casting::PartialUpperBound, DataTypeName},
    error::Error,
    tabular::{operations::triescan_append::AppendInstruction, traits::table_schema::TableSchema},
};

use super::{
    execution_plan::{ExecutionNodeRef, ExecutionOperation, ExecutionTree},
    DatabaseInstance,
};

/// A [`TypeTree`] is just a [`TypeTreeNode`]
pub(super) type TypeTree = TypeTreeNode;

/// Tree that represents the types of an TypeTreeNode operator tree.
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
        for (entry_index, entry) in self.schema.iter().enumerate() {
            result += &format!("{entry}");

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
    pub(super) fn from_execution_tree(
        instance: &DatabaseInstance,
        previous_trees: &HashMap<usize, TypeTree>,
        tree: &ExecutionTree,
    ) -> Result<Self, Error> {
        let mut type_tree = Self::propagate_up(instance, previous_trees, tree.root())?;
        Self::propagate_down(&mut type_tree, None, tree.root());

        Ok(type_tree)
    }

    /// Types are propagated from bottom to top through the [`ExecutionTree`].
    fn propagate_up(
        instance: &DatabaseInstance,
        previous_trees: &HashMap<usize, TypeTree>,
        node: ExecutionNodeRef,
    ) -> Result<TypeTreeNode, Error> {
        let node_rc = node.get_rc();
        let node_operation = &node_rc.borrow().operation;

        match node_operation {
            ExecutionOperation::FetchExisting(id, order) => {
                let schema = instance.table_schema(*id).permuted(order);

                Ok(TypeTreeNode::new(schema, vec![]))
            }
            ExecutionOperation::FetchNew(index) => {
                let schema = previous_trees
                    .get(index)
                    .map_or(TableSchema::default(), |t| t.schema.clone());

                Ok(TypeTreeNode::new(schema, vec![]))
            }
            ExecutionOperation::Join(subtrees, bindings) => {
                debug_assert!(subtrees.len() == bindings.num_relations());

                let mut subtype_nodes = Vec::<TypeTreeNode>::with_capacity(subtrees.len());
                for subtree in subtrees {
                    let subtype_node =
                        Self::propagate_up(instance, previous_trees, subtree.clone())?;

                    if subtype_node.schema.is_empty() {
                        return Ok(TypeTreeNode::default());
                    }

                    subtype_nodes.push(subtype_node);
                }

                let mut result_schema = TableSchema::with_capacity(subtrees.len());

                for output_index in 0..bindings.num_output_columns() {
                    let mut min_entry_opt: Option<DataTypeName> = None;

                    for column in bindings.joined_columns(output_index) {
                        let current_entry = subtype_nodes[column.relation]
                            .schema
                            .get_entry(column.column);

                        if let Some(min_entry) = min_entry_opt {
                            let current_min = Self::entry_min(&min_entry, current_entry)
                                .ok_or(Error::InvalidExecutionPlan)?;

                            min_entry_opt = Some(*current_min);
                        } else {
                            min_entry_opt = Some(*current_entry);
                        }
                    }

                    result_schema.push(min_entry_opt.expect(
                        "There must be at least one column that is joined at this positon.",
                    ));
                }

                Ok(TypeTreeNode::new(result_schema, subtype_nodes))
            }
            ExecutionOperation::Union(subtrees) => {
                let mut subtype_nodes = Vec::<TypeTreeNode>::with_capacity(subtrees.len());
                for subtree in subtrees {
                    let subtype_node =
                        Self::propagate_up(instance, previous_trees, subtree.clone())?;

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
                let mut result_schema_entries = TableSchema::with_capacity(subtrees.len());
                for column_index in 0..arity {
                    for subtype_node in &subtype_nodes {
                        if subtype_node.schema.is_empty() {
                            continue;
                        }

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

                Ok(TypeTreeNode::new(result_schema_entries, subtype_nodes))
            }
            ExecutionOperation::Minus(left, right) => {
                let subtypenode_left = Self::propagate_up(instance, previous_trees, left.clone())?;
                let subtypenode_right =
                    Self::propagate_up(instance, previous_trees, right.clone())?;

                let result_schema = if !subtypenode_right.schema.is_empty() {
                    let arity = subtypenode_left.schema.arity();

                    // Will copy the type of the left subtree as long
                    // as they are compatbile with the types of the right subtree
                    let mut result_schema_entries = TableSchema::new();
                    for column_index in 0..arity {
                        let current_left = subtypenode_left.schema.get_entry(column_index);
                        let current_right = subtypenode_right.schema.get_entry(column_index);

                        if Self::compatible(current_left, current_right) {
                            result_schema_entries.push(*current_left);
                        } else {
                            return Err(Error::InvalidExecutionPlan);
                        }
                    }

                    result_schema_entries
                } else {
                    subtypenode_left.schema.clone()
                };

                let subtype_nodes = vec![subtypenode_left, subtypenode_right];

                Ok(TypeTreeNode::new(result_schema, subtype_nodes))
            }
            ExecutionOperation::Project(subtree, reordering) => {
                let subtype_node = Self::propagate_up(instance, previous_trees, subtree.clone())?;

                let new_schema = if !subtype_node.schema.is_empty() {
                    subtype_node.schema.reordered(reordering)
                } else {
                    TableSchema::default()
                };

                Ok(TypeTreeNode::new(new_schema, vec![subtype_node]))
            }
            ExecutionOperation::Filter(subtree, _condition) => {
                let subtype_node = Self::propagate_up(instance, previous_trees, subtree.clone())?;
                Ok(TypeTreeNode::new(
                    subtype_node.schema.clone(),
                    vec![subtype_node],
                ))
            }
            ExecutionOperation::SelectEqual(subtree, classes) => {
                let subtype_node = Self::propagate_up(instance, previous_trees, subtree.clone())?;

                let mut new_schema = subtype_node.schema.clone();

                if !new_schema.is_empty() {
                    for class in classes {
                        let mut min_type = *new_schema.get_entry(class[0]);

                        // First calculate the minimum type
                        for &index in class {
                            let current_entry = new_schema.get_entry(index);
                            if let Some(min_entry) = Self::entry_min(&min_type, current_entry) {
                                min_type = *min_entry;
                            } else {
                                return Err(Error::InvalidExecutionPlan);
                            }
                        }

                        // Then replace each entry in the new schema with the minimal type
                        for &index in class {
                            *new_schema.get_entry_mut(index) = min_type;
                        }
                    }
                }

                Ok(TypeTreeNode::new(new_schema, vec![subtype_node]))
            }
            ExecutionOperation::AppendColumns(subtree, instructions) => {
                // TODO: Revisit type propagation for this case
                // This is the only case where it seems to be necessary to have additional casting in the trie scan

                let subtype_node = Self::propagate_up(instance, previous_trees, subtree.clone())?;
                let mut new_schema = TableSchema::new();

                for (gap_index, gap_instructions) in instructions.iter().enumerate() {
                    for instruction in gap_instructions {
                        match instruction {
                            AppendInstruction::RepeatColumn(repeat_index) => {
                                new_schema
                                    .add_entry_cloned(subtype_node.schema.get_entry(*repeat_index));
                            }
                            AppendInstruction::Constant(value) => {
                                new_schema.add_entry(value.get_type());
                            }
                            AppendInstruction::Arithmetic(expression) => {
                                if subtype_node.schema.is_empty() {
                                    continue;
                                }

                                let mut operation_type: Option<DataTypeName> = None;

                                // We check whether the type of each leaf node has the same upper bound
                                for leaf in expression.iter() {
                                    let current_type = match leaf {
                                        StackOperation::Push(StackValue::Constant(constant)) => {
                                            constant.get_type().partial_upper_bound()
                                        }
                                        StackOperation::Push(StackValue::Reference(
                                            column_index,
                                        )) => subtype_node
                                            .schema
                                            .get_entry(*column_index)
                                            .partial_upper_bound(),
                                        _ => continue,
                                    };

                                    if let Some(operation_type) = operation_type {
                                        if !Self::compatible(&operation_type, &current_type) {
                                            return Err(Error::InvalidExecutionPlan);
                                        }
                                    } else {
                                        operation_type = Some(current_type);
                                    }
                                }

                                new_schema.add_entry(operation_type.expect("operation_type will be assigned because the the operation tree must contain at least one leaf node."));
                            }
                        }
                    }

                    if gap_index < instructions.len() - 1 {
                        new_schema.add_entry_cloned(subtype_node.schema.get_entry(gap_index));
                    }
                }

                Ok(TypeTreeNode::new(new_schema, vec![subtype_node]))
            }
            ExecutionOperation::AppendNulls(subtree, num_nulls) => {
                let subtype_node = Self::propagate_up(instance, previous_trees, subtree.clone())?;
                let mut new_schema = subtype_node.schema.clone();

                for _ in 0..*num_nulls {
                    // TODO: We do not have access to the logical types here; how should we handle nulls?
                    new_schema.add_entry(DataTypeName::String);
                }

                Ok(TypeTreeNode::new(new_schema, vec![subtype_node]))
            }
            ExecutionOperation::Subtract(node_main, nodes_subtract, infos) => {
                let mut subtype_nodes = Vec::<TypeTreeNode>::with_capacity(nodes_subtract.len());
                subtype_nodes.push(Self::propagate_up(
                    instance,
                    previous_trees,
                    node_main.clone(),
                )?);

                for (node, info) in nodes_subtract.iter().zip(infos) {
                    let subtype_node = Self::propagate_up(instance, previous_trees, node.clone())?;

                    for (subtract_layer, main_layer) in info.used_layers.iter().enumerate() {
                        let current_main = subtype_nodes[0].schema.get_entry(*main_layer);
                        let current_subtract = subtype_node.schema.get_entry(subtract_layer);

                        if !Self::compatible(current_main, current_subtract) {
                            return Err(Error::InvalidExecutionPlan);
                        }
                    }

                    subtype_nodes.push(subtype_node);
                }

                Ok(TypeTreeNode::new(
                    subtype_nodes[0].schema.clone(),
                    subtype_nodes,
                ))
            }
            ExecutionOperation::Aggregate(subtree, aggregation_instructions) => {
                let subtype_node = Self::propagate_up(instance, previous_trees, subtree.clone())?;
                let mut new_schema = TableSchema::new();

                // Clone types for group-by columns
                for column_index in 0..aggregation_instructions.group_by_column_count {
                    new_schema.add_entry_cloned(subtype_node.schema.get_entry(column_index));
                }

                // Determine type for aggregate output column
                new_schema.add_entry(match aggregation_instructions.aggregate_operation {
                    AggregateOperation::Count => DataTypeName::I64,
                    _ => *subtype_node
                        .schema
                        .get_entry(aggregation_instructions.aggregated_column_index),
                });

                Ok(TypeTreeNode::new(new_schema, vec![subtype_node]))
            }
        }
    }

    // Propagates types from the top of a [`TypeTree`] to the bottom.
    fn propagate_down(
        type_node: &mut TypeTreeNode,
        schema_map_opt: Option<HashMap<usize, DataTypeName>>,
        execution_node: ExecutionNodeRef,
    ) {
        if type_node.schema.is_empty() {
            return;
        }

        if let Some(schema_map) = schema_map_opt {
            for (index, schema_entry) in schema_map {
                *type_node.schema.get_entry_mut(index) = schema_entry;
            }
        }

        let node_rc = execution_node.get_rc();
        let node_operation = &node_rc.borrow().operation;

        match node_operation {
            ExecutionOperation::FetchExisting(_, _) => {}
            ExecutionOperation::FetchNew(_) => {}
            ExecutionOperation::Join(subtrees, bindings) => {
                let mut subtype_map =
                    vec![HashMap::<usize, DataTypeName>::new(); bindings.num_relations()];

                for output_index in 0..bindings.num_output_columns() {
                    for input_column in bindings.joined_columns(output_index) {
                        subtype_map[input_column.relation].insert(
                            input_column.column,
                            *type_node.schema.get_entry(output_index),
                        );
                    }
                }

                for (tree_index, schema_map) in subtype_map.into_iter().enumerate() {
                    Self::propagate_down(
                        &mut type_node.subnodes[tree_index],
                        Some(schema_map),
                        subtrees[tree_index].clone(),
                    );
                }
            }
            ExecutionOperation::Union(subtrees) => {
                let mut schema_map = HashMap::<usize, DataTypeName>::new();
                for (column_index, schema_entry) in type_node.schema.iter().enumerate() {
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
            ExecutionOperation::Minus(left, right) => {
                let mut schema_map = HashMap::<usize, DataTypeName>::new();
                for (column_index, schema_entry) in type_node.schema.iter().enumerate() {
                    schema_map.insert(column_index, *schema_entry);
                }

                Self::propagate_down(
                    &mut type_node.subnodes[0],
                    Some(schema_map.clone()),
                    left.clone(),
                );

                Self::propagate_down(&mut type_node.subnodes[1], Some(schema_map), right.clone());
            }
            ExecutionOperation::Project(subtree, reordering) => {
                let mut schema_map = HashMap::<usize, DataTypeName>::new();
                for (index, value) in reordering.iter() {
                    schema_map.insert(*index, *type_node.schema.get_entry(*value));
                }

                Self::propagate_down(
                    &mut type_node.subnodes[0],
                    Some(schema_map),
                    subtree.clone(),
                );
            }
            ExecutionOperation::Filter(subtree, _assignments) => {
                let mut schema_map = HashMap::<usize, DataTypeName>::new();
                for (column_index, schema_entry) in type_node.schema.iter().enumerate() {
                    schema_map.insert(column_index, *schema_entry);
                }

                Self::propagate_down(
                    &mut type_node.subnodes[0],
                    Some(schema_map),
                    subtree.clone(),
                );
            }
            ExecutionOperation::SelectEqual(subtree, _classes) => {
                let mut schema_map = HashMap::<usize, DataTypeName>::new();
                for (column_index, schema_entry) in type_node.schema.iter().enumerate() {
                    schema_map.insert(column_index, *schema_entry);
                }

                Self::propagate_down(
                    &mut type_node.subnodes[0],
                    Some(schema_map),
                    subtree.clone(),
                );
            }
            ExecutionOperation::AppendColumns(subtree, instructions) => {
                // TODO: Revisit type propagation for this case
                // This is the only case where it seems to be necessary to have additional casting in the trie scan

                let mut schema_map = HashMap::<usize, DataTypeName>::new();
                let mut not_appended_index: usize = 0;

                for (gap_index, gap_instructions) in
                    instructions.iter().take(instructions.len() - 1).enumerate()
                {
                    not_appended_index += gap_instructions.len();
                    schema_map.insert(gap_index, *type_node.schema.get_entry(not_appended_index));
                    not_appended_index += 1;
                }

                // Overwrite type of input columns to an operation to maximum type
                for instructions in instructions {
                    for instruction in instructions {
                        if let AppendInstruction::Arithmetic(expression) = instruction {
                            for input_index in expression.references() {
                                let max_type = schema_map
                                    .get(&input_index)
                                    .expect(
                                        "operation tree should only contain valid input indices",
                                    )
                                    .partial_upper_bound();

                                schema_map.insert(input_index, max_type);
                            }
                        }
                    }
                }

                Self::propagate_down(
                    &mut type_node.subnodes[0],
                    Some(schema_map),
                    subtree.clone(),
                );
            }
            ExecutionOperation::AppendNulls(subtree, num_nulls) => {
                let mut schema_map = HashMap::<usize, DataTypeName>::new();

                for (index, schema_entry) in type_node
                    .schema
                    .iter()
                    .take(type_node.schema.arity() - num_nulls)
                    .enumerate()
                {
                    schema_map.insert(index, *schema_entry);
                }

                Self::propagate_down(
                    &mut type_node.subnodes[0],
                    Some(schema_map),
                    subtree.clone(),
                );
            }
            ExecutionOperation::Subtract(node_main, nodes_subtract, infos) => {
                let mut schema_map = HashMap::<usize, DataTypeName>::new();
                for (column_index, schema_entry) in type_node.schema.iter().enumerate() {
                    schema_map.insert(column_index, *schema_entry);
                }

                for ((type_node, tree_node), info) in type_node
                    .subnodes
                    .iter_mut()
                    .skip(1)
                    .zip(nodes_subtract.iter())
                    .zip(infos.iter())
                {
                    let mut sub_schema_map = HashMap::<usize, DataTypeName>::new();
                    for (subtract_layer, main_layer) in info.used_layers.iter().enumerate() {
                        let main_type = schema_map
                            .get(main_layer)
                            .expect("used_layers may not contain values >= arity of the main scan");
                        sub_schema_map.insert(subtract_layer, *main_type);
                    }

                    Self::propagate_down(type_node, Some(sub_schema_map), tree_node.clone());
                }

                Self::propagate_down(
                    &mut type_node.subnodes[0],
                    Some(schema_map),
                    node_main.clone(),
                );
            }
            ExecutionOperation::Aggregate(subtree, aggregation_instructions) => {
                let mut schema_map = HashMap::<usize, DataTypeName>::new();

                // Clone group by columns
                for (key, t) in type_node
                    .schema
                    .iter()
                    .take(aggregation_instructions.group_by_column_count)
                    .enumerate()
                {
                    schema_map.insert(key, *t);
                }

                // Handle aggregated column
                // Propagate type information from aggregate output column to aggregate input column if aggregate operation has non-static type
                // Otherwise, the input type can not be inferred
                if aggregation_instructions
                    .aggregate_operation
                    .static_output_type()
                    .is_none()
                {
                    schema_map.insert(
                        aggregation_instructions.aggregated_column_index,
                        *type_node
                            .schema
                            .get_entry(aggregation_instructions.group_by_column_count),
                    );
                }

                Self::propagate_down(
                    &mut type_node.subnodes[0],
                    Some(schema_map),
                    subtree.clone(),
                );
            }
        }
    }

    /// Returns whether the given [`DataTypeName`]s are compatible with each other.
    /// I.e. if it would make sense to have values from both columns in one column.
    /// The rules for this are as following:
    ///     * The underlying data types must be compatible (e.g. `U32` is compatible with `U64` but not with `Float`)
    ///     * Either both columns contain keys to a dictionary or both do not
    /// TODO: Consider nulls
    fn compatible(entry_a: &DataTypeName, entry_b: &DataTypeName) -> bool {
        entry_a.partial_cmp(entry_b).is_some()
    }

    /// Of the given [`DataTypeName`] returns the one which is greater
    /// or `None` if the entries are not compatible (see function `compatible`).
    fn entry_max<'a>(
        entry_a: &'a DataTypeName,
        entry_b: &'a DataTypeName,
    ) -> Option<&'a DataTypeName> {
        if !Self::compatible(entry_a, entry_b) {
            return None;
        }

        if let Some(Ordering::Greater) = entry_a.partial_cmp(entry_b) {
            Some(entry_a)
        } else {
            Some(entry_b)
        }
    }

    /// Of the given [`DataTypeName`] returns the one which is smaller
    /// or `None` if the entries are not compatible (see function `compatible`).
    fn entry_min<'a>(
        entry_a: &'a DataTypeName,
        entry_b: &'a DataTypeName,
    ) -> Option<&'a DataTypeName> {
        if !Self::compatible(entry_a, entry_b) {
            return None;
        }

        if let Some(Ordering::Less) = entry_a.partial_cmp(entry_b) {
            Some(entry_a)
        } else {
            Some(entry_b)
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        datatypes::{DataTypeName, StorageValueT},
        management::{
            database::{ColumnOrder, TableId},
            execution_plan::ExecutionTree,
            DatabaseInstance, ExecutionPlan,
        },
        tabular::{
            operations::{
                triescan_append::AppendInstruction, triescan_project::ProjectReordering,
                JoinBindings,
            },
            table_types::trie::Trie,
            traits::{table::Table, table_schema::TableSchema},
        },
    };

    use super::{TypeTree, TypeTreeNode};

    fn build_execution_tree() -> ExecutionTree {
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

        let mut current_id = TableId::default();

        let mut execution_tree = ExecutionPlan::default();

        let id_a = current_id.increment();
        let id_b = current_id.increment();
        let id_c = current_id.increment();

        let node_load_a = execution_tree.fetch_existing(id_a);
        let node_load_b_1 = execution_tree.fetch_existing(id_b);
        let node_load_b_2 = execution_tree.fetch_existing(id_b);
        let node_load_c_1 = execution_tree.fetch_existing(id_c);
        let node_load_c_2 = execution_tree.fetch_existing(id_c);
        let node_load_temp = execution_tree.fetch_new(0);

        let node_minus = execution_tree.minus(node_load_a, node_load_b_1);

        let node_left_union = execution_tree.union(vec![node_load_b_2, node_load_c_1]);
        let node_right_union = execution_tree.union(vec![node_load_c_2, node_load_temp]);

        let node_join = execution_tree.join(
            vec![node_left_union, node_right_union],
            JoinBindings::new(vec![vec![0, 1], vec![1, 2]]),
        );

        let node_project =
            execution_tree.project(node_join, ProjectReordering::from_vector(vec![0, 2], 3));

        let node_root = execution_tree.union(vec![node_project, node_minus]);
        execution_tree.write_temporary(node_root, "Test Tree");

        ExecutionTree::new(execution_tree)
    }

    fn build_expected_type_tree_up() -> TypeTree {
        let schema_a = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U32]);
        let schema_b = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U32]);
        let schema_c = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U64]);
        let schema_temp = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U32]);

        let schema_union_left = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U64]);
        let schema_union_right = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U64]);

        let schema_join = TableSchema::from_vec(vec![
            DataTypeName::U32,
            DataTypeName::U32,
            DataTypeName::U64,
        ]);

        let schema_project = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U64]);

        let schema_minus = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U32]);

        let schema_root = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U64]);

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
        let schema_a = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U64]);
        let schema_b_minus = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U64]);
        let schema_b_union = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U32]);
        let schema_c_left = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U32]);
        let schema_c_right = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U64]);
        let schema_temp = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U64]);

        let schema_union_left = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U32]);
        let schema_union_right = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U64]);

        let schema_join = TableSchema::from_vec(vec![
            DataTypeName::U64,
            DataTypeName::U32,
            DataTypeName::U64,
        ]);

        let schema_project = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U64]);

        let schema_minus = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U64]);

        let schema_root = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U64]);

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
        let trie_a = Trie::from_rows(&[vec![StorageValueT::U64(1), StorageValueT::U32(2)]]);
        let trie_b = Trie::from_rows(&[vec![StorageValueT::U32(1), StorageValueT::U32(2)]]);
        let trie_c = Trie::from_rows(&[vec![StorageValueT::U32(1), StorageValueT::U64(2)]]);

        let schema_a = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U32]);
        let schema_b = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U32]);
        let schema_c = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U64]);
        let schema_temp = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U32]);

        let mut instance = DatabaseInstance::new();
        instance.register_add_trie("TableA", schema_a, ColumnOrder::default(), trie_a);
        instance.register_add_trie("TableB", schema_b, ColumnOrder::default(), trie_b);
        instance.register_add_trie("TableC", schema_c, ColumnOrder::default(), trie_c);

        let mut type_trees = HashMap::<usize, TypeTree>::new();
        type_trees.insert(
            0,
            TypeTreeNode {
                schema: schema_temp,
                subnodes: vec![],
            },
        );

        let execution_tree = build_execution_tree();

        let type_tree_up = TypeTree::propagate_up(&instance, &type_trees, execution_tree.root());
        assert!(type_tree_up.is_ok());

        let type_tree_up = type_tree_up.unwrap();
        let expected_tree_up = build_expected_type_tree_up();
        assert_eq!(type_tree_up, expected_tree_up);

        let type_tree = TypeTree::from_execution_tree(&instance, &type_trees, &execution_tree);
        let type_tree = type_tree.unwrap();
        let expected_tree_down = build_expected_type_tree_down();
        assert_eq!(type_tree, expected_tree_down);
    }

    #[test]
    fn test_append() {
        let trie_a = Trie::from_rows(&[vec![StorageValueT::U32(1)]]);
        let trie_b = Trie::from_rows(&[vec![StorageValueT::U64(1 << 35), StorageValueT::U32(2)]]);

        let schema_a = TableSchema::from_vec(vec![DataTypeName::U32]);
        let schema_b = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U32]);

        let mut instance = DatabaseInstance::new();
        let id_a = instance.register_add_trie("TableA", schema_a, ColumnOrder::default(), trie_a);
        let id_b = instance.register_add_trie("TableB", schema_b, ColumnOrder::default(), trie_b);

        let mut execution_tree = ExecutionPlan::default();

        let fetch_a = execution_tree.fetch_existing(id_a);
        let fetch_b = execution_tree.fetch_existing(id_b);

        let append_a = execution_tree.append_columns(
            fetch_a,
            vec![vec![], vec![AppendInstruction::RepeatColumn(0)]],
        );

        let union = execution_tree.union(vec![append_a, fetch_b]);
        execution_tree.write_temporary(union, "Test");
        let execution_tree = ExecutionTree::new(execution_tree);

        let type_trees = HashMap::<usize, TypeTree>::new();
        let type_tree = TypeTree::from_execution_tree(&instance, &type_trees, &execution_tree);

        let expect_a = TableSchema::from_vec(vec![DataTypeName::U64]);
        let expect_b = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U32]);
        let expect_append = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U32]);
        let expect_union = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U32]);

        let expected_type_tree = TypeTreeNode::new(
            expect_union,
            vec![
                TypeTreeNode::new(expect_append, vec![TypeTreeNode::new(expect_a, vec![])]),
                TypeTreeNode::new(expect_b, vec![]),
            ],
        );

        assert_eq!(expected_type_tree, type_tree.unwrap());
    }

    #[test]
    fn test_equal_col() {
        let trie_a = Trie::from_rows(&[vec![StorageValueT::U32(1), StorageValueT::U64(1 << 34)]]);
        let trie_b = Trie::from_rows(&[vec![
            StorageValueT::U64(1 << 35),
            StorageValueT::U64(1 << 36),
        ]]);

        let schema_a = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U64]);
        let schema_b = TableSchema::from_vec(vec![DataTypeName::U64, DataTypeName::U64]);

        let mut instance = DatabaseInstance::new();
        let id_a = instance.register_add_trie("TableA", schema_a, ColumnOrder::default(), trie_a);
        let id_b = instance.register_add_trie("TableB", schema_b, ColumnOrder::default(), trie_b);

        let mut execution_tree = ExecutionPlan::default();

        let fetch_a = execution_tree.fetch_existing(id_a);
        let fetch_b = execution_tree.fetch_existing(id_b);

        let node_eq_col = execution_tree.select_equal(fetch_a, vec![vec![0, 1]]);

        let node_join = execution_tree.join(
            vec![node_eq_col, fetch_b],
            JoinBindings::new(vec![vec![0, 1], vec![1, 2]]),
        );

        execution_tree.write_temporary(node_join, "Test");
        let execution_tree = ExecutionTree::new(execution_tree);

        let type_trees = HashMap::<usize, TypeTree>::new();
        let type_tree = TypeTree::from_execution_tree(&instance, &type_trees, &execution_tree);

        let expect_a = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U32]);
        let expect_b = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U64]);
        let expect_eq = TableSchema::from_vec(vec![DataTypeName::U32, DataTypeName::U32]);
        let expect_join = TableSchema::from_vec(vec![
            DataTypeName::U32,
            DataTypeName::U32,
            DataTypeName::U64,
        ]);

        let expected_type_tree = TypeTreeNode::new(
            expect_join,
            vec![
                TypeTreeNode::new(expect_eq, vec![TypeTreeNode::new(expect_a, vec![])]),
                TypeTreeNode::new(expect_b, vec![]),
            ],
        );

        assert_eq!(expected_type_tree, type_tree.unwrap());
    }
}
