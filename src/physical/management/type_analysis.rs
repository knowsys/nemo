use crate::physical::tabular::traits::table_schema::TableSchema;

/// Tree that represents the types of an operator tree.
#[derive(Debug)]
pub struct TypeTree {
    /// Datatypes of the columns of the table represented by this node
    pub schema: TableSchema,
    /// Subnodes of this node
    pub subnodes: Vec<TypeTree>,
}

impl TypeTree {
    /// Create new [`TypeTreeNode`].
    pub fn new(schema: TableSchema, subnodes: Vec<TypeTree>) -> Self {
        Self { schema, subnodes }
    }
}
