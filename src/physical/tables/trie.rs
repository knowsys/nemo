use super::{Table, TableSchema};
use crate::logical::Permutator;
use crate::physical::columns::{
    AdaptiveColumnBuilder, AdaptiveColumnBuilderT, Column, ColumnBuilder, GenericIntervalColumn,
    IntervalColumnEnum, IntervalColumnT,
};
use crate::physical::datatypes::{data_value::VecT, DataTypeName, DataValueT};
use std::fmt::Debug;

/// Represents one attribute in [`TrieSchema`].
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct TrieSchemaEntry {
    /// Label of the column
    pub label: usize,

    /// Datatype used in the column
    pub datatype: DataTypeName,
}

/// Schema for [`Trie`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrieSchema {
    attributes: Vec<TrieSchemaEntry>,
}

impl TrieSchema {
    /// Contruct new schema from vector of entries.
    pub fn new(attributes: Vec<TrieSchemaEntry>) -> Self {
        Self { attributes }
    }
}

impl TableSchema for TrieSchema {
    fn arity(&self) -> usize {
        self.attributes.len()
    }

    fn get_type(&self, index: usize) -> DataTypeName {
        self.attributes[index].datatype
    }

    fn get_label(&self, index: usize) -> usize {
        self.attributes[index].label
    }

    fn find_index(&self, label: usize) -> Option<usize> {
        for (index, elem) in self.attributes.iter().enumerate() {
            if elem.label == label {
                return Some(index);
            }
        }

        None
    }
}

/// Implementation of a trie data structure.
/// The underlying data is oragnized in IntervalColumns.
#[derive(Debug, PartialEq)]
pub struct Trie {
    // TODO: could be generic in column type (one of accepted types still is IntervalColumnT)
    schema: TrieSchema,
    columns: Vec<IntervalColumnT>,
}

impl Trie {
    /// Construct a new Trie from a given schema and a vector of IntervalColumns.
    pub fn new(schema: TrieSchema, columns: Vec<IntervalColumnT>) -> Self {
        Self { schema, columns }
    }

    /// Return reference to all columns.
    pub fn columns(&self) -> &Vec<IntervalColumnT> {
        &self.columns
    }

    /// Return reference to a column given an index.
    ///
    /// # Panics
    /// Panics if index is out of range
    pub fn get_column(&self, index: usize) -> &IntervalColumnT {
        &self.columns[index]
    }
}

impl Table for Trie {
    type Schema = TrieSchema;

    fn from_cols(schema: Self::Schema, cols: Vec<VecT>) -> Self {
        debug_assert!({
            // assert that columns have the same length
            cols.get(0)
                .map(|col| {
                    let len = col.len();
                    cols.iter().all(|col| col.len() == len)
                })
                .unwrap_or(true)
        });

        let permutator = Permutator::sort_from_multiple_vec(&cols)
            .expect("debug assert above ensures that cols have the same length");
        let mut sorted_cols: Vec<VecT> =
            cols.iter()
                .map(|col| match col {
                    VecT::U64(vec) => VecT::U64(permutator.permutate(vec).expect(
                        "length matches since permutator is constructed from these vectores",
                    )),
                    VecT::Float(vec) => VecT::Float(permutator.permutate(vec).expect(
                        "length matches since permutator is constructed from these vectores",
                    )),
                    VecT::Double(vec) => VecT::Double(permutator.permutate(vec).expect(
                        "length matches since permutator is constructed from these vectores",
                    )),
                })
                .collect();

        // NOTE: we talk about "condensed" and "uncondesed" in the following
        // "uncondesed" refers to the input version of the column vectors (and their indices), i.e. they have the same length and no duplicates have been removed
        // "condesed" refers to the "cleaned up" version of the columns where duplicates have been removed (largely) and a trie structure is resembled
        // we name our variables accordingly to clarify which version a column vector or index corresponds to
        let mut uncondensed_interval_starts: Vec<Vec<usize>> = vec![vec![0]];
        let mut condensed_data_builders: Vec<AdaptiveColumnBuilderT> = vec![];
        let mut condensed_interval_starts_builders: Vec<AdaptiveColumnBuilder<usize>> = vec![];

        for i in 0..(sorted_cols.len() - 1) {
            let mut current_uncondensed_interval_starts = vec![0];
            let mut current_condensed_data: AdaptiveColumnBuilderT =
                AdaptiveColumnBuilderT::new(sorted_cols[i].get_type());
            let mut current_condensed_interval_starts_builder: AdaptiveColumnBuilder<usize> =
                AdaptiveColumnBuilder::new();

            if !sorted_cols[i].is_empty() {
                let mut interval_index = 0;

                let mut current_val = sorted_cols[i]
                    .get(0)
                    .expect("we just checked that the length is > 0");
                current_condensed_data.add(current_val);
                current_condensed_interval_starts_builder.add(0);

                for j in 1..sorted_cols[i].len() {
                    let possible_next_val = sorted_cols[i]
                        .get(j)
                        .expect("the index is guaranteed to be in range");

                    if possible_next_val != current_val
                        || j >= uncondensed_interval_starts[i]
                            .get(interval_index + 1)
                            .copied()
                            .unwrap_or_else(|| sorted_cols[i].len())
                    {
                        current_uncondensed_interval_starts.push(j);
                        current_val = possible_next_val;
                        current_condensed_data.add(current_val);
                    }

                    // if the second condition above is true, we need to adjust additional intercal information
                    if j >= uncondensed_interval_starts[i]
                        .get(interval_index + 1)
                        .copied()
                        .unwrap_or_else(|| sorted_cols[i].len())
                    {
                        current_condensed_interval_starts_builder
                            .add(current_condensed_data.count() - 1);
                        interval_index += 1;
                    }
                }
            }

            uncondensed_interval_starts.push(current_uncondensed_interval_starts);
            condensed_data_builders.push(current_condensed_data);
            condensed_interval_starts_builders.push(current_condensed_interval_starts_builder);
        }
        // the last column always stays as is
        if let Some(last_col) = sorted_cols.pop() {
            condensed_data_builders.push(
                (0..last_col.len())
                    .into_iter()
                    .map(|i| last_col.get(i).expect("index is guaranteed to be in range"))
                    .collect::<AdaptiveColumnBuilderT>(),
            );
            condensed_interval_starts_builders.push(
                uncondensed_interval_starts
                    .pop()
                    .expect("intervals vector has as many elements as sorted_cols vector")
                    .into_iter()
                    .collect::<AdaptiveColumnBuilder<usize>>(),
            );
        }

        macro_rules! build_interval_column {
            ($col_builder:ident, $interval_builder:ident; $($variant:ident);+) => {
                match $col_builder {
                    $(AdaptiveColumnBuilderT::$variant(vec) => IntervalColumnT::$variant(
                    IntervalColumnEnum::GenericIntervalColumn(GenericIntervalColumn::new(
                        vec.finalize(),
                        $interval_builder.finalize(),
                    )),
                )),+
                }
            }
        }

        Self::new(
            schema,
            condensed_data_builders
                .into_iter()
                .zip(condensed_interval_starts_builders)
                .map(|(col, iv)| build_interval_column!(col, iv; U64; Float; Double))
                .collect(),
        )
    }

    fn from_rows(schema: Self::Schema, rows: Vec<Vec<DataValueT>>) -> Self {
        let mut cols: Vec<VecT> = (0..schema.arity())
            .map(|i| VecT::new(schema.get_type(i)))
            .collect();

        for row in rows {
            for (i, element) in row.into_iter().enumerate() {
                cols[i].push(&element);
            }
        }

        Self::from_cols(schema, cols)
    }

    fn row_num(&self) -> usize {
        self.columns.last().map_or(0, |c| c.len())
    }

    fn schema(&self) -> &Self::Schema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::{Trie, TrieSchema, TrieSchemaEntry};
    use crate::physical::datatypes::{data_value::VecT, DataTypeName, DataValueT};
    use crate::physical::tables::table::Table;
    use crate::physical::util::make_gict;
    use test_log::test;

    fn get_test_schema() -> TrieSchema {
        TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 0,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 1,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 2,
                datatype: DataTypeName::U64,
            },
        ])
    }

    #[test]
    /// Tests general functionality of trie, including:
    ///     * Construction
    ///     * Getting row number
    fn test_trie() {
        let column_fst = make_gict(&[1, 2, 3], &[0]);
        let column_snd = make_gict(&[2, 3, 4, 1, 2], &[0, 2, 3]);
        let column_trd = make_gict(&[3, 4, 5, 7, 2, 1], &[0, 2, 3, 4, 5]);

        let column_vec = vec![column_fst, column_snd, column_trd];

        let schema = get_test_schema();

        let trie = Trie::new(schema, column_vec);
        assert_eq!(trie.row_num(), 6);

        let empty_trie = Trie::new(TrieSchema::new(vec![]), vec![]);
        assert_eq!(empty_trie.row_num(), 0);
    }

    /// helper methods that returns the following table as columns
    /// 1 3 8
    /// 1 2 7
    /// 2 3 9
    /// 1 2 8
    /// 2 6 9
    fn get_test_table_as_cols() -> (TrieSchema, Vec<VecT>) {
        (
            get_test_schema(),
            vec![
                VecT::U64(vec![1, 1, 2, 1, 2]),
                VecT::U64(vec![3, 2, 3, 2, 6]),
                VecT::U64(vec![8, 7, 9, 8, 9]),
            ],
        )
    }

    /// helper methods that returns the following table as rows
    /// 1 3 8
    /// 1 2 7
    /// 2 3 9
    /// 1 2 8
    /// 2 6 9
    fn get_test_table_as_rows() -> (TrieSchema, Vec<Vec<DataValueT>>) {
        (
            get_test_schema(),
            vec![
                vec![DataValueT::U64(1), DataValueT::U64(3), DataValueT::U64(8)],
                vec![DataValueT::U64(1), DataValueT::U64(2), DataValueT::U64(7)],
                vec![DataValueT::U64(2), DataValueT::U64(3), DataValueT::U64(9)],
                vec![DataValueT::U64(1), DataValueT::U64(2), DataValueT::U64(8)],
                vec![DataValueT::U64(2), DataValueT::U64(6), DataValueT::U64(9)],
            ],
        )
    }

    /// helper methods that returns the following table as the expected trie
    /// 1 3 8
    /// 1 2 7
    /// 2 3 9
    /// 1 2 8
    /// 2 6 9
    fn get_test_table_as_trie() -> Trie {
        let schema = get_test_schema();

        let column_fst = make_gict(&[1, 2], &[0]);
        let column_snd = make_gict(&[2, 3, 3, 6], &[0, 2]);
        let column_trd = make_gict(&[7, 8, 8, 9, 9], &[0, 2, 3, 4]);

        let column_vec = vec![column_fst, column_snd, column_trd];

        Trie::new(schema, column_vec)
    }

    #[test]
    fn construct_trie_from_cols() {
        let (schema, cols) = get_test_table_as_cols();
        let expected_trie = get_test_table_as_trie();

        let constructed_trie = Trie::from_cols(schema, cols);

        assert_eq!(expected_trie, constructed_trie);
    }

    #[test]
    fn construct_trie_from_rows() {
        let (schema, rows) = get_test_table_as_rows();
        let expected_trie = get_test_table_as_trie();

        let constructed_trie = Trie::from_rows(schema, rows);

        assert_eq!(expected_trie, constructed_trie);
    }
}
