use crate::logical::Permutator;
use crate::physical::columnar::builders::{
    ColumnBuilder, ColumnBuilderAdaptive, ColumnBuilderAdaptiveT,
};
use crate::physical::columnar::columns::{
    Column, IntervalColumn, IntervalColumnEnum, IntervalColumnGeneric, IntervalColumnT,
};
use crate::physical::datatypes::{data_value::VecT, DataTypeName, DataValueT};
use crate::physical::dictionary::{Dictionary, PrefixedStringDictionary};
use crate::physical::tabular::tables::{Table, TableSchema};
use std::fmt;
use std::fmt::Debug;
use std::iter;

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
#[derive(Clone, Debug, PartialEq, Eq)]
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

    /// Return mutable reference to all columns.
    pub fn columns_mut(&mut self) -> &mut Vec<IntervalColumnT> {
        &mut self.columns
    }

    /// Return reference to a column given an index.
    ///
    /// # Panics
    /// Panics if index is out of range
    pub fn get_column(&self, index: usize) -> &IntervalColumnT {
        &self.columns[index]
    }

    /// Return a [`DebugTrie`] from the [`Trie`]
    pub fn debug<'a>(&'a self, dict: &'a PrefixedStringDictionary) -> DebugTrie<'a> {
        DebugTrie { trie: self, dict }
    }

    /// Returns the sum of the lengths of each column
    pub fn num_elements(&self) -> usize {
        let mut result = 0;

        for column in &self.columns {
            result += column.len();
        }

        result
    }

    // TODO: unify this with Display Trait implementation
    pub(crate) fn format_as_csv(
        &self,
        f: &mut fmt::Formatter<'_>,
        dict: &PrefixedStringDictionary,
    ) -> fmt::Result {
        if self
            .columns
            .first()
            .map_or(true, |column| column.is_empty())
        {
            return writeln!(f);
        }

        // outer vecs are build in reverse order
        let mut last_interval_lengths: Vec<usize> = self
            .columns
            .last()
            .expect("we return early if columns are empty")
            .iter()
            .map(|_| 1)
            .collect();
        let mut str_cols: Vec<Vec<String>> = vec![self
            .columns
            .last()
            .expect("we return early if columns are empty")
            .iter()
            .map(|val| match val {
                DataValueT::U64(constant) => dict
                    .entry(constant.try_into().unwrap())
                    .unwrap_or_else(|| format!("<{constant} should have been interned>")),
                _ => val.to_string(),
            })
            .collect()];
        for column_index in (0..(self.columns.len() - 1)).rev() {
            let current_column = &self.columns[column_index];
            let last_column = &self.columns[column_index + 1];

            let current_interval_lengths: Vec<usize> = (0..current_column.len())
                .map(|element_index_in_current_column| {
                    last_column
                        .int_bounds(element_index_in_current_column)
                        .map(|index_in_interval| last_interval_lengths[index_in_interval])
                        .sum()
                })
                .collect();

            let padding_lengths = current_interval_lengths.iter().map(|length| length - 1);

            str_cols.push(
                current_column
                    .iter()
                    .zip(padding_lengths)
                    .flat_map(|(val, pl)| {
                        iter::once(match val {
                            DataValueT::U64(constant) => {
                                dict.entry(constant.try_into().unwrap()).unwrap_or_else(|| {
                                    format!("<{constant} should have been interned>")
                                })
                            }
                            _ => val.to_string(),
                        })
                        .chain(
                            iter::repeat(match val {
                                DataValueT::U64(constant) => {
                                    dict.entry(constant.try_into().unwrap()).unwrap_or_else(|| {
                                        format!("<{constant} should have been interned>")
                                    })
                                }
                                _ => val.to_string(),
                            })
                            .take(pl),
                        )
                    })
                    .collect(),
            );

            last_interval_lengths = current_interval_lengths;
        }

        for row_index in 0..str_cols[0].len() {
            for col_index in (0..str_cols.len()).rev() {
                write!(f, "{}", str_cols[col_index][row_index])?;
                if col_index > 0 {
                    write!(f, ",")?;
                }
            }
            writeln!(f,)?;
        }

        Ok(())
    }
}

/// [`Trie`] which also contains an associated dictionary for displaying proper names
#[derive(Debug)]
pub struct DebugTrie<'a> {
    trie: &'a Trie,
    dict: &'a PrefixedStringDictionary,
}

impl fmt::Display for DebugTrie<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.trie.format_as_csv(f, self.dict)
    }
}

// TODO: unify this with debug above
impl fmt::Display for Trie {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.columns.is_empty() {
            writeln!(f)?;
            return Ok(());
        }

        // outer vecs are build in reverse order
        let mut last_interval_lengths: Vec<usize> = self
            .columns
            .last()
            .expect("we return early if columns are empty")
            .iter()
            .map(|_| 1)
            .collect();
        let mut str_cols: Vec<Vec<String>> = vec![self
            .columns
            .last()
            .expect("we return early if columns are empty")
            .iter()
            .map(|val| val.to_string())
            .collect()];
        for column_index in (0..(self.columns.len() - 1)).rev() {
            let current_column = &self.columns[column_index];
            let last_column = &self.columns[column_index + 1];

            let current_interval_lengths: Vec<usize> = (0..current_column.len())
                .map(|element_index_in_current_column| {
                    last_column
                        .int_bounds(element_index_in_current_column)
                        .map(|index_in_interval| last_interval_lengths[index_in_interval])
                        .sum()
                })
                .collect();

            let padding_lengths = current_interval_lengths.iter().map(|length| length - 1);

            str_cols.push(
                current_column
                    .iter()
                    .zip(padding_lengths)
                    .flat_map(|(val, pl)| {
                        iter::once(val.to_string()).chain(iter::repeat(" ".to_string()).take(pl))
                    })
                    .collect(),
            );

            last_interval_lengths = current_interval_lengths;
        }

        for row_index in 0..str_cols[0].len() {
            for col_index in (0..str_cols.len()).rev() {
                write!(f, "{} ", str_cols[col_index][row_index])?;
            }
            writeln!(f)?;
        }
        Ok(())
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

        macro_rules! build_interval_column {
            ($col_builder:ident, $interval_builder:ident; $($variant:ident);+) => {
                match $col_builder {
                    $(ColumnBuilderAdaptiveT::$variant(vec) => IntervalColumnT::$variant(
                    IntervalColumnEnum::IntervalColumnGeneric(IntervalColumnGeneric::new(
                        vec.finalize(),
                        $interval_builder.finalize(),
                    )),
                )),+
                }
            }
        }

        // if the first col is empty, then all of them are; we return early in this case
        if cols.get(0).map(|col| col.is_empty()).unwrap_or(true) {
            return Self::new(
                schema,
                cols
                    .into_iter()
                    .map(|_| {
                        let empty_data_col = ColumnBuilderAdaptiveT::new(DataTypeName::U64);
                        let empty_interval_col = ColumnBuilderAdaptive::<usize>::new();
                        build_interval_column!(empty_data_col, empty_interval_col; U64; Float; Double)
                    })
                    .collect(),
            );
        }

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

        // NOTE: we talk about "condensed" and "uncondensed" in the following
        // "uncondensed" refers to the input version of the column vectors (and their indices), i.e. they have the same length and no duplicates have been removed
        // "condensed" refers to the "cleaned up" version of the columns where duplicates have been removed (largely) and a trie structure is resembled
        // we name our variables accordingly to clarify which version a column vector or index corresponds to
        let mut last_uncondensed_interval_starts: Vec<usize> = vec![0];
        let mut condensed_data_builders: Vec<ColumnBuilderAdaptiveT> = vec![];
        let mut condensed_interval_starts_builders: Vec<ColumnBuilderAdaptive<usize>> = vec![];

        for sorted_col in sorted_cols.iter().take(sorted_cols.len() - 1) {
            let mut current_uncondensed_interval_starts = vec![0];
            let mut current_condensed_data: ColumnBuilderAdaptiveT =
                ColumnBuilderAdaptiveT::new(sorted_col.get_type());
            let mut current_condensed_interval_starts_builder: ColumnBuilderAdaptive<usize> =
                ColumnBuilderAdaptive::new();

            let mut uncondensed_interval_ends =
                last_uncondensed_interval_starts.iter().skip(1).copied();
            let mut uncondensed_interval_end = uncondensed_interval_ends
                .next()
                .unwrap_or_else(|| sorted_col.len());

            let mut current_val = sorted_col
                .get(0)
                .expect("we return early if the first column (and thus all) are empty");
            current_condensed_data.add(current_val);
            current_condensed_interval_starts_builder.add(0);

            for uncondensed_col_index in 1..sorted_col.len() {
                let possible_next_val = sorted_col
                    .get(uncondensed_col_index)
                    .expect("index is gauranteed to be in range");

                if possible_next_val != current_val
                    || uncondensed_col_index >= uncondensed_interval_end
                {
                    current_uncondensed_interval_starts.push(uncondensed_col_index);
                    current_val = possible_next_val;
                    current_condensed_data.add(current_val);
                }

                // if the second condition above is true, we need to adjust additional intercal information
                if uncondensed_col_index >= uncondensed_interval_end {
                    current_condensed_interval_starts_builder
                        .add(current_condensed_data.count() - 1);
                    uncondensed_interval_end = uncondensed_interval_ends
                        .next()
                        .unwrap_or_else(|| sorted_col.len());
                }
            }

            last_uncondensed_interval_starts = current_uncondensed_interval_starts;
            condensed_data_builders.push(current_condensed_data);
            condensed_interval_starts_builders.push(current_condensed_interval_starts_builder);
        }
        // the last column always stays as is
        if let Some(last_col) = sorted_cols.pop() {
            condensed_data_builders.push(
                (0..last_col.len())
                    .into_iter()
                    .map(|i| last_col.get(i).expect("index is guaranteed to be in range"))
                    .collect::<ColumnBuilderAdaptiveT>(),
            );
            condensed_interval_starts_builders.push(
                last_uncondensed_interval_starts
                    .into_iter()
                    .collect::<ColumnBuilderAdaptive<usize>>(),
            );
        }

        macro_rules! build_interval_column {
            ($col_builder:ident, $interval_builder:ident; $($variant:ident);+) => {
                match $col_builder {
                    $(ColumnBuilderAdaptiveT::$variant(data_col) => IntervalColumnT::$variant(
                    IntervalColumnEnum::IntervalColumnGeneric(IntervalColumnGeneric::new(
                        data_col.finalize(),
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
    use crate::physical::tabular::tables::Table;
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

    #[test]
    fn display_trie() {
        let trie = get_test_table_as_trie();
        let expected_output = "1 2 7 \
           \n    8 \
           \n  3 8 \
           \n2 3 9 \
           \n  6 9 \
           \n";

        let actual_output = format!("{}", trie);

        assert_eq!(expected_output, actual_output);
    }
}
