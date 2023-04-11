use bytesize::ByteSize;

use crate::generate_cast_statements;
use crate::logical::Permutator;
use crate::physical::columnar::operations::{ColumnScanCast, ColumnScanCastEnum};
use crate::physical::columnar::traits::columnscan::{ColumnScanCell, ColumnScanEnum};
use crate::physical::columnar::{
    adaptive_column_builder::{ColumnBuilderAdaptive, ColumnBuilderAdaptiveT},
    column_types::interval::{ColumnWithIntervals, ColumnWithIntervalsT},
    traits::{
        column::Column,
        columnbuilder::ColumnBuilder,
        columnscan::{ColumnScan, ColumnScanT},
    },
};
use crate::physical::datatypes::{storage_value::VecT, StorageTypeName, StorageValueT};
use crate::physical::dictionary::Dictionary;
use crate::physical::management::database::Dict;
use crate::physical::management::ByteSized;
use crate::physical::tabular::operations::triescan_project::ProjectReordering;
use crate::physical::tabular::traits::{table::Table, triescan::TrieScan};
use std::cell::UnsafeCell;
use std::fmt;
use std::fmt::Debug;
use std::iter;
use std::mem::size_of;

/// Implementation of a trie data structure.
/// The underlying data is oragnized in IntervalColumns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trie {
    types: Vec<StorageTypeName>,
    columns: Vec<ColumnWithIntervalsT>,
}

impl Trie {
    /// Construct a new Trie from a given schema and a vector of IntervalColumns.
    pub fn new(columns: Vec<ColumnWithIntervalsT>) -> Self {
        debug_assert!(!columns.is_empty());

        let types = columns.iter().map(|col| col.get_type()).collect();
        Self { types, columns }
    }

    /// Return reference to all columns.
    pub fn columns(&self) -> &Vec<ColumnWithIntervalsT> {
        &self.columns
    }

    /// Return mutable reference to all columns.
    pub fn columns_mut(&mut self) -> &mut Vec<ColumnWithIntervalsT> {
        &mut self.columns
    }

    /// Return reference to a column given an index.
    ///
    /// # Panics
    /// Panics if index is out of range
    pub fn get_column(&self, index: usize) -> &ColumnWithIntervalsT {
        &self.columns[index]
    }

    /// Return a [`DebugTrie`] from the [`Trie`]
    pub fn debug(&self, dict: Dict) -> DebugTrie {
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

    /// Convert the trie into a vector of columns with equal length.
    /// This will also reorder or leave out columns according to the given [`ProjectReordering`]
    /// TODO: unify this with Display Trait implementation and `format_as_csv` function
    pub fn as_column_vector(&self, project_reordering: &ProjectReordering) -> Vec<VecT> {
        if self.columns.is_empty() {
            return Vec::new();
        }

        // outer vecs are build in reverse order
        let mut last_interval_lengths: Vec<usize> = self
            .columns
            .last()
            .expect("we return early if columns are empty")
            .iter()
            .map(|_| 1)
            .collect();

        macro_rules! last_column_for_datatype {
            ($variant:ident) => {{
                vec![VecT::$variant(
                    self.columns
                        .last()
                        .expect("we return early if columns are empty")
                        .iter()
                        .map(|val| match val {
                            StorageValueT::$variant(constant) => constant,
                            _ => panic!("Unsupported type"),
                        })
                        .collect(),
                )]
            }};
        }

        let mut result_columns: Vec<VecT> = match self
            .types
            .last()
            .expect("we return early if columns are empty")
        {
            StorageTypeName::U32 => last_column_for_datatype!(U32),
            StorageTypeName::U64 => last_column_for_datatype!(U64),
            StorageTypeName::Float => last_column_for_datatype!(Float),
            StorageTypeName::Double => last_column_for_datatype!(Double),
        };

        for column_index in (0..(self.columns.len() - 1)).rev() {
            let current_column = &self.columns[column_index];
            let current_type = self.types[column_index];
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

            macro_rules! push_column_for_datatype {
                ($variant:ident) => {{
                    result_columns.push(VecT::$variant(
                        current_column
                            .iter()
                            .zip(padding_lengths)
                            .flat_map(|(val, pl)| {
                                iter::once(match val {
                                    StorageValueT::$variant(constant) => constant,
                                    _ => panic!("Unsupported type"),
                                })
                                .chain(
                                    iter::repeat(match val {
                                        StorageValueT::$variant(constant) => constant,
                                        _ => panic!("Unsupported type"),
                                    })
                                    .take(pl),
                                )
                            })
                            .collect(),
                    ));
                }};
            }

            match current_type {
                StorageTypeName::U32 => push_column_for_datatype!(U32),
                StorageTypeName::U64 => push_column_for_datatype!(U64),
                StorageTypeName::Float => push_column_for_datatype!(Float),
                StorageTypeName::Double => push_column_for_datatype!(Double),
            };

            last_interval_lengths = current_interval_lengths;
        }

        result_columns.reverse();
        project_reordering.transform_consumed(result_columns)
    }

    /// Determines how null values are represented.
    /// TODO: There should be a better place for this function.
    pub fn format_null(value: u64) -> String {
        format!("<__Null#{value}>")
    }

    // TODO: unify this with Display Trait implementation
    pub(crate) fn format_as_csv(&self, f: &mut fmt::Formatter<'_>, dict: &Dict) -> fmt::Result {
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
                StorageValueT::U64(constant) => dict
                    .entry(constant.try_into().unwrap())
                    .unwrap_or_else(|| Self::format_null(constant)),
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
                            StorageValueT::U64(constant) => dict
                                .entry(constant.try_into().unwrap())
                                .unwrap_or_else(|| Self::format_null(constant)),
                            _ => val.to_string(),
                        })
                        .chain(
                            iter::repeat(match val {
                                StorageValueT::U64(constant) => dict
                                    .entry(constant.try_into().unwrap())
                                    .unwrap_or_else(|| Self::format_null(constant)),
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

impl ByteSized for Trie {
    fn size_bytes(&self) -> ByteSize {
        // TODO: Think about including TrieSchema here, but maybe it will move anyways
        ByteSize::b(size_of::<Self>() as u64)
            + self
                .columns
                .iter()
                .fold(ByteSize::b(0), |acc, column| acc + column.size_bytes())
    }
}

/// [`Trie`] which also contains an associated dictionary for displaying proper names
#[derive(Debug)]
pub struct DebugTrie<'a> {
    trie: &'a Trie,
    dict: Dict,
}

impl fmt::Display for DebugTrie<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.trie.format_as_csv(f, &self.dict)
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
    fn from_cols(cols: Vec<VecT>) -> Self {
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
                    $(ColumnBuilderAdaptiveT::$variant(vec) => ColumnWithIntervalsT::$variant(
                        ColumnWithIntervals::new(
                            vec.finalize(),
                            $interval_builder.finalize(),
                        ),
                    )),+
                }
            }
        }

        // if the first col is empty, then all of them are; we return early in this case
        if cols.get(0).map(|col| col.is_empty()).unwrap_or(true) {
            return Self::new(
                cols
                    .into_iter()
                    .map(|_| {
                        let empty_data_col = ColumnBuilderAdaptiveT::new(StorageTypeName::U64, Default::default(), Default::default());
                        let empty_interval_col = ColumnBuilderAdaptive::<usize>::default();
                        build_interval_column!(empty_data_col, empty_interval_col; U32; U64; Float; Double)
                    })
                    .collect(),
            );
        }

        let permutator = Permutator::sort_from_multiple_vec(&cols)
            .expect("debug assert above ensures that cols have the same length");
        let sorted_cols: Vec<VecT> =
            cols.iter()
                .map(|col| match col {
                    VecT::U32(vec) => VecT::U32(permutator.permutate(vec).expect(
                        "length matches since permutator is constructed from these vectores",
                    )),
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

        for sorted_col in sorted_cols.iter() {
            let mut current_uncondensed_interval_starts = vec![0];
            let mut current_condensed_data: ColumnBuilderAdaptiveT = ColumnBuilderAdaptiveT::new(
                sorted_col.get_type(),
                Default::default(),
                Default::default(),
            );
            let mut current_condensed_interval_starts_builder: ColumnBuilderAdaptive<usize> =
                ColumnBuilderAdaptive::default();

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

        macro_rules! build_interval_column {
            ($col_builder:ident, $interval_builder:ident; $($variant:ident);+) => {
                match $col_builder {
                    $(ColumnBuilderAdaptiveT::$variant(data_col) => ColumnWithIntervalsT::$variant(
                        ColumnWithIntervals::new(
                            data_col.finalize(),
                            $interval_builder.finalize(),
                        ),
                    )),+
                }
            }
        }

        Self::new(
            condensed_data_builders
                .into_iter()
                .zip(condensed_interval_starts_builders)
                .map(|(col, iv)| build_interval_column!(col, iv; U32; U64; Float; Double))
                .collect(),
        )
    }

    fn from_rows(rows: &[Vec<StorageValueT>]) -> Self {
        debug_assert!(!rows.is_empty());

        let arity = rows[0].len();

        let mut cols: Vec<VecT> = (0..arity)
            .map(|i| VecT::new(rows[0][i].get_type()))
            .collect();

        for row in rows {
            for (i, element) in row.iter().enumerate() {
                cols[i].push(element);
            }
        }

        Self::from_cols(cols)
    }

    fn row_num(&self) -> usize {
        self.columns.last().map_or(0, |c| c.len())
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        &self.types
    }
}

/// Implementation of [`TrieScan`] for a [`Trie`].
#[derive(Debug)]
pub struct TrieScanGeneric<'a> {
    trie: &'a Trie,
    column_types: Vec<StorageTypeName>,
    layers: Vec<UnsafeCell<ColumnScanT<'a>>>,
    current_layer: Option<usize>,
}

impl<'a> TrieScanGeneric<'a> {
    /// Construct Trie iterator.
    pub fn new(trie: &'a Trie) -> Self {
        let column_types = trie.get_types().clone();
        Self::new_cast(trie, column_types)
    }

    /// Construct a new trie iterator but converts each column to the given types.
    pub fn new_cast(trie: &'a Trie, column_types: Vec<StorageTypeName>) -> Self {
        debug_assert!(trie.get_types().len() == column_types.len());

        let mut layers = Vec::<UnsafeCell<ColumnScanT<'a>>>::new();

        for (column_index, column_t) in trie.columns().iter().enumerate() {
            let src_column_type = trie.get_types()[column_index];
            let dst_column_type = column_types[column_index];

            if src_column_type == dst_column_type {
                layers.push(UnsafeCell::new(column_t.iter()));
            } else {
                macro_rules! add_layer_for_datatype {
                    ($src_name:ident, $dst_name:ident, $src_type:ty, $dst_type:ty) => {{
                        let reference_scan = if let ColumnScanT::$src_name(scan) = column_t.iter() {
                            scan
                        } else {
                            panic!("Expected a column scan of type {}", stringify!($type));
                        };

                        let new_scan = ColumnScanT::$dst_name(ColumnScanCell::new(
                            ColumnScanEnum::ColumnScanCast(ColumnScanCastEnum::$src_name(
                                ColumnScanCast::<$src_type, $dst_type>::new(reference_scan),
                            )),
                        ));

                        layers.push(UnsafeCell::new(new_scan));
                    }};
                }

                generate_cast_statements!(add_layer_for_datatype; src_column_type, dst_column_type);
            }
        }

        Self {
            trie,
            column_types,
            layers,
            current_layer: None,
        }
    }
}

impl<'a> TrieScan<'a> for TrieScanGeneric<'a> {
    fn up(&mut self) {
        self.current_layer = self.current_layer.and_then(|index| index.checked_sub(1));
    }

    fn down(&mut self) {
        match self.current_layer {
            None => {
                self.current_layer = Some(0);

                // This `reset` is necessary because of the following scenario:
                // Calling `up` at the first layer while the first layer still points to some position.
                // Going `down` from there without the `reset` would lead to the first scan
                // still pointing to the previous position instead of starting at `None` as expected.
                // This is not needed for the other path as calling `narrow` has a similar effect.
                self.layers[0].get_mut().reset();
            }
            Some(index) => {
                debug_assert!(
                    index < self.layers.len(),
                    "Called down while on the last layer"
                );

                let current_position = self.layers[index]
                    .get_mut()
                    .pos()
                    .expect("Going down is only allowed when on an element.");

                let next_index = index + 1;
                let next_layer_range = self
                    .trie
                    .get_column(next_index)
                    .int_bounds(current_position);

                self.layers[next_index].get_mut().narrow(next_layer_range);

                self.current_layer = Some(next_index);
            }
        }
    }

    fn current_scan(&mut self) -> Option<&mut ColumnScanT<'a>> {
        Some(self.layers[self.current_layer?].get_mut())
    }

    fn get_scan(&self, index: usize) -> Option<&UnsafeCell<ColumnScanT<'a>>> {
        Some(&self.layers[index])
    }

    fn get_types(&self) -> &Vec<StorageTypeName> {
        &self.column_types
    }
}

#[cfg(test)]
mod test {
    use super::{Trie, TrieScanGeneric};
    use crate::physical::columnar::traits::columnscan::ColumnScanT;
    use crate::physical::datatypes::{storage_value::VecT, StorageValueT};
    use crate::physical::tabular::traits::{table::Table, triescan::TrieScan};
    use crate::physical::util::make_column_with_intervals_t;
    use test_log::test;

    #[test]
    /// Tests general functionality of trie, including:
    ///     * Construction
    ///     * Getting row number
    fn test_trie() {
        let column_fst = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_snd = make_column_with_intervals_t(&[2, 3, 4, 1, 2], &[0, 2, 3]);
        let column_trd = make_column_with_intervals_t(&[3, 4, 5, 7, 2, 1], &[0, 2, 3, 4, 5]);

        let column_vec = vec![column_fst, column_snd, column_trd];

        let trie = Trie::new(column_vec);
        assert_eq!(trie.row_num(), 6);
    }

    /// helper methods that returns the following table as columns
    /// 1 3 8
    /// 1 2 7
    /// 2 3 9
    /// 1 2 8
    /// 2 6 9
    fn get_test_table_as_cols() -> Vec<VecT> {
        vec![
            VecT::U64(vec![1, 1, 2, 1, 2]),
            VecT::U64(vec![3, 2, 3, 2, 6]),
            VecT::U64(vec![8, 7, 9, 8, 9]),
        ]
    }

    /// helper methods that returns the following table as rows
    /// 1 3 8
    /// 1 2 7
    /// 2 3 9
    /// 1 2 8
    /// 2 6 9
    fn get_test_table_as_rows() -> Vec<Vec<StorageValueT>> {
        vec![
            vec![
                StorageValueT::U64(1),
                StorageValueT::U64(3),
                StorageValueT::U64(8),
            ],
            vec![
                StorageValueT::U64(1),
                StorageValueT::U64(2),
                StorageValueT::U64(7),
            ],
            vec![
                StorageValueT::U64(2),
                StorageValueT::U64(3),
                StorageValueT::U64(9),
            ],
            vec![
                StorageValueT::U64(1),
                StorageValueT::U64(2),
                StorageValueT::U64(8),
            ],
            vec![
                StorageValueT::U64(2),
                StorageValueT::U64(6),
                StorageValueT::U64(9),
            ],
        ]
    }

    /// helper methods that returns the following table as the expected trie
    /// 1 3 8
    /// 1 2 7
    /// 2 3 9
    /// 1 2 8
    /// 2 6 9
    fn get_test_table_as_trie() -> Trie {
        let column_fst = make_column_with_intervals_t(&[1, 2], &[0]);
        let column_snd = make_column_with_intervals_t(&[2, 3, 3, 6], &[0, 2]);
        let column_trd = make_column_with_intervals_t(&[7, 8, 8, 9, 9], &[0, 2, 3, 4]);

        let column_vec = vec![column_fst, column_snd, column_trd];

        Trie::new(column_vec)
    }

    #[test]
    fn construct_trie_from_cols() {
        let cols = get_test_table_as_cols();
        let expected_trie = get_test_table_as_trie();

        let constructed_trie = Trie::from_cols(cols);

        assert_eq!(expected_trie, constructed_trie);
    }

    #[test]
    fn construct_trie_from_rows() {
        let rows = get_test_table_as_rows();
        let expected_trie = get_test_table_as_trie();

        let constructed_trie = Trie::from_rows(&rows);

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

        let actual_output = format!("{trie}");

        assert_eq!(expected_output, actual_output);
    }

    fn scan_seek(int_scan: &mut TrieScanGeneric, value: u64) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = int_scan.current_scan()? {
            rcs.seek(value)
        } else {
            panic!("type should be u64");
        }
    }

    fn scan_next(int_scan: &mut TrieScanGeneric) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = int_scan.current_scan()? {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn scan_current(int_scan: &mut TrieScanGeneric) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = int_scan.current_scan()? {
            rcs.current()
        } else {
            panic!("type should be u64");
        }
    }

    #[test]
    fn test_trie_iter() {
        let column_fst = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_snd = make_column_with_intervals_t(&[2, 3, 4, 1, 2], &[0, 2, 3]);
        let column_trd = make_column_with_intervals_t(&[3, 4, 5, 7, 8, 7, 2, 1], &[0, 2, 5, 6, 7]);

        let column_vec = vec![column_fst, column_snd, column_trd];

        let trie = Trie::new(column_vec);
        let mut trie_iter = TrieScanGeneric::new(&trie);

        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.up();
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.up();
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_seek(&mut trie_iter, 3), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_seek(&mut trie_iter, 6), Some(7));
        assert_eq!(scan_current(&mut trie_iter), Some(7));

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(7));
        assert_eq!(scan_current(&mut trie_iter), Some(7));
        assert!(scan_next(&mut trie_iter).is_none());

        trie_iter.up();
        assert!(scan_next(&mut trie_iter).is_none());
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_seek(&mut trie_iter, 2), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert!(scan_next(&mut trie_iter).is_none());
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.up();
        assert!(scan_next(&mut trie_iter).is_none());
        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.up();
        assert!(scan_next(&mut trie_iter).is_none());
        assert!(scan_current(&mut trie_iter).is_none());
    }
}
