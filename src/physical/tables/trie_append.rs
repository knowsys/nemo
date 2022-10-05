use std::{collections::VecDeque, num::NonZeroUsize};

use crate::physical::{
    columns::{
        rle_column::RleElement, Column, ColumnEnum, GenericIntervalColumn, IntervalColumnEnum,
        IntervalColumnT, RleColumn, Step, VectorColumn,
    },
    datatypes::{DataTypeName, DataValueT, Double, Float},
    tables::TrieSchema,
};

use super::{Table, TableSchema, Trie, TrieSchemaEntry};

/// Add columns consisting of only a constant to a trie
/// Values are given as a slice of [`DataValueT`] vectors
/// Its interpreted as follows:
/// Append columns containing the value in ith vector at the ith gap in the original trie
/// Example: Given a trie with schema: xyz and values [[2], [], [3, 4], [1]]
/// results in a trie with "schema" 2xy34z1
pub fn trie_add_constant(mut trie: Trie, values: &[Vec<DataValueT>]) -> Trie {
    debug_assert!(values.len() == trie.schema().arity() + 1);

    // Construct the new schema
    let mut new_schema = Vec::<TrieSchemaEntry>::new();
    for gap_index in 0..=trie.schema().arity() {
        let current_values = &values[gap_index];
        for value in current_values.iter() {
            let datatype = match value {
                DataValueT::U64(_) => DataTypeName::U64,
                DataValueT::Float(_) => DataTypeName::Float,
                DataValueT::Double(_) => DataTypeName::Double,
            };

            // TODO: Label
            new_schema.push(TrieSchemaEntry { label: 0, datatype });
        }

        if gap_index < trie.schema().arity() {
            new_schema.push(TrieSchemaEntry {
                label: trie.schema().get_label(gap_index),
                datatype: trie.schema().get_type(gap_index),
            })
        }
    }

    // Add the new columns
    let mut new_columns = VecDeque::<IntervalColumnT>::new();
    for gap_index in (0..=trie.schema().arity()).rev() {
        let current_values = &values[gap_index];
        for value_t in current_values.iter().rev() {
            macro_rules! append_columns_for_datatype {
                ($value:ident, $variant:ident, $type:ty, $zero:expr) => {{
                    let new_data_column = if gap_index > 0 {
                        if let Some(prev_column_len) =
                            NonZeroUsize::new(trie.get_column(gap_index - 1).len())
                        {
                            ColumnEnum::RleColumn(RleColumn::<$type>::from_rle_elements(vec![
                                RleElement::new($value, prev_column_len, Step::Increment($zero)),
                            ]))
                        } else {
                            ColumnEnum::VectorColumn(VectorColumn::new(vec![]))
                        }
                    } else {
                        ColumnEnum::VectorColumn(VectorColumn::new(vec![$value]))
                    };

                    let new_interval_column = if gap_index > 0 {
                        if let Some(prev_column_len) =
                            NonZeroUsize::new(trie.get_column(gap_index - 1).len())
                        {
                            ColumnEnum::RleColumn(RleColumn::<usize>::from_rle_elements(vec![
                                RleElement::new(0, prev_column_len, Step::Increment(1)),
                            ]))
                        } else {
                            ColumnEnum::VectorColumn(VectorColumn::new(vec![]))
                        }
                    } else {
                        ColumnEnum::VectorColumn(VectorColumn::new(vec![0]))
                    };

                    new_columns.push_front(IntervalColumnT::$variant(
                        IntervalColumnEnum::GenericIntervalColumn(GenericIntervalColumn::new(
                            new_data_column,
                            new_interval_column,
                        )),
                    ));
                }};
            }

            match *value_t {
                DataValueT::U64(value) => append_columns_for_datatype!(value, U64, u64, 0),
                DataValueT::Float(value) => {
                    append_columns_for_datatype!(value, Float, Float, Float::new(0.0).unwrap())
                }
                DataValueT::Double(value) => {
                    append_columns_for_datatype!(value, Double, Double, Double::new(0.0).unwrap())
                }
            };
        }

        if gap_index > 0 {
            new_columns.push_front(trie.columns_mut().pop().expect(
                "We only pop schema.arity() many elements, so the vector will never be empty",
            ));
        }
    }

    Trie::new(
        TrieSchema::new(new_schema),
        Vec::<IntervalColumnT>::from(new_columns),
    )
}

/// Duplicates existing columns to new positions
/// However, this is limited so duplicated columns need to appear after the original one
/// Columns to be copied are given as a slice of [`usize`] vectors
/// Its interpreted as follows:
/// The usizes are interpreted as column indeces of the existing trie.
/// Append the duplicates of the columns in the ith vector at the ith gap in the original trie
/// Example: Given a trie with schema: xyz and values [[], [1], [0, 2]]
/// results in a trie with "schema" xyyzxz
pub fn trie_add_duplicates(mut trie: Trie, indices: &[Vec<usize>]) -> Trie {
    debug_assert!(indices.len() == trie.schema().arity());

    // Construct the new schema
    let mut new_schema = Vec::<TrieSchemaEntry>::new();
    for gap_index in 0..=trie.schema().arity() {
        if gap_index < trie.schema().arity() {
            new_schema.push(TrieSchemaEntry {
                label: trie.schema().get_label(gap_index),
                datatype: trie.schema().get_type(gap_index),
            })
        }

        let current_indeces = &indices[gap_index];
        for &index in current_indeces {
            let datatype = trie.schema().get_type(index);

            // TODO: Label
            new_schema.push(TrieSchemaEntry { label: 0, datatype });
        }
    }

    // Add the new columns
    let mut new_columns = VecDeque::<IntervalColumnT>::new();
    for gap_index in (0..=trie.schema().arity()).rev() {
        let current_indices = &indices[gap_index];
        for &index in current_indices.iter().rev() {
            if let IntervalColumnT::U64(reference_column) = trie.get_column(index) {
                let reference_data = reference_column.get_data_column();

            }
        }
    }

    Trie::new(
        TrieSchema::new(new_schema),
        Vec::<IntervalColumnT>::from(new_columns),
    )
}

#[cfg(test)]
mod test {
    use crate::physical::{
        columns::RangedColumnScanT,
        datatypes::{DataTypeName, DataValueT},
        tables::{IntervalTrieScan, Trie, TrieScan, TrieSchema, TrieSchemaEntry},
        util::make_gict,
    };

    use super::trie_add_constant;

    fn scan_next(int_scan: &mut IntervalTrieScan) -> Option<u64> {
        if let RangedColumnScanT::U64(rcs) = unsafe { &(*int_scan.current_scan()?.get()) } {
            rcs.next()
        } else {_mut
            panic!("type should be u64");
        }
    }

    fn scan_current(int_scan: &mut IntervalTrieScan) -> Option<u64> {
        unsafe {
            if let RangedColumnScanT::U64(rcs) = &(*int_scan.current_scan()?.get()) {
                rcs.current()
            } else {
                panic!("type should be u64");
            }
        }
    }

    #[test]
    fn test_constant() {
        let column_x = make_gict(&[1, 2, 3], &[0]);
        let column_y = make_gict(&[2, 4, 1, 5, 9], &[0, 2, 3]);
        let column_z = make_gict(&[5, 1, 7, 9, 3, 2, 4, 8], &[0, 1, 4, 5, 7]);

        let schema = TrieSchema::new(vec![
            TrieSchemaEntry {
                label: 10,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 11,
                datatype: DataTypeName::U64,
            },
            TrieSchemaEntry {
                label: 12,
                datatype: DataTypeName::U64,
            },
        ]);

        let trie = Trie::new(schema, vec![column_x, column_y, column_z]);
        let trie_appended = trie_add_constant(
            trie,
            &[
                vec![DataValueT::U64(2)],
                vec![],
                vec![DataValueT::U64(3), DataValueT::U64(4)],
                vec![DataValueT::U64(1)],
            ],
        );

        let mut trie_iter = IntervalTrieScan::new(&trie_appended);

        assert!(scan_current(&mut trie_iter).is_none());

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(5));
        assert_eq!(scan_current(&mut trie_iter), Some(5));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(7));
        assert_eq!(scan_current(&mut trie_iter), Some(7));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(9));
        assert_eq!(scan_current(&mut trie_iter), Some(9));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(5));
        assert_eq!(scan_current(&mut trie_iter), Some(5));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(2));
        assert_eq!(scan_current(&mut trie_iter), Some(2));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);

        trie_iter.up();
        assert_eq!(scan_next(&mut trie_iter), Some(9));
        assert_eq!(scan_current(&mut trie_iter), Some(9));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(3));
        assert_eq!(scan_current(&mut trie_iter), Some(3));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(4));
        assert_eq!(scan_current(&mut trie_iter), Some(4));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(8));
        assert_eq!(scan_current(&mut trie_iter), Some(8));

        trie_iter.down();
        assert!(scan_current(&mut trie_iter).is_none());
        assert_eq!(scan_next(&mut trie_iter), Some(1));
        assert_eq!(scan_current(&mut trie_iter), Some(1));
        assert_eq!(scan_next(&mut trie_iter), None);
        assert_eq!(scan_current(&mut trie_iter), None);
    }
}
