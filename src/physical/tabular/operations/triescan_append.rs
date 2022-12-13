use std::collections::VecDeque;

use crate::physical::{
    columnar::{
        adaptive_column_builder::ColumnBuilderAdaptive,
        column_types::interval::{ColumnWithIntervals, ColumnWithIntervalsT},
        traits::{column::Column, columnbuilder::ColumnBuilder},
    },
    datatypes::{DataValueT, Double, Float},
    tabular::{table_types::trie::Trie, traits::table::Table},
};

/// Add columns consisting of only a constant to a trie
/// Values are given as a slice of [`DataValueT`] vectors
/// Its interpreted as follows:
/// Append columns containing the value in ith vector at the ith gap in the original trie
/// Example: Given a trie with schema: xyz and values [[2], [], [3, 4], [1]]
/// results in a trie with "schema" 2xy34z1
pub fn trie_add_constant(mut trie: Trie, values: &[Vec<DataValueT>]) -> Trie {
    debug_assert!(values.len() == trie.get_types().len() + 1);

    // Add the new columns
    let mut new_columns = VecDeque::<ColumnWithIntervalsT>::new();
    for gap_index in (0..=trie.get_types().len()).rev() {
        let current_values = &values[gap_index];
        for value_t in current_values.iter().rev() {
            macro_rules! append_columns_for_datatype {
                ($value:ident, $variant:ident, $type:ty, $zero:expr) => {{
                    let target_length = gap_index.checked_sub(1).map(|i| trie.get_column(i).len()).unwrap_or(1);

                    // TODO: if this is performance critical, the ColumnRle should get an extra constructor for sequences of fixed increments
                    let new_data_column = std::iter::repeat($value).take(target_length).collect::<ColumnBuilderAdaptive<$type>>().finalize();
                    let new_interval_column = (0..target_length).collect::<ColumnBuilderAdaptive<usize>>().finalize();

                    new_columns.push_front(ColumnWithIntervalsT::$variant(
                        ColumnWithIntervals::new(
                            new_data_column,
                            new_interval_column,
                        ),
                    ));
                }};
            }

            match *value_t {
                DataValueT::U32(value) => append_columns_for_datatype!(value, U32, u32, 0),
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

    Trie::new(Vec::<ColumnWithIntervalsT>::from(new_columns))
}

/// Duplicates existing columns to new positions
/// However, this is limited so duplicated columns need to appear after the original one
/// Columns to be copied are given as a slice of [`usize`] vectors
/// Its interpreted as follows:
/// The usizes are interpreted as column indeces of the existing trie.
/// Append the duplicates of the columns in the ith vector at the ith gap in the original trie
/// Example: Given a trie with schema: xyz and values [[], [1], [0, 2]]
/// results in a trie with "schema" xyyzxz
pub fn trie_add_duplicates(trie: Trie, indices: &[Vec<usize>]) -> Trie {
    debug_assert!(indices.len() == trie.get_types().len());

    // Add the new columns
    let new_columns = VecDeque::<ColumnWithIntervalsT>::new();
    for gap_index in (0..=trie.get_types().len()).rev() {
        let current_indices = &indices[gap_index];
        for &index in current_indices.iter().rev() {
            if let ColumnWithIntervalsT::U64(reference_column) = trie.get_column(index) {
                let _ = reference_column.get_data_column();
            }
        }
    }

    Trie::new(Vec::<ColumnWithIntervalsT>::from(new_columns))
}

#[cfg(test)]
mod test {

    use crate::physical::{
        columnar::traits::columnscan::ColumnScanT,
        datatypes::DataValueT,
        tabular::{
            table_types::trie::{Trie, TrieScanGeneric},
            traits::triescan::TrieScan,
        },
        util::make_column_with_intervals_t,
    };

    use super::trie_add_constant;

    fn scan_next(int_scan: &mut TrieScanGeneric) -> Option<u64> {
        if let ColumnScanT::U64(rcs) = unsafe { &(*int_scan.current_scan()?.get()) } {
            rcs.next()
        } else {
            panic!("type should be u64");
        }
    }

    fn scan_current(int_scan: &mut TrieScanGeneric) -> Option<u64> {
        unsafe {
            if let ColumnScanT::U64(rcs) = &(*int_scan.current_scan()?.get()) {
                rcs.current()
            } else {
                panic!("type should be u64");
            }
        }
    }

    #[test]
    fn test_constant() {
        let column_x = make_column_with_intervals_t(&[1, 2, 3], &[0]);
        let column_y = make_column_with_intervals_t(&[2, 4, 1, 5, 9], &[0, 2, 3]);
        let column_z = make_column_with_intervals_t(&[5, 1, 7, 9, 3, 2, 4, 8], &[0, 1, 4, 5, 7]);

        let trie = Trie::new(vec![column_x, column_y, column_z]);
        let trie_appended = trie_add_constant(
            trie,
            &[
                vec![DataValueT::U64(2)],
                vec![],
                vec![DataValueT::U64(3), DataValueT::U64(4)],
                vec![DataValueT::U64(1)],
            ],
        );

        let mut trie_iter = TrieScanGeneric::new(&trie_appended);

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
