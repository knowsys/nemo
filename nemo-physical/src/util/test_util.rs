//! This module contains useful functions for testing trie related operations

#[cfg(test)]
pub(crate) mod test {
    use hashbrown::HashMap;

    use crate::{
        datatypes::{StorageTypeName, StorageValueT},
        tabular::{trie::Trie, triescan::PartialTrieScan},
    };

    /// Create a [Trie] from table rows of the 32-bit id type.
    pub(crate) fn trie_id32(rows: Vec<&[u32]>) -> Trie {
        let rows = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|&entry| StorageValueT::Id32(entry))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        Trie::from_rows(rows)
    }

    /// Create a [Trie] from table rows of the 64-bit integer type.
    pub(crate) fn trie_int64(rows: Vec<&[i64]>) -> Trie {
        let rows = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|&entry| StorageValueT::Int64(entry))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        Trie::from_rows(rows)
    }

    /// Return the current value of the [PartialTrieScan].
    pub(crate) fn partial_scan_current<'a, Scan: PartialTrieScan<'a>>(
        scan: &mut Scan,
        storage_type: StorageTypeName,
    ) -> Option<StorageValueT> {
        let column_scan = unsafe { &mut *scan.current_scan()?.get() };

        column_scan.current(storage_type)
    }

    /// Return the current value of the [PartialTrieScan] at some given layer.
    pub(crate) fn partial_scan_current_at_layer<'a, Scan: PartialTrieScan<'a>>(
        scan: &Scan,
        storage_type: StorageTypeName,
        layer: usize,
    ) -> Option<StorageValueT> {
        let column_scan = unsafe { &mut *scan.scan(layer).get() };

        column_scan.current(storage_type)
    }

    /// Move to the next value on the current layer of the [PartialTrieScan].
    pub(crate) fn partial_scan_next<'a, Scan: PartialTrieScan<'a>>(
        scan: &Scan,
        storage_type: StorageTypeName,
    ) -> Option<StorageValueT> {
        let column_scan = unsafe { &mut *scan.current_scan()?.get() };

        column_scan.next(storage_type)
    }

    /// Move to the next valu eon the current layer of the [PartialTrieScan]
    /// that is greater than the given value.
    pub(crate) fn partial_scan_seek<'a, Scan: PartialTrieScan<'a>>(
        scan: &Scan,
        value: StorageValueT,
    ) -> Option<StorageValueT> {
        let column_scan = unsafe { &mut *scan.current_scan()?.get() };

        column_scan.seek(value)
    }

    /// Navigate a [PartialTrieScan] in a depth first search manner,
    /// restricted to the given types,
    /// and compare the results to a list of expected result.
    pub(crate) fn trie_dfs<'a, Scan: PartialTrieScan<'a>>(
        scan: &mut Scan,
        types: &[StorageTypeName],
        expected: &[StorageValueT],
    ) {
        let mut next_type_map = HashMap::<StorageTypeName, StorageTypeName>::new();
        for adjacent_types in types.windows(2) {
            next_type_map.insert(adjacent_types[0].clone(), adjacent_types[1].clone());
        }

        let first_type = types.first().unwrap().clone();
        scan.down(first_type);
        assert_eq!(partial_scan_current(scan, first_type), None);

        let mut current_expected_index: usize = 0;

        while let Some(current_type) = scan.current_layer().map(|layer| scan.path_types()[layer]) {
            if let Some(next_value) = partial_scan_next(scan, current_type) {
                let current_expected_value = expected[current_expected_index];

                assert_eq!(current_expected_value, next_value);
                current_expected_index += 1;

                if scan.current_layer().unwrap() < scan.arity() - 1 {
                    scan.down(first_type);
                    assert_eq!(partial_scan_current(scan, first_type), None);
                }
            } else {
                scan.up();

                if let Some(next_type) = next_type_map.get(&current_type).cloned() {
                    scan.down(next_type);
                    assert_eq!(partial_scan_current(scan, next_type), None);
                }
            }
        }

        assert_eq!(current_expected_index, expected.len());
    }
}
