//! Module that allows callers to write data into columns of a database table.

use crate::{
    datatypes::{Double, Float, StorageTypeName, StorageValueT},
    datavalues::{AnyDataValue, DataValue},
    dictionary::Dictionary,
    management::database::Dict,
};
use std::cell::RefCell;
use std::cmp::Ordering;

/// Number of supported [`StorageTypeName`]s. See also [TableWriter::storage_type_idx].
const STORAGE_TYPE_COUNT: usize = 5;

/// Record that stores column ids and [`StorageTypeName`]s for each
/// colun in a table. This is used internally in [`TableWriter`].
#[derive(Debug)]
struct TypedTableRecord {
    col_ids: Vec<usize>,
    col_types: Vec<StorageTypeName>,
}

/// The [`ColumnWriter`] is used to send the data of a single table column to the database. The interface
/// allows values to be added one by one, and also provides some functionality for rolling back the last value,
/// which is convenient when writing whole tuples
#[derive(Debug)]
pub struct TableWriter<'a> {
    /// Dictionary that will be used in encoding some kinds of values for which we have no native representation.
    dict: &'a RefCell<Dict>,
    /// Number of columns in the table.
    col_num: usize,
    /// State of the current row. This is an internal buffer that will be written only when all columns have received a value.
    cur_row: Vec<AnyDataValue>,
    /// Current column in the current row that is to be filled next.
    cur_col_idx: usize,
    /// Representation of the internal values of the current row, filled only on commit.
    /// In particular, we do not make dictionary ids before really committing to a row.
    cur_row_storage_values: Vec<StorageValueT>,

    /// List of known typed tables
    tables: Vec<TypedTableRecord>,
    /// Current size of tables (in same order as in [`TableWriter::tables`])
    table_lengths: Vec<usize>,
    /// A linearlized trie data structure to find the table for a given list of column [`StorageTypeName`]s.
    /// The types correspond to numbers between `0` and [`STORAGE_TYPE_COUNT`]-1. The trie is a tree structure
    /// that has [`STORAGE_TYPE_COUNT`] branching degree but can be partial: a slice of length [`STORAGE_TYPE_COUNT`]
    /// can encode one inner node in the tree, where the usizes refer to the offset of the child node of the tree
    /// for each [`StorageTypeName`], or (on the last level) to the index of the respective [`TypedTableRecord`]
    /// in [TableWriter::tables]+1. In each case, number `0` indicates that no child node/no table has been alocated yet
    /// for this type combination or partial path.
    table_trie: Vec<usize>,

    //Vector of sorted (table_idx, row_idx) in [`TableWriter`].
    order: Vec<(usize, usize)>,

    /// List of all u64 value columns used across the tables.
    cols_u64: Vec<Vec<u64>>,
    /// List of all u32 value columns used across the tables.
    cols_u32: Vec<Vec<u32>>,
    /// List of all Float value columns used across the tables.
    cols_floats: Vec<Vec<Float>>,
    /// List of all Double value columns used across the tables.
    cols_doubles: Vec<Vec<Double>>,
    /// List of all i64 value columns used across the tables.
    cols_i64: Vec<Vec<i64>>,
}

impl<'a> TableWriter<'a> {
    /// Construct a new [`TableWriter`]. This is public to allow
    /// downstream implementations of [`crate::datasources::TableProvider`] to
    /// test their code.
    pub fn new(dict: &'a RefCell<Dict>, column_count: usize) -> Self {
        let mut cur_row = Vec::with_capacity(column_count);
        let mut cur_row_storage_values = Vec::with_capacity(column_count);
        let mut table_trie = Vec::with_capacity(column_count * STORAGE_TYPE_COUNT);

        let dummy_value = AnyDataValue::new_integer_from_i64(0);
        let dummy_storage_value = StorageValueT::Int64(0);
        for _i in 0..column_count {
            cur_row.push(dummy_value.clone());
            cur_row_storage_values.push(dummy_storage_value);
        }

        for _i in 0..STORAGE_TYPE_COUNT {
            table_trie.push(0);
        }

        let initial_capacity = 10;

        TableWriter {
            dict: dict,
            col_num: column_count,
            cur_row: cur_row,
            cur_col_idx: 0,
            cur_row_storage_values: cur_row_storage_values,
            tables: Vec::with_capacity(initial_capacity),
            table_lengths: Vec::with_capacity(initial_capacity),
            table_trie: table_trie,
            order: Vec::new(),
            cols_u64: Vec::with_capacity(initial_capacity),
            cols_u32: Vec::with_capacity(initial_capacity),
            cols_floats: Vec::with_capacity(initial_capacity),
            cols_doubles: Vec::with_capacity(initial_capacity),
            cols_i64: Vec::with_capacity(initial_capacity),
        }
    }

    /// Returns the number of columns on the table, i.e., the
    /// number of values that need to be written to make one row.
    pub fn column_number(&self) -> usize {
        self.col_num
    }

    /// Provide the next value that is to be added to the table. Values are added in a row based
    /// fashion. When the value for the last column was provided, the row is committed to the
    /// table. Alternatively, a partially built row can be abandonded by calling
    /// [`drop_current_row`](Column_Writer::drop_current_row).
    pub fn next_value(&mut self, value: AnyDataValue) {
        self.cur_row[self.cur_col_idx] = value;
        self.cur_col_idx += 1;
        if self.cur_col_idx >= self.col_num {
            self.cur_col_idx = 0;
            self.write_row();
        }
    }

    /// Forget about any previously added values that have not formed a complete row yet,
    /// and start a new row from the beginning.
    pub fn drop_current_row(&mut self) {
        self.cur_col_idx = 0;
    }

    pub(super) fn sort(&mut self) {
        // initialize
        let mut order: Vec<(usize, usize)> = Vec::with_capacity(self.size());
        // populate self.order
        for i in 0..self.table_lengths.len() {
            for j in 0..self.table_lengths[i] {
                order.push((i, j));
            }
        }
        // sort
        order.sort_by(
            |(first_table_idx, first_row_idx), (second_table_idx, second_row_idx)| {
                self.compare_rows(
                    &first_table_idx,
                    &first_row_idx,
                    &second_table_idx,
                    &second_row_idx,
                )
            },
        );
        //save
        self.order = order.clone();
    }

    /// Given two table indexes and rows indexes, compare the row indexes by type and value.
    fn compare_rows(
        &self,
        first_table_idx: &usize,
        first_row_idx: &usize,
        second_table_idx: &usize,
        second_row_idx: &usize,
    ) -> Ordering {
        let first_table: &TypedTableRecord = &self.tables[*first_table_idx];
        let second_table: &TypedTableRecord = &self.tables[*second_table_idx];

        for i in 0..self.col_num {
            match (first_table.col_types[i], second_table.col_types[i]) {
                (StorageTypeName::Id32, StorageTypeName::Id32) => {
                    let first = &self.cols_u32[first_table.col_ids[i]][*first_row_idx];
                    let second = &self.cols_u32[second_table.col_ids[i]][*second_row_idx];
                    if first.cmp(&second) != Ordering::Equal {
                        return first.cmp(&second);
                    }
                }
                (StorageTypeName::Id64, StorageTypeName::Id64) => {
                    let first = &self.cols_u64[first_table.col_ids[i]][*first_row_idx];
                    let second = &self.cols_u64[second_table.col_ids[i]][*second_row_idx];
                    if first.cmp(&second) != Ordering::Equal {
                        return first.cmp(&second);
                    }
                }
                (StorageTypeName::Int64, StorageTypeName::Int64) => {
                    let first = &self.cols_i64[first_table.col_ids[i]][*first_row_idx];
                    let second = &self.cols_i64[second_table.col_ids[i]][*second_row_idx];
                    if first.cmp(second) != Ordering::Equal {
                        return first.cmp(second);
                    }
                }
                (StorageTypeName::Float, StorageTypeName::Float) => {
                    let first = &self.cols_floats[first_table.col_ids[i]][*first_row_idx];
                    let second = &self.cols_floats[second_table.col_ids[i]][*second_row_idx];
                    if first.cmp(second) != Ordering::Equal {
                        return first.cmp(second);
                    }
                }
                (StorageTypeName::Double, StorageTypeName::Double) => {
                    let first = &self.cols_doubles[first_table.col_ids[i]][*first_row_idx];
                    let second = &self.cols_doubles[second_table.col_ids[i]][*second_row_idx];
                    if first.cmp(second) != Ordering::Equal {
                        return first.cmp(second);
                    }
                }
                (_first, _second) => {
                    let first = Self::storage_type_idx(_first);
                    let second = Self::storage_type_idx(_second);
                    let ordering = first.cmp(&second);
                    if ordering != Ordering::Equal {
                        return ordering;
                    }
                }
            }
        }
        Ordering::Equal
    }

    /// Returns the values on the nth column in the [`TableWriter`] represented by an iterator of
    /// [`StorageValueT`]s.
    pub(super) fn get_column<'b>(
        &'b self,
        column_idx: &'b usize,
    ) -> impl Iterator<Item = StorageValueT> + 'b {
        self.order
            .iter()
            .map(|(table_idx, row_idx)| self.get_storage_value(table_idx, column_idx, row_idx))
    }

    /// Return the [`StorageValueT`] corresponding to the [`table_idx`], [`column_idx`], and [`row_idx`].
    fn get_storage_value(
        &self,
        table_idx: &usize,
        column_idx: &usize,
        row_idx: &usize,
    ) -> StorageValueT {
        let deref_col_idx = self.tables[*table_idx].col_ids[*column_idx];
        match self.tables[*table_idx].col_types[*column_idx] {
            StorageTypeName::Id32 => StorageValueT::Id32(self.cols_u32[deref_col_idx][*row_idx]),
            StorageTypeName::Id64 => StorageValueT::Id64(self.cols_u64[deref_col_idx][*row_idx]),
            StorageTypeName::Int64 => StorageValueT::Int64(self.cols_i64[deref_col_idx][*row_idx]),
            StorageTypeName::Float => {
                StorageValueT::Float(self.cols_floats[deref_col_idx][*row_idx])
            }
            StorageTypeName::Double => {
                StorageValueT::Double(self.cols_doubles[deref_col_idx][*row_idx])
            }
        }
    }

    /// Returns the number of rows in the [`TableWriter`]
    pub fn size(&self) -> usize {
        self.table_lengths.iter().sum()
    }

    /// Returns the value of the row number `val_index` (according to the internal
    /// order of rows, not the insertion order) at position number `col_index`.
    /// None is returned if there are fewer values than `val_index`.
    pub(crate) fn get_value(&self, val_index: usize, col_index: usize) -> Option<StorageValueT> {
        let mut table_index = 0;
        let mut rows_before = 0;
        while self.tables.len() > table_index
            && rows_before + self.table_lengths[table_index] <= val_index
        {
            rows_before += self.table_lengths[table_index];
            table_index += 1;
        }

        if table_index == self.tables.len() {
            return None;
        }

        let local_val_index = val_index - rows_before;
        let idx_in_value_cols = self.tables[table_index].col_ids[col_index];
        match self.tables[table_index].col_types[col_index] {
            StorageTypeName::Id32 => Some(StorageValueT::Id32(
                self.cols_u32[idx_in_value_cols][local_val_index],
            )),
            StorageTypeName::Id64 => Some(StorageValueT::Id64(
                self.cols_u64[idx_in_value_cols][local_val_index],
            )),
            StorageTypeName::Int64 => Some(StorageValueT::Int64(
                self.cols_i64[idx_in_value_cols][local_val_index],
            )),
            StorageTypeName::Float => Some(StorageValueT::Float(
                self.cols_floats[idx_in_value_cols][local_val_index],
            )),
            StorageTypeName::Double => Some(StorageValueT::Double(
                self.cols_doubles[idx_in_value_cols][local_val_index],
            )),
        }
    }

    /// Writes the current row to the stored columns.
    fn write_row(&mut self) {
        for i in 0..self.col_num {
            self.cur_row_storage_values[i] = self.make_storage_value(i);
        }

        let table_record_id: usize = self.find_current_row_table();
        let table_record: &TypedTableRecord = &self.tables[table_record_id];

        for i in 0..self.col_num {
            match self.cur_row_storage_values[i] {
                StorageValueT::Id32(v) => {
                    self.cols_u32[table_record.col_ids[i]].push(v);
                }
                StorageValueT::Id64(v) => {
                    self.cols_u64[table_record.col_ids[i]].push(v);
                }
                StorageValueT::Int64(v) => {
                    self.cols_i64[table_record.col_ids[i]].push(v);
                }
                StorageValueT::Float(v) => {
                    self.cols_floats[table_record.col_ids[i]].push(v);
                }
                StorageValueT::Double(v) => {
                    self.cols_doubles[table_record.col_ids[i]].push(v);
                }
            }
        }
        self.table_lengths[table_record_id] += 1;
    }

    /// Finds a [`TypedTableRecord`] for the types required by the current values of
    /// [TableWriter::cur_row_storage_values], and creates a new one if required.
    /// The function searches for the correct value in a prefix trie for the list of types,
    /// and updates this structure to capture the new value, if necessary.
    /// Returns an index of [`TableWriter::tables`].
    fn find_current_row_table(&mut self) -> usize {
        let mut table_trie_block: usize = 0;
        for i in 0..self.col_num {
            let cur_type_id = Self::storage_type_idx(self.cur_row_storage_values[i].get_type());
            let next_block = self.table_trie[table_trie_block + cur_type_id];
            if next_block == 0 {
                // make new trie nodes and table record below current
                return self.add_current_row_table_record(i, table_trie_block);
            }
            table_trie_block = next_block;
        }
        table_trie_block - 1
    }

    /// Create and insert a new  [`TypedTableRecord`] for the types required by the current values of
    /// [TableWriter::cur_row_storage_values]. The parameters specify the lowest level of the trie and
    /// the location of the corresponding lowest existing trie node on the trie path that would be
    /// required for this record. All trie nodes below are newly initialised to complete the path.
    fn add_current_row_table_record(
        &mut self,
        last_trie_level: usize,
        last_trie_block: usize,
    ) -> usize {
        let mut child_block = self.table_trie.len(); // start of the first new trie node block used below

        // add empty trie node slots for all remaining levels:
        self.table_trie.resize(
            self.table_trie.len() + (self.col_num - 1 - last_trie_level) * STORAGE_TYPE_COUNT,
            0,
        );

        // record path to leaf node:
        let mut cur_block = last_trie_block;
        for i in last_trie_level..self.col_num - 1 {
            let cur_type_id = Self::storage_type_idx(self.cur_row_storage_values[i].get_type());
            self.table_trie[cur_block + cur_type_id] = child_block;
            cur_block = child_block;
            child_block += STORAGE_TYPE_COUNT;
        }

        // make and store table record and new columns for the table's contents:
        let mut col_ids = Vec::with_capacity(self.col_num);
        let mut col_types = Vec::with_capacity(self.col_num);
        for i in 0..self.col_num {
            let st = self.cur_row_storage_values[i].get_type();
            col_types.push(st);
            match st {
                StorageTypeName::Id32 => {
                    col_ids.push(self.cols_u32.len());
                    self.cols_u32.push(Vec::new());
                }
                StorageTypeName::Id64 => {
                    col_ids.push(self.cols_u64.len());
                    self.cols_u64.push(Vec::new());
                }
                StorageTypeName::Int64 => {
                    col_ids.push(self.cols_i64.len());
                    self.cols_i64.push(Vec::new());
                }
                StorageTypeName::Float => {
                    col_ids.push(self.cols_floats.len());
                    self.cols_floats.push(Vec::new());
                }
                StorageTypeName::Double => {
                    col_ids.push(self.cols_doubles.len());
                    self.cols_doubles.push(Vec::new());
                }
            }
        }
        let new_table_id = self.tables.len();
        let typed_table_record = TypedTableRecord { col_ids, col_types };
        self.table_trie[cur_block
            + Self::storage_type_idx(self.cur_row_storage_values[self.col_num - 1].get_type())] =
            new_table_id + 1; // we recerve 0 for "no entry" in the trie
                              // storing new table
        self.tables.push(typed_table_record);
        self.table_lengths.push(0);

        new_table_id
    }

    /// Creates a suitable [`StorageValueT`] for the given data value.
    /// This can lead to changes in the dictionary.
    fn make_storage_value(&mut self, col_idx: usize) -> StorageValueT {
        let dv = &self.cur_row[col_idx];
        match dv {
            AnyDataValue::String(iv) => {
                // TODO: the string encoding with surrounding quotes is a cheap way to avoid confusion with IRIs.
                // Final solution should be a dictionary that accepts and reproduces datavalues.
                // TODO: A better approach would consume the dv to move the string instead of cloning it.
                let dict_id = self
                    .dict
                    .borrow_mut()
                    .add_string("\"".to_owned() + &iv.to_string_unchecked() + "\"")
                    .value();
                Self::storage_value_for_usize(dict_id)
            }
            AnyDataValue::LanguageTaggedString(_) => {
                todo!("We still need the dictionary to support this case properly")
            }
            AnyDataValue::Iri(iv) => {
                // TODO: the string encoding with surrounding brackets is a cheap way to avoid confusion with IRIs.
                // Final solution should be a dictionary that accepts and reproduces datavalues.
                // TODO: A better approach would consume the dv to move the string instead of cloning it.
                let dict_id = self
                    .dict
                    .borrow_mut()
                    .add_string("<".to_owned() + &iv.to_iri_unchecked() + ">")
                    .value();
                Self::storage_value_for_usize(dict_id)
            }
            AnyDataValue::Double(iv) => {
                StorageValueT::Double(Double::from_number(iv.to_f64_unchecked()))
            }
            AnyDataValue::UnsignedLong(iv) => Self::storage_value_for_u64(iv.to_u64_unchecked()),
            AnyDataValue::Long(iv) => StorageValueT::Int64(iv.to_i64_unchecked()),
            AnyDataValue::Other(odv) => {
                todo!("Other datavalue {:?} not supported in dictionary yet.", odv)
            }
        }
    }

    /// Find a good [`StorageValueT`] for a u64 number, considered as an actual integer (not as a dictionary id).
    ///
    /// TODO: Open design issue: Should we even support such values in [`DataValue`], since the internal types do not cover
    /// them in all cases, and moreover since there are always integers outside any i64 or u64 range that we have to handle
    /// separately anyway?
    fn storage_value_for_u64(value: u64) -> StorageValueT {
        if let Ok(i64value) = value.try_into() {
            StorageValueT::Int64(i64value)
        } else {
            todo!("Integers that are outside of the i64 range need some fallback representation, probably using the dictionary.")
        }
    }

    /// Find a good [`StorageValueT`] for a usize number. The method uses [`StorageValueT::U32`]
    /// if possible to safe memory.
    fn storage_value_for_usize(value: usize) -> StorageValueT {
        if let Ok(u32value) = value.try_into() {
            StorageValueT::Id32(u32value)
        } else {
            StorageValueT::Id64(value.try_into().unwrap())
        }
    }

    /// Assign numbers to [`StorageTypeName`]s in order to use them for addressing
    /// vectors.
    /// TODO see https://users.rust-lang.org/t/getting-the-position-index-of-an-enum/68311
    fn storage_type_idx(stn: StorageTypeName) -> usize {
        match stn {
            StorageTypeName::Id32 => 0,
            StorageTypeName::Id64 => 1,
            StorageTypeName::Int64 => 2,
            StorageTypeName::Float => 3,
            StorageTypeName::Double => 4,
        }
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use crate::{datatypes::StorageValueT, datavalues::AnyDataValue, management::database::Dict};

    use super::TableWriter;

    #[test]
    fn test_internal_table_structures() {
        let dict = Dict::new();
        let dict_ref = RefCell::new(dict);
        let mut tw = TableWriter::new(&dict_ref, 3);

        let v1 = AnyDataValue::new_string("a".to_string());
        let v2 = AnyDataValue::new_integer_from_i64(42);

        // new table #0, row #0
        tw.next_value(v1.clone());
        tw.next_value(v1.clone());
        tw.next_value(v1.clone());
        // new table #1, row #0
        tw.next_value(v1.clone());
        tw.next_value(v2.clone());
        tw.next_value(v1.clone());
        // table #0, row #1
        tw.next_value(v1.clone());
        tw.next_value(v1.clone());
        tw.next_value(v1.clone());
        // new table #2, row #0
        tw.next_value(v1.clone());
        tw.next_value(v1.clone());
        tw.next_value(v2.clone());
        // table #2, row #1
        tw.next_value(v1.clone());
        tw.next_value(v1.clone());
        tw.next_value(v2.clone());
        // table #2, row #2
        tw.next_value(v1.clone());
        tw.next_value(v1.clone());
        tw.next_value(v2.clone());

        assert_eq!(tw.tables.len(), 3);

        assert_eq!(tw.table_lengths[0], 2);
        assert_eq!(tw.cols_u32[tw.tables[0].col_ids[0]].len(), 2);
        assert_eq!(tw.cols_u32[tw.tables[0].col_ids[1]].len(), 2);
        assert_eq!(tw.cols_u32[tw.tables[0].col_ids[2]].len(), 2);

        assert_eq!(tw.table_lengths[1], 1);
        assert_eq!(tw.cols_u32[tw.tables[1].col_ids[0]].len(), 1);
        assert_eq!(tw.cols_i64[tw.tables[1].col_ids[1]].len(), 1);
        assert_eq!(tw.cols_u32[tw.tables[1].col_ids[2]].len(), 1);

        assert_eq!(tw.table_lengths[2], 3);
        assert_eq!(tw.cols_u32[tw.tables[2].col_ids[0]].len(), 3);
        assert_eq!(tw.cols_u32[tw.tables[2].col_ids[1]].len(), 3);
        assert_eq!(tw.cols_i64[tw.tables[2].col_ids[2]].len(), 3);

        assert_eq!(tw.get_value(0, 0), tw.get_value(0, 1));
        assert_ne!(tw.get_value(2, 1), tw.get_value(0, 0));
        assert_eq!(tw.get_value(2, 1), Some(StorageValueT::Int64(42)));
    }

    #[test]
    fn test_column_iterator_one_col() {
        let dict = Dict::new();
        let dict_ref = RefCell::new(dict);
        let mut tw = TableWriter::new(&dict_ref, 1);

        let v1 = AnyDataValue::new_string("c".to_string()); // 0
        let v2 = AnyDataValue::new_string("a".to_string()); // 1
        let v3 = AnyDataValue::new_string("b".to_string()); // 2
        let v4 = AnyDataValue::new_string("d".to_string()); // 3

        tw.next_value(v1.clone());
        tw.next_value(v2.clone());
        tw.next_value(v3.clone());
        tw.next_value(v4.clone());

        tw.sort();

        let mut iter = tw.get_column(&0);
        assert_eq!(iter.next(), Some(StorageValueT::Id32(0)));
        assert_eq!(iter.next(), Some(StorageValueT::Id32(1)));
        assert_eq!(iter.next(), Some(StorageValueT::Id32(2)));
        assert_eq!(iter.next(), Some(StorageValueT::Id32(3)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_column_iterator_two_cols() {
        let dict = Dict::new();
        let dict_ref = RefCell::new(dict);
        let mut tw = TableWriter::new(&dict_ref, 2);

        let v1 = AnyDataValue::new_integer_from_i64(1);
        let v2 = AnyDataValue::new_integer_from_i64(2);
        let v3 = AnyDataValue::new_integer_from_i64(3);
        let v4 = AnyDataValue::new_integer_from_i64(4);
        let v10 = AnyDataValue::new_integer_from_i64(10);
        let v20 = AnyDataValue::new_integer_from_i64(20);

        let va = AnyDataValue::new_string("a".to_string()); // 0
        let vb = AnyDataValue::new_string("b".to_string()); // 1

        // 1 "a"
        tw.next_value(v1.clone());
        tw.next_value(va.clone());
        // 2 10
        tw.next_value(v2.clone());
        tw.next_value(v10.clone());
        // 3 "b"
        tw.next_value(v3.clone());
        tw.next_value(vb.clone());
        // 4 20
        tw.next_value(v4.clone());
        tw.next_value(v20.clone());

        tw.sort();

        let mut first_iter = tw.get_column(&0);
        assert_eq!(first_iter.next(), Some(StorageValueT::Int64(1)));
        assert_eq!(first_iter.next(), Some(StorageValueT::Int64(2)));
        assert_eq!(first_iter.next(), Some(StorageValueT::Int64(3)));
        assert_eq!(first_iter.next(), Some(StorageValueT::Int64(4)));
        assert_eq!(first_iter.next(), None);

        let mut second_iter = tw.get_column(&1);
        assert_eq!(second_iter.next(), Some(StorageValueT::Id32(0)));
        assert_eq!(second_iter.next(), Some(StorageValueT::Int64(10)));
        assert_eq!(second_iter.next(), Some(StorageValueT::Id32(1)));
        assert_eq!(second_iter.next(), Some(StorageValueT::Int64(20)));
        assert_eq!(second_iter.next(), None);
    }
}
