//! Helpers shared by the unit tests: fixtures for values, rows and tries,
//! and a check for documented panics.

use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::{
    execution::InputColumn,
    function::StackValue,
    storage::{PayloadEncoding, StorageValue, Tag},
    trie::{RowBuffer, Trie},
    value::{DataValue, TimezoneOffset},
};

/// Asserts that a call panics with a message that contains `expected`.
#[track_caller]
pub(crate) fn assert_panics<T>(expected: &str, call: impl FnOnce() -> T) {
    let Err(payload) = catch_unwind(AssertUnwindSafe(call)) else {
        panic!("the call returned; a panic mentioning {expected:?} was expected");
    };
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or_default();

    assert!(
        message.contains(expected),
        "the panic {message:?} does not mention {expected:?}"
    );
}

/// Returns the number that a lexical form denotes, and panics if the form is malformed.
pub(crate) fn number(lexical: &str) -> DataValue {
    DataValue::decimal_from_lexical(lexical).expect("a well-formed number lexical")
}

/// Returns the timezone offset of the given number of minutes from UTC.
pub(crate) fn zoned(minutes: i16) -> Option<TimezoneOffset> {
    Some(TimezoneOffset::new(minutes).expect("the offset is in range"))
}

/// Returns the payload of a temporal value, and panics if the value has none.
pub(crate) fn payload<T: PayloadEncoding<Encoded = Option<u64>>>(value: T) -> u64 {
    value
        .to_payload()
        .expect("the value is in the payload range")
}

/// Returns the result of an operation as an owned value, and panics if the operation failed.
pub(crate) fn value(result: StackValue<'_>) -> DataValue {
    result
        .as_result()
        .expect("the operation succeeds")
        .to_owned()
}

/// A number of rows for which the join of a unary relation runs in cells.
///
/// The estimated work of the join, one value visit per row,
/// exceeds the [parallel_work](crate::tuning::Tuning::parallel_work) of the default tuning.
pub(crate) const CELL_ROWS: i64 = 200_000;

/// Returns the storage value of an integer.
pub(crate) fn int(value: i64) -> StorageValue {
    StorageValue::integer(value)
}

/// Returns a buffer of the given arity with the given rows of integers.
pub(crate) fn int_buffer<R: AsRef<[i64]>>(arity: usize, rows: &[R]) -> RowBuffer {
    let mut buffer = RowBuffer::new(arity);
    for row in rows {
        let row: Vec<StorageValue> = row.as_ref().iter().map(|&value| int(value)).collect();
        buffer.push_row(&row);
    }

    buffer
}

/// Returns the trie of the given arity with the given rows.
pub(crate) fn trie<R: AsRef<[StorageValue]>>(arity: usize, rows: &[R]) -> Trie {
    let mut buffer = RowBuffer::new(arity);
    for row in rows {
        buffer.push_row(row.as_ref());
    }

    Trie::from_buffer(buffer)
}

/// Returns the trie of the given arity with the given rows of integers.
pub(crate) fn int_trie<R: AsRef<[i64]>>(arity: usize, rows: &[R]) -> Trie {
    Trie::from_buffer(int_buffer(arity, rows))
}

/// Returns the rows of a trie in storage order,
/// and asserts that their number is the trie's row count.
pub(crate) fn rows(trie: &Trie) -> Vec<Vec<StorageValue>> {
    let mut rows = Vec::new();
    trie.for_each_row(|row| rows.push(row.to_vec()));
    assert_eq!(trie.row_count(), rows.len());

    rows
}

/// Returns the rows of a trie of integers in storage order,
/// and asserts that every value is a 64-bit integer.
pub(crate) fn int_rows(trie: &Trie) -> Vec<Vec<i64>> {
    rows(trie)
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| {
                    assert_eq!(value.tag, Tag::Int);
                    i64::from_payload(value.payload)
                })
                .collect()
        })
        .collect()
}

/// Returns the given column of the given input of a join.
pub(crate) fn at(input: usize, column: usize) -> InputColumn {
    InputColumn { input, column }
}
