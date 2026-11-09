//! Storage values, the tagged and order-preserving form in which tries store values.
//!
//! A [StorageValue] is a single value as it is stored in a node of a
//! [trie](crate::trie::Trie).
//! Conceptually, it is a pair of a [Tag], which identifies the type of the value,
//! and a 32-bit or 64-bit payload.
//! It encodes an *inline* value directly in its payload,
//! and for every other value it contains an index into one of the stores of the
//! [dictionary](crate::dictionary::Dictionary).
//!
//! The payload encoding of each inline type preserves order:
//! comparing two payloads of the same tag as unsigned integers
//! compares the values that they encode.
//! Ordering by tag first and by payload second gives the *storage order*,
//! the total order in which tries keep their values.
//! It is neither the term order of the [values](crate::value), another total order,
//! nor their value order, [value_cmp](crate::value::ValueRef::value_cmp), which is partial.
//! A value of an inline type that the encoding cannot represent
//! is stored in the dictionary under the wide tag of its domain.

pub(crate) mod encoding;
mod tag;
mod value;

pub(crate) use encoding::PayloadEncoding;
pub(crate) use tag::Tag32;

pub use tag::Tag;
pub use value::StorageValue;
