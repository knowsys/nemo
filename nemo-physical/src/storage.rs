//! Tagged order-preserving storage values.
//!
//! A [StorageValue] represents a single value as it is stored in a node of a [trie](crate::trie::Trie).
//! Conceptually, it is a pair of a [Tag], identifying the type of the value,
//! and a 32-bit or 64-bit payload.
//! It encodes primitive values directly,
//! or contains an index into one of the stores of the [dictionary](crate::dictionary::Dictionary)
//! for everything else.
//!
//! The payload encoding of each primitive type preserves order:
//! comparing two payloads of the same tag as unsigned integers
//! compares the values they encode.
//! Ordering by tag first and by payload second
//! gives the "storage order",
//! the total order in which tries keep their values.
//! It is neither the term order of the [values](crate::value), another total order,
//! nor their value order, [value_cmp](crate::value::ValueRef::value_cmp), which is partial.
//! A primitive value that its type's encoding cannot represent
//! is stored in the dictionary under the wide tag of its domain.

pub(crate) mod encoding;
mod tag;
mod value;

pub(crate) use encoding::PayloadEncoding;
pub(crate) use tag::Tag32;

pub use tag::Tag;
pub use value::StorageValue;
