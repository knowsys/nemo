//! This module defines data structures for storing row-based data in arbitrary types.

pub(crate) mod sorted_tuple_buffer;
pub(crate) mod tuple_buffer;

pub use tuple_buffer::TupleBuffer;
