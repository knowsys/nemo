//! Physical layer aggregates, which have no understanding of the logical meaning of the aggregated values.
//!
//! This for example allows aggregating [`crate::datatypes::StorageTypeName::U64`] values, even tough this may not make sense on the logical layer,
//! where these values may correspond to dictionary entries in arbitrary order.
//! Thus, any users of this module have to ensure they use the aggregate operations in a logically meaningful way.

pub mod operation;
pub mod processors;
