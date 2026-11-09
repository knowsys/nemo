//! The byte streams that the formats read and write, independent of their content.
//!
//! [compression] wraps a byte stream to compress or decompress it,
//! and [remote] has the requests for remote resources and the clients that send them.

pub mod compression;
pub mod remote;
