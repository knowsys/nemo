//! The streams the formats read and write, independent of what they
//! carry: [compression] around a byte stream, and
//! [remote] resources fetched into one.

pub mod compression;
pub mod remote;
