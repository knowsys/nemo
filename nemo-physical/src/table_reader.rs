//! This modules defines the [`Resource`] type, which is part of the public interface of this crate.
//!

/// Resource that can be referenced in source declarations in Nemo programs
/// Resources are resolved using `nemo::io::resource_providers::ResourceProviders`
///
/// Resources currently can be either an IRI or a (possibly relative) file path.
pub type Resource = String;
