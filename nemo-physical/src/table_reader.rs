//! This modules defines the [`TableReader`] trait, which is part of the interface to nmo_logical.
//!

use crate::{builder_proxy::PhysicalBuilderProxyEnum, error::ReadingError};

/// Resource that can be referenced in source declarations in Nemo programs
/// Resources are resolved using `nemo::io::resource_providers::ResourceProviders`
///
/// Resources currently can be either an IRI or a (possibly relative) file path.
pub type Resource = String;

/// A general interface for reading tables from resources.
///
/// This is called from the physical layer to ask a reader to fill the
/// Vector of [builder proxies][PhysicalBuilderProxyEnum].
///
/// # Note
/// This is the physical interface to access all readers, instantiated on the logical layer.
/// Therefore every reader needs to implement this trait.
pub trait TableReader: std::fmt::Debug {
    /// Read the table into multiple [`ColumnBuilderProxy`][crate::builder_proxy::PhysicalColumnBuilderProxy]
    fn read_into_builder_proxies<'a: 'b, 'b>(
        self: Box<Self>,
        builder_proxies: &'b mut Vec<PhysicalBuilderProxyEnum<'a>>,
    ) -> Result<(), ReadingError>;
}
