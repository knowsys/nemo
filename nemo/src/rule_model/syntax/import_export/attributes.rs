//! This module contains constants relating to accepted attributes

/// Name of the attribute for specifying the resource in import/export directives.
pub(crate) const ATTRIBUTE_NAME_RESOURCE: &str = "resource";
/// Name of the attribute for specifying the format in import/export directives.
pub(crate) const ATTRIBUTE_NAME_FORMAT: &str = "format";
/// Name of the attribute for specifying a base IRI in import/export directives.
pub(crate) const ATTRIBUTE_NAME_BASE: &str = "base";
/// Name of the attribute for specifying a delimiter in import/export directives for delimiter-separated values format.
pub(crate) const ATTRIBUTE_NAME_DSV_DELIMITER: &str = "delimiter";
/// Name of the attribute for specifying the compression in import/export directives.
pub(crate) const ATTRIBUTE_NAME_COMPRESSION: &str = "compression";
/// Name of the attribute for specifying the limit in import/export directives.
pub(crate) const ATTRIBUTE_NAME_LIMIT: &str = "limit";
