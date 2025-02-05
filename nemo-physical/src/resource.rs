//! This modules defines the [Resource] type, which is part of the public interface of this crate.
//!

use core::fmt;

/// Resource that can be referenced in source declarations in Nemo programs
/// Resources are resolved using `nemo::io::resource_providers::ResourceProviders`
///
/// Resources currently can be either an IRI or a (possibly relative) file path.
/// TODO: It is not clear why this needs to be in `nemo-physical` given that it is only used and
/// resolved by code in `nemo`.
///

use oxiri::Iri;
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Parameters {
    pub query: Option<String>,
    // header
}

/// Define two Resource types that are used for Import and Export
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resource {
    /// Specify location for a local source or target
    Path(String),
    /// Specify location for a web source or target
    Iri{
        /// Contains the endpoint / http address for a request in the form of an Iri
        iri: Iri<String>,
        /// A collection of parameters that are passed considered in the http-request
        /// Each tuple consists of a keyword & the value e.g. ("query", "SELECT ...")
        /// Currently query is the onliest keyword allowed
        parameters : Parameters,
    }
    
}

impl Resource {
    /// Add parameters to an existing resource
    pub fn add_parameter(&mut self, name: &str, parameter: String) -> Option<()> {
        match self {
            Resource::Path(..)=> None,
            Resource::Iri { parameters, .. } => {
                match name {
                    "query" => {parameters.query = Some(parameter);
                    Some(())
                },
                    _ => None
                }
            }
        }
    }
}

/// Implement Display for Resource enum
impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Resource::Path(p) => write!(f, "{}", p),
            Resource::Iri { iri, parameters } => write!(f, "{} {:?}", iri, parameters)
        }
    }
}