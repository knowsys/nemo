//! This modules defines the [Resource] type, which is part of the public interface of this crate.
//!

use std::fmt;
use urlencoding::encode;

use super::datavalues::{AnyDataValue, DataValue};
/// Resource that can be referenced in source declarations in Nemo programs
/// Resources are resolved using `nemo::io::resource_providers::ResourceProviders`
///
/// Resources currently can be either an IRI or a (possibly relative) file path.
/// TODO: It is not clear why this needs to be in `nemo-physical` given that it is only used and
/// resolved by code in `nemo`.
///
use oxiri::Iri;
use thiserror::Error;

/// Type of error that is returned when parsing of Iri fails
#[derive(Copy, Clone, Debug, Error)]
pub enum ResourceValidationErrorKind {
    /// Parsing error for Iri
    #[error("Invalid IRI format")]
    ImportExportInvalidIri,
    /// Error for wrong attribute type
    #[error("Invalid attribute type")]
    ImportExportAttributeValueType,
}

/// Represents different types of HTTP parameters
#[derive(Debug, Clone, Copy)]
pub enum HttpParamType {
    /// Type of parameters that are passed as headers in web-request
    Headers,
    /// Type of parameters that are merged into the Iri of the web-request
    Get,
    /// Type of parameter that is appended at the Iri of the web-request
    Fragment,
    /// Type of parameters that are passed in the body of a POST-request
    Post,
}

/// Define a series of Parameters that can be passed to the HTTP-Request
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpParameters {
    /// Specifies additional headers for a GET/ POST-request
    pub headers: Option<Vec<(String, String)>>,
    /// Contains all parameters that are added to the Iri
    pub get: Option<Vec<(String, String)>>,
    /// Specifies a fragment (#) that is appended to the Iri after the get-parameters
    pub fragment: Option<String>,
    /// Contains parameters that are send in the body of a POST-request
    pub post: Option<Vec<(String, String)>>,
}

/// Define two Resource types that are used for Import and Export
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resource {
    /// Specify location for a local source or target
    Path(String),
    /// Specify location for a web source or target
    Iri {
        /// Contains the endpoint / http address for a request in the form of an Iri
        iri: Iri<String>,
        /// A collection of parameters that are passed considered in the http-request
        /// Each tuple consists of a keyword & the value e.g. ("query", "SELECT ...")
        /// Currently query is the onliest keyword allowed
        parameters: HttpParameters,
    },
}

impl Resource {
    /// Create a new [Rule].
    pub fn new(
        path: Option<String>,
        iri: Option<Iri<String>>,
        headers: Option<Vec<(String, String)>>,
        get: Option<Vec<(String, String)>>,
        fragment: Option<String>,
        post: Option<Vec<(String, String)>>,
    ) -> Self {
        if let Some(path) = path {
            Self::Path(path)
        } else if let Some(iri) = iri {
            Self::Iri {
                iri: (iri),
                parameters: (HttpParameters {
                    headers,
                    get,
                    fragment,
                    post,
                }),
            }
        } else {
            panic!("Neither path nor iri specified for building a Resource");
        }
    }

    /// Returns the local path of a resource, possibly stripped
    pub fn path() -> Option<String> {
        todo!()
    }

    /// Construct a full Iri that possibly contains the GET- and Fragment Parameter
    pub fn as_string(&self) -> String {
        match self {
            Self::Iri { iri, parameters } => {
                let mut uri = iri.to_string();

                // Construct the full URL with query and fragment
                let get = &parameters.get;
                // If Get parameter are not empty append them ENCODED into the Iri
                if let Some(get) = get {
                    if !get.is_empty() {
                        uri.push('?');
                        uri.push_str(
                            &get.iter()
                                .map(|(key, value)| format!("{}={}", encode(key), encode(value)))
                                .collect::<Vec<String>>()
                                .join("&"),
                        );
                    }
                };

                if let Some(fragment) = &parameters.fragment {
                    uri.push_str(&format!("#{}", fragment));
                }

                uri
            }
            Self::Path(path) => path.clone(), //format!("file://{}", path)
        }
    }

    /// Return the headers if resource is of variant [Resource::Iri]
    pub fn headers(&self) -> Option<&Vec<(String, String)>> {
        match self {
            Self::Iri { parameters, .. } => parameters.headers.as_ref(),
            Self::Path(..) => None,
        }
    }

    /// Return the body parameter of HttpParameters
    pub fn body(&self) -> Option<&Vec<(String, String)>> {
        match self {
            Self::Iri { parameters, .. } => parameters.post.as_ref(),
            Self::Path(..) => None,
        }
    }

    /// Returns the file extension of a path or Iri, based on the last '.'
    pub fn file_extension(&self) -> Option<&str> {
        match self {
            Self::Path(path) => path.rfind('.').map(|index| &path[index..]),
            Self::Iri { iri, .. } => iri.authority()?.rfind('.').map(|index| &iri[index..]),
        }
    }

    /// Remove a certain file extension from the resource
    pub fn strip_file_extension_unchecked(&self, suffix: &str) -> &str {
        match self {
            Self::Path(path) => path
                .strip_suffix(suffix)
                .expect("suffix should ve verified"),
            Self::Iri { iri, .. } => iri.strip_suffix(suffix).expect("suffix should ve verified"),
        }
    }
}

/// Implement Display for Resource enum
impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Resource::Path(p) => write!(f, "{}", p),
            Resource::Iri { iri, parameters } => write!(f, "{} {:?}", iri, parameters),
        }
    }
}

/// Builder for a WebResource
#[derive(Debug, Default)]
pub struct ResourceBuilder {
    path: Option<String>,
    iri: Option<Iri<String>>,
    headers: Option<Vec<(String, String)>>,
    get: Option<Vec<(String, String)>>,
    fragment: Option<String>,
    post: Option<Vec<(String, String)>>,
}

impl ResourceBuilder {

    /// Strips a local iri of its prefix
    pub fn strip_local_iri(string: String) -> String {
        // Could be possibly also handled via the Oxiri/schema
        string
            .strip_prefix("file://localhost")
            .or_else(|| string.strip_prefix("file://"))
            .map(String::from)
            .expect("It was already checked, that string contains at least file://")
    }

    /// Parses a string into a valid Iri
    pub fn to_web_iri(string: String) -> Result<Iri<String>, ResourceValidationErrorKind> {
        Iri::parse(string)
            .map_err(|_| ResourceValidationErrorKind::ImportExportInvalidIri)
    }

    /// Create a new [ResourceBuilder] from a web-Iri
    pub fn from_valid_iri(iri: Iri<String>) -> Self {
        Self {
            iri: Some(iri),
            ..Default::default()
        }
    }

    /// Verify if [ResourceBuilder] contains a valid web-Iri
    pub fn has_iri(&self) -> bool {
        self.iri.is_some()
    }

    /// Register key-value pairs of parameters to the respec
    pub fn parameter_mut(
        &mut self,
        parameter_type: HttpParamType,
        key: String,
        value: String,
    ) -> &mut Self {
        match parameter_type {
            HttpParamType::Headers => {
                self.headers.get_or_insert_with(Vec::new).push((key, value));
            }
            HttpParamType::Get => {
                self.get.get_or_insert_with(Vec::new).push((key, value));
            }
            HttpParamType::Fragment => {
                self.fragment = Some(value);
            }
            HttpParamType::Post => {
                self.post.get_or_insert_with(Vec::new).push((key, value));
            }
        }
        self
    }

    /// Builds a [Resource] with type [Resource::Iri] or [Resource::Path] that contains all registered parameters
    pub fn finalize(self) -> Resource {
        Resource::new(
            self.path,
            self.iri,
            self.headers,
            self.get,
            self.fragment,
            self.post,
        )
    }
}

impl TryFrom<AnyDataValue> for ResourceBuilder {
    type Error = ResourceValidationErrorKind;

    fn try_from(value: AnyDataValue) -> Result<Self, Self::Error> {
        let mut resource_builder = Self::default();
        let string = if let Some(s) = value.to_plain_string() {
            s
        } else if let Some(s) = value.to_iri() {
            s
        } else {
            // Is it even possible that the type is incorrect, since this is verified beforehand?
            return Err(ResourceValidationErrorKind::ImportExportAttributeValueType);
        };

        match string {
            string if string.starts_with("http:") || string.starts_with("https:") => {
                resource_builder.iri = Some(Self::to_web_iri(string)?);
            }
            // Strip a local iri of the prefix
            string if string.starts_with("file://") => {
                resource_builder.path = Some(Self::strip_local_iri(string));
            }
            _ => {
                resource_builder.path = Some(string);
            }
        }
        Ok(resource_builder)
    }
}
