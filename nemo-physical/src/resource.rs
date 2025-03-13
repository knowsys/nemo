//! This modules defines the [Resource] type, which is part of the public interface of this crate.
//!

use super::datavalues::{AnyDataValue, DataValue, ValueDomain};
use log::debug;
/// Resource that can be referenced in source declarations in Nemo programs
/// Resources are resolved using `nemo::io::resource_providers::ResourceProviders`
///
/// Resources currently can be either an IRI or a (possibly relative) file path.
/// TODO: It is not clear why this needs to be in `nemo-physical` given that it is only used and
/// resolved by code in `nemo`.
///
use oxiri::Iri;
use path_slash::PathBufExt;
use serde;
use std::collections::HashSet;
use std::{fmt, iter, path::PathBuf};
use thiserror::Error;

/// Type of error that is returned when parsing of Iri fails
#[derive(Clone, Debug, Error)]
pub enum ResourceValidationErrorKind {
    /// Path is not valid
    #[error("Invalid path")]
    InvalidPath,
    /// IRI is not valid
    #[error("Invalid IRI")]
    InvalidIri,
    /// Resource has invalid [ValueDomain]
    /// #"parameter `{parameter}` was given as a `{given}`, expected `{expected}`"#
    #[error("Resource was given as {:?}, expected: {:?}", given, expected)]
    InvalidResourceFormat {
        /// Collection of expected [ValueDomain] for HTTP parameter
        expected: Vec<ValueDomain>,
        /// The actual [ValueDomain] of the HTTP parameter
        given: ValueDomain,
    },
    /// HTTP parameter is invalid
    #[error("HTTP parameter was given as {:?}, expected: {:?}", expected, given)]
    HttpParameterNotInValueDomain {
        /// Collection of expected [ValueDomain] for HTTP parameter
        expected: Vec<ValueDomain>,
        /// The actual [ValueDomain] of the HTTP parameter
        given: ValueDomain,
    },
    /// HTTP parameter given for non-HTTP resource
    #[error("Unexpected HTTP parameter for non-HTTP resource")]
    UnexpectedHttpParameter,
    /// Duplicate HTTP header
    #[error("Duplicate HTTP header: {0}")]
    DuplicateHttpHeader(String),
    /// Duplicate IRI fragment
    #[error("Duplicate IRI fragment")]
    DuplicateFragment,
    /// Unsupported IRI scheme
    #[error("IRI scheme is not supported: {0}")]
    UnsupportedIriScheme(String),
}

/// Parameters than can be specified for HTTP requests
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize)]
pub struct HttpParameters {
    /// Headers sent with the request
    headers: Vec<(String, String)>,
    /// Parameters sent as part of the IRI's query string
    get_parameters: Vec<(String, String)>,
    /// Fragment part of the IRI
    fragment: Option<String>,
    /// Parameters send in a POST-request
    post_parameters: Vec<(String, String)>,
}

/// An external resource that can be imported or exported to
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resource {
    /// A local file path
    Path(PathBuf),
    /// An IRI
    Http {
        /// Base IRI of the resource
        iri: Iri<String>,
        /// Additional parameters sent with the HTTP request
        parameters: HttpParameters,
    },
}

impl Resource {
    /// Return if resource is an Iri
    pub fn is_http(&self) -> bool {
        matches!(self, Self::Http { .. })
    }

    /// Return if resource is a path
    pub fn is_path(&self) -> bool {
        matches!(self, Self::Path(..))
    }

    /// Return local path of a resource
    pub fn as_path(&self) -> Option<&PathBuf> {
        match self {
            Self::Http { .. } => None,
            Self::Path(path) => Some(path),
        }
    }

    /// Return the headers if resource is of variant [Resource::Http]
    pub fn headers(&self) -> Box<dyn Iterator<Item = &(String, String)> + '_> {
        match self {
            Self::Http { parameters, .. } => Box::new(parameters.headers.iter()),
            Self::Path(..) => Box::new(iter::empty()),
        }
    }

    /// Return the GET parameters
    pub fn get_parameters(&self) -> Box<dyn Iterator<Item = &(String, String)> + '_> {
        match self {
            Self::Http { parameters, .. } => Box::new(parameters.get_parameters.iter()),
            Self::Path(..) => Box::new(iter::empty()),
        }
    }

    /// Return the fragment
    pub fn fragment(&self) -> Option<&String> {
        match self {
            Self::Http { parameters, .. } => parameters.fragment.as_ref(),
            Self::Path(..) => None,
        }
    }

    /// Return the POST parameters
    pub fn post_parameters(&self) -> Box<dyn Iterator<Item = &(String, String)> + '_> {
        match self {
            Self::Http { parameters, .. } => Box::new(parameters.post_parameters.iter()),
            Self::Path(..) => Box::new(iter::empty()),
        }
    }

    /// Check if resource has non-empty POST parameters
    pub fn has_post_parameters(&self) -> bool {
        self.post_parameters().next().is_some()
    }

    /// Returns the file extension of a path or IRI based on the last '.'
    pub fn file_extension(&self) -> Option<&str> {
        match self {
            Self::Path(path) => path.extension().and_then(|ext| ext.to_str()),
            Self::Http { iri, .. } => iri.path().rfind('.').map(|index| &iri[index..]),
        }
    }

    /// Remove a certain file extension from the resource
    pub fn strip_file_extension(&mut self, suffix: &str) -> &Self {
        match self {
            Self::Path(path) => {
                *path = path.with_extension("");
            }
            Self::Http { iri, .. } => {
                if let Some(path) = iri.path().strip_suffix(&format!("'.'{suffix}")) {
                    *iri = Iri::parse_unchecked(format!(
                        "{}://{}{}{}{}",
                        iri.scheme(),
                        iri.authority().unwrap_or_default(),
                        path,
                        iri.query()
                            .map(|query| format!("?{}", query))
                            .unwrap_or_default(),
                        iri.fragment()
                            .map(|fragment| format!("#{}", fragment))
                            .unwrap_or_default()
                    ));
                }
            }
        }
        self
    }
}

/// Implement Display for Resource enum
impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http { iri, parameters } => {
                let get_parameters = serde_urlencoded::to_string(&parameters.get_parameters)
                    .map_err(|_| fmt::Error)?;
                let fragment = self
                    .fragment()
                    .map(|fragment| format!("#{fragment}"))
                    .unwrap_or_default();

                if get_parameters.is_empty() {
                    write!(f, "{iri}{fragment}")
                } else {
                    write!(f, "{iri}?{get_parameters}{fragment}")
                }
            }
            Self::Path(path) => {
                write!(f, "{}", path.display())
            }
        }
    }
}

/// Builder for a WebResource
#[derive(Debug)]
pub struct ResourceBuilder {
    resource: Resource,
    header_names: HashSet<String>,
}

impl ResourceBuilder {
    /// Returns the path of a local iri
    fn strip_local_iri(iri: Iri<String>) -> Result<String, ResourceValidationErrorKind> {
        match iri.authority() {
            None | Some("") | Some("localhost") => Ok(format!(
                "{}{}{}",
                iri.path(),
                iri.query()
                    .map(|query| format!("?{}", query))
                    .unwrap_or_default(),
                iri.fragment()
                    .map(|query| format!("#{}", query))
                    .unwrap_or_default()
            )),
            _ => Err(ResourceValidationErrorKind::InvalidIri),
        }
    }

    /// Validate key-value pairs of HttpHeaders
    pub fn validate_headers(headers: AnyDataValue) -> Result<(), ResourceValidationErrorKind> {
        let mut builder = TryInto::<Self>::try_into(String::from("http://foo.org"))?;
        let vec = builder.unpack_headers(headers)?;
        // … assert that headers is a Map
        for (key, value) in vec {
            builder.add_header(key, value)?;
        }
        Ok(())
    }

    /// Validate GET- or POST-Parameter with type [ValueDomain::Map]
    /// Each value is expected to be a tuple containing elemtents of type [ValueDomain::PlainString], [ValueDomain::Int] or [ValueDomain::Iri]
    pub fn validate_http_parameters(
        parameters: AnyDataValue,
    ) -> Result<(), ResourceValidationErrorKind> {
        let mut builder = TryInto::<Self>::try_into(String::from("http://foo.org"))?;
        let vec = builder.unpack_http_parameters(parameters)?;
        // … assert that headers is a Map
        for (key, value) in vec {
            // GET parameter and POST parameter have the same validation process so calling either add_get_parameter or add_post_parameter works for validation
            builder.add_get_parameter(key, value)?;
        }
        Ok(())
    }

    /// Convert headers into a key-value iterator
    pub fn unpack_headers(
        &self,
        map: AnyDataValue,
    ) -> Result<impl Iterator<Item = (String, String)>, ResourceValidationErrorKind> {
        // Expects a map
        let keys = map.map_keys();
        let result = keys
            .into_iter()
            .flat_map(|keys| {
                keys.map(|key| {
                    Ok((
                        self.as_lexical_value(key)?,
                        self.as_lexical_value(map.map_element_unchecked(key))?,
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(result.into_iter())
    }

    /// Flatten HTTP parameters into a key-value vector
    pub fn unpack_http_parameters(
        &self,
        parameters: AnyDataValue,
    ) -> Result<impl Iterator<Item = (String, String)>, ResourceValidationErrorKind> {
        // Expect a map where each value is a tuple
        let keys = parameters.map_keys();

        let result = keys
            .into_iter()
            .flat_map(|keys| {
                keys.flat_map(|key| {
                    let tuple = parameters.map_element_unchecked(key);
                    (0..tuple.len_unchecked()).map(|idx| {
                        let element = tuple.tuple_element_unchecked(idx);
                        Ok((self.as_lexical_value(key)?, self.as_lexical_value(element)?))
                    })
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(result.into_iter())
    }

    /// Try to convert a single HTTP parameter value into  a [String]
    fn as_lexical_value(
        &self,
        value: &AnyDataValue,
    ) -> Result<String, ResourceValidationErrorKind> {
        match value.value_domain() {
            ValueDomain::PlainString | ValueDomain::Int | ValueDomain::Iri => {
                Ok(value.lexical_value())
            }
            _ => Err(ResourceValidationErrorKind::HttpParameterNotInValueDomain {
                expected: vec![ValueDomain::PlainString, ValueDomain::Int, ValueDomain::Iri],
                given: value.value_domain(),
            }),
        }
    }

    /// Add HTTP header
    pub fn add_header(
        &mut self,
        key: String,
        value: String,
    ) -> Result<&mut Self, ResourceValidationErrorKind> {
        match &mut self.resource {
            Resource::Http { parameters, .. } => {
                if self.header_names.insert(key.clone()) {
                    parameters.headers.push((key, value));
                    Ok(self)
                } else {
                    Err(ResourceValidationErrorKind::DuplicateHttpHeader(key))
                }
            }
            Resource::Path(..) => Err(ResourceValidationErrorKind::UnexpectedHttpParameter),
        }
    }

    /// Add GET parameter
    pub fn add_get_parameter(
        &mut self,
        key: String,
        value: String,
    ) -> Result<&mut Self, ResourceValidationErrorKind> {
        match &mut self.resource {
            Resource::Http { parameters, .. } => {
                parameters.get_parameters.push((key, value));
                Ok(self)
            }
            Resource::Path(..) => Err(ResourceValidationErrorKind::UnexpectedHttpParameter),
        }
    }

    /// Add fragment to IRI
    pub fn set_fragment(
        &mut self,
        value: String,
    ) -> Result<&mut Self, ResourceValidationErrorKind> {
        match &mut self.resource {
            Resource::Http { parameters, .. } => {
                if parameters.fragment.is_some() {
                    Err(ResourceValidationErrorKind::DuplicateFragment)
                } else {
                    parameters.fragment = Some(value);
                    Ok(self)
                }
            }
            Resource::Path(path) => {
                path.push(format!("#{value}"));
                Ok(self)
            }
        }
    }

    /// Add POST parameter
    pub fn add_post_parameter(
        &mut self,
        key: String,
        value: String,
    ) -> Result<&mut Self, ResourceValidationErrorKind> {
        match &mut self.resource {
            Resource::Http { parameters, .. } => {
                parameters.post_parameters.push((key, value));
                Ok(self)
            }
            Resource::Path(..) => Err(ResourceValidationErrorKind::UnexpectedHttpParameter),
        }
    }

    /// Build a [Resource] with the given parameters
    pub fn finalize(self) -> Resource {
        debug!("Created resource: {:?}", self.resource);
        self.resource
    }
}

impl TryFrom<Iri<String>> for ResourceBuilder {
    type Error = ResourceValidationErrorKind;

    /// Create a new [ResourceBuilder] from a web-Iri
    fn try_from(iri: Iri<String>) -> Result<Self, Self::Error> {
        match iri.scheme() {
            "file" => {
                let path = Self::strip_local_iri(iri)?;
                Ok(Self {
                    resource: Resource::Path(PathBuf::from_slash(path)),
                    header_names: HashSet::new(),
                })
            }
            "http" | "https" => {
                let fragment = iri.fragment().map(String::from);
                let get_parameters = serde_urlencoded::from_str::<Vec<(String, String)>>(
                    iri.query().unwrap_or_default(),
                )
                .map_err(|_| ResourceValidationErrorKind::InvalidIri)?;

                Ok(Self {
                    resource: Resource::Http {
                        iri: Iri::parse_unchecked(format!(
                            "{}://{}{}",
                            iri.scheme(),
                            iri.authority().unwrap_or_default(),
                            iri.path()
                        )),
                        parameters: HttpParameters {
                            headers: Vec::new(),
                            get_parameters,
                            fragment,
                            post_parameters: Vec::new(),
                        },
                    },
                    header_names: HashSet::new(),
                })
            }
            _ => Err(ResourceValidationErrorKind::UnsupportedIriScheme(
                String::from(iri.scheme()),
            )),
        }
    }
}

impl TryFrom<String> for ResourceBuilder {
    type Error = ResourceValidationErrorKind;

    fn try_from(string: String) -> Result<Self, Self::Error> {
        if let Ok(iri) = Iri::parse(string.to_owned()) {
            iri.try_into()
        } else {
            Ok(Self {
                resource: Resource::Path(PathBuf::from_slash(string)),
                header_names: HashSet::new(),
            })
        }
    }
}

impl TryFrom<AnyDataValue> for ResourceBuilder {
    type Error = ResourceValidationErrorKind;

    fn try_from(value: AnyDataValue) -> Result<Self, Self::Error> {
        value
            .to_plain_string()
            .or_else(|| value.to_iri())
            .ok_or(ResourceValidationErrorKind::InvalidResourceFormat {
                expected: vec![ValueDomain::PlainString, ValueDomain::Iri],
                given: value.value_domain(),
            })?
            .try_into()
    }
}
