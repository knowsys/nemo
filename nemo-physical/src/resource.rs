//! This modules defines the [Resource] type, which is part of the public interface of this crate.
//!

use super::datavalues::{AnyDataValue, DataValue, ValueDomain};
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

    /// Return the headers if resource is of variant [Resource::Iri]
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
                        "{}//{}{}?{}#{}",
                        iri.scheme(),
                        iri.authority().unwrap_or_default(),
                        path,
                        iri.query().unwrap_or_default(),
                        iri.fragment().unwrap_or_default()
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
            None | Some("") | Some("localhost") => Ok(format!("{}?{}",iri.path())),
            _ => Err(ResourceValidationErrorKind::InvalidIri),
        }
    }

    
    /// Validate key-value pairs of HttpHeaders
    pub fn validate_headers(map: AnyDataValue) -> Result<(), ResourceValidationErrorKind> {
        if let Some(keys) = map.map_keys() {
            let mut key_names = HashSet::new();
            for key in keys {
                if !matches!(
                    key.value_domain(),
                    ValueDomain::PlainString | ValueDomain::Int | ValueDomain::Iri
                ) {
                    return Err(ResourceValidationErrorKind::HttpParameterNotInValueDomain {
                        expected: vec![
                            ValueDomain::PlainString,
                            ValueDomain::Int,
                            ValueDomain::Iri,
                        ],
                        given: key.value_domain(),
                    });
                }
                let key_name: String = key.lexical_value();
                if !key_names.insert(key_name.clone()) {
                    return Err(ResourceValidationErrorKind::DuplicateHttpHeader(key_name));
                }

                let value = map.map_element_unchecked(key);
                if !matches!(
                    value.value_domain(),
                    ValueDomain::PlainString | ValueDomain::Int | ValueDomain::Iri
                ) {
                    return Err(ResourceValidationErrorKind::HttpParameterNotInValueDomain {
                        expected: vec![
                            ValueDomain::PlainString,
                            ValueDomain::Int,
                            ValueDomain::Iri,
                        ],
                        given: key.value_domain(),
                    });
                }
            }
        }
        Ok(())

    }

    /// Validate GET- or POST-Parameter with type [ValueDomain::Map]
    /// Each value is expected to be a tuple containing elemtents of type [ValueDomain::PlainString], [ValueDomain::Int] or [ValueDomain::Iri]
    pub fn validate_http_parameters(map: AnyDataValue) -> Result<(), ResourceValidationErrorKind> {
        // Expect each value as a tuple of values
        if let Some(keys) = map.map_keys() {
            for key in keys {
                if !matches!(
                    key.value_domain(),
                    ValueDomain::PlainString | ValueDomain::Int | ValueDomain::Iri
                ) {
                    return Err(ResourceValidationErrorKind::HttpParameterNotInValueDomain {
                        expected: vec![
                            ValueDomain::PlainString,
                            ValueDomain::Int,
                            ValueDomain::Iri,
                        ],
                        given: key.value_domain(),
                    });
                }
                let tuple = map.map_element_unchecked(key);

                if !matches!(tuple.value_domain(), ValueDomain::Tuple) {
                    return Err(ResourceValidationErrorKind::HttpParameterNotInValueDomain {
                        expected: vec![ValueDomain::Tuple],
                        given: key.value_domain(),
                    });
                }

                for idx in 0..tuple.len_unchecked() {
                    let element = tuple.tuple_element_unchecked(idx);
                    if !matches!(
                        element.value_domain(),
                        ValueDomain::PlainString | ValueDomain::Int | ValueDomain::Iri
                    ) {
                        return Err(ResourceValidationErrorKind::HttpParameterNotInValueDomain {
                            expected: vec![
                                ValueDomain::PlainString,
                                ValueDomain::Int,
                                ValueDomain::Iri,
                            ],
                            given: key.value_domain(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Convert headers into a key-value iterator
    fn unpack_headers(self, map: AnyDataValue) -> impl Iterator<Item = (String, String)> {
        // Expect a map where each value is a
        let keys = map.map_keys();
        let result = keys
            .into_iter()
            .flat_map(|keys| {
                keys.map(|key| {
                    (
                        key.lexical_value(),
                        map.map_element_unchecked(key).lexical_value(),
                    )
                })
            })
            .collect::<Vec<_>>();
        result.into_iter()
    }

    /// Flatten HTTP parameters into a key-value vector
    pub fn unpack_http_parameters(map: AnyDataValue) -> impl Iterator<Item = (String, String)> {
        // Expect a map where each value is a tuple
        let keys = map.map_keys();

        let mut vec: Vec<(String, String)> = Vec::new();
        if let Some(keys_iter) = keys {
            for key in keys_iter {
                let tuple = map.map_element_unchecked(key);
                for idx in 0..tuple.len_unchecked() {
                    let element = tuple.tuple_element_unchecked(idx);
                    vec.push((key.lexical_value(), element.lexical_value()));
                }
            }
        }
        vec.into_iter()
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
            },
            "http" | "https" => {
                let fragment = iri.fragment().map(String::from);
                let get_parameters = serde_urlencoded::from_str::<Vec<(String, String)>>(
                    iri.query().unwrap_or_default()
                ).map_err(|_| ResourceValidationErrorKind::InvalidIri)?;
                
                Ok(Self {
                    resource: Resource::Http {
                        iri: Iri::parse_unchecked(format!("{}{}",iri.authority().unwrap_or_default(), iri.path())),
                        parameters: HttpParameters {
                            headers : Vec::new(),
                            get_parameters,
                            fragment,
                            post_parameters: Vec::new(),
                        },
                    },
                    header_names: HashSet::new(),
                })
            },
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
