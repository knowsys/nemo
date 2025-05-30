//! This modules defines the [Resource] type, which is part of the public interface of this crate.
//!

use log;

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
use serde::Serialize;
use std::collections::HashSet;
use std::{fmt, iter, path::PathBuf};
use thiserror::Error;

/// Type of error that is returned when parsing of Iri fails
#[derive(Clone, Debug, Error, PartialEq)]
pub enum ResourceValidationErrorKind {
    /// IRI is not valid
    #[error("Invalid IRI: {0}")]
    InvalidIri(String),
    /// Resource has invalid [ValueDomain]
    #[error("Resource was given as `{given:?}`, expected one of: `{expected:?}`")]
    InvalidResourceFormat {
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
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
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
    /// A resource that can be retrieved by an HTTP request
    Http {
        /// Base IRI of the resource
        iri: Iri<String>,
        /// Additional parameters sent with the HTTP request
        parameters: HttpParameters,
    },
    /// Use stdin for import, stdout for export
    Pipe,
}

impl Resource {
    /// Whether the resource supports compression
    pub fn supports_compression(&self) -> bool {
        match self {
            Resource::Pipe => false, // never compress input from standard in or output to standard out
            Resource::Http { .. } => true, // reqwest handles transport layer compression, but retrieved resources may still need decompression
            Resource::Path(_) => true,     // for local files, we need to handle compression
        }
    }

    /// Return if resource is an Iri
    pub fn is_http(&self) -> bool {
        matches!(self, Self::Http { .. })
    }

    /// Return if resource is a path
    pub fn is_path(&self) -> bool {
        matches!(self, Self::Path(..))
    }

    /// Return if resource points to pipe
    pub fn is_pipe(&self) -> bool {
        matches!(self, Self::Pipe)
    }

    /// Return the headers
    pub fn headers(&self) -> Box<dyn Iterator<Item = &(String, String)> + '_> {
        match self {
            Self::Http { parameters, .. } => Box::new(parameters.headers.iter()),
            _ => Box::new(iter::empty()),
        }
    }

    /// Return the GET parameters
    pub fn get_parameters(&self) -> Box<dyn Iterator<Item = &(String, String)> + '_> {
        match self {
            Self::Http { parameters, .. } => Box::new(parameters.get_parameters.iter()),
            _ => Box::new(iter::empty()),
        }
    }

    /// Return the fragment
    pub fn fragment(&self) -> Option<&String> {
        match self {
            Self::Http { parameters, .. } => parameters.fragment.as_ref(),
            _ => None,
        }
    }

    /// Return the POST parameters
    pub fn post_parameters(&self) -> Box<dyn Iterator<Item = &(String, String)> + '_> {
        match self {
            Self::Http { parameters, .. } => Box::new(parameters.post_parameters.iter()),
            _ => Box::new(iter::empty()),
        }
    }

    /// Returns the file extension of the resource, based on the last
    /// `.` in the name. Treats an empty extension (i.e., a name
    /// ending in `.`) as `None`.
    pub fn file_extension(&self) -> Option<String> {
        self.file_extension_uncompressed(None)
    }

    /// Returns the file extension of the resource, based on the last
    /// `.` in the name. Treats an empty extension (i.e., a name
    /// ending in `.`) as `None`. If `compression_extension` is given,
    /// returns the file extension for the corresponding uncompressed
    /// file (i.e., the extension remaining after removing
    /// `compression_extension`).
    pub fn file_extension_uncompressed(
        &self,
        compression_extension: Option<&str>,
    ) -> Option<String> {
        match self {
            Self::Path(path) => {
                let mut the_path = PathBuf::from(path);

                if let Some(extension) = path.extension() {
                    if extension.to_str() == compression_extension {
                        the_path = PathBuf::from(path.file_stem().unwrap_or_default());
                    }
                };

                the_path
                    .extension()
                    .filter(|extension| !extension.is_empty())
                    .and_then(|extension| extension.to_str())
                    .map(|extension| extension.to_string())
            }
            Self::Http { iri, .. } => {
                let path = iri.path();
                path.rfind('.').and_then(|idx| {
                    let start = idx + 1;

                    if start < path.len() {
                        let extension = &path[start..];
                        if Some(extension) == compression_extension {
                            let uncompressed_path = &path[..(start.saturating_sub(1))];
                            uncompressed_path.rfind('.').and_then(|idx| {
                                let start = idx + 1;

                                if start < uncompressed_path.len() {
                                    Some(uncompressed_path[start..].to_string())
                                } else {
                                    None
                                }
                            })
                        } else {
                            Some(extension.to_string())
                        }
                    } else {
                        None
                    }
                })
            }
            _ => None,
        }
    }
}

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
            Self::Path(path) => write!(f, "{}", path.display()),
            Self::Pipe => f.write_str("standard in / standard out"),
        }
    }
}

/// Returns the path of a local iri
fn strip_local_iri(iri: Iri<String>) -> Result<String, ResourceValidationErrorKind> {
    if !iri.path().starts_with('/') {
        return Err(ResourceValidationErrorKind::InvalidIri(format!(
            "Local IRI path `{}` should start with '/'",
            iri.path()
        )));
    }
    match iri.authority() {
        None | Some("") | Some("localhost") => Ok(format!(
            "{}{}{}",
            iri.path(),
            iri.query()
                .map(|query| format!("?{query}"))
                .unwrap_or_default(),
            iri.fragment()
                .map(|query| format!("#{query}"))
                .unwrap_or_default()
        )),
        _ => Err(ResourceValidationErrorKind::InvalidIri(format!(
            "Authority `{}`is not supported in local IRI",
            iri.authority().unwrap_or_default()
        ))),
    }
}

/// Builder collecting parameters into a [Resource]
#[derive(Debug)]
pub struct ResourceBuilder {
    resource: Resource,
    header_names: HashSet<String>,
}

impl ResourceBuilder {
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
            _ => Err(ResourceValidationErrorKind::UnexpectedHttpParameter),
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
            _ => Err(ResourceValidationErrorKind::UnexpectedHttpParameter),
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
            _ => Err(ResourceValidationErrorKind::UnexpectedHttpParameter),
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
            _ => Err(ResourceValidationErrorKind::UnexpectedHttpParameter),
        }
    }

    /// Create a [ResourceBuilder] for [Resource::Pipe]
    pub fn pipe_resource_builder() -> Self {
        Self::try_from(String::from("")).expect("Empty string is valid resource for pipe")
    }

    /// Create a default [ResourceBuilder]
    pub fn default_resource_builder(predicate_name: &str, default_extension: String) -> Self {
        if predicate_name.is_empty() {
            Self::pipe_resource_builder()
        } else {
            ResourceBuilder::try_from(format!("{predicate_name}.{default_extension}"))
                .expect("default name is valid")
        }
    }

    /// Whether the resource supports compression
    pub fn supports_compression(&self) -> bool {
        self.resource.supports_compression()
    }

    /// Return the file extension of the underlying resource, if any
    pub fn file_extension(&self) -> Option<String> {
        self.resource.file_extension()
    }

    /// Build a [Resource] with the collected parameters
    pub fn finalize(self) -> Resource {
        log::debug!("Created resource: {:?}", self.resource);
        self.resource
    }
}

impl TryFrom<Iri<String>> for ResourceBuilder {
    type Error = ResourceValidationErrorKind;

    fn try_from(iri: Iri<String>) -> Result<Self, Self::Error> {
        match iri.scheme() {
            "file" => {
                let path = strip_local_iri(iri)?;
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
                .map_err(|err| ResourceValidationErrorKind::InvalidIri(err.to_string()))?;

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
        } else if string.is_empty() {
            Ok(Self {
                resource: Resource::Pipe,
                header_names: HashSet::new(),
            })
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
        match value.value_domain() {
            ValueDomain::PlainString => value.to_plain_string_unchecked().try_into(),
            ValueDomain::Iri => Iri::parse(value.to_iri_unchecked())
                .map_err(|err| ResourceValidationErrorKind::InvalidIri(err.to_string()))?
                .try_into(),
            _ => Err(ResourceValidationErrorKind::InvalidResourceFormat {
                expected: vec![ValueDomain::PlainString, ValueDomain::Iri],
                given: value.value_domain(),
            }),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Resource, ResourceBuilder, ResourceValidationErrorKind};
    use test_log::test;

    #[test]
    fn create_http_resource() -> Result<(), ResourceValidationErrorKind> {
        let valid_iri = String::from("http://example.org/directory?query=A&query=B#fragment");
        let builder = ResourceBuilder::try_from(valid_iri.clone()).expect("IRI is valid");
        assert_eq!(builder.finalize().to_string(), valid_iri);

        let start_iri = String::from("http://example.org/directory");
        let mut builder = ResourceBuilder::try_from(start_iri).expect("IRI is valid");
        builder.add_get_parameter(String::from("query"), String::from("A"))?;
        builder.add_get_parameter(String::from("query"), String::from("B"))?;
        builder.set_fragment(String::from("fragment"))?;
        assert_eq!(builder.finalize().to_string(), valid_iri);
        Ok(())
    }

    #[test]
    fn duplicate_http_parameters() -> Result<(), ResourceValidationErrorKind> {
        let valid_iri = String::from("http://example.org#fragment");
        let mut builder = ResourceBuilder::try_from(valid_iri.clone()).expect("IRI is valid");
        let err = builder.set_fragment(String::from("fragment")).unwrap_err();
        assert_eq!(err, ResourceValidationErrorKind::DuplicateFragment);

        let key = String::from("Accept");
        let value = String::from("text/html");
        builder.add_header(key.clone(), value.clone())?;
        let err = builder.add_header(key.clone(), value).unwrap_err();
        assert_eq!(err, ResourceValidationErrorKind::DuplicateHttpHeader(key));

        Ok(())
    }

    #[test]
    fn create_path_resource() {
        let builder =
            ResourceBuilder::try_from(String::from("file:/directory/file.extension#fragment"))
                .expect("Path is valid");
        let resource = builder.finalize();
        assert_eq!(
            resource.to_string(),
            String::from("/directory/file.extension#fragment")
        );

        let builder = ResourceBuilder::try_from(String::from(
            "file://localhost/directory/file.extension#fragment",
        ))
        .expect("Path is valid");
        let resource = builder.finalize();
        assert_eq!(
            resource.to_string(),
            String::from("/directory/file.extension#fragment")
        );

        let mut builder =
            ResourceBuilder::try_from(String::from("file:///directory/file.extension#fragment"))
                .expect("Path is valid");
        assert!(builder
            .add_get_parameter(String::from("query"), String::from("Select"))
            .is_err());
        assert!(builder
            .add_post_parameter(String::from("query"), String::from("Select"))
            .is_err());
        assert!(builder
            .add_header(String::from("query"), String::from("Select"))
            .is_err());
        assert!(builder.set_fragment(String::from("fragment")).is_err());

        assert!(
            ResourceBuilder::try_from(String::from("file:directory/file.extension#fragment"))
                .is_err()
        );
    }

    #[test]
    fn file_extension() {
        let resource =
            ResourceBuilder::try_from(String::from("https://example.org/foo/bar/quux.csv.gz"))
                .unwrap()
                .finalize();
        assert_eq!(resource.file_extension(), Some("gz".to_string()));

        let resource =
            ResourceBuilder::try_from(String::from("https://example.org/foo/bar/quux.csv"))
                .unwrap()
                .finalize();
        assert_eq!(resource.file_extension(), Some("csv".to_string()));

        let resource = ResourceBuilder::try_from(String::from("https://example.org/foo/bar/quux"))
            .unwrap()
            .finalize();
        assert_eq!(resource.file_extension(), None);

        let resource =
            ResourceBuilder::try_from(String::from("https://example.org/foo/bar/quux.csv.gz."))
                .unwrap()
                .finalize();
        assert_eq!(resource.file_extension(), None);

        let resource = ResourceBuilder::try_from(String::from(
            "https://example.org/foo/bar/quux.csv?dot=.gz#other.rdf",
        ))
        .unwrap()
        .finalize();
        assert_eq!(resource.file_extension(), Some("csv".to_string()));

        let resource = ResourceBuilder::try_from(String::from(
            "https://example.org/foo/bar/quux.csv.gz.?dot=.gz#other.rdf",
        ))
        .unwrap()
        .finalize();
        assert_eq!(resource.file_extension(), None);

        let resource = ResourceBuilder::try_from(String::from("https://example.org"))
            .unwrap()
            .finalize();
        assert_eq!(resource.file_extension(), None);

        let resource = ResourceBuilder::try_from(String::from("foo/bar/quux.csv.gz"))
            .unwrap()
            .finalize();
        assert_eq!(resource.file_extension(), Some("gz".to_string()));

        let resource = ResourceBuilder::try_from(String::from("foo/bar/quux.csv"))
            .unwrap()
            .finalize();
        assert_eq!(resource.file_extension(), Some("csv".to_string()));

        let resource = ResourceBuilder::try_from(String::from("foo/bar/quux"))
            .unwrap()
            .finalize();
        assert_eq!(resource.file_extension(), None);

        let resource = ResourceBuilder::try_from(String::from("foo/bar/quux."))
            .unwrap()
            .finalize();
        assert_eq!(resource.file_extension(), None);

        assert_eq!(Resource::Pipe.file_extension(), None);
    }

    #[test]
    fn file_extension_uncompressed() {
        let resource =
            ResourceBuilder::try_from(String::from("https://example.org/foo/bar/quux.csv.gz"))
                .unwrap()
                .finalize();
        assert_eq!(
            resource.file_extension_uncompressed(Some("gz")),
            Some("csv".to_string())
        );

        let resource =
            ResourceBuilder::try_from(String::from("https://example.org/foo/bar/quux.csv"))
                .unwrap()
                .finalize();
        assert_eq!(
            resource.file_extension_uncompressed(Some("gz")),
            Some("csv".to_string())
        );

        let resource = ResourceBuilder::try_from(String::from("https://example.org/foo/bar/quux"))
            .unwrap()
            .finalize();
        assert_eq!(resource.file_extension_uncompressed(Some("gz")), None);

        let resource =
            ResourceBuilder::try_from(String::from("https://example.org/foo/bar/quux.csv.gz."))
                .unwrap()
                .finalize();
        assert_eq!(resource.file_extension_uncompressed(Some("gz")), None);

        let resource = ResourceBuilder::try_from(String::from(
            "https://example.org/foo/bar/quux.csv?dot=.gz#other.rdf",
        ))
        .unwrap()
        .finalize();
        assert_eq!(
            resource.file_extension_uncompressed(Some("gz")),
            Some("csv".to_string())
        );

        let resource = ResourceBuilder::try_from(String::from(
            "https://example.org/foo/bar/quux.csv.gz.?dot=.gz#other.rdf",
        ))
        .unwrap()
        .finalize();
        assert_eq!(resource.file_extension_uncompressed(Some("gz")), None);

        let resource = ResourceBuilder::try_from(String::from("https://example.org"))
            .unwrap()
            .finalize();
        assert_eq!(resource.file_extension_uncompressed(Some("gz")), None);

        let resource = ResourceBuilder::try_from(String::from("foo/bar/quux.csv.gz"))
            .unwrap()
            .finalize();
        assert_eq!(
            resource.file_extension_uncompressed(Some("gz")),
            Some("csv".to_string())
        );

        let resource = ResourceBuilder::try_from(String::from("foo/bar/quux.csv"))
            .unwrap()
            .finalize();
        assert_eq!(
            resource.file_extension_uncompressed(Some("gz")),
            Some("csv".to_string())
        );

        let resource = ResourceBuilder::try_from(String::from("foo/bar/quux"))
            .unwrap()
            .finalize();
        assert_eq!(resource.file_extension_uncompressed(Some("gz")), None);

        let resource = ResourceBuilder::try_from(String::from("foo/bar/quux."))
            .unwrap()
            .finalize();
        assert_eq!(resource.file_extension_uncompressed(Some("gz")), None);

        assert_eq!(Resource::Pipe.file_extension_uncompressed(Some("gz")), None);
    }

    #[test]
    fn pipe() {
        assert_eq!(
            ResourceBuilder::try_from(String::from(""))
                .expect("Valid shortcut for Pipe")
                .finalize(),
            Resource::Pipe
        );
        assert_eq!(
            ResourceBuilder::default_resource_builder("", String::new()).finalize(),
            Resource::Pipe
        );
    }
}
