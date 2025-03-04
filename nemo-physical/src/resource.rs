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
use std::collections::HashSet;
use std::{fmt, iter, path::PathBuf};

use thiserror::Error;
use urlencoding::encode;

/// Type of error that is returned when parsing of Iri fails
#[derive(Clone, Debug, Error)]
pub enum ResourceValidationErrorKind {
    /// Resource is not a valid path
    #[error("Invalid Path")]
    ImportExportInvalidPath,
    /// Parsing error for Iri
    #[error("Invalid IRI format")]
    ImportExportInvalidIri,
    /// Http-Parameters have wrong type
    #[error("Invalid Http-parameter: {0}")]
    ImportExportInvalidHttpParameter(String),
    /// Error when Http-Parameters are provided for a local resource
    #[error("Unexpected Http-parameter for local resource")]
    ImportExportUnexpectedHttpParameter,
    /// Same Header-key is supplied several times
    #[error("http_header_parameters contains duplicate key: {0}")]
    ImportExportDuplicateHttpHeader(String),
    /// Fragment is supplied several times
    #[error("http_fragment is supplied several times")]
    ImportExportDuplicateFragment,
    /// The resource is parsed into an Iri but has unexpected scheme
    #[error("Iri-scheme is invalid: {0}")]
    ImportExportUnknownIriScheme(String),
}

/// Define a series of Parameters that can be passed to the HTTP-Request
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HttpParameters {
    /// Specifies additional headers for a GET/ POST-request
    pub headers: Vec<(String, String)>,
    /// Contains all parameters that are added to the Iri
    pub get_parameters: Vec<(String, String)>,
    /// Specifies a fragment (#) that is appended to the Iri after the get-parameters
    pub fragment: Option<String>,
    /// Contains parameters that are send in the body of a POST-request
    pub post_parameters: Vec<(String, String)>,
}

/// Define two Resource types that are used for Import and Export
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resource {
    /// Specify location for a local source or target
    Path(PathBuf),
    /// Specify location for a web source or target
    Iri {
        /// Contains the endpoint / http address for a request in the form of an Iri
        iri: Iri<String>,
        /// A collection of parameters that are passed considered in the http-request
        parameters: HttpParameters,
    },
}

impl Resource {
    /// Returns the local path of a resource, possibly stripped
    pub fn as_string(&self) -> String {
        if self.is_path() {
            self.as_path().to_string_lossy().to_string()
        } else if self.is_iri() {
            self.as_iri()
        } else {
            panic!("Resource is neither path nor Iri.")
        }
    }

    /// Return if resource is an Iri
    pub fn is_iri(&self) -> bool {
        matches!(self, Self::Iri { .. })
    }

    /// Return if resource is a path
    pub fn is_path(&self) -> bool {
        matches!(self, Self::Path(..))
    }

    /// Return local path of a resource
    pub fn as_path(&self) -> &PathBuf {
        match self {
            Self::Iri { .. } => panic!("Resource no local path"),
            Self::Path(path) => path,
        }
    }

    /// Return iri of a resource
    pub fn as_iri(&self) -> String {
        Self::internal_as_iri(self, false)
    }

    /// Return iri of a resource
    pub fn as_iri_encoded(&self) -> String {
        Self::internal_as_iri(self, true)
    }

    /// Constructs the full Iri and possibly encode the GET-Parameter
    pub fn internal_as_iri(&self, encode_get: bool) -> String {
        let mut get_parameters = String::new();
        let mut is_first = true;

        for (key, value) in self.get_parameters() {
            if is_first {
                get_parameters.push('?');
                is_first = false;
            } else {
                get_parameters.push('&');
            }
            // TODO: could also be replaced with an inline function
            if encode_get {
                get_parameters.push_str(&format!("{}={}", encode(key), encode(value)));
            } else {
                get_parameters.push_str(&format!("{}={}", key, value));
            }
        }

        // Construct the full URL with query and fragment
        let fragment = {
            if let Some(fragment) = self.fragment() {
                format!("#{}", fragment)
            } else {
                String::new()
            }
        };
        if let Self::Iri { iri, .. } = self {
            format!("{}{}{}", iri, get_parameters, fragment)
        } else {
            panic!("Resource is a path not Iri")
        }
    }

    /// Return the headers if resource is of variant [Resource::Iri]
    pub fn header_parameters(&self) -> Box<dyn Iterator<Item = &(String, String)> + '_> {
        match self {
            Self::Iri { parameters, .. } => Box::new(parameters.headers.iter()),
            Self::Path(..) => Box::new(iter::empty()),
        }
    }

    /// Return the get parameter of HttpParameters
    pub fn get_parameters(&self) -> Box<dyn Iterator<Item = &(String, String)> + '_> {
        match self {
            Self::Iri { parameters, .. } => Box::new(parameters.get_parameters.iter()),
            Self::Path(..) => Box::new(iter::empty()),
        }
    }

    /// Return the fragment parameter of HttpParameters
    pub fn fragment(&self) -> Option<&String> {
        match self {
            Self::Iri { parameters, .. } => parameters.fragment.as_ref(),
            Self::Path(..) => None,
        }
    }

    /// Return the body parameter of HttpParameters
    pub fn post_parameters(&self) -> Box<dyn Iterator<Item = &(String, String)> + '_> {
        match self {
            Self::Iri { parameters, .. } => Box::new(parameters.post_parameters.iter()),
            Self::Path(..) => Box::new(iter::empty()),
        }
    }

    /// Check if resource has non-empty POST parameters
    pub fn has_post_parameters(&self) -> bool {
        match self {
            Self::Iri { parameters, .. } => !parameters.post_parameters.is_empty(),
            Self::Path(..) => false,
        }
    }

    /// Returns the file extension of a path or Iri, based on the last '.'
    pub fn file_extension(&self) -> Option<&str> {
        match self {
            Self::Path(path) => path.extension().and_then(|ext| ext.to_str()),
            Self::Iri { iri, .. } => iri.path().rfind('.').map(|index| &iri[index..]),
        }
    }

    /// Remove a certain file extension from the resource
    pub fn strip_file_extension_unchecked(&self, suffix: &str) -> String {
        match self {
            Self::Path(path) => path.with_extension("").to_string_lossy().to_string(),
            Self::Iri { iri, .. } => iri
                .strip_suffix(suffix)
                .map(String::from)
                .expect("It should be verified that the suffix exists"),
        }
    }
}

/// Implement Display for Resource enum
impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_string())
    }
}

/// Provide path for local resource or iri for web-resource
#[derive(Debug)]
pub enum ResourceLocation {
    /// Local path to a resource
    Path(PathBuf),
    /// An Iri for a web-resource
    Iri(Iri<String>),
}

/// Builder for a WebResource
#[derive(Debug)]
pub struct ResourceBuilder {
    location: ResourceLocation,
    headers: Vec<(String, String)>,
    get_parameters: Vec<(String, String)>,
    fragment: Option<String>,
    post_parameters: Vec<(String, String)>,
}

impl ResourceBuilder {
    /// Returns the path of a local iri
    pub fn strip_local_iri(iri: Iri<String>) -> Result<String, ResourceValidationErrorKind> {
        match iri.authority() {
            None | Some("") | Some("localhost") => Ok(String::from(iri.path())),
            _ => Err(ResourceValidationErrorKind::ImportExportInvalidIri),
        }
    }

    /// Verify if [ResourceBuilder] contains a valid web-Iri
    pub fn has_web_iri(&self) -> bool {
        matches!(self.location, ResourceLocation::Iri(..))
    }

    /// Validate key-value pairs of HttpHeaders
    pub fn validate_header(map: AnyDataValue) -> Result<(), ResourceValidationErrorKind> {
        if let Some(keys) = map.map_keys() {
            let mut key_names = HashSet::new();
            for key in keys {
                // Validate ValueDomain of key
                if !matches!(
                    key.value_domain(),
                    ValueDomain::PlainString | ValueDomain::Int | ValueDomain::Iri
                ) {
                    return Err(
                        ResourceValidationErrorKind::ImportExportInvalidHttpParameter(format!(
                            "key of http-parameter has wrong type: {:?}",
                            key.value_domain()
                        )),
                    );
                }
                let key_name: String = key.lexical_value();
                // Check if key is a duplicate
                if !key_names.insert(key_name.clone()) {
                    return Err(
                        ResourceValidationErrorKind::ImportExportDuplicateHttpHeader(key_name),
                    );
                }

                let value = map.map_element_unchecked(key);
                // Validate ValueDomain of each value
                if !matches!(
                    value.value_domain(),
                    ValueDomain::PlainString | ValueDomain::Int | ValueDomain::Iri
                ) {
                    return Err(
                        ResourceValidationErrorKind::ImportExportInvalidHttpParameter(format!(
                            "element has wrong type: {:?}",
                            value.value_domain()
                        )),
                    );
                }
            }
            Ok(())
        } else {
            // Return Ok, if the parameter was empty
            Ok(())
        }
    }

    /// Validate GET- or POST-Parameter with type [ValueDomain::Map]
    /// Each value is expected to be a tuple containing elemtents of type [ValueDomain::PlainString], [ValueDomain::Int] or [ValueDomain::Iri]
    pub fn validate_post_get_parameter(
        map: AnyDataValue,
    ) -> Result<(), ResourceValidationErrorKind> {
        // Expect each value as a tuple (of values), where each element can be converted into a string
        if let Some(keys) = map.map_keys() {
            for key in keys {
                // Validate ValueDomain of key
                if !matches!(
                    key.value_domain(),
                    ValueDomain::PlainString | ValueDomain::Int | ValueDomain::Iri
                ) {
                    return Err(
                        ResourceValidationErrorKind::ImportExportInvalidHttpParameter(format!(
                            "key of http-parameter has wrong type: {:?}",
                            key.value_domain()
                        )),
                    );
                }
                let tuple = map.map_element_unchecked(key);

                // Validate ValueDomain of expected tuple
                if !matches!(tuple.value_domain(), ValueDomain::Tuple) {
                    return Err(
                        ResourceValidationErrorKind::ImportExportInvalidHttpParameter(format!(
                            "value of http-parameter is not a tuple: {:?}",
                            tuple.value_domain()
                        )),
                    );
                }

                let mut idx = 0;
                while let Some(element) = tuple.tuple_element(idx) {
                    idx += 1;
                    // Validate ValueDomain of each element in tuple
                    if !matches!(
                        element.value_domain(),
                        ValueDomain::PlainString | ValueDomain::Int | ValueDomain::Iri
                    ) {
                        return Err(
                            ResourceValidationErrorKind::ImportExportInvalidHttpParameter(format!(
                                "element in tuple has wrong type: {:?}",
                                element.value_domain()
                            )),
                        );
                    }
                }
            }
            Ok(())
        } else {
            // Return Ok, if the parameter was empty
            Ok(())
        }
    }

    /// Convert Header-Parameter into a key-value vector
    pub fn unpack_header_parameter(map: AnyDataValue) -> impl Iterator<Item = (String, String)> {
        // Expect a map of key-value pairs
        let keys = map.map_keys();

        let mut vec: Vec<(String, String)> = Vec::new();
        if let Some(keys_iter) = keys {
            for key in keys_iter {
                let element = map.map_element_unchecked(key);
                vec.push((key.lexical_value(), element.lexical_value()));
            }
        }
        vec.into_iter()
    }

    /// Flatten each parameter-map into a key-value vector
    pub fn unpack_get_post_parameter(map: AnyDataValue) -> impl Iterator<Item = (String, String)> {
        // Expect each value as a tuple (of values), where each element can be converted into a string
        let keys = map.map_keys();

        let mut vec: Vec<(String, String)> = Vec::new();
        if let Some(keys_iter) = keys {
            for key in keys_iter {
                let value = map.map_element_unchecked(key);
                let mut idx = 0;
                // Unpack inner tuple
                while let Some(element) = value.tuple_element(idx) {
                    idx += 1;
                    vec.push((key.lexical_value(), element.lexical_value()));
                }
            }
        }
        vec.into_iter()
    }

    /// Add key-value pair to HeaderParameter
    pub fn header_mut(
        &mut self,
        key: String,
        value: String,
    ) -> Result<&mut Self, ResourceValidationErrorKind> {
        if self.headers.iter().any(|(k, _)| k == &key) {
            Err(ResourceValidationErrorKind::ImportExportDuplicateHttpHeader(key))
        } else {
            self.headers.push((key, value));
            Ok(self)
        }
    }

    /// Add key-value pair to GetParameter
    pub fn get_mut(&mut self, key: String, value: String) -> &Self {
        self.get_parameters.push((key, value));
        self
    }

    /// Add fragment to HttpParameter
    pub fn fragment_mut(
        &mut self,
        value: String,
    ) -> Result<&mut Self, ResourceValidationErrorKind> {
        if self.fragment.is_some() {
            return Err(ResourceValidationErrorKind::ImportExportDuplicateFragment);
        } else {
            self.fragment = Some(value);
        }
        Ok(self)
    }

    /// Add key-value pair to PostParameter
    pub fn post_mut(&mut self, key: String, value: String) -> &Self {
        self.post_parameters.push((key, value));
        self
    }

    /// Builds a [Resource] with type [Resource::Iri] or [Resource::Path] that contains all registered parameters
    pub fn finalize(self) -> Result<Resource, ResourceValidationErrorKind> {
        match self.location {
            ResourceLocation::Path(path) => {
                if self.headers.is_empty()
                    && self.get_parameters.is_empty()
                    && self.fragment.is_none()
                    && self.post_parameters.is_empty()
                {
                    Ok(Resource::Path(path))
                } else {
                    Err(ResourceValidationErrorKind::ImportExportUnexpectedHttpParameter)
                }
            }
            ResourceLocation::Iri(iri) => Ok(Resource::Iri {
                iri: (iri),
                parameters: (HttpParameters {
                    headers: self.headers,
                    get_parameters: self.get_parameters,
                    fragment: self.fragment,
                    post_parameters: self.post_parameters,
                }),
            }),
        }
    }
}

impl From<String> for ResourceBuilder {
    /// Create a new [ResourceBuilder] from a local path
    fn from(path: String) -> Self {
        Self {
            location: ResourceLocation::Path(PathBuf::from_slash(path)),
            headers: Vec::new(),
            get_parameters: Vec::new(),
            fragment: None,
            post_parameters: Vec::new(),
        }
    }
}

impl From<Iri<String>> for ResourceBuilder {
    /// Create a new [ResourceBuilder] from a web-Iri
    fn from(iri: Iri<String>) -> Self {
        Self {
            location: ResourceLocation::Iri(iri),
            headers: Vec::new(),
            get_parameters: Vec::new(),
            fragment: None,
            post_parameters: Vec::new(),
        }
    }
}

impl TryFrom<AnyDataValue> for ResourceBuilder {
    type Error = ResourceValidationErrorKind;

    fn try_from(value: AnyDataValue) -> Result<Self, Self::Error> {
        let string = value.to_plain_string().or_else(|| value.to_iri()).ok_or(
            ResourceValidationErrorKind::ImportExportInvalidHttpParameter(String::from(
                "Resource or Endpoint",
            )),
        )?;
        match Iri::parse(string.to_owned()) {
            Ok(iri) => {
                let scheme = iri.scheme();
                debug!("scheme:{}", scheme);
                match scheme {
                    "http" | "https" => Ok(Self::from(iri)),
                    "file" => Ok(Self::from(Self::strip_local_iri(iri)?)),
                    _ => Err(ResourceValidationErrorKind::ImportExportUnknownIriScheme(
                        String::from(scheme),
                    )),
                }
            }
            Err(_err) => Ok(Self::from(string)),
        }
    }
}
