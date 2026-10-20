//! The kinds of problems the IO layer reports,
//! and the resource a diagnostic is about.

use std::{fmt::Display, path::Path};

use nemo_physical::{diagnostic_kinds, diagnostics::Diagnostic};

/// The name of the field naming the resource a problem is about.
const RESOURCE: &str = "resource";

/// Diagnostics that name the resource they are about,
/// a file path, an IRI or an endpoint.
///
/// The resource is the field named `resource`,
/// declared on a kind or added to a diagnostic that came from deeper down.
pub trait AboutResource: Sized {
    /// Names the resource the problem is about.
    fn with_resource(self, resource: impl Into<String>) -> Self;

    /// Returns the resource the problem is about, if named.
    fn resource(&self) -> Option<&str>;
}

impl AboutResource for Diagnostic {
    fn with_resource(self, resource: impl Into<String>) -> Self {
        self.with_field(RESOURCE, resource)
    }

    fn resource(&self) -> Option<&str> {
        self.field(RESOURCE)
    }
}

diagnostic_kinds! {
    /// A problem reading or writing a resource: the bytes could not be
    /// reached, or they were not what the format expects.
    pub enum IoError {
        Unreadable { resource: String, reason: String }
            => UNREADABLE = 101, error, "could not read `{resource}`: {reason}",
               explain "A resource could not be opened or read.";
        Unwritable { resource: String, reason: String }
            => UNWRITABLE = 102, error, "could not write `{resource}`: {reason}",
               explain "A resource could not be created or written.";
        StreamUnreadable { reason: String }
            => INPUT_UNREADABLE = 103, error, "could not read the input: {reason}",
               explain "Reading an input failed partway, for example because of \
                        malformed quoting, invalid UTF-8, a truncated record, or a \
                        compressed stream that ends early.";
        StreamUnwritable { reason: String }
            => OUTPUT_UNWRITABLE = 104, error, "could not write the output: {reason}",
               explain "Writing an output failed partway.";
        NotJson { reason: String }
            => NOT_JSON = 105, error, "invalid JSON: {reason}",
               explain "The document of a JSON import is not valid JSON.";
        NotABaseIri { base: String, reason: String }
            => NOT_A_BASE_IRI = 106, error, "`{base}` is not a valid base IRI: {reason}",
               explain "The `base` option of an import is not an absolute IRI that \
                        relative IRIs can be resolved against.";
        UnsupportedArrowType { resource: String, data_type: String }
            => UNSUPPORTED_ARROW_TYPE = 108, error,
               "`{resource}` has a column of unsupported Arrow type `{data_type}`",
               explain "A column of a Parquet file has an Arrow type that Nemo cannot \
                        convert into values.";
        SparqlResponseUnreadable { reason: String }
            => SPARQL_RESPONSE_UNREADABLE = 109, error,
               "could not read the SPARQL response: {reason}",
               explain "Reading the TSV response of a SPARQL endpoint failed partway.";
        Unfetchable { url: String, reason: String }
            => UNFETCHABLE = 111, error, "could not fetch `{url}`: {reason}",
               explain "A remote resource could not be fetched, either because no \
                        connection could be made or because the server responded with \
                        an error status.";
        SparqlResponseCut { endpoint: String, reason: String }
            => SPARQL_RESPONSE_CUT = 112, error,
               "`{endpoint}` cut off its response: {reason}",
               explain "A SPARQL endpoint stopped partway through its response and \
                        appended an error message, typically a timeout. The rows before \
                        the message are not the complete result, so none of them are \
                        kept. A bound import's request is sent again in halves first, \
                        so the error remains where a single binding is cut off.",
               hint "narrow the query";
    }
}

impl IoError {
    /// Returns the diagnostic of a file that cannot be read, naming it as the resource.
    pub(crate) fn unreadable(path: &Path, reason: impl Display) -> Diagnostic {
        let resource = path.display().to_string();
        IoError::Unreadable {
            resource: resource.clone(),
            reason: reason.to_string(),
        }
        .diagnostic()
        .with_resource(resource)
    }

    /// Returns the diagnostic of a file that cannot be written, naming it as the resource.
    pub(crate) fn unwritable(path: &Path, reason: impl Display) -> Diagnostic {
        let resource = path.display().to_string();
        IoError::Unwritable {
            resource: resource.clone(),
            reason: reason.to_string(),
        }
        .diagnostic()
        .with_resource(resource)
    }

    /// Returns the diagnostic of a stream that cannot be read.
    pub(crate) fn stream_unreadable(reason: impl Display) -> Diagnostic {
        IoError::StreamUnreadable {
            reason: reason.to_string(),
        }
        .diagnostic()
    }

    /// Returns the diagnostic of a stream that cannot be written.
    pub(crate) fn stream_unwritable(reason: impl Display) -> Diagnostic {
        IoError::StreamUnwritable {
            reason: reason.to_string(),
        }
        .diagnostic()
    }
}
