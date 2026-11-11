//! The kinds of problems the IO layer reports,
//! and the resource a diagnostic is about.

use std::{fmt::Display, path::Path};

use nemo_physical::{diagnostic_kinds, diagnostics::Diagnostic};

/// The name of the diagnostic field that names the resource a problem is about.
const RESOURCE: &str = "resource";

/// Diagnostics that name the resource they are about, such as a file path, an IRI or an endpoint.
///
/// The resource is the diagnostic's field `resource`.
/// Either the diagnostic's kind declares that field,
/// or [with_resource](Self::with_resource) adds it, for example to a diagnostic of the physical
/// layer.
pub trait AboutResource: Sized {
    /// Names the resource the problem is about.
    fn with_resource(self, resource: impl Into<String>) -> Self;

    /// Returns the resource the problem is about, or `None` if none is named.
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
    /// A problem reading or writing a resource.
    ///
    /// Either the resource's bytes could not be reached,
    /// or they were not what the format expects.
    pub enum IoError {
        Unreadable { resource: String, reason: String }
            => UNREADABLE = 101, error, "could not read `{resource}`: {reason}",
               explain "A resource could not be opened or read.";
        Unwritable { resource: String, reason: String }
            => UNWRITABLE = 102, error, "could not write `{resource}`: {reason}",
               explain "A resource could not be created or written.";
        InputUnreadable { reason: String }
            => INPUT_UNREADABLE = 103, error, "could not read the input: {reason}",
               explain "Reading an input failed partway, for example because of \
                        malformed quoting, invalid UTF-8, a truncated record, or a \
                        compressed stream that ends early.";
        OutputUnwritable { reason: String }
            => OUTPUT_UNWRITABLE = 104, error, "could not write the output: {reason}",
               explain "Writing an output failed partway.";
        HeaderLacksColumn { name: String }
            => HEADER_LACKS_COLUMN = 114, error, "the header has no column `{name}`",
               explain "An import that names its columns reads the first record of the \
                        resource as the header, and finds each column by its name. One of \
                        the names is not in the header.";
        HeaderRepeatsColumn { name: String }
            => HEADER_REPEATS_COLUMN = 115, error,
               "the header has more than one column `{name}`",
               explain "An import that names its columns finds each column by its name in \
                        the header, which is the first record of the resource. The header \
                        has one of the names more than once, so the name does not say which \
                        column is meant.";
        NotJson { reason: String }
            => NOT_JSON = 105, error, "invalid JSON: {reason}",
               explain "The document of a JSON import is not valid JSON.";
        NotRecords { place: String }
            => NOT_RECORDS = 116, error, "{place} is not an array of records",
               explain "A JSON import reads an array of records, one record per row. The \
                        document is not an array, or the value that the `path` option names \
                        inside it is not one.";
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
        PartialResult { url: String, reason: String }
            => PARTIAL_RESULT = 113, error,
               "`{url}` returned only part of the result: {reason}",
               explain "The endpoint responded, but said in its status or headers that the \
                        response holds only part of the result: it stopped at its time limit, \
                        or at the most rows it returns, as Virtuoso does behind DBpedia. \
                        None of the rows are kept, since they are not the result. A bound \
                        import's request is sent again in halves first, so the error \
                        remains where a single binding still asks for too much.",
               hint "narrow the query, or give the import a `where` clause so that each \
                     request asks for less";
        SparqlResponseCut { endpoint: String, reason: String }
            => SPARQL_RESPONSE_CUT = 112, error,
               "`{endpoint}` cut off its response: {reason}",
               explain "A SPARQL endpoint stopped partway through its response: it \
                        appended an error message, typically a timeout, or the response \
                        ends partway through a row. The rows before are not the complete \
                        result, so none of them are kept. A bound import's request is sent \
                        again in halves first, so the error remains where a single binding \
                        is cut off.",
               hint "narrow the query";
    }
}

impl IoError {
    /// Returns the diagnostic of a file that cannot be read, naming it as the resource.
    pub(crate) fn unreadable(path: &Path, reason: impl Display) -> Diagnostic {
        IoError::Unreadable {
            resource: path.display().to_string(),
            reason: reason.to_string(),
        }
        .diagnostic()
    }

    /// Returns the diagnostic of a file that cannot be written, naming it as the resource.
    pub(crate) fn unwritable(path: &Path, reason: impl Display) -> Diagnostic {
        IoError::Unwritable {
            resource: path.display().to_string(),
            reason: reason.to_string(),
        }
        .diagnostic()
    }

    /// Returns the diagnostic of a stream that cannot be read.
    pub(crate) fn input_unreadable(reason: impl Display) -> Diagnostic {
        IoError::InputUnreadable {
            reason: reason.to_string(),
        }
        .diagnostic()
    }

    /// Returns the diagnostic of a stream that cannot be written.
    pub(crate) fn output_unwritable(reason: impl Display) -> Diagnostic {
        IoError::OutputUnwritable {
            reason: reason.to_string(),
        }
        .diagnostic()
    }
}
