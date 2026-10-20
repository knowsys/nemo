//! File formats: the serializations data is read from and written to.
//!
//! Each format comes with its name, its media type and file extension,
//! the number of columns it fixes, and the options a read or a write of it accepts.

use crate::stream::compression::Compression;

/// A serialization data is read from or written to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    /// Comma-separated values.
    Csv,
    /// Tab-separated values.
    Tsv,
    /// Values separated by a declared delimiter.
    Dsv,
    /// An RDF serialization: a named variant, or the generic `rdf`
    /// format name, which resolves by the resource's extension.
    Rdf(Option<RdfVariant>),
    /// A JSON document (reading) or array of rows (writing).
    Json,
    /// A Parquet file.
    Parquet,
    /// The solutions of a SPARQL query.
    Sparql,
}

/// The RDF serialization variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdfVariant {
    /// N-Triples (`.nt`).
    NTriples,
    /// N-Quads (`.nq`).
    NQuads,
    /// Turtle (`.ttl`).
    Turtle,
    /// TriG (`.trig`).
    TriG,
    /// RDF/XML (`.rdf`).
    RdfXml,
}

/// The options of the shared vocabulary a format accepts.
///
/// Any other option of that vocabulary is an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormatOptions {
    /// Whether `base` is meaningful.
    pub base: bool,
    /// Whether `compression` is meaningful.
    pub compression: bool,
    /// Whether `http_headers` is meaningful (reading a possibly remote
    /// resource).
    pub headers: bool,
    /// Whether `limit` (at most this many rows) applies.
    pub limit: bool,
    /// Whether `format` (one column format per column, `skip` dropping
    /// a column) applies.
    pub formats: bool,
}

/// What goes with a format:
/// its name, and its conventional file extension and media type if it has them.
#[derive(Debug, Clone, Copy)]
struct Entry {
    /// The name of the format.
    name: &'static str,
    /// The conventional file extension.
    extension: Option<&'static str>,
    /// The media type.
    media_type: Option<&'static str>,
}

impl FileFormat {
    /// Every format there is a name for, the generic RDF format included:
    /// what the lookups by name and by extension search.
    const ALL: [FileFormat; 12] = [
        FileFormat::Csv,
        FileFormat::Tsv,
        FileFormat::Dsv,
        FileFormat::Rdf(None),
        FileFormat::Rdf(Some(RdfVariant::NTriples)),
        FileFormat::Rdf(Some(RdfVariant::NQuads)),
        FileFormat::Rdf(Some(RdfVariant::Turtle)),
        FileFormat::Rdf(Some(RdfVariant::TriG)),
        FileFormat::Rdf(Some(RdfVariant::RdfXml)),
        FileFormat::Json,
        FileFormat::Parquet,
        FileFormat::Sparql,
    ];

    /// Returns what goes with the format, from the one table every lookup reads.
    ///
    /// The RDF variants' rows are [theirs](RdfVariant::entry).
    fn entry(self) -> Entry {
        match self {
            FileFormat::Csv => Entry {
                name: "csv",
                extension: Some("csv"),
                media_type: Some("text/csv"),
            },
            FileFormat::Tsv => Entry {
                name: "tsv",
                extension: Some("tsv"),
                media_type: Some("text/tab-separated-values"),
            },
            FileFormat::Dsv => Entry {
                name: "dsv",
                extension: Some("dsv"),
                media_type: None,
            },
            FileFormat::Rdf(None) => Entry {
                name: "rdf",
                extension: None,
                media_type: None,
            },
            FileFormat::Rdf(Some(variant)) => {
                let (name, extension, media_type) = variant.entry();
                Entry {
                    name,
                    extension: Some(extension),
                    media_type: Some(media_type),
                }
            }
            FileFormat::Json => Entry {
                name: "json",
                extension: Some("json"),
                media_type: Some("application/json"),
            },
            FileFormat::Parquet => Entry {
                name: "parquet",
                extension: Some("parquet"),
                media_type: Some("application/vnd.apache.parquet"),
            },
            // The solutions arrive as SPARQL TSV: the media type is the
            // response's, and no file carries the format.
            FileFormat::Sparql => Entry {
                name: "sparql",
                extension: None,
                media_type: Some("text/tab-separated-values"),
            },
        }
    }

    /// Returns the format a name names, if any.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|format| format.name() == name)
    }

    /// Returns the name of the format, the inverse of [from_name](Self::from_name).
    pub fn name(self) -> &'static str {
        self.entry().name
    }

    /// Returns the format a resource's extension names, after any compression suffix.
    ///
    /// This is how the generic formats resolve.
    /// A `.dsv` name resolves to nothing, since it says nothing about the delimiter.
    pub(crate) fn from_extension(resource: &str) -> Option<Self> {
        let (_, extension) = Compression::strip_suffix(resource).rsplit_once('.')?;
        Self::ALL
            .into_iter()
            .filter(|format| *format != FileFormat::Dsv)
            .find(|format| format.extension() == Some(extension))
    }

    /// Returns the conventional file extension, if the format has one.
    ///
    /// A generic RDF format has none until it resolves.
    pub fn extension(self) -> Option<&'static str> {
        self.entry().extension
    }

    /// Returns the media type a fetch asks for, if the format has one.
    ///
    /// For SPARQL, it is the type of the response the reader parses.
    pub fn media_type(self) -> Option<&'static str> {
        self.entry().media_type
    }

    /// Returns the number of columns the format fixes, if it fixes one:
    /// for RDF statements, and for the node/edge triples of the JSON reader.
    pub fn arity(self) -> Option<usize> {
        match self {
            FileFormat::Rdf(Some(variant)) => Some(variant.arity()),
            FileFormat::Json => Some(3),
            _ => None,
        }
    }

    /// Returns whether the format is delimiter-separated text.
    pub fn is_dsv(self) -> bool {
        matches!(self, FileFormat::Csv | FileFormat::Tsv | FileFormat::Dsv)
    }

    /// Returns the RDF variant the format fixes,
    /// or for the generic format the one the resource's extension names.
    ///
    /// Returns `None` for any other format and for a resource whose extension names no variant.
    pub fn rdf_variant(self, resource: Option<&str>) -> Option<RdfVariant> {
        match self {
            FileFormat::Rdf(Some(variant)) => Some(variant),
            FileFormat::Rdf(None) => match Self::from_extension(resource?)? {
                FileFormat::Rdf(variant) => variant,
                _ => None,
            },
            _ => None,
        }
    }

    /// Returns the options the format accepts, for reading or for writing.
    pub fn options(self, reading: bool) -> FormatOptions {
        match self {
            FileFormat::Rdf(_) => FormatOptions {
                base: true,
                compression: true,
                headers: reading,
                limit: true,
                formats: reading,
            },
            // A JSON import is read whole; its compression detects
            // from the name alone.
            FileFormat::Json => FormatOptions {
                base: false,
                compression: !reading,
                headers: reading,
                limit: !reading,
                formats: false,
            },
            // Parquet compresses internally, and remote Parquet is
            // future work: the resource alone.
            FileFormat::Parquet => FormatOptions {
                base: false,
                compression: false,
                headers: false,
                limit: !reading,
                formats: false,
            },
            FileFormat::Csv | FileFormat::Tsv | FileFormat::Dsv => FormatOptions {
                base: false,
                compression: true,
                headers: reading,
                limit: true,
                formats: true,
            },
            FileFormat::Sparql => FormatOptions {
                base: true,
                compression: false,
                headers: true,
                limit: false,
                formats: true,
            },
        }
    }
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl RdfVariant {
    /// Returns the name, the file extension, and the media type of the variant,
    /// from the one table every lookup reads.
    fn entry(self) -> (&'static str, &'static str, &'static str) {
        match self {
            RdfVariant::NTriples => ("ntriples", "nt", "application/n-triples"),
            RdfVariant::NQuads => ("nquads", "nq", "application/n-quads"),
            RdfVariant::Turtle => ("turtle", "ttl", "text/turtle"),
            RdfVariant::TriG => ("trig", "trig", "application/trig"),
            RdfVariant::RdfXml => ("rdfxml", "rdf", "application/rdf+xml"),
        }
    }

    /// Returns the conventional file extension.
    pub fn extension(self) -> &'static str {
        let (_, extension, _) = self.entry();
        extension
    }

    /// Returns the media type of the serialization.
    pub fn media_type(self) -> &'static str {
        let (_, _, media_type) = self.entry();
        media_type
    }

    /// Returns whether statements carry a graph column.
    pub fn quads(self) -> bool {
        matches!(self, RdfVariant::NQuads | RdfVariant::TriG)
    }

    /// Returns whether a statement of the variant takes one line,
    /// as in N-Triples and N-Quads.
    pub fn line_based(self) -> bool {
        matches!(self, RdfVariant::NTriples | RdfVariant::NQuads)
    }

    /// Returns the delivered arity: four columns for the quad variants, the graph first,
    /// and three otherwise.
    pub fn arity(self) -> usize {
        if self.quads() { 4 } else { 3 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NAMES: [&str; 12] = [
        "csv", "tsv", "dsv", "rdf", "ntriples", "nquads", "turtle", "trig", "rdfxml", "json",
        "parquet", "sparql",
    ];

    #[test]
    fn names_round_trip() {
        for name in NAMES {
            let format = FileFormat::from_name(name).expect("a known name");
            assert_eq!(format.name(), name);
        }
        assert_eq!(FileFormat::from_name("xml"), None);
    }

    #[test]
    fn extensions_resolve_through_compression() {
        assert_eq!(
            FileFormat::from_extension("graph.ttl.gz"),
            Some(FileFormat::Rdf(Some(RdfVariant::Turtle)))
        );
        assert_eq!(
            FileFormat::from_extension("rows.csv"),
            Some(FileFormat::Csv)
        );
        assert_eq!(FileFormat::from_extension("noext"), None);
        // A URL's query and fragment are not part of its name.
        assert_eq!(
            FileFormat::from_extension("https://h/rows.csv.gz?dl=1"),
            Some(FileFormat::Csv)
        );
        assert_eq!(
            FileFormat::from_extension("https://h/graph.ttl#top"),
            Some(FileFormat::Rdf(Some(RdfVariant::Turtle)))
        );
        assert_eq!(
            FileFormat::Rdf(None).rdf_variant(Some("data.nq")),
            Some(RdfVariant::NQuads)
        );
        assert_eq!(FileFormat::Rdf(None).rdf_variant(Some("data.csv")), None);
        assert_eq!(
            FileFormat::Rdf(Some(RdfVariant::TriG)).rdf_variant(None),
            Some(RdfVariant::TriG)
        );
        assert_eq!(FileFormat::Json.rdf_variant(Some("x.ttl")), None);
    }

    #[test]
    fn metadata_is_per_format() {
        assert_eq!(FileFormat::Csv.media_type(), Some("text/csv"));
        assert_eq!(
            FileFormat::Rdf(Some(RdfVariant::Turtle)).media_type(),
            Some("text/turtle")
        );
        assert_eq!(FileFormat::Rdf(None).media_type(), None);
        assert_eq!(FileFormat::Dsv.extension(), Some("dsv"));
        assert_eq!(FileFormat::Sparql.extension(), None);
        assert_eq!(FileFormat::Rdf(Some(RdfVariant::NQuads)).arity(), Some(4));
        assert_eq!(FileFormat::Json.arity(), Some(3));
        assert_eq!(FileFormat::Csv.arity(), None);
        assert!(FileFormat::Tsv.is_dsv() && !FileFormat::Json.is_dsv());
        assert!(FileFormat::Json.options(true).headers && !FileFormat::Json.options(false).headers);
    }
}
