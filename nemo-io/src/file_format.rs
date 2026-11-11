//! File formats: the serializations that data is read from and written to.
//!
//! Each format has a name, and may have a media type, a file extension and a fixed number of
//! columns.
//! It accepts its own options for reading and for writing.

use std::fmt;

use crate::compression::Compression;

/// A serialization that data is read from or written to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    /// Comma-separated values.
    Csv,
    /// Tab-separated values.
    Tsv,
    /// Values separated by a declared delimiter.
    Dsv,
    /// An RDF serialization, either a named variant or `None` for the generic format `rdf`.
    ///
    /// The variant of the generic format is the one that the resource's extension names
    /// ([rdf_variant](Self::rdf_variant)).
    Rdf(Option<RdfVariant>),
    /// A JSON document when reading, and an array of rows when writing.
    Json,
    /// A Parquet file.
    Parquet,
    /// The solutions of a SPARQL query.
    Sparql,
}

/// A named RDF serialization.
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

/// The shared options that one format accepts.
///
/// Several formats share options such as `limit`.
/// Each field says whether the format accepts one of them.
/// An option that the format does not accept is an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptedOptions {
    /// Whether the format accepts `base`, the IRI that relative IRIs are resolved against.
    pub base: bool,
    /// Whether the format accepts `compression`.
    pub compression: bool,
    /// Whether the format accepts `http_headers`, which only a read of a possibly remote
    /// resource uses.
    pub http_headers: bool,
    /// Whether the format accepts `limit`, the largest number of rows.
    pub limit: bool,
    /// Whether the format accepts `format`, one
    /// [ColumnFormat](crate::value_format::ColumnFormat) per column.
    pub format: bool,
    /// Whether the format accepts `columns`, the names of the columns in the resource.
    pub columns: bool,
}

/// The name of a format, and its conventional file extension and media type if it has them.
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
    /// Every format that has a name, including the generic RDF format.
    ///
    /// The lookups by name and by extension search this list.
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

    /// Returns the name, the file extension and the media type of the format.
    ///
    /// This is the one table that every lookup reads.
    /// The entries of the named RDF variants are in [RdfVariant::entry].
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
            // The solutions arrive as SPARQL TSV. The media type is the
            // response's, and since no file has this format, there is no extension.
            FileFormat::Sparql => Entry {
                name: "sparql",
                extension: None,
                media_type: Some("text/tab-separated-values"),
            },
        }
    }

    /// Returns the format with the given name, or `None` for an unknown name.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|format| format.name() == name)
    }

    /// Returns the name of the format, the inverse of [from_name](Self::from_name).
    pub fn name(self) -> &'static str {
        self.entry().name
    }

    /// Returns the format that the extension of a resource's name names,
    /// after [stripping](Compression::strip_suffix) any compression suffix.
    ///
    /// Returns `None` for a `.dsv` name, which does not say what the delimiter is.
    pub(crate) fn from_extension(resource: &str) -> Option<Self> {
        let (_, extension) = Compression::strip_suffix(resource).rsplit_once('.')?;

        Self::ALL
            .into_iter()
            .filter(|format| *format != FileFormat::Dsv)
            .find(|format| format.extension() == Some(extension))
    }

    /// Returns the conventional file extension, or `None` if the format has none.
    ///
    /// The generic RDF format has none.
    pub fn extension(self) -> Option<&'static str> {
        self.entry().extension
    }

    /// Returns the media type to ask for when fetching a resource of the format,
    /// or `None` if the format has none.
    ///
    /// For SPARQL, it is the media type of the responses that the reader parses.
    pub fn media_type(self) -> Option<&'static str> {
        self.entry().media_type
    }

    /// Returns the number of columns that the format fixes, or `None` if it fixes none.
    ///
    /// A named RDF variant fixes its [width](RdfVariant::width),
    /// and JSON fixes three, for the triples that the [JSON reader](crate::json) produces.
    pub fn width(self) -> Option<usize> {
        match self {
            FileFormat::Rdf(Some(variant)) => Some(variant.width()),
            FileFormat::Json => Some(3),
            _ => None,
        }
    }

    /// Returns whether the format is delimiter-separated text.
    pub fn is_dsv(self) -> bool {
        matches!(self, FileFormat::Csv | FileFormat::Tsv | FileFormat::Dsv)
    }

    /// Returns the RDF variant of the format.
    ///
    /// For the generic RDF format, it is the variant that the extension of `resource` names.
    /// Returns `None` for a format that is not RDF,
    /// and for the generic one if `resource` is `None` or its extension names no variant.
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

    /// Returns the options that the format accepts,
    /// for reading if `reading` is true and for writing otherwise.
    pub fn options(self, reading: bool) -> AcceptedOptions {
        match self {
            FileFormat::Rdf(_) => AcceptedOptions {
                base: true,
                compression: true,
                http_headers: reading,
                limit: true,
                format: reading,
                columns: false,
            },
            // A JSON document is read whole, and its compression is always detected.
            FileFormat::Json => AcceptedOptions {
                base: false,
                compression: !reading,
                http_headers: reading,
                limit: !reading,
                format: false,
                columns: false,
            },
            // Parquet has its own compression, and is read from local
            // files only.
            FileFormat::Parquet => AcceptedOptions {
                base: false,
                compression: false,
                http_headers: false,
                limit: !reading,
                format: false,
                columns: false,
            },
            FileFormat::Csv | FileFormat::Tsv | FileFormat::Dsv => AcceptedOptions {
                base: false,
                compression: true,
                http_headers: reading,
                limit: true,
                format: true,
                columns: true,
            },
            FileFormat::Sparql => AcceptedOptions {
                base: true,
                compression: false,
                http_headers: true,
                limit: false,
                format: true,
                columns: false,
            },
        }
    }
}

impl fmt::Display for FileFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl RdfVariant {
    /// Returns the name, the file extension and the media type of the variant.
    ///
    /// This is the one table of the variants that every lookup reads.
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

    /// Returns whether the variant's statements are quads, which have a graph.
    pub fn quads(self) -> bool {
        matches!(self, RdfVariant::NQuads | RdfVariant::TriG)
    }

    /// Returns whether the variant puts each statement on a line of its own,
    /// as N-Triples and N-Quads do.
    pub(crate) fn line_based(self) -> bool {
        matches!(self, RdfVariant::NTriples | RdfVariant::NQuads)
    }

    /// Returns the number of columns of the variant's rows:
    /// four for quads, with the graph first, and three for triples.
    pub fn width(self) -> usize {
        if self.quads() { 4 } else { 3 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The name of every format.
    const NAMES: [&str; 12] = [
        "csv", "tsv", "dsv", "rdf", "ntriples", "nquads", "turtle", "trig", "rdfxml", "json",
        "parquet", "sparql",
    ];

    /// Every format's name maps back to the format, and an unknown name maps to none.
    #[test]
    fn names_round_trip() {
        for name in NAMES {
            let format = FileFormat::from_name(name).expect("a known name");
            assert_eq!(format.name(), name);
        }

        assert_eq!(FileFormat::from_name("xml"), None);
    }

    /// The extension of a resource's name gives its format, ignoring a compression suffix and a
    /// URL's query and fragment, and it gives the variant of the generic RDF format.
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

    /// Each format has its own media type, extension, width and options.
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
        assert_eq!(FileFormat::Rdf(Some(RdfVariant::NQuads)).width(), Some(4));
        assert_eq!(FileFormat::Json.width(), Some(3));
        assert_eq!(FileFormat::Csv.width(), None);
        assert!(FileFormat::Tsv.is_dsv() && !FileFormat::Json.is_dsv());
        assert!(
            FileFormat::Json.options(true).http_headers
                && !FileFormat::Json.options(false).http_headers
        );
    }
}
