//! Exports: the directives a program declares, and writing a
//! predicate's rows out in the serialization each one names.

use nemo_io::{
    compression::Compression,
    written::{Writer, Written},
};
use nemo_physical::{diagnostics::Diagnostic, value::DataValue};

use crate::{
    diagnostics::{AboutResource, TranslationError},
    engine::{Engine, compile::translation::exports::rdf_export_arity},
};

/// One export directive: the predicate and the file serialization it declares.
///
/// # Example
///
/// ```
/// use nemo::{
///     engine::{Engine, ExportSpec},
///     model::{FileId, Program},
/// };
///
/// let (program, _) = Program::parse("pair(1, 2) . pair(3, 4) .", FileId::new(0));
/// let mut engine = Engine::new();
/// engine.load(program)?;
/// engine.reason()?;
///
/// let spec = ExportSpec::csv("pair");
/// assert_eq!(spec.file_name(), "pair.csv");
/// let bytes = engine.export_bytes(&spec)?;
/// let text = String::from_utf8(bytes).expect("CSV is text");
/// assert_eq!(text.lines().count(), 2);
/// # Ok::<(), nemo::diagnostics::Diagnostic>(())
/// ```
#[derive(Debug, Clone)]
pub struct ExportSpec {
    /// The exported predicate, resolved.
    pub predicate: String,
    /// The declared resource name, or `None` for the conventional `predicate.extension`.
    pub resource: Option<String>,
    /// The serialization written.
    pub format: ExportFormat,
    /// The stream compression (never applied to Parquet, which
    /// compresses internally).
    pub compression: Compression,
    /// The most rows written, if declared.
    pub limit: Option<u64>,
}

/// The serialization of one export, with the options of its format.
#[derive(Debug, Clone)]
pub enum ExportFormat {
    /// Delimiter-separated values.
    Dsv {
        /// The column delimiter.
        delimiter: u8,
        /// The file extension of the format name.
        extension: String,
        /// The declared column formats, or `None` to write every column in the `term` format.
        formats: Option<Vec<nemo_io::format::ValueFormat>>,
    },
    /// An RDF serialization.
    ///
    /// The predicate must have three columns, or four with the graph first.
    Rdf {
        /// The serialization variant.
        variant: nemo_io::file_format::RdfVariant,
        /// The base IRI, written if the serialization carries one.
        base: Option<String>,
        /// The prefix declarations, the program's own,
        /// written if the serialization abbreviates.
        prefixes: Vec<(String, String)>,
    },
    /// A JSON array of row arrays.
    Json,
    /// A Parquet file over the Arrow interchange mapping.
    Parquet,
}

impl ExportFormat {
    /// Returns the conventional file extension of this serialization.
    pub fn extension(&self) -> &str {
        match self {
            ExportFormat::Dsv { extension, .. } => extension,
            ExportFormat::Rdf { variant, .. } => variant.extension(),
            ExportFormat::Json => "json",
            ExportFormat::Parquet => "parquet",
        }
    }
}

impl ExportSpec {
    /// Creates a specification writing a predicate as comma-separated values in the `term`
    /// column format.
    pub fn csv(predicate: impl Into<String>) -> Self {
        Self {
            predicate: predicate.into(),
            resource: None,
            format: ExportFormat::Dsv {
                delimiter: b',',
                extension: String::from("csv"),
                formats: None,
            },
            compression: Compression::Auto,
            limit: None,
        }
    }

    /// Returns whether the specification writes to standard output,
    /// which an empty resource name requests.
    pub fn is_stdout(&self) -> bool {
        self.resource.as_deref() == Some("")
    }

    /// Returns the file this specification writes to, relative to the export
    /// directory: the declared resource name, or the conventional
    /// `predicate.extension` with the characters of an IRI predicate
    /// that a file name cannot carry mangled to `_`. Empty for
    /// standard output.
    pub fn file_name(&self) -> String {
        let name = self.named();
        if name.is_empty() {
            return name;
        }
        // A gzipped stream carries `.gz`, because that is how every
        // reader that detects compression by name, this engine's
        // included, knows to unwrap it. Parquet never gzips.
        if self.compression == Compression::Gzip
            && !name.ends_with(".gz")
            && !matches!(self.format, ExportFormat::Parquet)
        {
            return format!("{name}.gz");
        }
        name
    }

    /// Returns the file name of the specification, without the `.gz` suffix that gzip
    /// compression adds.
    fn named(&self) -> String {
        self.resource.clone().unwrap_or_else(|| {
            let mangled: String = self
                .predicate
                .chars()
                .map(|character| {
                    if character.is_alphanumeric() || matches!(character, '.' | '-' | '_') {
                        character
                    } else {
                        '_'
                    }
                })
                .collect();
            format!("{mangled}.{}", self.format.extension())
        })
    }
}

/// One export's configured serializer,
/// one of the writers behind [export_specs](Engine::export_specs)
/// and [export_bytes](Engine::export_bytes).
enum ExportWriter {
    /// Delimiter-separated values.
    Dsv(nemo_io::dsv::DsvWriter),
    /// An RDF serialization.
    Rdf(nemo_io::rdf::RdfWriter),
    /// A JSON array of row arrays.
    Json(nemo_io::json::JsonWriter),
    /// A Parquet file, dispatched apart, since its writer wants the whole target and never a
    /// gzip stream.
    #[cfg(feature = "parquet")]
    Parquet(nemo_io::parquet::ParquetWriter),
}

impl ExportWriter {
    /// Writes rows to a streaming target.
    fn write<'a>(
        &self,
        target: impl std::io::Write,
        rows: impl Iterator<Item = &'a [DataValue]>,
    ) -> Result<Written, Diagnostic> {
        match self {
            ExportWriter::Dsv(writer) => writer.write(target, rows),
            ExportWriter::Rdf(writer) => writer.write(target, rows),
            ExportWriter::Json(writer) => writer.write(target, rows),
            #[cfg(feature = "parquet")]
            ExportWriter::Parquet(_) => unreachable!("Parquet writes dispatch before streaming"),
        }
    }
}

impl Engine {
    /// Writes export specifications' rows into files of a directory,
    /// or to the standard output for an empty resource,
    /// which a closed pipe ends quietly.
    pub(in crate::engine) fn write_exports(
        &mut self,
        directory: impl AsRef<std::path::Path>,
        specs: &[ExportSpec],
    ) -> Result<(), Diagnostic> {
        let directory = directory.as_ref();
        for spec in specs {
            if spec.is_stdout() {
                let bytes = self.export_bytes(spec)?;
                let mut stdout = std::io::stdout().lock();
                let written = std::io::Write::write_all(&mut stdout, &bytes)
                    .and_then(|()| std::io::Write::flush(&mut stdout));
                match written {
                    // A closed pipe means the reader has all it wants.
                    Err(error) if error.kind() != std::io::ErrorKind::BrokenPipe => {
                        return Err(Diagnostic::error(
                            nemo_io::codes::OUTPUT_UNWRITABLE,
                            format!("could not write the output: {error}"),
                        )
                        .with_resource(String::from("<stdout>")));
                    }
                    _ => continue,
                }
            }
            let path = directory.join(spec.file_name());
            let rows = self.export_rows(spec)?;
            let rows = rows.iter().map(Vec::as_slice);
            let written = match self.export_writer(spec)? {
                ExportWriter::Dsv(writer) => writer.write_path(&path, rows)?,
                ExportWriter::Rdf(writer) => writer.write_path(&path, rows)?,
                ExportWriter::Json(writer) => writer.write_path(&path, rows)?,
                #[cfg(feature = "parquet")]
                ExportWriter::Parquet(writer) => writer.write_path(&path, rows)?,
            };
            self.report_dropped(spec, written.dropped, path.display().to_string());
        }
        Ok(())
    }

    /// Serializes one export specification's rows into memory.
    pub(in crate::engine) fn serialize_export(
        &mut self,
        spec: &ExportSpec,
    ) -> Result<Vec<u8>, Diagnostic> {
        let rows = self.export_rows(spec)?;
        let name = spec.file_name();
        // A stream has no name to detect compression by: the option
        // decides alone.
        let target = (!spec.is_stdout()).then(|| std::path::Path::new(&name));
        let mut bytes = Vec::new();
        let writer = self.export_writer(spec)?;
        // Parquet compresses internally and needs its whole target.
        // Every other writer streams, through gzip if requested.
        #[cfg(feature = "parquet")]
        if let ExportWriter::Parquet(writer) = &writer {
            let written = writer.write(&mut bytes, rows.iter().map(Vec::as_slice))?;
            self.report_dropped(spec, written.dropped, name);
            return Ok(bytes);
        }
        let written = writer.write(
            spec.compression.wrap(&mut bytes, target),
            rows.iter().map(Vec::as_slice),
        )?;
        self.report_dropped(spec, written.dropped, name);
        Ok(bytes)
    }

    /// Returns the rows one export specification serializes, checked against
    /// the number of columns its format requires.
    fn export_rows(&mut self, spec: &ExportSpec) -> Result<Vec<Vec<DataValue>>, Diagnostic> {
        let mut rows = match self.arity_of(&spec.predicate) {
            Some(_) => self.rows(&spec.predicate)?,
            // Never used: an empty file states the empty result.
            None => Vec::new(),
        };
        if let Some(limit) = spec.limit {
            rows.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        }
        let arity = rows.first().map_or(0, Vec::len);
        match &spec.format {
            ExportFormat::Dsv {
                formats: Some(formats),
                ..
            } if formats.len() != arity && !rows.is_empty() => {
                Err(TranslationError::ExportFormatArity {
                    predicate: spec.predicate.clone(),
                    declared: formats.len().into(),
                    arity: arity.into(),
                }
                .diagnostic())
            }
            ExportFormat::Rdf { variant, .. } if !rows.is_empty() && arity != variant.arity() => {
                Err(rdf_export_arity(*variant, &spec.predicate, arity).diagnostic())
            }
            _ => Ok(rows),
        }
    }

    /// Returns the configured writer of one export specification.
    fn export_writer(&self, spec: &ExportSpec) -> Result<ExportWriter, Diagnostic> {
        use nemo_io::format::ValueFormat;

        Ok(match &spec.format {
            ExportFormat::Dsv {
                delimiter, formats, ..
            } => {
                let arity = self.arity_of(&spec.predicate).unwrap_or(0);
                let formats = formats
                    .clone()
                    .unwrap_or_else(|| vec![ValueFormat::Term; arity]);
                let mut options = nemo_io::dsv::DsvOptions::new(*delimiter, formats);
                options.compression = spec.compression;
                ExportWriter::Dsv(nemo_io::dsv::DsvWriter::new(options))
            }
            ExportFormat::Rdf {
                variant,
                base,
                prefixes,
            } => {
                let mut options = nemo_io::rdf::RdfWriterOptions::new(*variant);
                options.base = base.clone();
                options.prefixes = prefixes.clone();
                options.compression = spec.compression;
                ExportWriter::Rdf(nemo_io::rdf::RdfWriter::new(options))
            }
            ExportFormat::Json => ExportWriter::Json(nemo_io::json::JsonWriter {
                compression: spec.compression,
            }),
            #[cfg(feature = "parquet")]
            ExportFormat::Parquet => ExportWriter::Parquet(nemo_io::parquet::ParquetWriter::new(
                self.arity_of(&spec.predicate).unwrap_or(0),
            )),
            #[cfg(not(feature = "parquet"))]
            ExportFormat::Parquet => {
                return Err(TranslationError::FeatureUnavailable {
                    what: String::from("a Parquet export"),
                    feature: String::from("parquet"),
                }
                .diagnostic());
            }
        })
    }

    /// Reports the rows an export dropped, if any.
    fn report_dropped(&mut self, spec: &ExportSpec, dropped: u64, resource: String) {
        if dropped > 0 {
            self.pipeline.report(
                TranslationError::ExportDropped {
                    dropped: dropped.into(),
                    predicate: spec.predicate.clone(),
                }
                .diagnostic()
                .with_resource(resource),
            );
        }
    }
}
