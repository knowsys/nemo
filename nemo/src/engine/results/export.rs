//! Exports: a program's export directives as specifications,
//! and writing a predicate's rows in the serialization that a specification names.

use std::{io::Write, ops::ControlFlow, path::Path};

use nemo_io::{
    compression::Compression,
    write::{RowSink, Writer, Written},
};
use nemo_physical::{diagnostics::Diagnostic, management::ColumnOrder, value::ValueRef};

use crate::{
    diagnostics::{AboutResource, EngineError, TranslationError},
    engine::{Engine, compile::translation::exports::rdf_export_arity},
};

/// One export: the predicate, and the file and serialization to write it to.
///
/// [exports](Engine::exports) returns the program's export directives in this form.
///
/// # Example
///
/// ```
/// use nemo::{engine::{Engine, ExportSpec}, model::Program};
///
/// let (program, _) = Program::parse("pair(1, 2) . pair(3, 4) .");
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
    /// The resolved name of the exported predicate.
    pub predicate: String,
    /// The resource name, or `None` for the conventional `predicate.extension`
    /// (see [file_name](Self::file_name)).
    pub resource: Option<String>,
    /// The serialization.
    pub format: ExportFormat,
    /// The stream compression, which is never applied to Parquet,
    /// since Parquet compresses internally.
    pub compression: Compression,
    /// The largest number of rows to write, or `None` for no limit.
    pub limit: Option<u64>,
}

/// The serialization of one export, with the options of its format.
#[derive(Debug, Clone)]
pub enum ExportFormat {
    /// Delimiter-separated values.
    Dsv {
        /// The column delimiter.
        delimiter: u8,
        /// The file extension that goes with the format's name, such as `csv`.
        extension: String,
        /// The column formats, or `None` to write every column in the
        /// [Term](nemo_io::value_format::ValueFormat::Term) format.
        ///
        /// A column whose format admits a missing value writes a null as an empty cell.
        formats: Option<Vec<nemo_io::value_format::ColumnFormat>>,
        /// The names of the written columns, which are written first, as a header row,
        /// or `None` to write no header.
        columns: Option<Vec<String>>,
    },
    /// An RDF serialization.
    ///
    /// The predicate must have three columns, or four with the graph first.
    Rdf {
        /// The serialization variant.
        variant: nemo_io::file_format::RdfVariant,
        /// The base IRI, which is written if the serialization supports one.
        base: Option<String>,
        /// The program's prefix declarations,
        /// which are written if the serialization abbreviates IRIs.
        prefixes: Vec<(String, String)>,
    },
    /// A JSON array of row arrays.
    Json,
    /// A Parquet file, with the values mapped to Arrow types.
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
    /// Checks that rows with `arity` columns fit the specification's format:
    /// the `format` and `columns` options of delimiter-separated values list one format and
    /// one name per column, and an RDF statement has three columns, or four with its graph.
    fn check_arity(&self, arity: usize) -> Result<(), Diagnostic> {
        match &self.format {
            ExportFormat::Dsv {
                formats: Some(formats),
                ..
            } if formats.len() != arity => Err(TranslationError::ExportFormatArity {
                predicate: self.predicate.clone(),
                declared: formats.len().into(),
                arity: arity.into(),
            }
            .diagnostic()),
            ExportFormat::Dsv {
                columns: Some(names),
                ..
            } if names.len() != arity => Err(TranslationError::ColumnNameCount {
                names: names.len().into(),
                columns: arity.into(),
            }
            .diagnostic()),
            ExportFormat::Rdf { variant, .. } if arity != variant.width() => {
                Err(rdf_export_arity(*variant, &self.predicate, arity).diagnostic())
            }
            _ => Ok(()),
        }
    }

    /// Creates a specification that writes a predicate as comma-separated values,
    /// with every column in the [Term](nemo_io::value_format::ValueFormat::Term) format.
    pub fn csv(predicate: impl Into<String>) -> Self {
        Self {
            predicate: predicate.into(),
            resource: None,
            format: ExportFormat::Dsv {
                delimiter: b',',
                extension: String::from("csv"),
                formats: None,
                columns: None,
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

    /// Returns the name of the file this specification writes to, relative to the export
    /// directory.
    ///
    /// It is the resource name, or else the conventional `predicate.extension`,
    /// in which every character of the predicate other than a letter, a digit, `.`, `-` or `_`
    /// is replaced by `_`.
    /// Under gzip compression, `.gz` is appended if the name does not end with it,
    /// except for Parquet.
    /// The name is empty for standard output.
    pub fn file_name(&self) -> String {
        let name = self.named();
        if name.is_empty() {
            return name;
        }
        // A gzipped stream gets `.gz`, because every reader that detects
        // compression by name, this engine's included, needs it to unwrap
        // the stream. Parquet is never gzipped.
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

/// The configured writer of one export.
enum ExportWriter {
    /// Delimiter-separated values.
    Dsv(nemo_io::dsv::DsvWriter),
    /// An RDF serialization.
    Rdf(nemo_io::rdf::RdfWriter),
    /// A JSON array of row arrays.
    Json(nemo_io::json::JsonWriter),
    /// A Parquet file, which is written apart from the others,
    /// since its writer needs the whole target and never writes to a gzip stream.
    #[cfg(feature = "parquet")]
    Parquet(nemo_io::parquet::ParquetWriter),
}

impl ExportWriter {
    /// Writes a resource to a stream, compressed as the writer's options say,
    /// as [Writer::write_compressed_with] describes.
    ///
    /// `feed` writes the rows into the sink of the resource.
    /// A Parquet file compresses itself, and is written to the stream as it is.
    fn write_compressed_with(
        &self,
        target: impl Write + Send,
        name: Option<&Path>,
        feed: impl FnOnce(&mut dyn RowSink) -> Result<(), Diagnostic>,
    ) -> Result<Written, Diagnostic> {
        match self {
            ExportWriter::Dsv(writer) => writer.write_compressed_with(target, name, feed),
            ExportWriter::Rdf(writer) => writer.write_compressed_with(target, name, feed),
            ExportWriter::Json(writer) => writer.write_compressed_with(target, name, feed),
            #[cfg(feature = "parquet")]
            ExportWriter::Parquet(writer) => {
                let mut sink = writer.start(target);
                feed(&mut sink)?;

                sink.finish()
            }
        }
    }

    /// Writes a resource to a file, as [Writer::write_path_with] describes.
    ///
    /// `feed` writes the rows into the sink of the resource.
    fn write_path_with(
        &self,
        path: &Path,
        feed: impl FnOnce(&mut dyn RowSink) -> Result<(), Diagnostic>,
    ) -> Result<Written, Diagnostic> {
        match self {
            ExportWriter::Dsv(writer) => writer.write_path_with(path, feed),
            ExportWriter::Rdf(writer) => writer.write_path_with(path, feed),
            ExportWriter::Json(writer) => writer.write_path_with(path, feed),
            #[cfg(feature = "parquet")]
            ExportWriter::Parquet(writer) => writer.write_path_with(path, feed),
        }
    }
}

impl Engine {
    /// Writes the rows of export specifications into files of a directory,
    /// or to the standard output for an empty resource name.
    ///
    /// The rows are decoded from their table as they are written,
    /// so an export holds no second copy of the table.
    /// If the standard output is closed, the export to it ends without an error.
    /// Fails if no program is loaded, also for an empty list of specifications.
    pub(in crate::engine) fn write_exports(
        &mut self,
        directory: impl AsRef<Path>,
        specs: &[ExportSpec],
    ) -> Result<(), Diagnostic> {
        if self.program.is_none() {
            return Err(EngineError::NoProgram.diagnostic());
        }
        let directory = directory.as_ref();
        for spec in specs {
            if spec.is_stdout() {
                let bytes = self.export_bytes(spec)?;
                let mut stdout = std::io::stdout().lock();
                let written = stdout.write_all(&bytes).and_then(|()| stdout.flush());
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
            let writer = self.export_writer(spec)?;
            let written = writer.write_path_with(&path, |sink| self.feed_export(spec, sink))?;
            self.report_dropped(spec, written.dropped, path.display().to_string());
        }
        Ok(())
    }

    /// Serializes one export specification's rows into memory.
    pub(in crate::engine) fn serialize_export(
        &mut self,
        spec: &ExportSpec,
    ) -> Result<Vec<u8>, Diagnostic> {
        let name = spec.file_name();

        // The standard output has no name to detect compression by,
        // so the compression option alone decides.
        let target = (!spec.is_stdout()).then(|| Path::new(&name));
        let mut bytes = Vec::new();
        let writer = self.export_writer(spec)?;
        let written = writer
            .write_compressed_with(&mut bytes, target, |sink| self.feed_export(spec, sink))?;
        self.report_dropped(spec, written.dropped, name);

        Ok(bytes)
    }

    /// Writes the rows that one export specification selects into a sink,
    /// after checking the number of their columns against what the format requires.
    ///
    /// The rows are decoded from the predicate's table one at a time,
    /// and the read ends at the specification's limit.
    /// A predicate that the program declares as an output but never uses has no rows.
    ///
    /// # Errors
    ///
    /// Fails if no program is loaded,
    /// for a predicate that the program neither uses nor declares as an output,
    /// for a predicate that is not stored whole,
    /// and if the sink cannot be written.
    fn feed_export(&mut self, spec: &ExportSpec, sink: &mut dyn RowSink) -> Result<(), Diagnostic> {
        if self.program.is_none() {
            return Err(EngineError::NoProgram.diagnostic());
        }
        if self.arity_of(&spec.predicate).is_none() {
            // An output that the program declares but never uses has no table,
            // and is written as an empty file.
            if self.output_names().contains(&spec.predicate) {
                return Ok(());
            }

            return Err(EngineError::UnknownPredicate {
                predicate: spec.predicate.clone(),
            }
            .diagnostic());
        }

        let cursor = self.open_cursor(&spec.predicate)?;
        let limit = spec.limit.unwrap_or(u64::MAX);
        if cursor.remaining() == 0 || limit == 0 {
            return Ok(());
        }
        spec.check_arity(cursor.arity)?;

        let order = ColumnOrder::identity(cursor.arity);
        self.manager
            .ensure_order(cursor.table, cursor.range.clone(), &order);

        let mut row: Vec<ValueRef<'_>> = Vec::with_capacity(cursor.arity);
        let mut written = 0;
        let mut failure = None;
        self.manager
            .for_each_row_from(cursor.table, cursor.range, &order, 0, |values| {
                row.clear();
                row.extend(values.iter().map(|&value| {
                    self.dictionary
                        .decode(value)
                        .expect("stored values decode against their dictionary")
                }));
                if let Err(problem) = sink.write_row(&row) {
                    failure = Some(problem);
                    return ControlFlow::Break(());
                }

                written += 1;
                if written < limit {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(())
                }
            });

        failure.map_or(Ok(()), Err)
    }

    /// Returns the configured writer of one export specification.
    fn export_writer(&self, spec: &ExportSpec) -> Result<ExportWriter, Diagnostic> {
        use nemo_io::value_format::{ColumnFormat, ValueFormat};

        Ok(match &spec.format {
            ExportFormat::Dsv {
                delimiter,
                formats,
                columns,
                ..
            } => {
                let arity = self.arity_of(&spec.predicate).unwrap_or(0);
                let formats = formats
                    .clone()
                    .unwrap_or_else(|| vec![ColumnFormat::new(ValueFormat::Term); arity]);
                let mut options = nemo_io::dsv::DsvWriterOptions::new(*delimiter, formats);
                options.columns = columns.clone();
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
