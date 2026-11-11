//! Reading JSON records.

use std::{
    io::{BufRead, BufReader, Read},
    path::Path,
    str,
};

use nemo_physical::{
    diagnostics::Diagnostic, dictionary::Dictionary, function::EvaluationContext, value::DataValue,
};

use crate::{
    AboutResource,
    compression::Compression,
    error::IoError,
    load::{BlankNodes, Loaded, Reader, RowIntake},
    selection::RowSelection,
    value_format::{ColumnFormat, ValueFormat},
};

use super::{
    parse::{Json, ParseError, Parser},
    value::{self, JsonCell},
};

/// The options of a JSON resource for reading.
#[derive(Debug, Clone)]
pub struct JsonReaderOptions {
    /// Whether the resource has one record per line (JSON Lines),
    /// rather than being one document that holds an array of records.
    pub lines: bool,
    /// The format of each column.
    pub formats: Vec<ColumnFormat>,
    /// The key of each column, in the order of [formats](Self::formats),
    /// or `None` for records that are arrays with one element per column.
    ///
    /// With keys, a record is an object.
    /// A key that is not named here is not read, and a named key that a record does not have
    /// is a missing value.
    pub columns: Option<Vec<String>>,
    /// The place of the array of records inside the document, as a JSON Pointer (RFC 6901),
    /// or `None` if the document is the array.
    ///
    /// It does not apply to a resource with one record per line.
    pub path: Option<String>,
    /// The columns delivered and the rows admitted.
    pub selection: RowSelection,
    /// The largest number of well-formed rows to read, or `None` for no limit.
    pub limit: Option<u64>,
    /// The stream compression.
    pub compression: Compression,
}

impl JsonReaderOptions {
    /// Creates options for a document that is an array of records with the given column
    /// formats, each record an array.
    pub fn new(formats: impl IntoIterator<Item = impl Into<ColumnFormat>>) -> Self {
        Self {
            lines: false,
            formats: formats.into_iter().map(Into::into).collect(),
            columns: None,
            path: None,
            selection: RowSelection::default(),
            limit: None,
            compression: Compression::Auto,
        }
    }
}

/// A reader of JSON records: each record of the resource is one row.
///
/// The resource is one document that holds an array of records,
/// or has one record per line.
/// A record is an array with one element per column,
/// or an object if the options name the [keys](JsonReaderOptions::columns) of the columns.
/// Each cell is read by its column's format:
/// under [Term](ValueFormat::Term) the JSON value says what it is,
/// and under any other format it is the bare form of a value.
///
/// A record of the wrong kind or length, and a cell that its format does not read,
/// make the row malformed.
/// `null` and an absent key are missing values:
/// a fresh null in a column that admits one, and counted apart in any other column.
///
/// # Example
///
/// ```
/// use nemo_io::{
///     json::{JsonReader, JsonReaderOptions},
///     load::Reader,
///     value_format::{ColumnFormat, ValueFormat},
/// };
/// use nemo_physical::{dictionary::Dictionary, function::EvaluationContext};
///
/// let mut options = JsonReaderOptions::new([
///     ColumnFormat::new(ValueFormat::String),
///     ColumnFormat::optional(ValueFormat::Integer),
/// ]);
/// options.columns = Some(vec![String::from("name"), String::from("age")]);
/// let text = r#"[{"name": "Alice", "age": 34}, {"name": "Bob"}, {"name": 7}]"#;
/// let loaded = JsonReader::new(options)
///     .read(text.as_bytes(), &mut Dictionary::new(), EvaluationContext::now())
///     .expect("the text is JSON");
///
/// // Bob has no age, which his column admits, and a number is no name.
/// assert_eq!((loaded.rows.len(), loaded.counts.malformed), (2, 1));
/// ```
#[derive(Debug, Clone)]
pub struct JsonReader {
    /// The options of the read resources.
    options: JsonReaderOptions,
}

/// The records of one read, as they become the rows of its load.
struct Records<'a> {
    /// The options of the read resource.
    options: &'a JsonReaderOptions,
    /// The intake of the read's rows.
    intake: RowIntake,
    /// The nulls that the resource names.
    blanks: BlankNodes,
    /// The delivered values of the current record.
    kept: Vec<DataValue>,
}

impl JsonReader {
    /// Creates a reader with the given options.
    pub fn new(options: JsonReaderOptions) -> Self {
        Self { options }
    }

    /// Returns the number of elements of the first record of a file,
    /// or `None` if the file has no record or its first record is not an array.
    ///
    /// `lines` and `pointer` are the [lines](JsonReaderOptions::lines) and the
    /// [path](JsonReaderOptions::path) of the options.
    /// This gives the number of columns of a resource whose formats and keys are not given.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or read, or is not JSON.
    pub fn sniff_width(
        path: impl AsRef<Path>,
        lines: bool,
        pointer: Option<&str>,
        compression: Compression,
    ) -> Result<Option<usize>, Diagnostic> {
        let path = path.as_ref();

        Self::sniff_width_from(compression.open_file(path)?, lines, pointer)
            .map_err(|diagnostic| diagnostic.with_resource(path.display().to_string()))
    }

    /// Returns the number of elements of the first record of a stream,
    /// as [sniff_width](Self::sniff_width) does for a file.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream cannot be read or is not JSON.
    pub fn sniff_width_from(
        mut source: impl Read,
        lines: bool,
        path: Option<&str>,
    ) -> Result<Option<usize>, Diagnostic> {
        let mut bytes = Vec::new();
        source
            .read_to_end(&mut bytes)
            .map_err(IoError::input_unreadable)?;
        let text = text_of(&bytes)?;
        let text = if lines {
            text.lines()
                .find(|line| !line.trim().is_empty())
                .unwrap_or("")
        } else {
            text
        };

        let first = match (lines, path) {
            (true, _) if text.is_empty() => None,
            (true, _) => Some(Parser::document(text).map_err(not_json)?),
            (false, None) => Parser::new(text).first_element().map_err(not_json)?,
            (false, Some(path)) => {
                let document = Parser::document(text).map_err(not_json)?;

                records_at(&document, path)?.first().cloned()
            }
        };

        Ok(match first {
            Some(Json::Array(elements)) => Some(elements.len()),
            _ => None,
        })
    }

    /// Reads the records of a document.
    fn read_document(
        &self,
        text: &str,
        records: &mut Records<'_>,
        dictionary: &mut Dictionary,
    ) -> Result<(), Diagnostic> {
        match &self.options.path {
            // The records are read one at a time, so the document is never held as a whole.
            None => {
                if !text.trim_start().starts_with('[') {
                    return Err(match Parser::document(text) {
                        Ok(_) => not_records("the document"),
                        Err(error) => not_json(error),
                    });
                }

                let mut parser = Parser::new(text);
                parser
                    .elements(|parser| {
                        let record = parser.value()?;
                        records.take(&record, dictionary, 0);

                        Ok(())
                    })
                    .and_then(|()| parser.end())
                    .map_err(not_json)
            }
            Some(path) => {
                let document = Parser::document(text).map_err(not_json)?;
                for record in records_at(&document, path)? {
                    records.take(record, dictionary, 0);
                }

                Ok(())
            }
        }
    }

    /// Reads the records of a resource with one record per line,
    /// until the limit of well-formed rows is reached.
    ///
    /// A line that is not JSON is one malformed row, and an empty line is no record.
    fn read_lines(
        &self,
        source: impl Read,
        records: &mut Records<'_>,
        dictionary: &mut Dictionary,
    ) -> Result<(), Diagnostic> {
        let limit = self.options.limit.unwrap_or(u64::MAX);
        let mut source = BufReader::new(source);
        let (mut line, mut number) = (Vec::new(), 0u64);
        while records.intake.well_formed() < limit {
            line.clear();
            let length = source
                .read_until(b'\n', &mut line)
                .map_err(IoError::input_unreadable)?;
            if length == 0 {
                break;
            }

            number += 1;
            let record = str::from_utf8(&line).ok().map(str::trim);
            match record.map(Parser::document) {
                Some(Ok(record)) => records.take(&record, dictionary, number),
                Some(Err(_)) if record.is_some_and(str::is_empty) => {}
                _ => records.intake.count_malformed(number),
            }
        }

        Ok(())
    }
}

impl Records<'_> {
    /// Takes one record as a row, or counts it.
    ///
    /// `line` is the record's line, or `0` if it is not known.
    fn take(&mut self, record: &Json<'_>, dictionary: &mut Dictionary, line: u64) {
        let options = self.options;
        let formats = &options.formats;
        let fits = match (&options.columns, record) {
            (None, Json::Array(elements)) => elements.len() == formats.len(),
            (Some(_), Json::Object(_)) => true,
            _ => false,
        };
        if !fits {
            self.intake.count_malformed(line);
            return;
        }

        // A cell that is not read ends the row, except for a missing value:
        // the cells after it still decide whether the row is malformed.
        self.kept.clear();
        let mut unadmitted = false;
        for (column, format) in formats.iter().enumerate() {
            if format.value == ValueFormat::Skip {
                continue;
            }

            let cell = match (&options.columns, record) {
                (Some(names), record) => record.member(&names[column]),
                (None, Json::Array(elements)) => Some(&elements[column]),
                (None, _) => None,
            };
            let delivered = self.intake.selection().keeps(column) && !unadmitted;
            let mut null = |label: &str| self.blanks.null(label, dictionary);
            match value::read(cell, *format, &mut null) {
                JsonCell::Value(value) if delivered => self.kept.push(value),
                JsonCell::Missing if delivered => {
                    self.kept.push(DataValue::null(dictionary.fresh_null()));
                }
                JsonCell::Value(_) | JsonCell::Missing => {}
                JsonCell::Unadmitted => unadmitted = true,
                JsonCell::Malformed => {
                    self.intake.count_malformed(line);
                    return;
                }
            }
        }

        if unadmitted {
            self.intake.count_missing(line);
        } else {
            self.intake.take(&self.kept, dictionary, line);
        }
    }
}

impl Reader for JsonReader {
    fn compression(&self) -> Compression {
        self.options.compression
    }

    /// Reads all records of a resource.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream fails, if a document is not JSON,
    /// or if the document, or the value at the options' path, is not an array.
    fn read(
        &self,
        mut source: impl Read,
        dictionary: &mut Dictionary,
        context: EvaluationContext,
    ) -> Result<Loaded, Diagnostic> {
        let options = &self.options;
        let arity = (0..options.formats.len())
            .filter(|&column| {
                options.formats[column].value != ValueFormat::Skip
                    && options.selection.keeps(column)
            })
            .count();
        let mut records = Records {
            options,
            intake: RowIntake::of_arity(arity, options.selection.clone(), context),
            blanks: BlankNodes::open(None, "document", dictionary),
            kept: Vec::with_capacity(arity),
        };

        let outcome = if options.lines {
            self.read_lines(source, &mut records, dictionary)
        } else {
            let mut bytes = Vec::new();
            source
                .read_to_end(&mut bytes)
                .map_err(IoError::input_unreadable)
                .and_then(|_| text_of(&bytes))
                .and_then(|text| self.read_document(text, &mut records, dictionary))
        };
        records.blanks.close(dictionary);
        outcome?;

        let loaded = records.intake.finish();
        tracing::debug!(rows = loaded.rows.len(), counts = ?loaded.counts, "read a JSON resource");

        Ok(loaded)
    }
}

/// Returns the bytes of a document as text, or the error of a document that is not UTF-8.
fn text_of(bytes: &[u8]) -> Result<&str, Diagnostic> {
    str::from_utf8(bytes).map_err(|error| {
        IoError::NotJson {
            reason: error.to_string(),
        }
        .diagnostic()
    })
}

/// Returns the error of a text that is not JSON.
fn not_json(error: ParseError) -> Diagnostic {
    IoError::NotJson {
        reason: format!("{} at byte {}", error.reason, error.position),
    }
    .diagnostic()
}

/// Returns the error of a place in a document that holds no array of records.
fn not_records(place: &str) -> Diagnostic {
    IoError::NotRecords {
        place: place.to_owned(),
    }
    .diagnostic()
}

/// Returns the records of the array that a pointer names inside a document.
fn records_at<'a, 'b>(document: &'b Json<'a>, path: &str) -> Result<&'b [Json<'a>], Diagnostic> {
    match document.at(path) {
        Some(Json::Array(records)) => Ok(records),
        _ => Err(not_records(&format!("the value at `{path}`"))),
    }
}
