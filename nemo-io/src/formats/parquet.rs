//! Parquet, the standard columnar file format, read and written through Arrow.
//!
//! Parquet has its own compression, so no stream compression applies.

use std::{fs::File, path::Path};

use arrow::{
    array::{Array, AsArray, RecordBatch, RecordBatchOptions},
    datatypes::DataType,
};
use parquet::arrow::{ArrowWriter, ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};

use nemo_physical::{
    diagnostics::Diagnostic, dictionary::Dictionary, function::EvaluationContext, value::DataValue,
};

use crate::{
    AboutResource,
    arrow::ColumnKind,
    error::IoError,
    load::Loaded,
    selection::RowSelection,
    written::{Written, create},
};

/// A reader of Parquet files, which decodes record batches into row buffers.
///
/// It reads a file rather than a stream, since Parquet is read by seeking:
/// first the footer, then the record batches.
/// The columns' Arrow types are read as values:
/// - integers of every width as integers,
/// - 32-bit and 64-bit floats as floats and doubles,
/// - strings, including dictionary-encoded ones, as strings,
/// - booleans as booleans.
///
/// A column of any other type fails the read with an error that names the type.
/// A row with a null cell is malformed and counted, since a value cannot be missing in a table.
#[derive(Debug, Clone, Default)]
pub struct ParquetReader {
    /// The columns delivered and the rows admitted.
    selection: RowSelection,
}

impl ParquetReader {
    /// Creates a reader that delivers every column and admits every row.
    pub fn new() -> Self {
        Self {
            selection: RowSelection::default(),
        }
    }

    /// Creates a reader that delivers the columns and rows of a selection.
    ///
    /// A column that is not delivered still decides whether a row is well-formed,
    /// but it is read from the file only if its statistics allow a null.
    pub fn with_selection(selection: RowSelection) -> Self {
        Self { selection }
    }

    /// Returns the number of columns of a file, read from its schema alone.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or has no Parquet schema.
    pub fn arity(path: impl AsRef<Path>) -> Result<usize, Diagnostic> {
        let path = path.as_ref();
        let file = File::open(path).map_err(|error| IoError::unreadable(path, &error))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| IoError::unreadable(path, error))?;
        Ok(builder.schema().fields().len())
    }

    /// Reads all rows of a file, interning the values as they arrive.
    ///
    /// `context` is the evaluation context of the selection's condition.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or read as Parquet,
    /// or if a column has a type that the reader does not support.
    pub fn read_path(
        &self,
        path: impl AsRef<Path>,
        dictionary: &mut Dictionary,
        context: EvaluationContext,
    ) -> Result<Loaded, Diagnostic> {
        let path = path.as_ref();
        let file = File::open(path).map_err(|error| IoError::unreadable(path, &error))?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| IoError::unreadable(path, error))?;
        // Every column decides whether a row is well-formed, including the columns that are not
        // delivered, so a column of an unsupported type fails the file even if it is not
        // delivered.
        if let Some(outside) = builder
            .schema()
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Dictionary(_, value_type) => value_type.as_ref(),
                data_type => data_type,
            })
            .find(|data_type| !supported(data_type))
        {
            return Err(unsupported_type(path, outside));
        }
        // A column that is not delivered is read only if it may have a null,
        // which makes its row malformed.
        let width = builder.schema().fields().len();
        let delivered: Vec<bool> = (0..width)
            .map(|position| self.selection.keeps(position))
            .collect();
        let read: Vec<usize> = (0..width)
            .filter(|&position| delivered[position] || may_hold_nulls(builder.metadata(), position))
            .collect();
        let delivered: Vec<bool> = read.iter().map(|&position| delivered[position]).collect();
        if read.len() < width {
            let mask = ProjectionMask::roots(builder.parquet_schema(), read);
            builder = builder.with_projection(mask);
        }
        let reader = builder
            .build()
            .map_err(|error| IoError::unreadable(path, error))?;

        let arity = delivered.iter().filter(|&&delivered| delivered).count();
        let mut loaded = Loaded::new(arity, context);
        let mut row = Vec::new();
        let mut kept: Vec<DataValue> = Vec::new();
        for batch in reader {
            let batch = batch.map_err(|error| IoError::unreadable(path, error))?;
            // Dictionary-encoded columns are read as their value type.
            let columns: Vec<arrow::array::ArrayRef> = batch
                .columns()
                .iter()
                .map(|column| match column.data_type() {
                    DataType::Dictionary(_, value_type) => arrow::compute::cast(column, value_type)
                        .map_err(|error| IoError::unreadable(path, error)),
                    _ => Ok(column.clone()),
                })
                .collect::<Result<_, _>>()?;
            for index in 0..batch.num_rows() {
                kept.clear();
                // `None` here is a null cell, so the row is malformed.
                let complete = columns.iter().zip(&delivered).all(|(column, &delivered)| {
                    if !delivered {
                        return !column.is_null(index);
                    }
                    cell(column.as_ref(), index).is_some_and(|value| {
                        kept.push(value);
                        true
                    })
                });
                if complete {
                    loaded.take(&kept, &self.selection, dictionary, &mut row, 0);
                } else {
                    loaded.count_malformed(0);
                }
            }
        }
        tracing::debug!(
            rows = loaded.rows.len(),
            malformed = loaded.malformed,
            excluded = loaded.excluded,
            errors = ?loaded.errors,
            "read a Parquet file"
        );
        Ok(loaded)
    }
}

/// A writer of Parquet files, which maps columns onto Arrow arrays as [ColumnKind] describes.
///
/// Each column's type is inferred from all rows written,
/// and a column with values of different kinds becomes text.
/// All rows are written as one record batch.
#[derive(Debug, Clone, Default)]
pub struct ParquetWriter {
    /// The number of columns of the rows, which the schema of a file without rows still
    /// declares.
    arity: usize,
}

impl ParquetWriter {
    /// Creates a writer of rows with the given number of columns.
    pub fn new(arity: usize) -> Self {
        Self { arity }
    }

    /// Writes rows to a target.
    ///
    /// # Errors
    ///
    /// Returns an error if the target cannot be written.
    ///
    /// # Panics
    ///
    /// Panics if a row does not have the writer's number of columns.
    pub fn write<'a>(
        &self,
        target: impl std::io::Write + Send,
        rows: impl IntoIterator<Item = &'a [DataValue]>,
    ) -> Result<Written, Diagnostic> {
        let rows: Vec<&[DataValue]> = rows.into_iter().collect();
        for row in &rows {
            assert_eq!(
                row.len(),
                self.arity,
                "a row of a Parquet export has the writer's number of columns"
            );
        }
        let kinds: Vec<ColumnKind> = (0..self.arity)
            .map(|column| {
                rows.iter()
                    .fold(None, |running, row| {
                        Some(ColumnKind::unify(running, row[column].as_ref()))
                    })
                    .unwrap_or(ColumnKind::Text)
            })
            .collect();
        let schema = ColumnKind::schema(&kinds);
        let arrays: Vec<arrow::array::ArrayRef> = kinds
            .iter()
            .enumerate()
            .map(|(index, &kind)| kind.array(rows.iter().map(move |row| row[index].as_ref())))
            .collect();
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )
        .map_err(IoError::stream_unwritable)?;

        let mut writer =
            ArrowWriter::try_new(target, schema, None).map_err(IoError::stream_unwritable)?;
        writer
            .write(&batch)
            .and_then(|()| writer.close().map(|_| ()))
            .map_err(IoError::stream_unwritable)?;
        Ok(Written {
            rows: rows.len() as u64,
            dropped: 0,
        })
    }

    /// Writes rows to a file, creating its parent directories.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created or written.
    ///
    /// # Panics
    ///
    /// Panics if a row does not have the writer's number of columns, as [write](Self::write)
    /// does.
    pub fn write_path<'a>(
        &self,
        path: impl AsRef<Path>,
        rows: impl IntoIterator<Item = &'a [DataValue]>,
    ) -> Result<Written, Diagnostic> {
        let path = path.as_ref();
        // The Parquet writer buffers its target itself.
        let file = create(path)?;
        self.write(file, rows)
            .map_err(|diagnostic| diagnostic.with_resource(path.display().to_string()))
    }
}

/// Returns whether a column of the given Arrow type can be read, as [ParquetReader] describes.
fn supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
    )
}

/// Returns the value of one cell, read by the column's Arrow type, or `None` for a null cell.
///
/// The column's type must be [supported].
fn cell(column: &dyn Array, row: usize) -> Option<DataValue> {
    if column.is_null(row) {
        return None;
    }
    let value = match column.data_type() {
        DataType::Boolean => DataValue::boolean(column.as_boolean().value(row)),
        DataType::Int8 => DataValue::integer(
            column
                .as_primitive::<arrow::datatypes::Int8Type>()
                .value(row)
                .into(),
        ),
        DataType::Int16 => DataValue::integer(
            column
                .as_primitive::<arrow::datatypes::Int16Type>()
                .value(row)
                .into(),
        ),
        DataType::Int32 => DataValue::integer(
            column
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(row)
                .into(),
        ),
        DataType::Int64 => DataValue::integer(
            column
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row),
        ),
        DataType::UInt8 => DataValue::integer(
            column
                .as_primitive::<arrow::datatypes::UInt8Type>()
                .value(row)
                .into(),
        ),
        DataType::UInt16 => DataValue::integer(
            column
                .as_primitive::<arrow::datatypes::UInt16Type>()
                .value(row)
                .into(),
        ),
        DataType::UInt32 => DataValue::integer(
            column
                .as_primitive::<arrow::datatypes::UInt32Type>()
                .value(row)
                .into(),
        ),
        DataType::UInt64 => {
            let value = column
                .as_primitive::<arrow::datatypes::UInt64Type>()
                .value(row);
            match i64::try_from(value) {
                Ok(value) => DataValue::integer(value),
                // Beyond the signed range, the value is read as a wide integer
                // from its lexical form.
                Err(_) => return DataValue::decimal_from_lexical(&value.to_string()),
            }
        }
        DataType::Float32 => DataValue::float(
            column
                .as_primitive::<arrow::datatypes::Float32Type>()
                .value(row),
        ),
        DataType::Float64 => DataValue::double(
            column
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(row),
        ),
        DataType::Utf8 => DataValue::string(column.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => DataValue::string(column.as_string::<i64>().value(row)),
        DataType::Utf8View => DataValue::string(column.as_string_view().value(row)),
        other => unreachable!("column type {other} was rejected before"),
    };
    Some(value)
}

/// Returns whether a column of a file may have a null cell.
///
/// Returns `false` only if the statistics of every row group count no null in the column.
/// The columns are flat, so a column's position is the position of its leaf.
fn may_hold_nulls(metadata: &parquet::file::metadata::ParquetMetaData, position: usize) -> bool {
    metadata.row_groups().iter().any(|group| {
        group
            .column(position)
            .statistics()
            .and_then(parquet::file::statistics::Statistics::null_count_opt)
            != Some(0)
    })
}

/// Returns the error for a column of an unsupported type.
fn unsupported_type(path: &Path, data_type: &DataType) -> Diagnostic {
    IoError::UnsupportedArrowType {
        resource: path.display().to_string(),
        data_type: data_type.to_string(),
    }
    .diagnostic()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::TempDirectory;

    /// A written file is read back:
    /// columns of one kind keep their values, with floats and doubles apart,
    /// and a column of mixed kinds goes through text.
    #[test]
    fn written_parquet_reads_back() {
        let rows: Vec<Vec<DataValue>> = vec![
            vec![
                DataValue::integer(1),
                DataValue::double(0.5),
                DataValue::string("plain"),
                DataValue::float(0.25),
            ],
            vec![
                DataValue::integer(2),
                DataValue::double(1.5),
                DataValue::iri("http://example.org/a"),
                DataValue::float(1.25),
            ],
        ];
        let directory = TempDirectory::new();
        let path = directory.path().join("rows.parquet");
        let written = ParquetWriter::new(4)
            .write_path(&path, rows.iter().map(Vec::as_slice))
            .expect("the rows write");
        assert_eq!(written.rows, 2);

        assert_eq!(ParquetReader::arity(&path).expect("the schema reads"), 4);
        let mut dictionary = Dictionary::new();
        let loaded = ParquetReader::new()
            .read_path(&path, &mut dictionary, EvaluationContext::now())
            .expect("the file reads back");
        assert_eq!((loaded.rows.len(), loaded.malformed), (2, 0));
        let read = loaded.decoded(&dictionary);
        assert_eq!(read[0][0], DataValue::integer(1));
        assert_eq!(read[1][1], DataValue::double(1.5));
        assert_eq!(read[0][2], DataValue::string("plain"));
        assert_eq!(read[0][3], DataValue::float(0.25));
        // The mixed third column went through text, so the IRI is read back
        // as a string of its text.
        assert_eq!(read[1][2], DataValue::string("http://example.org/a"));
    }

    /// A null in a column that is not delivered still makes its row malformed.
    #[test]
    fn a_column_left_out_still_decides_whether_a_row_is_well_formed() {
        use std::sync::Arc;

        use arrow::{
            array::Int64Array,
            datatypes::{Field, Schema},
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![Some(10), None])),
            ],
        )
        .expect("the batch builds");
        let directory = TempDirectory::new();
        let path = directory.path().join("nulls.parquet");
        let mut writer =
            ArrowWriter::try_new(File::create(&path).expect("the file opens"), schema, None)
                .expect("the writer starts");
        writer.write(&batch).expect("the batch writes");
        writer.close().expect("the file closes");

        let selection = RowSelection {
            columns: Some(vec![true, false]),
            filter: None,
        };
        let loaded = ParquetReader::with_selection(selection)
            .read_path(&path, &mut Dictionary::new(), EvaluationContext::now())
            .expect("the file reads");
        // The second row's null makes it malformed, whether its column is delivered or not.
        assert_eq!((loaded.rows.len(), loaded.malformed), (1, 1));
        assert_eq!(loaded.rows.arity(), 1);
    }

    /// A file written without rows keeps the writer's number of columns,
    /// and its load has as many columns as the reader delivers.
    #[test]
    fn a_file_without_rows_keeps_its_columns() {
        let directory = TempDirectory::new();
        let path = directory.path().join("empty.parquet");
        let written = ParquetWriter::new(2)
            .write_path(&path, std::iter::empty())
            .expect("no rows write");
        assert_eq!(written.rows, 0);
        assert_eq!(ParquetReader::arity(&path).expect("the schema reads"), 2);

        let read = |reader: ParquetReader| {
            reader
                .read_path(&path, &mut Dictionary::new(), EvaluationContext::now())
                .expect("the file reads")
        };
        let whole = read(ParquetReader::new());
        assert_eq!((whole.rows.len(), whole.rows.arity()), (0, 2));

        let selection = RowSelection {
            columns: Some(vec![false, true]),
            filter: None,
        };
        let selected = read(ParquetReader::with_selection(selection));
        assert_eq!((selected.rows.len(), selected.rows.arity()), (0, 1));
    }
}
