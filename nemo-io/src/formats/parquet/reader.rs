//! Reading Parquet files.

use std::{fs::File, path::Path};

use arrow::{
    array::{Array, AsArray},
    datatypes::DataType,
};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};

use nemo_physical::{
    diagnostics::Diagnostic, dictionary::Dictionary, function::EvaluationContext, value::DataValue,
};

use crate::{error::IoError, load::Loaded, selection::RowSelection};

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
    pub fn width(path: impl AsRef<Path>) -> Result<usize, Diagnostic> {
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

                    arrow_value(column.as_ref(), index).is_some_and(|value| {
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
fn arrow_value(column: &dyn Array, row: usize) -> Option<DataValue> {
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
