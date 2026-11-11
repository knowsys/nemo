//! Writing Parquet files.

use std::{io::Write, path::Path};

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use parquet::arrow::ArrowWriter;

use nemo_physical::{diagnostics::Diagnostic, value::DataValue};

use crate::{
    AboutResource,
    arrow::ColumnKind,
    error::IoError,
    write::{Written, create},
};

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
        target: impl Write + Send,
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
        let arrays: Vec<ArrayRef> = kinds
            .iter()
            .enumerate()
            .map(|(index, &kind)| kind.array(rows.iter().map(move |row| row[index].as_ref())))
            .collect();
        let batch = RecordBatch::try_new_with_options(
            schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )
        .map_err(IoError::output_unwritable)?;

        let mut writer =
            ArrowWriter::try_new(target, schema, None).map_err(IoError::output_unwritable)?;
        writer
            .write(&batch)
            .and_then(|()| writer.close().map(|_| ()))
            .map_err(IoError::output_unwritable)?;

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
