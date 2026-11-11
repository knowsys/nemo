//! Writing Parquet files.

use std::{io::Write, path::Path};

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use parquet::arrow::ArrowWriter;

use nemo_physical::{
    diagnostics::Diagnostic,
    value::{DataValue, ValueRef},
};

use crate::{
    AboutResource,
    arrow::ColumnKind,
    error::IoError,
    write::{RowSink, Written, create, write_rows},
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

    /// Starts a file on a target.
    pub fn start<W: Write + Send>(&self, target: W) -> ParquetSink<W> {
        ParquetSink {
            arity: self.arity,
            target,
            rows: Vec::new(),
        }
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
        let mut sink = self.start(target);
        write_rows(&mut sink, rows)?;

        sink.finish()
    }

    /// Writes a file, creating its parent directories.
    ///
    /// `feed` writes the rows into the sink of the file.
    ///
    /// # Errors
    ///
    /// Returns the error of `feed`, or an error if the file cannot be created or written.
    ///
    /// # Panics
    ///
    /// Panics if a row does not have the writer's number of columns.
    pub fn write_path_with(
        &self,
        path: impl AsRef<Path>,
        feed: impl FnOnce(&mut dyn RowSink) -> Result<(), Diagnostic>,
    ) -> Result<Written, Diagnostic> {
        let path = path.as_ref();

        // The Parquet writer buffers its target itself.
        let written = create(path).and_then(|file| {
            let mut sink = self.start(file);
            feed(&mut sink)?;

            sink.finish()
        });

        written.map_err(|diagnostic| diagnostic.with_resource(path.display().to_string()))
    }

    /// Writes rows to a file as [write_path_with](Self::write_path_with) does.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created or written.
    ///
    /// # Panics
    ///
    /// Panics if a row does not have the writer's number of columns.
    pub fn write_path<'a>(
        &self,
        path: impl AsRef<Path>,
        rows: impl IntoIterator<Item = &'a [DataValue]>,
    ) -> Result<Written, Diagnostic> {
        self.write_path_with(path, |sink| write_rows(sink, rows))
    }
}

/// The sink of the rows of one Parquet file.
///
/// The rows are collected and written as one record batch when the file is finished,
/// since each column's type is inferred from all of its values.
#[derive(Debug)]
pub struct ParquetSink<W: Write + Send> {
    /// The number of columns of a row.
    arity: usize,
    /// The target of the file.
    target: W,
    /// The rows collected so far.
    rows: Vec<Vec<DataValue>>,
}

impl<W: Write + Send> RowSink for ParquetSink<W> {
    /// Collects one row.
    ///
    /// # Panics
    ///
    /// Panics if the row does not have the writer's number of columns.
    fn write_row(&mut self, row: &[ValueRef<'_>]) -> Result<(), Diagnostic> {
        assert_eq!(
            row.len(),
            self.arity,
            "a row of a Parquet export has the writer's number of columns"
        );
        self.rows
            .push(row.iter().map(|&value| value.to_owned()).collect());

        Ok(())
    }

    fn finish(self) -> Result<Written, Diagnostic> {
        let rows = self.rows;
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
            ArrowWriter::try_new(self.target, schema, None).map_err(IoError::output_unwritable)?;
        writer
            .write(&batch)
            .and_then(|()| writer.close().map(|_| ()))
            .map_err(IoError::output_unwritable)?;

        Ok(Written {
            rows: rows.len() as u64,
            dropped: 0,
        })
    }
}
