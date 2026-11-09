//! Reading a predicate's rows as Arrow record batches.
//!
//! The rows are converted to Arrow arrays by [nemo-io's mapping](nemo_io::arrow),
//! and passed to Python through the Arrow C stream interface (`__arrow_c_stream__`),
//! which libraries such as pandas, polars and duckdb read without pyarrow.
//! The mapping loses information: a column whose values are all integers, all doubles
//! or all booleans becomes an Arrow array of that type, and every other column becomes text.
//! The engine's `rows` and `cursor` keep the exact values.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::Schema,
    ffi_stream::FFI_ArrowArrayStream,
    record_batch::{RecordBatchIterator, RecordBatchOptions},
};
use nemo_io::arrow::ColumnKind;
use pyo3::{prelude::*, types::PyCapsule};

/// The rows of a predicate as Arrow record batches,
/// which other libraries read through the Arrow C stream interface.
#[pyclass(frozen, module = "nmo")]
#[derive(Debug)]
pub struct ArrowStream {
    /// The schema shared by the batches.
    schema: Arc<Schema>,
    /// The record batches.
    batches: Vec<RecordBatch>,
}

#[pymethods]
impl ArrowStream {
    /// Returns the Arrow C stream capsule.
    ///
    /// Each call returns a new stream over the same batches.
    /// `requested_schema` is ignored.
    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let reader = RecordBatchIterator::new(
            self.batches.clone().into_iter().map(Ok),
            self.schema.clone(),
        );
        let stream = FFI_ArrowArrayStream::new(Box::new(reader));
        PyCapsule::new_with_value(py, stream, c"arrow_array_stream")
    }

    /// Returns the number of rows across all batches.
    fn __len__(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }
}

/// Reads the rows of a predicate into an [ArrowStream].
///
/// Raises `DiagnosticsError`, rendered against `sources`, if the rows cannot be read.
pub(crate) fn stream(
    py: Python<'_>,
    engine: &mut nemo::engine::Engine,
    sources: &nemo::diagnostics::render::SourceTexts,
    predicate: &str,
) -> PyResult<ArrowStream> {
    // The whole cursor is read first, since the type of a column depends on all of its
    // values, and every array needs that type.
    let mut cursor = engine
        .cursor(predicate)
        .map_err(|diagnostic| crate::errors::raise(py, vec![diagnostic], sources))?;
    let arity = engine.predicate(predicate).map_or(0, |info| info.arity);
    let mut chunks = Vec::new();
    while !cursor.is_done() {
        let chunk = engine
            .read_chunk(&mut cursor, 1 << 16)
            .map_err(|diagnostic| crate::errors::raise(py, vec![diagnostic], sources))?;
        if chunk.is_empty() {
            break;
        }
        chunks.push(chunk);
    }

    let kinds: Vec<ColumnKind> = (0..arity)
        .map(|column| {
            chunks
                .iter()
                .flat_map(|chunk| chunk.column(column))
                .fold(None, |running, value| {
                    Some(ColumnKind::unify(running, value.as_ref()))
                })
                .unwrap_or(ColumnKind::Text)
        })
        .collect();

    let schema = ColumnKind::schema(&kinds);
    let batches: Vec<RecordBatch> = chunks
        .iter()
        .map(|chunk| {
            let arrays: Vec<ArrayRef> = kinds
                .iter()
                .enumerate()
                .map(|(index, &kind)| {
                    kind.array(chunk.column(index).iter().map(|value| value.as_ref()))
                })
                .collect();
            // The batch states its row count, since the row of a predicate without columns
            // is in no column.
            RecordBatch::try_new_with_options(
                schema.clone(),
                arrays,
                &RecordBatchOptions::new().with_row_count(Some(chunk.len())),
            )
            .map_err(|error| crate::errors::NemoError::new_err(error.to_string()))
        })
        .collect::<PyResult<_>>()?;
    let batches = if batches.is_empty() {
        vec![RecordBatch::new_empty(schema.clone())]
    } else {
        batches
    };
    Ok(ArrowStream { schema, batches })
}
