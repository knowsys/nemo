//! Arrow interchange: results as columnar record batches.
//!
//! The engine's chunks are column-major already.
//! This module projects them onto Arrow arrays through
//! [nemo-io's shared mapping](nemo_io::arrow)
//! and hands them over through the Arrow C stream interface (`__arrow_c_stream__`),
//! which pandas, polars, and duckdb consume, with no pyarrow dependency on this side.
//!
//! Typing is per column over the whole result.
//! Uniformly integer, double, or boolean columns become native arrays,
//! and every other column becomes text:
//! plain strings raw, IRIs bare, everything else in canonical term syntax.
//! The typed API (`rows`, `query`) keeps the exact values, and this is the dataframe projection.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch},
    datatypes::Schema,
    ffi_stream::FFI_ArrowArrayStream,
    record_batch::{RecordBatchIterator, RecordBatchOptions},
};
use nemo_io::arrow::ColumnKind;
use pyo3::{prelude::*, types::PyCapsule};

/// A finished columnar result, handed to consumers through the Arrow C
/// stream interface.
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
    /// Each call yields a fresh stream over the same batches.
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

/// Reads a predicate into an [ArrowStream].
pub(crate) fn stream(
    py: Python<'_>,
    engine: &mut nemo::engine::Engine,
    sources: &nemo::diagnostics::render::SourceTexts,
    predicate: &str,
) -> PyResult<ArrowStream> {
    // Drain the cursor first: column types are inferred over the whole
    // result, so every chunk is needed before the first array builds.
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
            // The row count travels with the batch: a nullary predicate
            // holds its row in no column at all.
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
