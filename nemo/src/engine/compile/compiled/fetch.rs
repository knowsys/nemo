//! The remote fetch of a bound SPARQL import, which its rule performs instead of a join.

use nemo_physical::{management::TableId, value::DataValue};

/// The remote fetch of a SPARQL import with a `where` clause.
///
/// Per application, the rows of the `where` clause's atoms are sent to the endpoint
/// as `VALUES` blocks, and the solutions are stored in the head table.
#[cfg(feature = "sparql")]
#[derive(Debug)]
pub(crate) struct CompiledFetch {
    /// The endpoint URL.
    pub(crate) endpoint: String,
    /// The validated query to send, with any pushed-down condition added.
    pub(crate) query: nemo_io::sparql::SparqlQuery,
    /// Request headers, applied to every request.
    pub(crate) headers: Vec<(String, String)>,
    /// At most this many binding rows per atom per request.
    pub(crate) page_size: usize,
    /// The columns of the query's projection a response delivers,
    /// and the condition rows must pass.
    pub(crate) selection: nemo_io::selection::RowSelection,
    /// The head predicate's name, for the fetch's diagnostics.
    pub(crate) predicate: String,
    /// Per body atom, how the fetch reads each of the atom's stored columns.
    pub(crate) atoms: Vec<Vec<FetchColumn>>,
    /// A scratch table of the head's arity,
    /// in which a response's rows are deduplicated against the head before they are stored.
    pub(crate) scratch: TableId,
    /// The directive's source position, for the fetch's diagnostics.
    pub(crate) span: Option<crate::model::Span>,
    /// What a failed request does.
    pub(crate) on_error: crate::model::OnError,
}

/// One stored column of an atom of the `where` clause, as the fetch reads it.
#[cfg(feature = "sparql")]
#[derive(Debug, Clone)]
pub(crate) enum FetchColumn {
    /// Bound to a column of the import (a `VALUES` column),
    /// by its position among the import's source columns.
    Bound(usize),
    /// Constrained to a constant (a filter of the body atom).
    Constant(DataValue),
    /// Unconstrained: an anonymous variable.
    Free,
}
