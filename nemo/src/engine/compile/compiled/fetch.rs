//! The compiled fetch of a SPARQL import with a `where` clause,
//! which the import's rule performs instead of a join.

use nemo_physical::{management::TableId, value::DataValue};

/// The fetch of a SPARQL import with a `where` clause.
///
/// Each application of the rule sends the rows of the `where` clause's atoms to the endpoint
/// as `VALUES` blocks, and stores the solutions in the head table.
#[derive(Debug)]
pub(crate) struct CompiledFetch {
    /// The endpoint URL.
    pub(crate) endpoint: String,
    /// The validated query, with the import's condition added if it has a SPARQL form.
    pub(crate) query: nemo_io::sparql::SparqlQuery,
    /// The HTTP headers sent with every request.
    pub(crate) headers: Vec<(String, String)>,
    /// The largest number of rows of one atom that a request sends.
    pub(crate) page_size: usize,
    /// Which columns of a response are delivered, and the condition that a row must satisfy.
    pub(crate) selection: nemo_io::selection::RowSelection,
    /// The name of the head predicate, for the fetch's diagnostics.
    pub(crate) predicate: String,
    /// Per body atom, how the fetch reads each of the atom's stored columns.
    pub(crate) atoms: Vec<Vec<FetchColumn>>,
    /// A scratch table of the head's arity,
    /// in which a response's rows are deduplicated against the head before they are stored.
    pub(crate) scratch: TableId,
    /// The source position of the import directive, for the fetch's diagnostics.
    pub(crate) span: Option<crate::model::Span>,
    /// What happens when a request fails.
    pub(crate) on_error: crate::model::OnError,
}

/// How a fetch reads one stored column of an atom of the `where` clause.
#[derive(Debug, Clone)]
pub(crate) enum FetchColumn {
    /// The column binds a projected variable of the query, which a request sends as a column
    /// of a `VALUES` block.
    ///
    /// The number is the variable's position in the query's projection.
    Bound(usize),
    /// The column must have this value.
    Constant(DataValue),
    /// The column binds nothing in the query.
    ///
    /// It has an anonymous variable, or a variable that an earlier column of the atom binds.
    Free,
}
