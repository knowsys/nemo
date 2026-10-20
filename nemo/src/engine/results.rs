//! Reading stored facts back: the program's [predicates], and paged access to their stored rows.
//!
//! [cursor](crate::engine::Engine::cursor) opens a [PredicateCursor],
//! and [read_chunk](crate::engine::Engine::read_chunk) advances it
//! by one bounded, owned [RowChunk] at a time.
//! The cursor holds no borrow of the engine, since each read borrows it briefly and returns.
//! So the cursor can be kept as plain data where Rust borrows cannot be held,
//! as in the Python and JavaScript bindings,
//! and memory stays bounded by the read budget rather than the predicate's size.

pub(super) mod export;
pub(super) mod predicates;
pub(super) mod rows;

use std::ops::Range;

use nemo_physical::{
    management::{Revision, TableId},
    value::DataValue,
};

/// A cursor over one predicate's stored rows, in storage order.
///
/// A cursor is plain data:
/// the predicate's revision range and row count are fixed when the cursor is opened.
/// A read fails with N0206 for a cursor whose table has changed since it was opened,
/// rather than return rows that no longer match the table.
///
/// # Example
///
/// ```
/// use nemo::{
///     engine::Engine,
///     model::{FileId, Program},
/// };
///
/// let (program, _) = Program::parse("n(1) . n(2) . n(3) . n(4) . n(5) .", FileId::new(0));
/// let mut engine = Engine::new();
/// engine.load(program)?;
/// engine.reason()?;
///
/// let mut cursor = engine.cursor("n")?;
/// let mut read = 0;
/// while !cursor.is_done() {
///     let chunk = engine.read_chunk(&mut cursor, 2)?;
///     assert!(chunk.len() <= 2);
///     read += chunk.len();
/// }
/// assert_eq!(read, 5);
/// # Ok::<(), nemo::diagnostics::Diagnostic>(())
/// ```
#[derive(Debug, Clone)]
pub struct PredicateCursor {
    /// The table read.
    pub(crate) table: TableId,
    /// The revision range fixed at opening.
    pub(crate) range: Range<Revision>,
    /// The columns per row.
    pub(crate) arity: usize,
    /// Rows already delivered.
    pub(crate) position: usize,
    /// The total row count at opening, which guards against invalidation.
    pub(crate) rows: usize,
    /// The table's revision blocks at opening, the other guard against invalidation.
    ///
    /// A position counts rows block by block,
    /// and a consolidation or a drop numbers them anew without changing the count.
    pub(crate) blocks: Vec<Range<Revision>>,
}

impl PredicateCursor {
    /// The default number of rows per read.
    pub const BUDGET: usize = 1 << 16;

    /// Returns the rows delivered so far.
    pub fn position(&self) -> usize {
        self.position
    }

    /// Returns the rows not yet delivered.
    pub fn remaining(&self) -> usize {
        self.rows - self.position
    }

    /// Returns whether every row has been delivered.
    pub fn is_done(&self) -> bool {
        self.position >= self.rows
    }
}

/// One bounded portion of a predicate's rows, decoded into owned values and stored
/// column-major.
///
/// This matches how the engine stores rows,
/// and how columnar formats such as Arrow and dataframes expect them.
///
/// # Example
///
/// ```
/// use nemo::{
///     engine::Engine,
///     model::{FileId, Program},
///     values::DataValue,
/// };
///
/// let (program, _) = Program::parse("pair(1, 2) . pair(3, 4) .", FileId::new(0));
/// let mut engine = Engine::new();
/// engine.load(program)?;
/// engine.reason()?;
///
/// let mut cursor = engine.cursor("pair")?;
/// let chunk = engine.read_chunk(&mut cursor, 100)?;
/// assert_eq!((chunk.len(), chunk.arity()), (2, 2));
/// assert!(chunk.column(0).contains(&DataValue::integer(3)));
/// assert!(chunk.rows().all(|row| row.len() == 2));
/// # Ok::<(), nemo::diagnostics::Diagnostic>(())
/// ```
#[derive(Debug, Clone)]
pub struct RowChunk {
    /// The columns, every vector holding one value per row.
    columns: Vec<Vec<DataValue>>,
    /// The number of rows, carried apart so that nullary predicates (no columns) count their row.
    rows: usize,
}

impl RowChunk {
    /// Creates an empty chunk of the given arity.
    pub(crate) fn new(arity: usize) -> Self {
        Self {
            columns: vec![Vec::new(); arity],
            rows: 0,
        }
    }

    /// Appends one decoded row, with one value per column.
    pub(crate) fn push(&mut self, values: &[DataValue]) {
        for (column, value) in self.columns.iter_mut().zip(values) {
            column.push(value.clone());
        }
        self.rows += 1;
    }

    /// Returns the number of rows.
    pub fn len(&self) -> usize {
        self.rows
    }

    /// Returns whether the chunk holds no rows.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Returns the number of columns.
    pub fn arity(&self) -> usize {
        self.columns.len()
    }

    /// Returns the values of one column, one per row.
    pub fn column(&self, index: usize) -> &[DataValue] {
        &self.columns[index]
    }

    /// Returns one row, gathered across the columns.
    pub fn row(&self, index: usize) -> Vec<DataValue> {
        self.columns
            .iter()
            .map(|column| column[index].clone())
            .collect()
    }

    /// Returns the rows in order.
    ///
    /// Each is gathered across the columns,
    /// so consumers thinking in columns should prefer [column](Self::column).
    pub fn rows(&self) -> impl Iterator<Item = Vec<DataValue>> + '_ {
        (0..self.rows).map(|index| self.row(index))
    }
}
