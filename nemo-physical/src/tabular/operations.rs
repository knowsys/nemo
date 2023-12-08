//! This module collects operations over tries

/// Module for materializing tries
pub mod materialize;
pub use materialize::materialize;

/// Module for defining [`TrieScanAggregate`]
pub mod triescan_aggregate;
pub use triescan_aggregate::TrieScanAggregate;

/// Module for defining [`TrieScanProject`]
pub mod triescan_project;
pub use triescan_project::TrieScanProject;

/// Module for defining [`TrieScanPrune`]
pub mod triescan_prune;
pub use triescan_prune::TrieScanPrune;

/// Module for defining [`TrieScanSelectEqual`] and [`TrieScanRestrictValues`]
pub mod triescan_select;
pub use triescan_select::TrieScanRestrictValues;
pub use triescan_select::TrieScanSelectEqual;

/// Module for defining [`TrieScanMinus`]
pub mod triescan_minus;
pub use triescan_minus::TrieScanMinus;

/// Module for defining [`TrieScanUnion`]
pub mod triescan_union;
pub use triescan_union::TrieScanUnion;

/// Module for defining [`TrieScanJoin`]
pub mod triescan_join;
pub use triescan_join::JoinBindings;
pub use triescan_join::TrieScanJoin;

/// Module for defining append functionality
pub mod triescan_append;

/// Module for defining [`TrieScanNulls`]
pub mod triescan_nulls;
pub use triescan_nulls::TrieScanNulls;

/// Module implementing functionality for projecting and reordering tries.
pub mod project_reorder;

/// END OF OLD OPERATIONS
use std::collections::HashMap;
use std::{fmt::Debug, hash::Hash, iter::IntoIterator};

use super::triescan::TrieScanEnum;

pub mod join;
pub mod projectreorder;
pub mod prune;
pub mod subtract;
pub mod union;

/// Marker for a column
///
/// This is used in [OperationTable].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OperationColumnMarker(usize);

/// This object is used to reference columns of input/output tables
/// of a data base operation.
#[derive(Debug, Clone)]
pub struct OperationTable(Vec<OperationColumnMarker>);

impl OperationTable {
    /// Return the number of columns associated with this table.
    pub fn arity(&self) -> usize {
        self.0.len()
    }

    /// Return the position of a marker in this table.
    pub fn position(&self, marker: &OperationColumnMarker) -> Option<usize> {
        self.0
            .iter()
            .position(|current_marker| current_marker == marker)
    }
}

impl IntoIterator for OperationTable {
    type Item = OperationColumnMarker;
    type IntoIter = std::vec::IntoIter<OperationColumnMarker>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Helper object to translate [OperationTable] like structures
/// from the users of the physical crate into actual [OperationTable]s
#[derive(Debug)]
pub struct OperationTableGenerator<ExternalMarker>
where
    ExternalMarker: Clone + PartialEq + Eq + Hash,
{
    /// Associates an external marker with an [OperationColumnMarker]
    map: HashMap<ExternalMarker, OperationColumnMarker>,
}

/// Trait for objects that are able to generate [TrieScanEnum],
/// which implement certain data base operations.
pub(crate) trait OperationGenerator {
    /// Generate a
    fn generate<'a>(&'_ self, input: Vec<TrieScanEnum<'a>>) -> TrieScanEnum<'a>;
}
