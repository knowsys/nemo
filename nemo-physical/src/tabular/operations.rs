//! This module collects operations over tries

pub mod filter;
pub mod function;
pub mod join;
pub mod projectreorder;
pub mod prune;
pub mod subtract;
pub mod union;

use std::collections::HashMap;
use std::ops::Deref;
use std::{fmt::Debug, hash::Hash, iter::IntoIterator};

use crate::dictionary::meta_dv_dict::MetaDvDictionary;

use super::triescan::TrieScanEnum;

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

impl Deref for OperationTable {
    type Target = [OperationColumnMarker];

    fn deref(&self) -> &[OperationColumnMarker] {
        &self.0
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
    _map: HashMap<ExternalMarker, OperationColumnMarker>,
}

/// Trait for objects that are able to generate [TrieScanEnum],
/// which implement certain data base operations.
pub(crate) trait OperationGenerator {
    /// Generate a
    fn generate<'a>(
        &'_ self,
        input: Vec<TrieScanEnum<'a>>,
        dictionary: &'a MetaDvDictionary,
    ) -> TrieScanEnum<'a>;
}
