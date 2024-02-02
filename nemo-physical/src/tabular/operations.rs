//! This module defines operations over tries

pub mod filter;
pub mod function;
pub mod join;
pub mod projectreorder;
pub mod prune;
pub mod subtract;
pub mod union;

use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    hash::Hash,
    iter::IntoIterator,
    ops::Deref,
};

use delegate::delegate;

use crate::{dictionary::meta_dv_dict::MetaDvDictionary, util::mapping::permutation::Permutation};

use self::{
    filter::GeneratorFilter, function::GeneratorFunction, join::GeneratorJoin,
    subtract::GeneratorSubtract, union::GeneratorUnion,
};

use super::triescan::TrieScanEnum;

/// Marker for a column
///
/// This is used in [OperationTable].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OperationColumnMarker(usize);

/// This object is used to reference columns of input/output tables
/// of a data base operation.
#[derive(Debug, Default, Clone)]
pub struct OperationTable(Vec<OperationColumnMarker>);

impl OperationTable {
    /// Create a [OperationTable] which marks each column with a distinct [OperationColumnMarker].
    pub fn new_unique(arity: usize) -> Self {
        Self((0..arity).map(|i| OperationColumnMarker(i)).collect())
    }

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

    /// Push a new marker to end of this table.
    pub fn push(&mut self, marker: OperationColumnMarker) {
        self.0.push(marker);
    }

    /// Pushes a new unique [OperationColumnMarker] at the end of the table.
    ///
    /// Returns a reference to the newly added [OperationColumnMarker].
    pub fn push_new(&mut self) -> &OperationColumnMarker {
        self.push(OperationColumnMarker(self.0.len()));
        self.0.last().expect("Value has been pushed above")
    }

    /// Pushes a new unique [OperationColumnMarker] at given index in the table.
    ///
    /// Returns a reference to the newly added [OperationColumnMarker].
    pub fn push_new_at(&mut self, index: usize) -> &OperationColumnMarker {
        self.0.insert(index, OperationColumnMarker(self.0.len()));
        &self.0[index]
    }

    /// Return the [OperationColumnMarker] of the column with the given index.
    ///
    /// # Panics
    /// Panics if there is no column with this index.
    pub fn get(&self, index: usize) -> &OperationColumnMarker {
        &self.0[index]
    }

    /// Returns an iterator over the [OperationColumnMarker]s in the table.
    pub fn iter(&self) -> std::slice::Iter<'_, OperationColumnMarker> {
        self.0.iter()
    }

    /// Reorder the columns of this table with the given [Permutation]
    /// and return the reordered [OperationTable].
    pub fn apply_permutation(&self, permutation: &Permutation) -> Self {
        Self(permutation.permute(&self.0))
    }

    /// Align the markers of this table to the markers
    /// with the markers of the target table.
    ///
    /// Return a pair, consisting of the [Permutation] such that if applied to this table,
    /// would result in a table where the order of its markers
    /// is the same as that of the target table
    /// and the reordered [OperationTable].
    ///
    /// # Panics
    /// Panics if this table contains a marker that is not available in `target`.
    pub fn align(&self, target: &Self) -> (Self, Permutation) {
        let mut indices = (0..self.arity()).collect::<Vec<_>>();
        indices.sort_by_key(|&index| {
            target
                .position(self.get(index))
                .expect("Function assumes that every marker in this table is available in target")
        });

        let permutation = Permutation::from_vector(indices);
        (Self(permutation.permute(&self.0)), permutation)
    }

    /// Remove all `markers` from this table
    /// that are not in the `target` table.
    ///
    /// Returns the altered table as a new object.
    pub fn restrict(&self, target: &Self) -> Self {
        Self(
            self.0
                .iter()
                .cloned()
                .filter(|marker| target.contains(marker))
                .collect(),
        )
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

/// Helper object to obtain [OperationTable]s
/// mainly by providing a translation between
/// user defined "markers" (supplied as a generic parameter to this object)
/// and [OperationColumnMarker]s.
#[derive(Debug, Default)]
pub struct OperationTableGenerator<ExternalMarker>
where
    ExternalMarker: Clone + PartialEq + Eq + Hash,
{
    /// Associates an external marker with an [OperationColumnMarker]
    map: HashMap<ExternalMarker, OperationColumnMarker>,
}

impl<ExternalMarker> OperationTableGenerator<ExternalMarker>
where
    ExternalMarker: Clone + PartialEq + Eq + Hash,
{
    /// Create a new
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Add a new marker.
    pub fn add_marker(&mut self, marker: ExternalMarker) {
        let next_marker = OperationColumnMarker(self.map.len());
        match self.map.entry(marker) {
            Entry::Vacant(entry) => {
                entry.insert(next_marker);
            }
            Entry::Occupied(_) => {}
        }
    }

    /// Generate an [OperationTable] from an iterator.
    ///
    /// # Panics
    /// Panics if there is no known translation from the external markers
    /// into [OperationColumnMarker]s.
    pub fn operation_table<'a, ExternalIterator: Iterator<Item = &'a ExternalMarker>>(
        &self,
        iterator: ExternalIterator,
    ) -> OperationTable
    where
        ExternalMarker: 'a,
    {
        OperationTable(
            iterator
                .map(|marker| {
                    self.map
                        .get(marker)
                        .expect("Function assumes that every relevant external marker is known")
                        .clone()
                })
                .collect(),
        )
    }

    /// Returns the [OperationColumnMarker] associated with the argument.
    pub fn get<'a>(&'a self, marker: &ExternalMarker) -> Option<&'a OperationColumnMarker> {
        self.map.get(marker)
    }
}

/// Trait for objects that are able to generate [TrieScanEnum],
/// which implement certain data base operations.
pub(crate) trait OperationGenerator {
    /// Generate a [TrieScanEnum].
    ///
    /// Returns `None` if executing this operation would result in an empty table.
    fn generate<'a>(
        &'_ self,
        input: Vec<Option<TrieScanEnum<'a>>>,
        dictionary: &'a MetaDvDictionary,
    ) -> Option<TrieScanEnum<'a>>;
}

pub(crate) enum OperationGeneratorEnum {
    /// Join
    Join(GeneratorJoin),
    /// Union
    Union(GeneratorUnion),
    /// Subtract
    Subtract(GeneratorSubtract),
    /// Filter
    Filter(GeneratorFilter),
    /// Function
    Function(GeneratorFunction),
}

impl OperationGenerator for OperationGeneratorEnum {
    delegate! {
        to match self {
            Self::Join(generator) => generator,
            Self::Union(generator) => generator,
            Self::Subtract(generator) => generator,
            Self::Filter(generator) => generator,
            Self::Function(generator) => generator,
        } {
            fn generate<'a>(
                &'_ self,
                input: Vec<Option<TrieScanEnum<'a>>>,
                dictionary: &'a MetaDvDictionary,
            ) -> Option<TrieScanEnum<'a>>;
        }
    }
}

impl Debug for OperationGeneratorEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Join(generator) => f.write_fmt(format_args!("Join ({generator:?})")),
            Self::Union(_generator) => f.write_fmt(format_args!("Union")),
            Self::Subtract(generator) => f.write_fmt(format_args!("Subtract ({generator:?})")),
            Self::Filter(generator) => f.write_fmt(format_args!("Filter ({generator:?})")),
            Self::Function(generator) => f.write_fmt(format_args!("Function ({generator:?})")),
        }
    }
}
