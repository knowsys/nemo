//! `VALUES` blocks: the values that a query is asked with,
//! as a pattern of the query and as the lookup that a reader matches solutions against.

use std::collections::HashSet;

use nemo_physical::value::DataValue;
use spargebra::{
    algebra::GraphPattern,
    term::{GroundTerm, Variable},
};

use super::SparqlQuery;

/// One `VALUES` block: rows of values over some of a query's projected columns.
///
/// Each value is sent as its [ground terms](SparqlQuery::ground_terms),
/// which the endpoint matches by its own notion of equal terms.
/// A [SparqlReader](super::SparqlReader) given the block
/// ([with_values](super::SparqlReader::with_values)) keeps only the solutions that match a
/// row as Nemo compares values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValuesBlock {
    /// The positions of the projected columns that the rows bind.
    pub positions: Vec<usize>,
    /// The rows, one value per position.
    pub rows: Vec<Vec<DataValue>>,
}

impl ValuesBlock {
    /// Returns the block as a `VALUES` pattern over the query's projected variables.
    ///
    /// Each row becomes one row for each combination of its values' ground terms:
    /// one row for a row without integers, and `2^k` rows for a row with `k` integers.
    /// A row with a value that has no ground term, such as a null, is left out.
    pub(super) fn pattern(&self, variables: &[String]) -> GraphPattern {
        let mut bindings: Vec<Vec<Option<GroundTerm>>> = Vec::new();
        for row in &self.rows {
            let mut combinations: Vec<Vec<Option<GroundTerm>>> = vec![Vec::new()];
            for value in row {
                let terms = SparqlQuery::ground_terms(value);
                combinations = combinations
                    .into_iter()
                    .flat_map(|prefix| {
                        terms.iter().map(move |term| {
                            let mut combination = prefix.clone();
                            combination.push(Some(term.clone()));
                            combination
                        })
                    })
                    .collect();
            }

            bindings.extend(combinations);
        }

        GraphPattern::Values {
            variables: self
                .positions
                .iter()
                .map(|&position| Variable::new_unchecked(variables[position].as_str()))
                .collect(),
            bindings,
        }
    }
}

/// One `VALUES` block in the form that a reader matches solutions against.
#[derive(Debug, Clone)]
pub(super) struct ValuesLookup {
    /// The positions of the projected columns that the rows bind.
    positions: Vec<usize>,
    /// The rows, one value per position.
    rows: HashSet<Vec<DataValue>>,
}

impl ValuesLookup {
    /// Creates the lookup of a block.
    pub(super) fn new(block: &ValuesBlock) -> Self {
        Self {
            positions: block.positions.clone(),
            rows: block.rows.iter().cloned().collect(),
        }
    }

    /// Returns the positions of the projected columns that the rows bind.
    pub(super) fn positions(&self) -> &[usize] {
        &self.positions
    }

    /// Returns whether a solution's values in the block's columns form one of the block's rows.
    ///
    /// # Panics
    ///
    /// Panics if the solution has no value in one of the block's columns.
    pub(super) fn matches(&self, values: &[Option<DataValue>]) -> bool {
        let key: Vec<DataValue> = self
            .positions
            .iter()
            .map(|&position| {
                values[position]
                    .clone()
                    .expect("a solution has a value in every column of a block")
            })
            .collect();

        self.rows.contains(&key)
    }
}

#[cfg(test)]
mod tests;
