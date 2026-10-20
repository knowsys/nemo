//! The rules of the written program, as responses name them.

use serde::{Deserialize, Serialize};

/// A rule of the written program, together with one of its head atoms (nev: `Rule`).
///
/// A bound import (an import with a `where` clause) counts as such a rule:
/// its predicate is the head, and the atoms of its `where` clause are the body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRule {
    /// The rule's statement id.
    pub id: usize,
    /// The head atom.
    pub relevant_head_predicate: PredicateWithParameters,
    /// The head atom's index over the rule's head.
    pub relevant_head_predicate_index: usize,
    /// The body's atoms in body order, a negated one's name prefixed
    /// with `~`.
    pub body_predicates: Vec<PredicateWithParameters>,
    /// The rule as written.
    pub string_representation: String,
}

/// An atom of a rule: its predicate and its terms as written (nev:
/// `PredicateWithParameters`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredicateWithParameters {
    /// The predicate, by resolved name.
    pub name: String,
    /// The terms, as written.
    pub parameters: Vec<String>,
}
