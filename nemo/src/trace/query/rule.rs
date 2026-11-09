//! The rules of the written program, as responses name them.

use serde::{Deserialize, Serialize};

/// A rule of the written program, together with one of its head atoms (nev: `Rule`).
///
/// A bound import, which is an import with a `where` clause, counts as such a rule.
/// Its predicate is the head, and the atoms of its `where` clause are the body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRule {
    /// The rule's statement id.
    pub id: usize,
    /// The head atom.
    pub relevant_head_predicate: PredicateWithParameters,
    /// The head atom's index among the rule's head atoms.
    pub relevant_head_predicate_index: usize,
    /// The atoms of the body, in body order,
    /// with the name of a negated atom prefixed by `~`.
    pub body_predicates: Vec<PredicateWithParameters>,
    /// The rule as written.
    pub string_representation: String,
}

/// An atom of a rule: its predicate and its terms as written (nev:
/// `PredicateWithParameters`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredicateWithParameters {
    /// The predicate's resolved name, with a `~` before it for a negated body atom.
    pub name: String,
    /// The terms as written, in declaration order.
    pub parameters: Vec<String>,
}
