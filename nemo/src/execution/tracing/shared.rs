//! This module contains shared data structures
//! for defining tracing queries and responses.

use itertools::Itertools;
use nemo_physical::datavalues::AnyDataValue;
use serde::{Deserialize, Serialize};

use crate::{
    chase_model::{
        components::{
            atom::{primitive_atom::PrimitiveAtom, variable_atom::VariableAtom},
            rule::ChaseRule,
        },
        ChaseAtom,
    },
    rule_model::components::{tag::Tag, term::primitive::ground::GroundTerm},
};

/// A predicate name with the parameters used with it in a rule (essentially an atom)
#[derive(Debug, Serialize, Deserialize)]
pub struct PredicateWithParameters {
    /// The predicate name
    pub name: String,
    /// The list of stringified parameters
    pub parameters: Vec<String>,
}

impl PredicateWithParameters {
    fn from_positive_variable_atom(atom: &VariableAtom) -> Self {
        Self {
            name: atom.predicate().to_string(),
            parameters: atom.terms().map(ToString::to_string).collect(),
        }
    }

    fn from_negative_variable_atom(atom: &VariableAtom) -> Self {
        Self {
            name: format!("~ {}", atom.predicate()),
            parameters: atom.terms().map(ToString::to_string).collect(),
        }
    }
}

impl From<&PrimitiveAtom> for PredicateWithParameters {
    fn from(atom: &PrimitiveAtom) -> Self {
        Self {
            name: atom.predicate().to_string(),
            parameters: atom.terms().map(ToString::to_string).collect(),
        }
    }
}

/// Identifies for a rule (i.e. its index)
pub type RuleId = usize;

/// Necessary Information for a rule
#[derive(Debug, Serialize, Deserialize)]
pub struct Rule {
    /// The rule's index
    pub id: RuleId,
    /// The rule's head predicte used to derive the node further up in the tree
    pub relevant_head_predicate: PredicateWithParameters,
    /// The head index of the relevant head predicate
    pub relevant_head_predicate_index: usize,
    /// The rule's body predictes (in order)
    pub body_predicates: Vec<PredicateWithParameters>,
    /// The rule as is occurs in the program
    pub string_representation: String,
}

impl Rule {
    pub(crate) fn from_rule_and_head(
        rule_idx: usize,
        rule: &ChaseRule,
        head_idx: usize,
        head: &PrimitiveAtom,
    ) -> Self {
        Self {
            id: rule_idx,
            relevant_head_predicate: PredicateWithParameters::from(head),
            relevant_head_predicate_index: head_idx,
            body_predicates: rule
                .positive_body()
                .into_iter()
                .map(PredicateWithParameters::from_positive_variable_atom)
                .chain(
                    rule.negative_body()
                        .into_iter()
                        .map(PredicateWithParameters::from_negative_variable_atom),
                )
                .collect(),
            string_representation:
                "WE SHOULD REBASE THIS BRANCH ON THE CURRENT MAIN TO MAKE THIS WORK".to_string(),
        }
    }

    pub(crate) fn all_possible_single_head_rules(
        rule_idx: usize,
        rule: &ChaseRule,
    ) -> impl Iterator<Item = Self> + '_ {
        rule.head()
            .iter()
            .enumerate()
            .map(move |(head_idx, head)| Self::from_rule_and_head(rule_idx, rule, head_idx, head))
    }

    pub(crate) fn possible_rules_for_head_predicate<'a, 'b>(
        rule_idx: usize,
        rule: &'a ChaseRule,
        head_predicate: &'a Tag,
    ) -> impl Iterator<Item = Self> + 'a {
        rule.head()
            .iter()
            .enumerate()
            .filter(|(_, head)| head.predicate() == *head_predicate)
            .map(move |(head_idx, head)| Self::from_rule_and_head(rule_idx, rule, head_idx, head))
    }
}

/// Identifier for a table entry
pub type TableEntryId = usize;

/// Query selecting one or multiple table entries
#[derive(Debug, Serialize, Deserialize)]
pub enum TableEntryQuery {
    /// Specific table entry identified by [TableEntryId]
    Entry(TableEntryId),
    /// Query string potentially selecting multiple table entries
    Query(String),
}

/// Structure for requesting only a part of the results
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct PaginationQuery {
    /// Starting index of the result
    pub start: usize,
    /// How many results are expected
    pub count: usize,
}

/// Response for a [PaginationQuery]
/// indicating the starting index and whether there are more results
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct PaginationResponse {
    /// Start index of the result
    pub start: usize,
    /// Whether there are more entries in this table
    pub more: bool,
}

// Serialization for any datavalues
mod any_datavalue_serde {
    use nemo_physical::datavalues::AnyDataValue;
    use serde::{ser::SerializeSeq, Deserialize, Deserializer, Serializer};

    use crate::rule_model::components::term::primitive::ground::GroundTerm;

    pub fn serialize_vec<S>(value: &Vec<AnyDataValue>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(value.len()))?;
        for item in value {
            seq.serialize_element(&item.to_string())?;
        }
        seq.end()
    }

    pub fn deserialize_vec<'de, D>(deserializer: D) -> Result<Vec<AnyDataValue>, D::Error>
    where
        D: Deserializer<'de>,
    {
        println!("deserialize anydatavalue:");

        let string_vec = Vec::<String>::deserialize(deserializer)?;
        string_vec
            .into_iter()
            .map(|string| {
                GroundTerm::parse(&string)
                    .map(|term| term.value())
                    .map_err(serde::de::Error::custom)
            })
            .collect()
    }
}

/// Response containig the entries of a table node
#[derive(Debug, Serialize, Deserialize)]
pub struct TableEntryResponse {
    /// Identifier of the table entry
    pub entry_id: TableEntryId,
    /// Terms in the table entry
    #[serde(
        serialize_with = "any_datavalue_serde::serialize_vec",
        deserialize_with = "any_datavalue_serde::deserialize_vec"
    )]
    pub terms: Vec<AnyDataValue>,
}

impl TableEntryResponse {
    /// Create a new [TableEntryResponse] with terms given as a list of [String]s.
    ///
    /// Retrurns `None` if any term cannot be parsed.
    pub fn new_from_string(entry_id: TableEntryId, terms: &[String]) -> Option<Self> {
        let parsed_terms: Result<Vec<AnyDataValue>, _> = terms
            .iter()
            .map(|s| GroundTerm::parse(s).map(|term| term.value()))
            .collect();

        parsed_terms
            .ok()
            .map(|terms| TableEntryResponse { entry_id, terms })
    }

    /// Return a list of each term converted to string.
    pub fn terms_string(&self) -> Vec<String> {
        self.terms.iter().map(|term| term.to_string()).collect()
    }
}
