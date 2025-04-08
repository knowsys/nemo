//! This module contains shared data structures
//! for defining tracing queries and responses.

use nemo_physical::datavalues::AnyDataValue;
use serde::{Deserialize, Serialize};

/// Identifier for a rule
pub type RuleId = usize;
/// Identifier for a table entry
pub type TableEntryId = usize;

/// Query selecting one or multiple table entries
#[derive(Debug, Serialize, Deserialize)]
pub enum TableEntryQuery {
    /// Specific table entry
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
        let mut seq = serializer.serialize_seq(Some(value.len()))?; // Begin sequence serialization
        for item in value {
            seq.serialize_element(&item.to_string())?; // Serialize each element
        }
        seq.end() // Finish sequence serialization
    }

    pub fn deserialize_vec<'de, D>(deserializer: D) -> Result<Vec<AnyDataValue>, D::Error>
    where
        D: Deserializer<'de>,
    {
        println!("deserialize anydatavalue:");

        let string_vec = Vec::<String>::deserialize(deserializer)?; // Deserialize as Vec<String>
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
