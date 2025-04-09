/*
 * Generated by cue.
 *
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: v0.1.0
 *
 * Generated by: https://openapi-generator.tech
 */

use serde::{Deserialize, Serialize};

use super::TableResponseBaseTableEntries;

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableResponseBase {
    #[serde(rename = "predicate")]
    pub predicate: String,
    #[serde(rename = "tableEntries")]
    pub table_entries: Box<TableResponseBaseTableEntries>,
    #[serde(rename = "possibleRulesAbove")]
    pub possible_rules_above: Vec<i32>,
    #[serde(rename = "possibleRulesBelow")]
    pub possible_rules_below: Vec<i32>,
}
