/*
 * Generated by cue.
 *
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: v0.1.0
 *
 * Generated by: https://openapi-generator.tech
 */

use crate::models;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableResponseBaseTableEntriesPagination {
    #[serde(rename = "start")]
    pub start: i32,
    /// count not required since this is the length of the array
    #[serde(rename = "moreEntriesExist")]
    pub more_entries_exist: bool,
}

impl TableResponseBaseTableEntriesPagination {
    pub fn new(start: i32, more_entries_exist: bool) -> TableResponseBaseTableEntriesPagination {
        TableResponseBaseTableEntriesPagination {
            start,
            more_entries_exist,
        }
    }
}
