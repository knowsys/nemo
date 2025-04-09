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

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableResponseBaseTableEntriesPagination {
    #[serde(rename = "start")]
    pub start: usize,
    #[serde(rename = "moreEntriesExist")]
    pub more_entries_exist: bool,
}

impl From<TableResponseBaseTableEntriesPagination>
    for nemo::execution::tracing::shared::PaginationResponse
{
    fn from(value: TableResponseBaseTableEntriesPagination) -> Self {
        Self {
            start: value.start,
            more: value.more_entries_exist,
        }
    }
}

impl From<nemo::execution::tracing::shared::PaginationResponse>
    for TableResponseBaseTableEntriesPagination
{
    fn from(value: nemo::execution::tracing::shared::PaginationResponse) -> Self {
        Self {
            start: value.start,
            more_entries_exist: value.more,
        }
    }
}
