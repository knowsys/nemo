//! This module contains helper functions to display separated lists of values.

//! Object holding functions to create separated lists of values

use std::fmt::Display;
#[derive(Debug, Copy, Clone)]
pub struct DisplaySeperatedList {}

impl DisplaySeperatedList {
    //! Display a seperated list of values
    pub fn display<T: Display>(list: impl Iterator<Item = T>, separator: &str) -> String {
        list.map(|entry| entry.to_string())
            .collect::<Vec<_>>()
            .join(separator)
    }
}
