//! This module defines [RuleFile].

use std::{fs::read_to_string, path::PathBuf};

use crate::error::Error;

/// Contains the content and name of a rule file
#[derive(Debug, Default)]
pub struct RuleFile {
    /// Name of the file
    name: String,
    /// Content of the file
    content: String,
}

impl RuleFile {
    /// Create a new [RuleFile].
    pub fn new(content: String, name: String) -> Self {
        Self { name, content }
    }

    /// Load from the given path.
    pub fn load(path: PathBuf) -> Result<Self, Error> {
        let name = path.to_string_lossy().to_string();
        let content = read_to_string(path.clone()).map_err(|error| Error::IoReading {
            error,
            filename: name.clone(),
        })?;

        Ok(Self::new(name, content))
    }

    /// Return a reference to the content of this file.
    pub fn content(&self) -> &str {
        &self.content
    }

    /// Return a reference to the name of this file.
    pub fn name(&self) -> &str {
        &self.name
    }
}
