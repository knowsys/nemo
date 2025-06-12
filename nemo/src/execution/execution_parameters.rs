//! This module defines [ExecutionParameters].

use std::collections::HashMap;

use crate::{
    io::{resource_providers::ResourceProviders, ImportManager},
    rule_model::components::term::primitive::{
        ground::GroundTerm, variable::global::GlobalVariable,
    },
};

/// Externally modify the export statements of the program
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum ExportParameters {
    /// Keep all exports as is
    #[default]
    Keep,
    /// Disable all exports
    None,
    /// Export all IDB predicates (those used in rule heads)
    Idb,
    /// Export all EDB predicates (those for which facts are given or imported)
    Edb,
    /// Export all predicates
    All,
}

/// External parameters affecting the execution
#[derive(Debug)]
pub struct ExecutionParameters {
    /// Responsible for resolving inputs
    pub(crate) import_manager: ImportManager,
    /// Definition of global variables
    pub(crate) global_variables: HashMap<GlobalVariable, GroundTerm>,
    /// Modification of the export statements
    pub(crate) export_parameters: ExportParameters,
}

impl Default for ExecutionParameters {
    fn default() -> Self {
        Self {
            import_manager: ImportManager::new(ResourceProviders::default()),
            global_variables: Default::default(),
            export_parameters: ExportParameters::default(),
        }
    }
}

impl ExecutionParameters {
    /// Set gloal variables.
    ///
    /// If any parameter is invalid it will return it as an error
    pub fn set_global<Iter>(&mut self, iterator: Iter) -> Result<(), String>
    where
        Iter: Iterator<Item = (String, String)>,
    {
        let mut result = HashMap::new();
        for (key, value) in iterator {
            let variable = GlobalVariable::new(&key);

            let Ok(term) = GroundTerm::parse(&value) else {
                return Err(key);
            };

            result.insert(variable, term);
        }

        self.global_variables = result;
        Ok(())
    }

    /// Set export parameters.
    pub fn set_export_parameters(&mut self, parameters: ExportParameters) {
        self.export_parameters = parameters;
    }

    /// Set the import manager.
    pub fn set_import_manager(&mut self, import_manager: ImportManager) {
        self.import_manager = import_manager;
    }
}
