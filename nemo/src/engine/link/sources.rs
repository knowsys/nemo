//! What modules an engine can read besides the files next to a program:
//! module roots, and module texts registered by name.

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    diagnostics::{AboutResource, EngineError, ModuleError},
    engine::{Engine, link::location::Location},
    model::ModuleName,
};

/// The module roots and registered module texts of an engine.
#[derive(Debug, Default)]
pub(in crate::engine) struct ModuleSources {
    /// The module roots by name, with their directories.
    pub(super) roots: HashMap<String, PathBuf>,
    /// The registered module texts, by the name a `@use` in the program writes for them.
    texts: HashMap<ModuleName, Arc<str>>,
}

impl ModuleSources {
    /// Returns the registered module texts by location.
    ///
    /// A name is resolved as a `@use` in the program resolves it,
    /// so whether it begins with a root is decided by the roots declared now.
    pub(super) fn texts(&self) -> HashMap<Location, Arc<str>> {
        self.texts
            .iter()
            .filter_map(|(name, text)| {
                let location = Location::resolve(name, &Location::program(), &self.roots).ok()?;
                Some((location, text.clone()))
            })
            .collect()
    }
}

impl Engine {
    /// Registers a module text under the name a `@use` in the program writes for it,
    /// such as `lib::tc`, or `shared::tc` for a module in the root `shared`.
    ///
    /// A `@use` of the module, in the program or in another module, reads this text
    /// instead of the module's file.
    /// This serves modules that exist only in memory, such as an editor's unsaved buffer
    /// or a text in a browser.
    /// Registering a name again replaces its text.
    ///
    /// # Errors
    ///
    /// Returns a parse error if `name` is not a module name,
    /// and N0480 for a text of 4 GiB or more.
    pub fn add_module(&mut self, name: &str, text: impl Into<String>) -> Result<(), Diagnostic> {
        let module = ModuleName::parse(name)?;
        let text: String = text.into();
        if u32::try_from(text.len()).is_err() {
            return Err(EngineError::SourceTooLarge {
                name: name.to_owned(),
            }
            .diagnostic()
            .with_resource(name.to_owned()));
        }
        self.modules.texts.insert(module, Arc::from(text));
        Ok(())
    }

    /// Declares a module root: a directory whose modules any file names as `name::module`.
    ///
    /// The modules in a root name each other relative to their own directories,
    /// and cannot name modules outside the root with `super`.
    /// Declaring a name again replaces its directory.
    ///
    /// # Errors
    ///
    /// Returns N0422 if `name` is not a single plain name, or is `super`.
    pub fn add_module_root(
        &mut self,
        name: &str,
        directory: impl Into<PathBuf>,
    ) -> Result<(), Diagnostic> {
        let not_a_root = || {
            ModuleError::NotARootName {
                name: name.to_owned(),
            }
            .diagnostic()
        };
        let module = ModuleName::parse(name).map_err(|_| not_a_root())?;
        let [root] = module.segments.as_slice() else {
            return Err(not_a_root());
        };
        if module.parents > 0 {
            return Err(not_a_root());
        }
        self.modules.roots.insert(root.clone(), directory.into());
        Ok(())
    }
}
