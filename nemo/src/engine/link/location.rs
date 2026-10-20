//! Where a module name leads: the name resolved against the directory of the file
//! containing the `@use`, or against a module root.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use crate::{diagnostics::ModuleError, model::ModuleName};

/// A module, or a directory of modules, independent of where its tree lies on disk.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct Location {
    /// The module root it lies in,
    /// or `None` for the tree around the program containing the outermost `@use`.
    pub(super) root: Option<String>,
    /// How many directories above the program's directory it lies, zero inside a root.
    pub(super) parents: usize,
    /// The directories below, then, for a module, its name.
    pub(super) segments: Vec<String>,
}

impl Location {
    /// Returns the location of the program's directory.
    pub(super) fn program() -> Self {
        Self {
            root: None,
            parents: 0,
            segments: Vec::new(),
        }
    }

    /// Returns the location of the directory a module lies in.
    pub(super) fn directory(&self) -> Self {
        let mut directory = self.clone();
        directory.segments.pop();
        directory
    }

    /// Resolves a module name written in a file in the directory `from`.
    ///
    /// A name of two or more parts whose first part is one of `roots` names a module in that root.
    /// Any other name is relative to `from`.
    ///
    /// # Errors
    ///
    /// Returns an error if the name's `super` steps lead out of the root `from` lies in.
    pub(super) fn resolve(
        name: &ModuleName,
        from: &Location,
        roots: &HashMap<String, PathBuf>,
    ) -> Result<Self, ModuleError> {
        if let Some(root) = root_of(name, roots) {
            return Ok(Self {
                root: Some(root.to_owned()),
                parents: 0,
                segments: name.segments[1..].to_vec(),
            });
        }
        let mut location = from.clone();
        for _ in 0..name.parents {
            if location.segments.pop().is_some() {
                continue;
            }
            if let Some(root) = &location.root {
                return Err(ModuleError::LeavesRoot {
                    module: name.to_string(),
                    root: root.clone(),
                });
            }
            location.parents += 1;
        }
        location.segments.extend(name.segments.iter().cloned());
        Ok(location)
    }

    /// Returns the directory the location's tree lies in:
    /// its root's, or the program's.
    ///
    /// # Panics
    ///
    /// Panics if the location's root is not one of `roots`.
    pub(super) fn tree<'a>(
        &self,
        program: &'a Path,
        roots: &'a HashMap<String, PathBuf>,
    ) -> &'a Path {
        match &self.root {
            Some(root) => roots.get(root).expect("locations lie in declared roots"),
            None => program,
        }
    }

    /// Returns the path of the module file at this location, whose tree lies in `tree`.
    pub(super) fn file(&self, tree: &Path) -> PathBuf {
        let mut path = self.directory().path(tree);
        if let Some(name) = self.segments.last() {
            path.push(format!("{name}.rls"));
        }
        path
    }

    /// Returns the path of the directory at this location, whose tree lies in `tree`.
    pub(super) fn path(&self, tree: &Path) -> PathBuf {
        let mut path = tree.to_path_buf();
        for _ in 0..self.parents {
            path.push("..");
        }
        path.extend(&self.segments);
        path
    }
}

/// Returns the root a module name begins with:
/// its first part, if the name has two or more parts, no `super` step, and the root is declared.
pub(super) fn root_of<'a>(
    name: &'a ModuleName,
    roots: &HashMap<String, PathBuf>,
) -> Option<&'a str> {
    let first = name.segments.first()?;
    (name.parents == 0 && name.segments.len() >= 2 && roots.contains_key(first))
        .then_some(first.as_str())
}
