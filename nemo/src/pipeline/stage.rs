//! Transformations as named stages.

use std::sync::Arc;

use nemo_physical::diagnostics::Diagnostic;

use crate::pipeline::Commit;

/// A program transformation: one named stage rewriting a revision
/// through a [Commit].
///
/// A stage names the stages that must have run before it.
/// The pipeline checks this as it runs, so a stage list in the wrong order
/// panics at the first stage whose required stage is missing.
/// What a stage writes stays in the language of a loaded program:
/// no `@use` and no module-qualified name, which only linking resolves.
/// Warnings go to the pipeline's report sink, and errors abort the stage.
pub trait Transformation: Send + Sync {
    /// Returns the stage's unique name.
    fn name(&self) -> &'static str;

    /// Returns the names of the stages that must have run before this
    /// one.
    fn requires(&self) -> &'static [&'static str] {
        &[]
    }

    /// Rewrites the program through the commit.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic for a problem in the program that keeps the
    /// stage from rewriting it.
    fn apply(&self, commit: &mut Commit<'_>) -> Result<(), Diagnostic>;
}

/// A list of stages, applied in order.
///
/// A stage can be added [before](Self::insert_before) or [after](Self::insert_after)
/// any named stage.
///
/// # Example
///
/// ```
/// use nemo::{
///     diagnostics::Diagnostic,
///     pipeline::{Commit, Stages, Transformation},
/// };
///
/// /// A stage that keeps every statement.
/// struct Keep(&'static str);
///
/// impl Transformation for Keep {
///     fn name(&self) -> &'static str {
///         self.0
///     }
///
///     fn apply(&self, _commit: &mut Commit<'_>) -> Result<(), Diagnostic> {
///         Ok(())
///     }
/// }
///
/// let mut stages = Stages::new();
/// stages.push(Keep("normalize"));
/// stages.push(Keep("optimize"));
/// stages.insert_before("optimize", Keep("inline"));
/// assert_eq!(stages.names(), ["normalize", "inline", "optimize"]);
/// ```
#[derive(Default, Clone)]
pub struct Stages {
    /// The stages, in application order.
    entries: Vec<Arc<dyn Transformation>>,
}

impl std::fmt::Debug for Stages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.names()).finish()
    }
}

impl Stages {
    /// Creates an empty stage list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the stage names, in application order.
    pub fn names(&self) -> Vec<&'static str> {
        self.entries.iter().map(|entry| entry.name()).collect()
    }

    /// Appends a stage.
    pub fn push(&mut self, stage: impl Transformation + 'static) {
        self.entries.push(Arc::new(stage));
    }

    /// Inserts a stage directly before the named one.
    ///
    /// # Panics
    ///
    /// Panics if no stage has the name. [names](Self::names) lists them.
    pub fn insert_before(&mut self, name: &str, stage: impl Transformation + 'static) {
        let position = self.position(name);
        self.entries.insert(position, Arc::new(stage));
    }

    /// Inserts a stage directly after the named one.
    ///
    /// # Panics
    ///
    /// Panics if no stage has the name. [names](Self::names) lists them.
    pub fn insert_after(&mut self, name: &str, stage: impl Transformation + 'static) {
        let position = self.position(name);
        self.entries.insert(position + 1, Arc::new(stage));
    }

    /// Returns the stages, in application order.
    pub(crate) fn entries(&self) -> &[Arc<dyn Transformation>] {
        &self.entries
    }

    /// Returns the position of the named stage.
    fn position(&self, name: &str) -> usize {
        self.entries
            .iter()
            .position(|entry| entry.name() == name)
            .unwrap_or_else(|| panic!("there is no stage named `{name}`: {:?}", self.names()))
    }
}
