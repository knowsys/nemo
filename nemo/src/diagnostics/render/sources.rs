//! The source texts a run was read from.
//!
//! The engine assigns the [FileId] a text is addressed by,
//! since linking also reads the module files a program pulls in through `@use`.
//! This registry holds copies of the engine's texts,
//! so that rendering can quote any source a diagnostic points into.

use std::collections::HashMap;

use nemo_physical::diagnostics::FileId;

/// One source text: how reports name it, and its content.
#[derive(Debug)]
struct SourceText {
    /// The name diagnostics show:
    /// a path, or a description made up for text the consumer itself supplied.
    name: String,
    /// The text spans point into.
    text: String,
}

/// The source texts of one run, addressed by the [FileId] that spans carry:
/// what rendering resolves a diagnostic's byte ranges against.
///
/// # Example
///
/// ```
/// use nemo::{
///     diagnostics::render::{Renderer, SourceTexts},
///     engine::Engine,
/// };
///
/// let mut engine = Engine::new();
/// let reports = engine.check_source("main.rls", "p(1) . q(?x) :- r(?x) .");
///
/// let mut sources = SourceTexts::new();
/// sources.absorb(engine.sources());
/// let shown: Vec<String> = reports
///     .diagnostics()
///     .iter()
///     .map(|report| Renderer::plain(&sources).show(report))
///     .collect();
/// assert!(shown.iter().all(|block| block.contains("main.rls")));
/// ```
#[derive(Debug, Default)]
pub struct SourceTexts {
    /// The sources, by the id the engine registered them under.
    files: HashMap<u32, SourceText>,
}

impl SourceTexts {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a registry over one text not registered with any engine, under file id 0.
    ///
    /// Parse diagnostics render against it before an engine exists,
    /// since a bare [Program::parse](crate::model::Program) numbers its spans from file 0.
    pub fn detached(name: impl Into<String>, text: impl Into<String>) -> Self {
        let mut sources = Self::new();
        sources.files.insert(
            0,
            SourceText {
                name: name.into(),
                text: text.into(),
            },
        );
        sources
    }

    /// Adds every given source this registry does not hold yet.
    ///
    /// The sources are typically the engine's [sources](crate::engine::Engine::sources):
    /// the texts registered with it, the module files linking read,
    /// and one source per module instance (each `@use` of a module).
    pub fn absorb<'a>(&mut self, sources: impl IntoIterator<Item = (FileId, &'a str, &'a str)>) {
        for (id, name, text) in sources {
            self.files.entry(id.index()).or_insert_with(|| SourceText {
                name: name.to_owned(),
                text: text.to_owned(),
            });
        }
    }

    /// Returns the text registered for a file.
    pub fn text(&self, file: FileId) -> &str {
        self.source(file).map_or("", |source| source.text.as_str())
    }

    /// Returns the display name registered for a file,
    /// or `None` for a source this run cannot show.
    pub fn name(&self, file: FileId) -> Option<&str> {
        self.source(file).map(|source| source.name.as_str())
    }

    /// Returns the source a file id addresses.
    fn source(&self, file: FileId) -> Option<&SourceText> {
        self.files.get(&file.index())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absorbed_sources_are_kept_once() {
        let mut sources = SourceTexts::new();
        let file = FileId::new(3);
        assert!(sources.name(file).is_none());

        sources.absorb([(file, "module.rls", "p(1) .\n")]);
        sources.absorb([(file, "other.rls", "q(1) .\n")]);
        assert_eq!(sources.name(file), Some("module.rls"));
        assert_eq!(sources.text(file), "p(1) .\n");
    }

    #[test]
    fn an_unregistered_file_resolves_to_nothing() {
        let sources = SourceTexts::new();
        assert!(sources.name(FileId::new(7)).is_none());
        assert_eq!(sources.text(FileId::new(7)), "");
    }
}
