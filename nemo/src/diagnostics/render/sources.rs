//! The source texts that rendering quotes.
//!
//! A span addresses its text by a [FileId].
//! The engine assigns the ids, since linking also reads the module files of `@use` directives.
//! A [SourceLookup] finds the text and the name of a file.
//! An [Engine](crate::engine::Engine) looks them up among the sources registered with it,
//! and [SourceTexts] keeps copies of texts apart from an engine.

use std::{collections::HashMap, fmt::Debug};

use nemo_physical::diagnostics::FileId;

/// The source texts that rendering quotes, addressed by the [FileId] of a span.
pub trait SourceLookup: Debug {
    /// Returns the text of a file, or an empty text for an unknown file.
    fn text(&self, file: FileId) -> &str;

    /// Returns the name that rendered diagnostics show for a file,
    /// or `None` for an unknown file.
    fn name(&self, file: FileId) -> Option<&str>;
}

/// One source text, with the name that rendered diagnostics show for it.
#[derive(Debug)]
struct SourceText {
    /// The name shown for the text, such as its path.
    name: String,
    /// The text itself.
    text: String,
}

/// A registry of source texts, addressed by the [FileId] of a span.
///
/// Rendering resolves a diagnostic's byte ranges against it.
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
    /// The sources, by the index of their file id.
    files: HashMap<u32, SourceText>,
}

impl SourceTexts {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a registry of one text under file id 0, without an engine.
    ///
    /// It suits the diagnostics of a text parsed outside an engine,
    /// since [Program::parse](crate::model::Program::parse) and the other `parse` functions
    /// give their spans the file id 0.
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

    /// Adds each of `sources` whose file id the registry does not contain yet.
    ///
    /// Each source is given by its file id, name and text,
    /// as [Engine::sources](crate::engine::Engine::sources) returns them.
    pub fn absorb<'a>(&mut self, sources: impl IntoIterator<Item = (FileId, &'a str, &'a str)>) {
        for (id, name, text) in sources {
            self.files.entry(id.index()).or_insert_with(|| SourceText {
                name: name.to_owned(),
                text: text.to_owned(),
            });
        }
    }

    /// Returns the text registered for a file, or an empty text for an unknown file.
    pub fn text(&self, file: FileId) -> &str {
        self.source(file).map_or("", |source| source.text.as_str())
    }

    /// Returns the name registered for a file, or `None` for an unknown file.
    pub fn name(&self, file: FileId) -> Option<&str> {
        self.source(file).map(|source| source.name.as_str())
    }

    /// Returns the source a file id addresses.
    fn source(&self, file: FileId) -> Option<&SourceText> {
        self.files.get(&file.index())
    }
}

impl SourceLookup for SourceTexts {
    fn text(&self, file: FileId) -> &str {
        SourceTexts::text(self, file)
    }

    fn name(&self, file: FileId) -> Option<&str> {
        SourceTexts::name(self, file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file absorbed twice keeps the name and text that it was first absorbed with.
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

    /// An unknown file has no name and an empty text.
    #[test]
    fn an_unregistered_file_resolves_to_nothing() {
        let sources = SourceTexts::new();
        assert!(sources.name(FileId::new(7)).is_none());
        assert_eq!(sources.text(FileId::new(7)), "");
    }
}
