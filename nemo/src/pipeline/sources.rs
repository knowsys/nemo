//! The source texts a pipeline registers: the texts programs were read from,
//! and the module instances made of them.

use std::sync::{Arc, OnceLock};

use crate::{
    model::{FileId, Span, UseDirective},
    pipeline::Pipeline,
};

/// One source text a program was read from: how diagnostics name it,
/// and what its spans point into.
#[derive(Debug)]
pub(super) struct SourceFile {
    /// The name diagnostics show: a path, or the name given to [Pipeline::add_source].
    name: String,
    /// The text, shared by the instances of a module file.
    text: Arc<str>,
    /// The byte offsets at which the text's lines start, computed at first need
    /// and shared like the text.
    lines: Arc<OnceLock<Box<[usize]>>>,
    /// For a module instance, the `@use` that made it,
    /// and `None` for a text added with [Pipeline::add_source].
    instance: Option<UseDirective>,
}

impl SourceFile {
    /// Returns where a span starts in the text, as [Span::line_column] counts,
    /// finding the line in the text's line index.
    fn line_column(&self, span: Span) -> (u32, u32) {
        let text = &*self.text;
        let lines = self.lines.get_or_init(|| {
            std::iter::once(0)
                .chain(text.match_indices('\n').map(|(index, _)| index + 1))
                .collect()
        });
        let start = (span.start as usize).min(text.len());
        let start = (0..=start)
            .rev()
            .find(|&index| text.is_char_boundary(index))
            .unwrap_or(0);
        let line = lines.partition_point(|&line_start| line_start <= start);
        let column = text[lines[line - 1]..start].chars().count() + 1;
        let saturate = |count: usize| u32::try_from(count).unwrap_or(u32::MAX);
        (saturate(line), saturate(column))
    }
}

impl Pipeline {
    /// Registers a source text under a name, returning the id its spans
    /// use.
    ///
    /// Registering a name again adds a new source under a new id.
    /// Ids are never reused, so spans taken from an earlier version
    /// of a file read again stay valid.
    pub fn add_source(&mut self, name: impl Into<String>, text: impl Into<String>) -> FileId {
        let text: String = text.into();
        self.push_source(SourceFile {
            name: name.into(),
            text: Arc::from(text),
            lines: Arc::default(),
            instance: None,
        })
    }

    /// Registers an instance of a registered module file: its name and
    /// text, under an id of its own, made by the given `@use`.
    ///
    /// # Panics
    ///
    /// Panics if the file is not registered.
    pub(crate) fn add_instance(&mut self, file: FileId, directive: UseDirective) -> FileId {
        let source = self.source(file).expect("instances copy registered files");
        let (name, text, lines) = (
            source.name.clone(),
            source.text.clone(),
            source.lines.clone(),
        );
        self.push_source(SourceFile {
            name,
            text,
            lines,
            instance: Some(directive),
        })
    }

    /// Returns the chain of `@use` directives that instantiated a file as a module instance,
    /// outermost first, or none for a text added with [add_source](Self::add_source).
    pub(crate) fn instances(&self, file: FileId) -> Vec<&UseDirective> {
        let mut chain = Vec::new();
        let mut current = Some(file);
        while let Some(directive) = current
            .and_then(|file| self.source(file))
            .and_then(|source| source.instance.as_ref())
        {
            chain.push(directive);
            current = directive.span.map(|span| span.file);
        }
        chain.reverse();
        chain
    }

    /// Appends a source, returning its id.
    fn push_source(&mut self, source: SourceFile) -> FileId {
        let id = FileId::new(u32::try_from(self.files.len()).expect("file counts stay below u32"));
        self.files.push(source);
        id
    }

    /// Returns the name of a registered source file.
    pub fn file_name(&self, file: FileId) -> Option<&str> {
        self.source(file).map(|source| source.name.as_str())
    }

    /// Returns the text of a registered source file.
    pub fn text(&self, file: FileId) -> Option<&str> {
        self.source(file).map(|source| &*source.text)
    }

    /// Returns where a span starts in its registered source file, as [Span::line_column] counts,
    /// or `None` if the file is not registered.
    pub(crate) fn line_column(&self, span: Span) -> Option<(u32, u32)> {
        self.source(span.file)
            .map(|source| source.line_column(span))
    }

    /// Returns every registered source, by id, name, and text, for rendering diagnostics.
    pub fn sources(&self) -> impl Iterator<Item = (FileId, &str, &str)> {
        self.files.iter().enumerate().map(|(index, source)| {
            let id = FileId::new(u32::try_from(index).expect("file counts stay below u32"));
            (id, source.name.as_str(), &*source.text)
        })
    }

    /// Returns the source text a span covers,
    /// or `None` if its file is not registered or the span lies outside its text.
    pub(crate) fn span_text(&self, span: Span) -> Option<&str> {
        self.source(span.file)?
            .text
            .get(span.start as usize..span.end as usize)
    }

    /// Returns the registered source a file id addresses.
    fn source(&self, file: FileId) -> Option<&SourceFile> {
        self.files.get(file.index() as usize)
    }
}
