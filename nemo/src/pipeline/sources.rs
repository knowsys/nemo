//! The source texts a pipeline registers: the texts programs were read from,
//! and the module instances made of them.

use std::sync::Arc;

use crate::{
    model::{FileId, UseDirective},
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
    /// For a module instance, the `@use` that made it,
    /// and `None` for a text added with [Pipeline::add_source].
    instance: Option<UseDirective>,
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
        let (name, text) = (source.name.clone(), source.text.clone());
        self.push_source(SourceFile {
            name,
            text,
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

    /// Returns every registered source, by id, name, and text, for rendering diagnostics.
    pub fn sources(&self) -> impl Iterator<Item = (FileId, &str, &str)> {
        self.files.iter().enumerate().map(|(index, source)| {
            let id = FileId::new(u32::try_from(index).expect("file counts stay below u32"));
            (id, source.name.as_str(), &*source.text)
        })
    }

    /// Returns the registered source a file id addresses.
    fn source(&self, file: FileId) -> Option<&SourceFile> {
        self.files.get(file.index() as usize)
    }
}
