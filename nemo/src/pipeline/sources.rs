//! The source texts that a pipeline registers: the texts that programs were read from,
//! and the module instances made of them.

use std::sync::{Arc, OnceLock};

use nemo_physical::diagnostics::{Diagnostic, Location};

use crate::{
    model::{FileId, Program, Span, UseDirective},
    pipeline::Pipeline,
};

/// One registered source text, with the name that diagnostics show for it.
#[derive(Debug)]
pub(super) struct SourceFile {
    /// The name that diagnostics show, as given to [Pipeline::add_source], such as a path.
    name: Arc<str>,
    /// The text, shared by the instances of a module file.
    text: Arc<str>,
    /// The byte offsets at which the text's lines start,
    /// computed when first needed and shared like the text.
    lines: Arc<OnceLock<Box<[usize]>>>,
    /// The `@use` directive that made a module instance,
    /// or `None` for a text added with [Pipeline::add_source].
    instance: Option<UseDirective>,
}

impl SourceFile {
    /// Returns the line and column where a span starts, as [Span::line_column] counts them.
    ///
    /// The line is looked up in the text's line index.
    fn line_column(&self, span: Span) -> (u32, u32) {
        let text = &*self.text;
        let lines = self
            .lines
            .get_or_init(|| Span::line_starts(text).into_boxed_slice());
        span.line_column_in(text, lines)
    }
}

impl Pipeline {
    /// Registers a source text under a name, and returns the file id for its spans.
    ///
    /// Registering a name again adds a new source under a new id.
    /// Ids are never reused,
    /// so the spans of an earlier version of a file that is read again stay valid.
    /// A [reserved](Self::reserve_files) id is never assigned either.
    pub fn add_source(&mut self, name: impl Into<String>, text: impl Into<String>) -> FileId {
        let (name, text): (String, String) = (name.into(), text.into());
        self.push_source(SourceFile {
            name: Arc::from(name),
            text: Arc::from(text),
            lines: Arc::default(),
            instance: None,
        })
    }

    /// Registers a module instance of the registered file `file`,
    /// made by the `@use` directive `directive`.
    ///
    /// The instance has the file's name and text, under a file id of its own.
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
    /// outermost first, or an empty chain for a text added with [add_source](Self::add_source).
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

    /// Reserves the file ids of a program's statements that no source is registered under,
    /// so that no source registered later gets one of them.
    ///
    /// The text of a program need not be registered.
    /// The spans of its statements then point into no source of the pipeline,
    /// and a source registered under their file id would be taken for the program's text.
    pub(crate) fn reserve_files(&mut self, program: &Program) {
        for statement in &program.statements {
            if let Some(span) = statement.span()
                && !self.files.contains_key(&span.file)
            {
                self.reserved_files.insert(span.file);
            }
        }
    }

    /// Adds a source under the next file id that is not reserved, and returns the id.
    fn push_source(&mut self, source: SourceFile) -> FileId {
        let after = |index: u32| index.checked_add(1).expect("file counts stay below u32");
        let mut index = self
            .files
            .last_key_value()
            .map_or(0, |(last, _)| after(last.index()));
        while self.reserved_files.contains(&FileId::new(index)) {
            index = after(index);
        }

        let id = FileId::new(index);
        self.files.insert(id, source);
        id
    }

    /// Returns the name of a source file, or `None` if the file is not registered.
    pub fn file_name(&self, file: FileId) -> Option<&str> {
        self.source(file).map(|source| &*source.name)
    }

    /// Returns the text of a source file, or `None` if the file is not registered.
    pub fn text(&self, file: FileId) -> Option<&str> {
        self.source(file).map(|source| &*source.text)
    }

    /// Returns the line and column where a span starts, as [Span::line_column] counts them,
    /// or `None` if its file is not registered.
    pub(crate) fn line_column(&self, span: Span) -> Option<(u32, u32)> {
        self.source(span.file)
            .map(|source| source.line_column(span))
    }

    /// Returns the location where a span starts: the name of its file, the line and the column.
    /// Returns `None` if its file is not registered.
    pub(crate) fn location(&self, span: Span) -> Option<Location> {
        let source = self.source(span.file)?;
        let (line, column) = source.line_column(span);
        Some(Location {
            source: Some(source.name.clone()),
            line,
            column,
        })
    }

    /// Returns a diagnostic in which every label whose file is registered has its location:
    /// the name of the file, and the line and column where the label's span starts.
    pub fn locate(&self, diagnostic: Diagnostic) -> Diagnostic {
        diagnostic.with_locations(|span| self.location(span))
    }

    /// Returns every registered source as its file id, name and text.
    pub fn sources(&self) -> impl Iterator<Item = (FileId, &str, &str)> {
        self.files
            .iter()
            .map(|(&id, source)| (id, &*source.name, &*source.text))
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
        self.files.get(&file)
    }
}
