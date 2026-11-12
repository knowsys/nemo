//! The source texts that a pipeline registers: the texts that programs were read from,
//! and the module instances made of them.
//!
//! A source is either *kept* or *transient*.
//! A kept source stays registered.
//! A transient source was registered for one call,
//! and [sweep_sources](Pipeline::sweep_sources) drops it once nothing points into it.

use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, OnceLock},
};

use nemo_physical::diagnostics::{Diagnostic, Location};

use crate::{
    model::{FileId, Program, Span, Statement, UseDirective},
    pipeline::Pipeline,
};

/// A source text registered with a pipeline, as [Pipeline::sources] lists it.
#[derive(Debug, Clone, Copy)]
pub struct RegisteredSource<'a> {
    /// The file id of the spans that point into the text.
    pub file: FileId,
    /// The name that diagnostics show for the text, such as its path.
    pub name: &'a str,
    /// The text.
    pub text: &'a str,
}

/// The source texts registered with a pipeline.
#[derive(Debug, Default)]
pub(super) struct Sources {
    /// The registered source texts, by file id.
    files: BTreeMap<FileId, SourceFile>,
    /// The file id that the next registered source gets.
    ///
    /// It is larger than every file id given before, also to a source that was dropped since,
    /// and than the file id of every loaded statement
    /// (see [reserve_files](Pipeline::reserve_files)).
    next: u32,
}

/// One registered source text, with the name that diagnostics show for it.
#[derive(Debug)]
struct SourceFile {
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
    /// Whether the source was registered for one call,
    /// and is dropped once nothing points into it.
    transient: bool,
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

/// Returns the file id after the one with the given index.
fn after(index: u32) -> u32 {
    index.checked_add(1).expect("file counts stay below u32")
}

impl Pipeline {
    /// Registers a source text under a name, and returns the file id for its spans.
    ///
    /// Registering a name again adds a new source under a new id.
    /// Ids are never reused,
    /// so the spans of an earlier version of a file that is read again stay valid.
    /// Neither is an id assigned that [load](Self::load) reserved,
    /// for the spans of a program whose text is not registered.
    /// The source stays registered.
    pub fn add_source(&mut self, name: impl Into<String>, text: impl Into<String>) -> FileId {
        self.register(name.into(), text.into(), false)
    }

    /// Registers a source text for one call, as [add_source](Self::add_source) does,
    /// except that [sweep_sources](Self::sweep_sources) drops the source once nothing points
    /// into it.
    pub(crate) fn add_transient_source(
        &mut self,
        name: impl Into<String>,
        text: impl Into<String>,
    ) -> FileId {
        self.register(name.into(), text.into(), true)
    }

    /// Registers a source text under a name, and returns the file id for its spans.
    fn register(&mut self, name: String, text: String, transient: bool) -> FileId {
        self.push_source(SourceFile {
            name: Arc::from(name),
            text: Arc::from(text),
            lines: Arc::default(),
            instance: None,
            transient,
        })
    }

    /// Registers a module instance of the registered file `file`,
    /// made by the `@use` directive `directive`.
    ///
    /// The instance has the file's name and text, under a file id of its own.
    /// It is transient.
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
            transient: true,
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

    /// Reserves the file ids of a program's statements,
    /// so that no source registered later gets one of them.
    ///
    /// The text of a program need not be registered.
    /// The spans of its statements then point into no source of the pipeline,
    /// and a source registered under their file id would be taken for the program's text.
    pub(crate) fn reserve_files(&mut self, program: &Program) {
        for statement in &program.statements {
            if let Some(span) = statement.span() {
                self.sources.next = self.sources.next.max(after(span.file.index()));
            }
        }
    }

    /// Adds a source under the next file id, and returns the id.
    fn push_source(&mut self, source: SourceFile) -> FileId {
        let id = FileId::new(self.sources.next);
        self.sources.next = after(self.sources.next);
        self.sources.files.insert(id, source);
        id
    }

    /// Drops every transient source that nothing in the pipeline points into.
    ///
    /// A statement version in the arena points into the file of its span,
    /// and a report into the files of its labels.
    /// A module instance points into the file of the `@use` directive that made it.
    pub(crate) fn sweep_sources(&mut self) {
        let statements = self.arena.statements().filter_map(Statement::span);
        let labels = self
            .reports
            .iter()
            .flat_map(|report| report.labels().iter().map(|label| label.span));
        let mut used: HashSet<FileId> = statements.chain(labels).map(|span| span.file).collect();

        // An instance's file leads to the files of the instances around it.
        let mut pending: Vec<FileId> = used.iter().copied().collect();
        while let Some(file) = pending.pop() {
            let directive = self
                .source(file)
                .and_then(|source| source.instance.as_ref());
            if let Some(span) = directive.and_then(|directive| directive.span)
                && used.insert(span.file)
            {
                pending.push(span.file);
            }
        }

        self.sources
            .files
            .retain(|file, source| !source.transient || used.contains(file));
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

    /// Returns every registered source.
    pub fn sources(&self) -> impl Iterator<Item = RegisteredSource<'_>> {
        self.sources
            .files
            .iter()
            .map(|(&file, source)| RegisteredSource {
                file,
                name: &source.name,
                text: &source.text,
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
        self.sources.files.get(&file)
    }
}
