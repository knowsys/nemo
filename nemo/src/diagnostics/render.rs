//! Rendering of diagnostics for people.
//!
//! Diagnostics are structured ([Diagnostic]: code, severity, labelled spans, named fields,
//! notes, hints).
//! This module renders them as text, for a terminal or for exception messages.
//! Tools with their own display, such as a language server, use the structured form directly.
//!
//! Rendering needs the source *texts* spans point into.
//! [SourceTexts] holds copies of the texts an engine registered,
//! so that any span the engine produced can be quoted.

mod sources;

pub use sources::SourceTexts;

use std::io::IsTerminal;

use ariadne::{Color, Config, IndexType, Report, ReportKind};

use nemo_physical::diagnostics::{Diagnostic, FileId, Severity, Span};

use crate::diagnostics::{AboutPredicate, AboutResource};

/// One label of a [ResolvedDiagnostic]: its message, and its source position if the source is known.
#[derive(Debug, Clone)]
pub struct ResolvedLabel {
    /// The label's message.
    pub message: String,
    /// The name of the source the span points into, if known.
    pub source: Option<String>,
    /// The 1-based line, if the source is known.
    pub line: Option<u32>,
    /// The 1-based column in characters, if the source is known.
    pub column: Option<u32>,
}

/// One diagnostic resolved against a run's sources, as plain data.
///
/// Each span is resolved to a source name, line and column.
/// The plain-text rendering is included.
#[derive(Debug, Clone)]
pub struct ResolvedDiagnostic {
    /// The stable code, like `"N0204"`.
    pub code: String,
    /// The severity.
    pub severity: Severity,
    /// The headline message.
    pub message: String,
    /// The labels, resolved.
    pub labels: Vec<ResolvedLabel>,
    /// Additional notes, led by the resource and the predicate the message does not name,
    /// as the rendering shows them.
    pub notes: Vec<String>,
    /// Hints toward a fix.
    pub hints: Vec<String>,
    /// The full plain rendering, snippets included.
    pub rendered: String,
}

impl ResolvedDiagnostic {
    /// Resolves a diagnostic against the sources of its run.
    pub fn new(diagnostic: &Diagnostic, sources: &SourceTexts) -> Self {
        let labels = diagnostic
            .labels()
            .iter()
            .map(|label| {
                let text = sources.text(label.span.file);
                let placed = sources.name(label.span.file).map(|name| {
                    let (line, column) = label.span.line_column(text);
                    (name.to_owned(), line, column)
                });
                ResolvedLabel {
                    message: label.message.clone(),
                    source: placed.as_ref().map(|(name, _, _)| name.clone()),
                    line: placed.as_ref().map(|&(_, line, _)| line),
                    column: placed.as_ref().map(|&(_, _, column)| column),
                }
            })
            .collect();
        let mut notes = subject_notes(diagnostic);
        notes.extend(diagnostic.notes().iter().cloned());
        Self {
            code: diagnostic.code().to_string(),
            severity: diagnostic.severity(),
            message: diagnostic.message().to_owned(),
            labels,
            notes,
            hints: diagnostic.hints().to_vec(),
            rendered: Renderer::plain(sources).show(diagnostic),
        }
    }
}

/// Returns the notes naming what a diagnostic is about, its resource and its predicate,
/// each one only if the message does not name it already.
fn subject_notes(diagnostic: &Diagnostic) -> Vec<String> {
    let message = diagnostic.message();
    let mut notes = Vec::new();
    if let Some(resource) = diagnostic.resource()
        && !message.contains(&format!("`{resource}`"))
    {
        notes.push(format!("in `{resource}`"));
    }
    if let Some(predicate) = diagnostic.predicate()
        && !message.contains(&format!("`{predicate}`"))
    {
        notes.push(format!("predicate `{predicate}`"));
    }
    notes
}

/// The escape sequences a stream accepts.
///
/// They are all empty if it accepts none, so the same code writes coloured and plain output.
#[derive(Debug, Clone, Copy)]
pub struct TerminalStyle {
    /// Whether escapes are written at all, which the snippet renderer is told as well.
    pub coloured: bool,
    /// The colour of errors.
    pub error: &'static str,
    /// The colour of warnings.
    pub warning: &'static str,
    /// The colour of context lines.
    pub accent: &'static str,
    /// The emphasis of a diagnostic's message.
    pub bold: &'static str,
    /// The emphasis of the summary's headline numbers.
    pub strong: &'static str,
    /// Returns to the stream's own appearance.
    pub reset: &'static str,
}

impl TerminalStyle {
    /// The colours, for a terminal that shows them.
    ///
    /// The accent is the snippet renderer's own note colour.
    const COLOURED: Self = Self {
        coloured: true,
        error: "\x1b[1;31m",
        warning: "\x1b[1;33m",
        accent: "\x1b[38;5;115m",
        bold: "\x1b[1m",
        strong: "\x1b[1;32m",
        reset: "\x1b[0m",
    };

    /// No escapes at all.
    const PLAIN: Self = Self {
        coloured: false,
        error: "",
        warning: "",
        accent: "",
        bold: "",
        strong: "",
        reset: "",
    };

    /// Returns the style the error stream takes: colours unless it is
    /// redirected, or `NO_COLOR` asks for none.
    pub fn detect() -> Self {
        if std::io::stderr().is_terminal() && std::env::var_os("NO_COLOR").is_none() {
            Self::COLOURED
        } else {
            Self::PLAIN
        }
    }

    /// Returns the colour a severity shows in.
    fn severity(self, severity: Severity) -> &'static str {
        match severity {
            Severity::Error => self.error,
            Severity::Warning => self.warning,
        }
    }
}

/// Renders diagnostics for a terminal (or any text sink).
///
/// Labelled spans are quoted from their sources as an underlined snippet,
/// and a diagnostic with nothing to quote becomes a compact block of the same shape.
///
/// # Example
///
/// ```
/// use nemo::{
///     diagnostics::render::{Renderer, SourceTexts},
///     model::{FileId, Program},
/// };
///
/// let text = "p(1) .\nq(?x) :- p(?x .";
/// let (_, diagnostics) = Program::parse(text, FileId::new(0));
/// let sources = SourceTexts::detached("example.rls", text);
/// let shown = Renderer::plain(&sources).show(&diagnostics[0]);
/// assert!(shown.contains("example.rls:2"));
/// assert!(shown.contains("q(?x) :- p(?x ."));
/// ```
#[derive(Debug)]
pub struct Renderer<'a> {
    /// The texts spans resolve against.
    sources: &'a SourceTexts,
    /// The escapes the target stream accepts.
    style: TerminalStyle,
}

impl<'a> Renderer<'a> {
    /// Creates a renderer for the error stream, coloured if it is a terminal.
    pub fn new(sources: &'a SourceTexts) -> Self {
        Self {
            sources,
            style: TerminalStyle::detect(),
        }
    }

    /// Creates a renderer that emits no escape sequences.
    ///
    /// It suits output that is not a terminal (exception text, logs).
    pub fn plain(sources: &'a SourceTexts) -> Self {
        Self {
            sources,
            style: TerminalStyle::PLAIN,
        }
    }

    /// Renders a diagnostic to a newline-terminated block.
    pub fn show(&self, diagnostic: &Diagnostic) -> String {
        // A label whose span points into a source this run did not read
        // keeps its message: dropping it would lose what it says.
        let mut located = Vec::new();
        let mut context: Vec<String> = Vec::new();
        for label in diagnostic.labels() {
            if self.sources.name(label.span.file).is_some() {
                located.push(label);
            } else {
                context.push(format!("{} (source not available)", label.message));
            }
        }

        context.extend(subject_notes(diagnostic));
        context.extend(diagnostic.notes().iter().cloned());

        if located.is_empty() {
            return self.block(diagnostic, &context);
        }
        self.snippet(diagnostic, &located, &context)
            .unwrap_or_else(|| self.block(diagnostic, &context))
    }

    /// Returns the full rendering: the source lines the located labels point at,
    /// quoted and underlined.
    ///
    /// Returns `None` only if the snippet writer fails,
    /// which no diagnostic is expected to make it do.
    fn snippet(
        &self,
        diagnostic: &Diagnostic,
        labels: &[&nemo_physical::diagnostics::Label],
        context: &[String],
    ) -> Option<String> {
        let kind = match diagnostic.severity() {
            Severity::Error => ReportKind::Error,
            Severity::Warning => ReportKind::Warning,
        };
        // Spans are byte ranges. The writer applies or drops the colours
        // itself, so plain output is a configuration flag, not a separate
        // code path.
        let config = Config::new()
            .with_index_type(IndexType::Byte)
            .with_color(self.style.coloured);

        let mut report = Report::build(kind, self.place(labels[0].span))
            .with_config(config)
            .with_code(diagnostic.code())
            .with_message(diagnostic.message());
        // The writer starts a new group, headed by the problem's location, whenever a label
        // lies above the one before it. So the labels go in source order: by file, in the
        // order the files first appear, and by position within a file.
        let first_in = |file: FileId| labels.iter().position(|label| label.span.file == file);
        let mut order: Vec<usize> = (0..labels.len()).collect();
        order.sort_by_key(|&index| {
            let span = labels[index].span;
            (first_in(span.file), span.start)
        });
        for index in order {
            let label = labels[index];
            // The first label marks the problem; the others are the
            // context it is seen against.
            let colour = match (index, diagnostic.severity()) {
                (0, Severity::Error) => Color::Red,
                (0, Severity::Warning) => Color::Yellow,
                _ => Color::BrightBlue,
            };
            let mut placed = ariadne::Label::new(self.place(label.span)).with_color(colour);
            if !label.message.is_empty() {
                placed = placed.with_message(&label.message);
            }
            report = report.with_label(placed);
        }
        for line in context {
            report = report.with_note(line);
        }
        for hint in diagnostic.hints() {
            report = report.with_help(hint);
        }

        // The cache quotes each named source once, however many labels
        // point into it.
        let mut quoted: Vec<(String, &str)> = labels
            .iter()
            .filter_map(|label| {
                let file = label.span.file;
                Some((self.sources.name(file)?.to_owned(), self.sources.text(file)))
            })
            .collect();
        quoted.sort_unstable();
        quoted.dedup();

        let mut rendered = Vec::new();
        report
            .finish()
            .write(ariadne::sources(quoted), &mut rendered)
            .ok()?;
        String::from_utf8(rendered).ok()
    }

    /// Returns a span as the snippet writer addresses it: the source's name and
    /// a byte range clamped into its text.
    fn place(&self, span: Span) -> (String, std::ops::Range<usize>) {
        let text = self.sources.text(span.file);
        let name = self.sources.name(span.file).unwrap_or_default().to_owned();
        let start = usize::try_from(span.start)
            .unwrap_or(usize::MAX)
            .min(text.len());
        let end = usize::try_from(span.end)
            .unwrap_or(usize::MAX)
            .clamp(start, text.len());
        (name, start..end)
    }

    /// Returns the compact block for a diagnostic with nothing to quote: the
    /// header the snippet renderer would write, then its context.
    fn block(&self, diagnostic: &Diagnostic, context: &[String]) -> String {
        let TerminalStyle {
            accent,
            bold,
            reset,
            ..
        } = self.style;
        let colour = self.style.severity(diagnostic.severity());
        let kind = match diagnostic.severity() {
            Severity::Error => "Error",
            Severity::Warning => "Warning",
        };
        let mut block = format!(
            "{colour}[{}] {kind}:{reset} {bold}{}{reset}\n",
            diagnostic.code(),
            diagnostic.message()
        );
        for line in context {
            block.push_str(&format!("{accent}Note:{reset} {line}\n"));
        }
        for hint in diagnostic.hints() {
            block.push_str(&format!("{accent}Help:{reset} {hint}\n"));
        }
        block
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nemo_physical::diagnostics::{Code, FileId};

    /// The plain rendering of a diagnostic over one two-line source.
    fn render(diagnostic: &Diagnostic) -> String {
        let sources = SourceTexts::detached("test.rls", "p(1, 2) .\nq(?x) :- p(?x, 3) .\n");
        Renderer::plain(&sources).show(diagnostic)
    }

    #[test]
    fn a_label_points_at_its_source_line() {
        // Bytes 19..27 are the body atom `p(?x, 3)` of the second line.
        let span = Span {
            file: FileId::new(0),
            start: 19,
            end: 27,
        };
        let rendered = render(
            &Diagnostic::error(Code::new(204), "the predicate `p` is unknown")
                .with_label(span, "not declared anywhere"),
        );
        assert!(
            rendered.starts_with("[N0204] Error: the predicate `p` is unknown"),
            "the header names the code and the message:\n{rendered}"
        );
        assert!(
            rendered.contains("test.rls:2:10"),
            "the snippet points at the span:\n{rendered}"
        );
        assert!(
            rendered.contains("q(?x) :- p(?x, 3) ."),
            "the snippet quotes the line:\n{rendered}"
        );
        assert!(
            rendered.contains("not declared anywhere"),
            "the snippet carries the label:\n{rendered}"
        );
    }

    #[test]
    fn labels_of_one_source_share_one_snippet_whatever_their_order() {
        // The problem is on the second line, its context on the first.
        let span = |start, end| Span {
            file: FileId::new(0),
            start,
            end,
        };
        let rendered = render(
            &Diagnostic::error(Code::new(401), "alias `p` is used by two `@use` directives")
                .with_label(span(10, 29), "here")
                .with_label(span(0, 9), "first used here"),
        );
        assert!(
            !rendered.contains("├─["),
            "one source, one snippet:\n{rendered}"
        );
        assert!(
            rendered.contains("first used here") && rendered.contains("here"),
            "both labels are shown:\n{rendered}"
        );
    }

    #[test]
    fn a_label_the_run_cannot_place_keeps_its_message() {
        // File 9 was never registered, so the label has no line to point
        // at, but its message is still shown.
        let span = Span {
            file: FileId::new(9),
            start: 0,
            end: 1,
        };
        let rendered = render(
            &Diagnostic::error(Code::new(204), "something is wrong")
                .with_label(span, "the offending name"),
        );
        let expected = [
            "[N0204] Error: something is wrong",
            "Note: the offending name (source not available)",
            "",
        ]
        .join("\n");
        assert_eq!(rendered, expected);
    }

    #[test]
    fn a_diagnostic_without_labels_still_shows_its_context() {
        let rendered = render(
            &Diagnostic::warning(Code::new(101), "3 rows were dropped")
                .with_resource("data.csv")
                .with_hint("check the column formats"),
        );
        let expected = [
            "[N0101] Warning: 3 rows were dropped",
            "Note: in `data.csv`",
            "Help: check the column formats",
            "",
        ]
        .join("\n");
        assert_eq!(rendered, expected);
    }

    #[test]
    fn a_message_naming_its_resource_has_no_note_repeating_it() {
        let rendered = render(
            &Diagnostic::error(Code::new(101), "could not read `data.csv`: not found")
                .with_resource("data.csv")
                .with_field("predicate", "p"),
        );
        let expected = [
            "[N0101] Error: could not read `data.csv`: not found",
            "Note: predicate `p`",
            "",
        ]
        .join("\n");
        assert_eq!(rendered, expected);
    }
}
