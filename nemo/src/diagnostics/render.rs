//! Rendering of diagnostics as text, for a terminal or for exception messages.
//!
//! A [Diagnostic] is structured data: a code, a severity, labelled spans, named fields,
//! notes and hints.
//! Rendering quotes the source lines that the spans point into, so it needs the source texts.
//! It finds them through a [SourceLookup]:
//! an [Engine](crate::engine::Engine) with the sources registered with it,
//! or a [SourceTexts], which keeps copies of texts.

mod sources;

pub use sources::{SourceLookup, SourceTexts};

use std::io::IsTerminal;

use ariadne::{Color, Config, IndexType, Report, ReportKind};

use nemo_physical::diagnostics::{Diagnostic, FileId, Severity, Span};

use crate::diagnostics::{AboutPredicate, AboutResource};

/// One label of a [ResolvedDiagnostic], with its message and, if the source is known,
/// its position.
#[derive(Debug, Clone)]
pub struct ResolvedLabel {
    /// The label's message.
    pub message: String,
    /// The name of the source that the label's span points into, if the source is known.
    pub source: Option<String>,
    /// The 1-based line, if the source is known.
    pub line: Option<u32>,
    /// The 1-based column in characters, if the source is known.
    pub column: Option<u32>,
}

/// A diagnostic resolved against its source texts, as plain data.
///
/// Each span is resolved to a source name, a line and a column,
/// and the plain-text rendering is included.
#[derive(Debug, Clone)]
pub struct ResolvedDiagnostic {
    /// The code, such as `"N0204"`.
    pub code: String,
    /// The severity.
    pub severity: Severity,
    /// The main message.
    pub message: String,
    /// The resolved labels.
    pub labels: Vec<ResolvedLabel>,
    /// The notes, in the order that the rendering shows them.
    ///
    /// They begin with a note naming the diagnostic's resource and one naming its predicate,
    /// each only if the message does not name it already.
    pub notes: Vec<String>,
    /// Hints toward a fix.
    pub hints: Vec<String>,
    /// The plain-text rendering, with the quoted source lines.
    pub rendered: String,
}

impl ResolvedDiagnostic {
    /// Resolves a diagnostic against the source texts in `sources`.
    pub fn new(diagnostic: &Diagnostic, sources: &dyn SourceLookup) -> Self {
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

/// Returns the notes that name a diagnostic's resource and its predicate.
///
/// A note is left out if the message already names its resource or predicate in backticks.
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

/// The escape sequences for colour and emphasis that an output stream accepts.
///
/// They are all empty for a stream that accepts none,
/// so that the same code writes coloured and plain output.
#[derive(Debug, Clone, Copy)]
pub struct TerminalStyle {
    /// Whether any escapes are written.
    ///
    /// The snippet writer applies its own colours, so it is given this flag as well.
    pub coloured: bool,
    /// The colour of errors.
    pub error: &'static str,
    /// The colour of warnings.
    pub warning: &'static str,
    /// The colour of secondary text, such as the `Note:` and `Help:` prefixes.
    pub accent: &'static str,
    /// The emphasis of a diagnostic's message.
    pub bold: &'static str,
    /// A stronger emphasis than [bold](Self::bold), in bold green.
    pub strong: &'static str,
    /// The sequence that restores the stream's default appearance.
    pub reset: &'static str,
}

impl TerminalStyle {
    /// The style for a terminal that shows colours.
    ///
    /// The accent is the colour that the snippet writer gives notes.
    const COLOURED: Self = Self {
        coloured: true,
        error: "\x1b[1;31m",
        warning: "\x1b[1;33m",
        accent: "\x1b[38;5;115m",
        bold: "\x1b[1m",
        strong: "\x1b[1;32m",
        reset: "\x1b[0m",
    };

    /// The style without any escapes.
    const PLAIN: Self = Self {
        coloured: false,
        error: "",
        warning: "",
        accent: "",
        bold: "",
        strong: "",
        reset: "",
    };

    /// Returns the style for the error stream.
    ///
    /// The style is coloured if the stream is a terminal
    /// and the environment variable `NO_COLOR` is not set.
    pub fn detect() -> Self {
        if std::io::stderr().is_terminal() && std::env::var_os("NO_COLOR").is_none() {
            Self::COLOURED
        } else {
            Self::PLAIN
        }
    }

    /// Returns the colour of a severity.
    fn severity(self, severity: Severity) -> &'static str {
        match severity {
            Severity::Error => self.error,
            Severity::Warning => self.warning,
        }
    }
}

/// The text renderer of diagnostics, for a terminal or any other text output.
///
/// The source lines of the labelled spans are quoted as a snippet, with the spans underlined.
/// A diagnostic with nothing to quote becomes a compact block with the same header, notes and
/// hints.
///
/// # Example
///
/// ```
/// use nemo::{diagnostics::render::{Renderer, SourceTexts}, model::Program};
///
/// let text = "p(1) .\nq(?x) :- p(?x .";
/// let (_, diagnostics) = Program::parse(text);
/// let sources = SourceTexts::detached("example.rls", text);
/// let shown = Renderer::plain(&sources).show(&diagnostics[0]);
/// assert!(shown.contains("example.rls:2"));
/// assert!(shown.contains("q(?x) :- p(?x ."));
/// ```
#[derive(Debug)]
pub struct Renderer<'a> {
    /// The source texts that spans are resolved against.
    sources: &'a dyn SourceLookup,
    /// The escape sequences that the output accepts.
    style: TerminalStyle,
}

impl<'a> Renderer<'a> {
    /// Creates a renderer for the error stream, in the style that [TerminalStyle::detect]
    /// returns.
    pub fn new(sources: &'a dyn SourceLookup) -> Self {
        Self {
            sources,
            style: TerminalStyle::detect(),
        }
    }

    /// Creates a renderer that writes no escape sequences.
    ///
    /// It suits output that is not a terminal, such as exception messages or logs.
    pub fn plain(sources: &'a dyn SourceLookup) -> Self {
        Self {
            sources,
            style: TerminalStyle::PLAIN,
        }
    }

    /// Renders a diagnostic as text that ends with a newline.
    pub fn show(&self, diagnostic: &Diagnostic) -> String {
        // A label whose span points into an unknown source becomes a note,
        // so that its message is not lost.
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

    /// Renders a diagnostic with the source lines that `labels` point at, quoted and underlined.
    ///
    /// `labels` are the diagnostic's labels whose sources are known,
    /// and `context` contains the lines shown as notes.
    /// Returns `None` only if the snippet writer fails, which no diagnostic is expected to cause.
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
        // Spans are byte ranges. The writer applies or omits the colours itself,
        // so plain output needs only a configuration flag.
        let config = Config::new()
            .with_index_type(IndexType::Byte)
            .with_color(self.style.coloured);

        let mut report = Report::build(kind, self.place(labels[0].span))
            .with_config(config)
            .with_code(diagnostic.code())
            .with_message(diagnostic.message());
        // The writer starts a new group, headed by the problem's location, whenever a label
        // lies above the one before it. So the labels are added in source order: by file, in
        // the order in which the files first appear among the labels, and by position within
        // a file.
        let first_in = |file: FileId| labels.iter().position(|label| label.span.file == file);
        let mut order: Vec<usize> = (0..labels.len()).collect();
        order.sort_by_key(|&index| {
            let span = labels[index].span;
            (first_in(span.file), span.start)
        });
        for index in order {
            let label = labels[index];
            // The first label marks the problem, and the others show its context.
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

        // The writer's cache gets each named source once, however many labels point into it.
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

    /// Returns a span in the form that the snippet writer accepts,
    /// as the source's name and a byte range clamped to its text.
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

    /// Returns the compact rendering of a diagnostic without quoted source lines.
    ///
    /// It has the header that the snippet writer would write,
    /// then the lines of `context` as notes, then the hints.
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

    /// Returns the plain rendering of a diagnostic against one source of two lines.
    fn render(diagnostic: &Diagnostic) -> String {
        let sources = SourceTexts::detached("test.rls", "p(1, 2) .\nq(?x) :- p(?x, 3) .\n");
        Renderer::plain(&sources).show(diagnostic)
    }

    /// A label's snippet names the source, line and column of its span, and quotes the line.
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

    /// The labels of one source share one snippet, even if a later label lies above an earlier
    /// one.
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

    /// A label in an unknown source is shown as a note with its message.
    #[test]
    fn a_label_the_run_cannot_place_keeps_its_message() {
        // File 9 was never registered, so the label has no line to point at.
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

    /// A diagnostic without labels is rendered as a compact block with its notes and hints.
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

    /// A message that names its resource gets no note for the resource,
    /// but a note for a predicate that it does not name.
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
