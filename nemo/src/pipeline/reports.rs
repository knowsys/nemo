//! The pipeline's reports, which are warnings and non-fatal errors,
//! and the labels added to diagnostics about statement versions.
//!
//! Every label of a report has its location if its file is registered,
//! since the functions that add a report or a label [locate](Pipeline::locate) it.
//!
//! A call that wants its own reports apart from the earlier ones takes a [ReportMark] when
//! it begins. The reports *since* the mark are then the call's own.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::Span,
    pipeline::{Pipeline, RewriteDeclaration, StatementId},
};

/// A position in a pipeline's reports, after the reports made when it was taken.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReportMark(usize);

impl Pipeline {
    /// Adds a diagnostic to the reports.
    pub fn report(&mut self, diagnostic: Diagnostic) {
        let diagnostic = self.locate(diagnostic);
        self.reports.push(diagnostic);
    }

    /// Returns the position after the reports made so far.
    pub(crate) fn mark_reports(&self) -> ReportMark {
        ReportMark(self.reports.len())
    }

    /// Takes the reports made since `mark` out of the reports, and returns them.
    pub(crate) fn take_reports(&mut self, mark: ReportMark) -> Vec<Diagnostic> {
        self.reports.split_off(mark.0)
    }

    /// Returns a diagnostic about a statement version, labeled with the `@use` directives that
    /// instantiated the statement if it belongs to a module instance.
    ///
    /// The labels distinguish the same problem in two instances of a module.
    pub(crate) fn label_instances(
        &self,
        diagnostic: Diagnostic,
        statement: StatementId,
    ) -> Diagnostic {
        self.instance_labels(statement)
            .into_iter()
            .fold(diagnostic, |diagnostic, (span, label)| {
                diagnostic.with_label(span, label)
            })
    }

    /// Labels the reports made since `mark` with the `@use` directives that
    /// instantiated a statement version, if it belongs to a module instance.
    pub(crate) fn label_instances_in_reports(&mut self, mark: ReportMark, statement: StatementId) {
        let labels = self.instance_labels(statement);
        if labels.is_empty() {
            return;
        }

        for report in self.take_reports(mark) {
            let report = labels.iter().fold(report, |report, (span, label)| {
                report.with_label(*span, label.clone())
            });
            let report = self.locate(report);
            self.reports.push(report);
        }
    }

    /// Replaces each report made since `mark` by what `present` makes of it,
    /// and drops the repeats among the results.
    ///
    /// Several statements of the compiled program can come from one statement
    /// of the written program, so a check over the statements can report
    /// one problem once per copy.
    pub(crate) fn present_reports(
        &mut self,
        mark: ReportMark,
        mut present: impl FnMut(Diagnostic) -> Diagnostic,
    ) {
        for report in self.take_reports(mark) {
            let report = present(report);
            if !self.reports[mark.0..].contains(&report) {
                self.reports.push(report);
            }
        }
    }

    /// Replaces each report made since `mark` by what `present` makes of it, and keeps repeats.
    ///
    /// Reasoning can report the same problem more than once.
    pub(crate) fn map_reports(
        &mut self,
        mark: ReportMark,
        present: impl FnMut(Diagnostic) -> Diagnostic,
    ) {
        let presented = self.take_reports(mark).into_iter().map(present);
        self.reports.extend(presented);
    }

    /// Returns the spans of the `@use` directives that instantiated a statement version,
    /// outermost first, each with a label naming its instance.
    ///
    /// Returns no spans for a statement of the main program.
    fn instance_labels(&self, statement: StatementId) -> Vec<(Span, String)> {
        let Some(span) = self.source_span(statement) else {
            return Vec::new();
        };

        self.instances(span.file)
            .into_iter()
            .filter_map(|directive| {
                Some((
                    directive.span?,
                    format!("in module instance `{}`", directive.alias),
                ))
            })
            .collect()
    }

    /// Returns the reports collected so far.
    pub fn reports(&self) -> &[Diagnostic] {
        &self.reports
    }

    /// Adds labels to a diagnostic about a statement version,
    /// from the substitutions recorded along its provenance.
    ///
    /// For every substituted occurrence that encloses one of the diagnostic's labels,
    /// it adds a label at the definition of the substituted value,
    /// which says that the variable is defined there.
    pub fn refine(&self, mut diagnostic: Diagnostic, statement: StatementId) -> Diagnostic {
        let labels: Vec<Span> = diagnostic.labels().iter().map(|label| label.span).collect();

        for id in self.ancestry(statement) {
            let Some((RewriteDeclaration::Replaces { substituted, .. }, _)) = self.declaration(id)
            else {
                continue;
            };

            for record in substituted {
                let relevant = record.occurrence.is_some_and(|occurrence| {
                    labels.iter().any(|label| {
                        label.file == occurrence.file
                            && occurrence.start <= label.start
                            && label.end <= occurrence.end
                    })
                });
                if relevant && let Some(definition) = record.definition {
                    diagnostic = diagnostic
                        .with_label(definition, format!("`{}` is defined here", record.variable));
                }
            }
        }

        diagnostic
    }
}
