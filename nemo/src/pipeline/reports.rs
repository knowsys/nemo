//! The pipeline's reports, which are warnings and non-fatal errors,
//! and the labels added to diagnostics about statement versions.
//!
//! Every label of a report has its location if its file is registered,
//! since the functions that add a report or a label [locate](Pipeline::locate) it.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::Span,
    pipeline::{Pipeline, RewriteDeclaration, StatementId},
};

impl Pipeline {
    /// Adds a diagnostic to the reports.
    pub fn report(&mut self, diagnostic: Diagnostic) {
        let diagnostic = self.locate(diagnostic);
        self.reports.push(diagnostic);
    }

    /// Drops the reports from index `from` on.
    pub(crate) fn withdraw_reports(&mut self, from: usize) {
        self.reports.truncate(from);
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

    /// Labels the reports from index `from` on with the `@use` directives that
    /// instantiated a statement version, if it belongs to a module instance.
    pub(crate) fn label_instances_in_reports(&mut self, from: usize, statement: StatementId) {
        let labels = self.instance_labels(statement);
        if labels.is_empty() {
            return;
        }
        let fresh: Vec<Diagnostic> = self.reports.drain(from..).collect();
        for report in fresh {
            let report = labels.iter().fold(report, |report, (span, label)| {
                report.with_label(*span, label.clone())
            });
            let report = self.locate(report);
            self.reports.push(report);
        }
    }

    /// Maps the reports from index `from` on through `present`, and drops repeats among them.
    ///
    /// Several statements of the compiled program can come from one statement
    /// of the written program, so a check over the statements can report
    /// one problem once per copy.
    pub(crate) fn present_reports(
        &mut self,
        from: usize,
        mut present: impl FnMut(Diagnostic) -> Diagnostic,
    ) {
        let fresh: Vec<Diagnostic> = self.reports.drain(from..).collect();
        for report in fresh {
            let report = present(report);
            if !self.reports[from..].contains(&report) {
                self.reports.push(report);
            }
        }
    }

    /// Maps the reports from index `from` on through `present`, and keeps repeats.
    ///
    /// Reasoning can report the same problem more than once.
    pub(crate) fn map_reports(
        &mut self,
        from: usize,
        present: impl FnMut(Diagnostic) -> Diagnostic,
    ) {
        let fresh: Vec<Diagnostic> = self.reports.drain(from..).map(present).collect();
        self.reports.extend(fresh);
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

        let mut pending = vec![statement];
        let mut seen = Vec::new();
        while let Some(id) = pending.pop() {
            if seen.contains(&id) {
                continue;
            }
            seen.push(id);
            let Some(edge) = self.created_by[id.index()] else {
                continue;
            };
            let edge = &self.edges[edge];
            if let Some(RewriteDeclaration::Replaces { substituted, .. }) = &edge.declaration {
                for record in substituted {
                    let relevant = record.occurrence.is_some_and(|occurrence| {
                        labels.iter().any(|label| {
                            label.file == occurrence.file
                                && occurrence.start <= label.start
                                && label.end <= occurrence.end
                        })
                    });
                    if relevant && let Some(definition) = record.definition {
                        diagnostic = diagnostic.with_label(
                            definition,
                            format!("`{}` is defined here", record.variable),
                        );
                    }
                }
            }
            pending.extend(edge.sources.iter().rev());
        }
        diagnostic
    }
}
