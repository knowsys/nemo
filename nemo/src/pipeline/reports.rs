//! The pipeline's reports: the warnings and non-fatal errors of a run,
//! and the labels added to diagnostics about statement versions.

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::Span,
    pipeline::{Pipeline, RewriteDeclaration, StatementId},
};

impl Pipeline {
    /// Adds a report to the sink.
    pub fn report(&mut self, diagnostic: Diagnostic) {
        self.reports.push(diagnostic);
    }

    /// Drops the reports from index `from` on.
    ///
    /// A method that suspends for data from the network (N0491) drops what it reported so far,
    /// and reports it again when it runs again on [resume](crate::engine::Engine::resume).
    pub(crate) fn withdraw_reports(&mut self, from: usize) {
        self.reports.truncate(from);
    }

    /// Returns a diagnostic about a statement version labeled with the `@use` directives that
    /// instantiated it, if a module instance holds it.
    ///
    /// The labels tell apart the same problem in two instances of a module.
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
    /// instantiated a statement version, if a module instance holds it.
    pub(crate) fn label_instances_in_reports(&mut self, from: usize, statement: StatementId) {
        let labels = self.instance_labels(statement);
        if labels.is_empty() {
            return;
        }
        for report in &mut self.reports[from..] {
            for (span, label) in &labels {
                *report = report.clone().with_label(*span, label.clone());
            }
        }
    }

    /// Maps the reports from index `from` on through `present`, dropping repeats.
    ///
    /// Several statements of the rewritten program can come from one statement
    /// of the program as written, so a check over the statements can report
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

    /// Maps the reports from index `from` on through `present`, keeping repeats.
    ///
    /// Reasoning can report the same problem once per round.
    pub(crate) fn map_reports(
        &mut self,
        from: usize,
        present: impl FnMut(Diagnostic) -> Diagnostic,
    ) {
        let fresh: Vec<Diagnostic> = self.reports.drain(from..).map(present).collect();
        self.reports.extend(fresh);
    }

    /// Returns the spans of the `@use` directives that instantiated a statement version,
    /// outermost first, each with the label naming its instance,
    /// or none for a statement of the main program.
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

    /// Returns the accumulated reports.
    pub fn reports(&self) -> &[Diagnostic] {
        &self.reports
    }

    /// Adds labels to a diagnostic about a statement version
    /// from the substitutions recorded on its provenance ancestry.
    ///
    /// For every substituted occurrence that encloses one of the diagnostic's labels,
    /// it adds a "defined here" label at the definition of the substituted value.
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
