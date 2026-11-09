//! Reports of a run: the JSON document, the HTML page that shows it,
//! and the tables that name the rows of the accounting.

mod page;
mod run_facts;
pub(super) mod tables;

pub use self::{
    run_facts::{ErrorCount, PredicateSize, ProgramText, RunFacts},
    tables::{NamedCount, PhaseTable},
};

use serde::Serialize;

use crate::telemetry::Recorder;

/// The JSON document that the reports of a run are built from.
///
/// It is the JSON report itself, and [page](Self::page) renders it as an HTML page.
/// Times are in nanoseconds and sizes in bytes throughout.
#[derive(Debug, Serialize)]
pub struct ReportDocument<'a> {
    /// The document's title.
    pub title: &'a str,
    /// The facts of a finished run, or `None` if the run is still going.
    pub run: Option<&'a RunFacts>,
    /// The recorded telemetry, or `None` if none was recorded.
    pub telemetry: Option<&'a Recorder>,
}

impl ReportDocument<'_> {
    /// Returns the document as JSON.
    pub fn json(&self) -> String {
        serde_json::to_string(self).expect("a document serializes")
    }

    /// Returns the HTML report of the document, as one self-contained page without external
    /// assets.
    ///
    /// If `live` is true, the page also subscribes to `/events`
    /// for the updates of the document that the page's server sends.
    /// Otherwise the page shows only the embedded document.
    pub fn page(&self, live: bool) -> String {
        page::render(&self.json(), live)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A document without run facts or telemetry has `null` in their place.
    #[test]
    fn documents_carry_their_absences_as_null() {
        let document = ReportDocument {
            title: "plain \"run\"",
            run: None,
            telemetry: None,
        };
        assert_eq!(
            document.json(),
            "{\"title\":\"plain \\\"run\\\"\",\"run\":null,\"telemetry\":null}"
        );
    }
}
