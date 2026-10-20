//! One rule's share of a run, as the recorder aggregates it.

/// A rule's aggregate over its applications:
/// the typed form of the per-rule entries of the recorder's snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleSummary {
    /// The rule's rendered text, if passed to
    /// [Recorder::set_rules](crate::telemetry::Recorder::set_rules), or empty.
    pub text: String,
    /// Applications run.
    pub applications: u64,
    /// Wall time across them, in nanoseconds.
    pub duration: u64,
    /// New rows stored.
    pub derived: u64,
    /// Rows dropped because of evaluation errors.
    pub dropped: u64,
}
