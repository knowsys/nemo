//! One rule's share of a run, as the recorder aggregates it.

/// A rule's aggregate over its applications.
///
/// It is the typed form of a rule's entry in the recorder's snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleSummary {
    /// The rule's rendered text, if passed to
    /// [Recorder::set_rules](crate::telemetry::Recorder::set_rules), or empty.
    pub text: String,
    /// The number of applications.
    pub applications: u64,
    /// The wall time of the applications, summed, in nanoseconds.
    pub duration: u64,
    /// The number of new rows stored.
    pub derived: u64,
    /// The number of rows dropped because of evaluation errors.
    pub dropped: u64,
}
