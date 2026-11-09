//! Markers: the names of the columns of a plan.

/// The name of a column in an [OperationPlan](crate::plan::OperationPlan),
/// which allocates it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Marker(pub(crate) u32);
