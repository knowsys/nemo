//! What is known about a structure's label.

/// What is known about a structure's label.
///
/// The order puts [Unknown](LabelBound::Unknown) first,
/// which is what lets a set of shapes merge every shape of one outline into an unknown-label one.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum LabelBound {
    /// Nothing: any label, or none.
    Unknown,
    /// Exactly this label, or exactly no label for `None`.
    Exactly(Option<Box<str>>),
}

impl LabelBound {
    /// Returns whether a structure's label lies within the bound.
    pub fn admits(&self, label: Option<&str>) -> bool {
        match self {
            LabelBound::Unknown => true,
            LabelBound::Exactly(exact) => exact.as_deref() == label,
        }
    }

    /// Returns whether every label within this bound lies within the other.
    pub(super) fn is_within(&self, other: &Self) -> bool {
        matches!(other, LabelBound::Unknown) || self == other
    }

    /// Returns the bounds together: the tighter one,
    /// or `None` if they exclude each other.
    pub(super) fn intersect(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (LabelBound::Unknown, bound) | (bound, LabelBound::Unknown) => Some(bound.clone()),
            (left, right) if left == right => Some(left.clone()),
            _ => None,
        }
    }
}
