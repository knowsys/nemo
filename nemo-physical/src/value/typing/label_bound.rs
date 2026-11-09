//! Bounds on the label of a tuple or a map.

/// What is known about the label of a tuple or a map.
///
/// [Unknown](LabelBound::Unknown) comes first in the order.
/// So among sorted shapes with the same length or keys, a shape with an unknown label
/// comes first, and can absorb the others.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum LabelBound {
    /// Nothing is known: the structure may have any label, or none.
    Unknown,
    /// The structure has exactly this label, or no label for `None`.
    Exactly(Option<Box<str>>),
}

impl LabelBound {
    /// Returns whether a structure with the given label lies within the bound.
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

    /// Returns the bound that both bounds allow, which is the tighter one,
    /// or `None` if they exclude each other.
    pub(super) fn intersect(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (LabelBound::Unknown, bound) | (bound, LabelBound::Unknown) => Some(bound.clone()),
            (left, right) if left == right => Some(left.clone()),
            _ => None,
        }
    }
}
