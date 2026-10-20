//! Marker orders: sequences of pairwise distinct markers.

use std::{ops::Deref, vec};

use crate::{management::ColumnOrder, plan::Marker};

/// A sequence of pairwise distinct markers:
/// the columns of a relation, or an order in which a consumer reads them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MarkerOrder(Vec<Marker>);

impl MarkerOrder {
    /// Creates the order of the given markers, which are pairwise distinct.
    pub(crate) fn new(markers: Vec<Marker>) -> Self {
        debug_assert!(
            markers
                .iter()
                .enumerate()
                .all(|(index, marker)| !markers[..index].contains(marker)),
            "the markers of an order are pairwise distinct"
        );

        Self(markers)
    }

    /// Returns the position of a marker in the order, or `None` if the order lacks it.
    pub(crate) fn position(&self, marker: Marker) -> Option<usize> {
        self.0.iter().position(|&candidate| candidate == marker)
    }

    /// Returns the column order that lays out columns carrying these markers in the given order:
    /// entry `i` is the column of the order's marker `i`.
    ///
    /// The given order rearranges these markers.
    pub(crate) fn column_order_to(&self, order: &[Marker]) -> ColumnOrder {
        let positions: Vec<usize> = self.positions(order).collect();

        ColumnOrder::new(&positions).expect("the order rearranges these markers")
    }

    /// Translates an order over these markers into one over the markers of `to`,
    /// replacing each marker by the marker at its position in `to`.
    ///
    /// An alias translates an order over its markers into one over its input's this way.
    pub(crate) fn translate(&self, order: &[Marker], to: &[Marker]) -> MarkerOrder {
        self.positions(order).map(|position| to[position]).collect()
    }

    /// Returns the markers of this order that lie among the given ones, in this order's sequence.
    pub(crate) fn restricted_to(&self, kept: &[Marker]) -> MarkerOrder {
        self.0
            .iter()
            .copied()
            .filter(|marker| kept.contains(marker))
            .collect()
    }

    /// Returns the positions of the given markers, each one of these.
    fn positions<'a>(&'a self, markers: &'a [Marker]) -> impl Iterator<Item = usize> + 'a {
        markers.iter().map(|&marker| {
            self.position(marker)
                .expect("an order over these markers names only them")
        })
    }
}

impl Deref for MarkerOrder {
    type Target = [Marker];

    fn deref(&self) -> &[Marker] {
        &self.0
    }
}

impl FromIterator<Marker> for MarkerOrder {
    fn from_iter<I: IntoIterator<Item = Marker>>(markers: I) -> Self {
        Self::new(markers.into_iter().collect())
    }
}

impl IntoIterator for MarkerOrder {
    type Item = Marker;
    type IntoIter = vec::IntoIter<Marker>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
