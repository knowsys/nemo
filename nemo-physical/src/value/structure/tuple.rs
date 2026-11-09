//! Tuples, owned and borrowed.

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::Range,
    ptr, slice,
};

use crate::{
    dictionary::{Dictionary, StoredStructure},
    value::{DataValue, ValueRef},
};

/// An owned tuple of data values with an optional label.
///
/// Term syntax writes it as `(2, 3)` or `rect(2, 3)`.
/// Its borrowed view is [TupleRef].
#[derive(Debug, Clone)]
pub struct TupleValue {
    /// The tuple's label, if any.
    label: Option<Box<str>>,
    /// The elements in order.
    elements: Box<[DataValue]>,
}

impl TupleValue {
    /// Creates an unlabeled tuple.
    pub fn new(elements: impl Into<Box<[DataValue]>>) -> Self {
        Self {
            label: None,
            elements: elements.into(),
        }
    }

    /// Creates a labeled tuple.
    pub fn labeled(label: impl Into<Box<str>>, elements: impl Into<Box<[DataValue]>>) -> Self {
        Self {
            label: Some(label.into()),
            elements: elements.into(),
        }
    }

    /// Returns the tuple's label, if any.
    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    /// Returns the elements in order.
    pub fn elements(&self) -> &[DataValue] {
        &self.elements
    }

    /// Returns the element at the 0-based index, or `None` if the tuple is not that long.
    pub fn element(&self, index: usize) -> Option<&DataValue> {
        self.elements.get(index)
    }

    /// Returns the number of elements.
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Returns whether the tuple has no elements.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Returns the borrowed view of this tuple.
    pub fn as_ref(&self) -> TupleRef<'_> {
        TupleRef(Backing::Owned(self))
    }
}

impl PartialEq for TupleValue {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for TupleValue {}

impl Ord for TupleValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().cmp(&other.as_ref())
    }
}

impl PartialOrd for TupleValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for TupleValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl fmt::Display for TupleValue {
    /// Writes the tuple in canonical term syntax.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

/// A borrowed tuple of data values with an optional label.
///
/// Term syntax writes it as `(2, 3)` or `rect(2, 3)`.
/// Its owned form is [TupleValue].
#[derive(Clone, Copy)]
pub struct TupleRef<'a>(Backing<'a>);

/// The storage that a [TupleRef] reads its tuple from.
#[derive(Clone, Copy)]
enum Backing<'a> {
    /// An owned tuple.
    Owned(&'a TupleValue),
    /// An entry of a dictionary's tuple store, which is read in place.
    ///
    /// Each element is decoded when it is read.
    Stored {
        /// The dictionary that stores the tuple.
        dictionary: &'a Dictionary,
        /// The index of the entry.
        index: u64,
    },
}

impl<'a> TupleRef<'a> {
    /// Creates the view of an entry of a dictionary's tuple store.
    ///
    /// The store must contain the entry.
    pub(crate) fn stored(dictionary: &'a Dictionary, index: u64) -> Self {
        Self(Backing::Stored { dictionary, index })
    }

    /// Returns the tuple's label, if any.
    pub fn label(self) -> Option<&'a str> {
        match self.0 {
            Backing::Owned(tuple) => tuple.label(),
            Backing::Stored { dictionary, index } => dictionary.stored_tuple(index).label(),
        }
    }

    /// Returns the number of elements.
    pub fn len(self) -> usize {
        match self.0 {
            Backing::Owned(tuple) => tuple.len(),
            Backing::Stored { dictionary, index } => dictionary.stored_tuple(index).len(),
        }
    }

    /// Returns whether the tuple has no elements.
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    /// Returns the element at the 0-based index, or `None` if the tuple is not that long.
    pub fn element(self, index: usize) -> Option<ValueRef<'a>> {
        match self.0 {
            Backing::Owned(tuple) => tuple.element(index).map(DataValue::as_ref),
            Backing::Stored {
                dictionary,
                index: entry,
            } => dictionary.stored_tuple(entry).element(index),
        }
    }

    /// Returns the elements in order.
    pub fn elements(self) -> impl ExactSizeIterator<Item = ValueRef<'a>> {
        match self.0 {
            Backing::Owned(tuple) => Elements::Owned(tuple.elements.iter()),
            Backing::Stored { dictionary, index } => {
                let structure = dictionary.stored_tuple(index);
                Elements::Stored {
                    structure,
                    positions: 0..structure.len(),
                }
            }
        }
    }

    /// Returns an owned copy of the tuple.
    pub fn to_owned(self) -> TupleValue {
        match self.0 {
            Backing::Owned(tuple) => tuple.clone(),
            Backing::Stored { .. } => TupleValue {
                label: self.label().map(Box::from),
                elements: self.elements().map(ValueRef::to_owned).collect(),
            },
        }
    }

    /// Returns the index of the tuple in the tuple store of `dictionary`,
    /// or `None` if the view does not read the tuple from that store.
    pub(crate) fn index_in(self, dictionary: &Dictionary) -> Option<u64> {
        match self.0 {
            Backing::Stored {
                dictionary: own,
                index,
            } if ptr::eq(own, dictionary) => Some(index),
            _ => None,
        }
    }
}

impl PartialEq for TupleRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        // A dictionary stores each tuple only once,
        // so two entries of one store are equal exactly if their indices are.
        if let (
            Backing::Stored { dictionary, index },
            Backing::Stored {
                dictionary: other_dictionary,
                index: other_index,
            },
        ) = (self.0, other.0)
            && ptr::eq(dictionary, other_dictionary)
        {
            return index == other_index;
        }

        self.label() == other.label()
            && self.len() == other.len()
            && self.elements().eq(other.elements())
    }
}

impl Eq for TupleRef<'_> {}

impl Ord for TupleRef<'_> {
    /// Orders tuples by label, with unlabeled tuples first,
    /// and then lexicographically by their elements.
    fn cmp(&self, other: &Self) -> Ordering {
        self.label()
            .cmp(&other.label())
            .then_with(|| self.elements().cmp(other.elements()))
    }
}

impl PartialOrd for TupleRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for TupleRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label().hash(state);
        state.write_usize(self.len());
        for element in self.elements() {
            element.hash(state);
        }
    }
}

impl fmt::Debug for TupleRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TupleRef({self})")
    }
}

impl fmt::Display for TupleRef<'_> {
    /// Writes the tuple in canonical term syntax.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write_term(f, true)
    }
}

impl TupleRef<'_> {
    /// Writes the tuple in term syntax.
    ///
    /// `bare` says whether an IRI, including the label, may be written without angle brackets,
    /// as for [ValueRef::write_term].
    pub(crate) fn write_term(&self, f: &mut fmt::Formatter<'_>, bare: bool) -> fmt::Result {
        if let Some(label) = self.label() {
            super::write_label(f, label, bare)?;
        }
        f.write_str("(")?;
        for (index, element) in self.elements().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            element.write_term(f, bare)?;
        }
        // An unlabeled tuple with one element is written with a trailing comma,
        // which distinguishes it from a term in parentheses.
        if self.len() == 1 && self.label().is_none() {
            f.write_str(",")?;
        }
        f.write_str(")")
    }
}

/// The elements of a [TupleRef], in order.
enum Elements<'a> {
    /// The elements of an owned tuple.
    Owned(slice::Iter<'a, DataValue>),
    /// The elements of a stored tuple, read by position.
    Stored {
        /// The stored tuple.
        structure: StoredStructure<'a>,
        /// The positions not yet read.
        positions: Range<usize>,
    },
}

impl<'a> Iterator for Elements<'a> {
    type Item = ValueRef<'a>;

    fn next(&mut self) -> Option<ValueRef<'a>> {
        match self {
            Elements::Owned(elements) => elements.next().map(DataValue::as_ref),
            Elements::Stored {
                structure,
                positions,
            } => positions.next().map(|position| {
                structure
                    .element(position)
                    .expect("the positions lie within the tuple")
            }),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Elements::Owned(elements) => elements.size_hint(),
            Elements::Stored { positions, .. } => positions.size_hint(),
        }
    }
}

impl ExactSizeIterator for Elements<'_> {}
