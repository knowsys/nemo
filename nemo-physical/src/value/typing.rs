//! Types: sets of domains, refined by the shapes of tuples and maps.
//!
//! A [Type] is an upper bound on the values that a *place*,
//! such as a column or the result of an expression, can hold.
//! It is a [DomainSet], refined by what is known about the inside of tuples and maps.
//! A structured domain is refined by a set of [shapes](Shape):
//! every value of that domain that the place can hold fits one of them.
//! A shape is either written out, as a [TupleShape] or a [MapShape],
//! or it is the type declared for a label, and refers to that type by the label.
//!
//! Written-out shapes nest up to [Type::DEPTH] levels.
//! Below that, a structure is described by its domain alone.
//! This keeps the lattice of types finite,
//! so that a fixpoint computation over types ends even if it builds ever deeper values.
//! A declared label is never unfolded, so a recursive declaration needs no depth limit.
//! [LabelTypes] contains the types declared for labels.

mod domain_set;
mod label_bound;
mod label_types;
mod map_shape;
mod shape;
mod tuple_shape;

use crate::value::{ValueDomain, ValueRef};

pub use domain_set::DomainSet;
pub use label_bound::LabelBound;
pub use label_types::LabelTypes;
pub use map_shape::MapShape;
pub use shape::Shape;
pub use tuple_shape::TupleShape;

/// An upper bound on the values that a place,
/// such as a column or the result of an expression, can hold.
///
/// A type consists of the possible domains, and of optional shapes for tuples and maps.
/// Like the domains, the shapes over-approximate:
/// - If there are shapes for tuples, every tuple that the place can hold fits one of them,
///   and likewise for maps.
/// - If there are none, nothing is known beyond the domain.
///
/// A domain that is not possible has no shapes.
/// The empty type describes a place that no value can reach.
///
/// # Example
///
/// ```
/// use nemo_physical::value::{DataValue, DomainSet, LabelTypes, TupleValue, Type, ValueDomain};
///
/// // A place that holds integers or strings.
/// let either = Type::of(ValueDomain::Integer).union(&Type::of(ValueDomain::String));
/// assert!(either.admits(DataValue::integer(7).as_ref(), &LabelTypes::NONE));
/// assert!(!either.admits(DataValue::boolean(true).as_ref(), &LabelTypes::NONE));
///
/// // Intersected with a numeric place, it keeps the integers.
/// let numeric = Type::of_domains(DomainSet::NUMERIC);
/// assert_eq!(either.intersect(&numeric).domains(), DomainSet::INTEGER);
///
/// // The type of a tuple records its shape, and shares no value with the first place.
/// let point = DataValue::tuple(TupleValue::labeled("point", [DataValue::integer(1)]));
/// let shaped = Type::of_value(point.as_ref());
/// assert!(shaped.tuple_shapes().is_some());
/// assert!(shaped.intersect(&either).is_empty());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Type {
    /// The possible domains.
    domains: DomainSet,
    /// The shapes of which every possible tuple fits one,
    /// or `None` if nothing is known about the tuples.
    tuples: Option<Box<[Shape<TupleShape>]>>,
    /// The shapes of which every possible map fits one,
    /// or `None` if nothing is known about the maps.
    maps: Option<Box<[Shape<MapShape>]>>,
}

impl Type {
    /// The number of levels to which written-out shapes nest.
    ///
    /// A structure this many levels deep is described by its domain alone.
    /// This bounds the height of the lattice of types,
    /// so that a fixpoint computation over types ends even if it builds ever deeper structures.
    pub const DEPTH: usize = 3;

    /// The empty type, which contains no value.
    pub const EMPTY: Self = Self::of_domains(DomainSet::EMPTY);

    /// The type of every value, of a place about which nothing is known.
    pub const ANY: Self = Self::of_domains(DomainSet::ANY);

    /// Creates the type of all values of one domain, without shapes.
    pub const fn of(domain: ValueDomain) -> Self {
        Self::of_domains(DomainSet::of(domain))
    }

    /// Creates the type of all values of a set of domains, without shapes.
    pub const fn of_domains(domains: DomainSet) -> Self {
        Self {
            domains,
            tuples: None,
            maps: None,
        }
    }

    /// Creates the type of exactly the tuples that fit a shape.
    pub fn tuple(shape: TupleShape) -> Self {
        Self {
            domains: DomainSet::TUPLE,
            tuples: Some(Box::new([Shape::Structural(shape)])),
            maps: None,
        }
    }

    /// Creates the type of exactly the maps that fit a shape.
    pub fn map(shape: MapShape) -> Self {
        Self {
            domains: DomainSet::MAP,
            tuples: None,
            maps: Some(Box::new([Shape::Structural(shape)])),
        }
    }

    /// Creates the type of the tuples or the maps with the given label,
    /// which fit the type declared for the label.
    ///
    /// `domain` says whether the type contains tuples or maps.
    ///
    /// # Panics
    ///
    /// Panics if `domain` is neither the tuple nor the map domain.
    pub fn declared(label: &str, domain: ValueDomain) -> Self {
        match domain {
            ValueDomain::Tuple => Self {
                domains: DomainSet::TUPLE,
                tuples: Some(Box::new([Shape::Declared(label.into())])),
                maps: None,
            },
            ValueDomain::Map => Self {
                domains: DomainSet::MAP,
                tuples: None,
                maps: Some(Box::new([Shape::Declared(label.into())])),
            },
            other => panic!("a declared label types tuples or maps, not {other:?}"),
        }
    }

    /// Returns the most precise type of one value.
    ///
    /// It is the value's domain, refined by the value's structure down to
    /// [DEPTH](Self::DEPTH) levels.
    pub fn of_value(value: ValueRef<'_>) -> Self {
        Self::of_value_within(value, Self::DEPTH)
    }

    /// Returns the type of [of_value](Self::of_value), refined down to `depth` more levels.
    fn of_value_within(value: ValueRef<'_>, depth: usize) -> Self {
        if depth == 0 {
            return Self::of(value.domain());
        }

        match value {
            ValueRef::Tuple(tuple) => Self::tuple(TupleShape {
                label: LabelBound::Exactly(tuple.label().map(Box::from)),
                elements: tuple
                    .elements()
                    .map(|element| Self::of_value_within(element, depth - 1))
                    .collect(),
            }),
            ValueRef::Map(map) => Self::map(
                MapShape::new(
                    LabelBound::Exactly(map.label().map(Box::from)),
                    map.pairs()
                        .map(|(key, value)| {
                            (key.to_owned(), Self::of_value_within(value, depth - 1))
                        })
                        .collect(),
                )
                .expect("a map holds each key once"),
            ),
            other => Self::of(other.domain()),
        }
    }

    /// Returns whether a value belongs to this type.
    ///
    /// A value that is not a tuple or a map belongs to the type if its domain does.
    /// A tuple or a map must also fit one of the shapes of its domain, if there are any,
    /// at every level that the shape describes.
    /// For a declared label, the value must have the label,
    /// and must fit the type that `labels` declares for it, if there is one.
    pub fn admits(&self, value: ValueRef<'_>, labels: &LabelTypes) -> bool {
        if !self.domains.contains(value.domain()) {
            return false;
        }

        match value {
            ValueRef::Tuple(tuple) => self.tuples.as_deref().is_none_or(|shapes| {
                shapes.iter().any(|shape| match shape {
                    Shape::Structural(shape) => {
                        shape.label.admits(tuple.label())
                            && shape.elements.len() == tuple.len()
                            && shape
                                .elements
                                .iter()
                                .zip(tuple.elements())
                                .all(|(element, value)| element.admits(value, labels))
                    }
                    Shape::Declared(label) => {
                        tuple.label() == Some(&**label)
                            && labels
                                .get(label)
                                .is_none_or(|declared| declared.admits(value, labels))
                    }
                })
            }),
            ValueRef::Map(map) => self.maps.as_deref().is_none_or(|shapes| {
                shapes.iter().any(|shape| match shape {
                    Shape::Structural(shape) => {
                        shape.label.admits(map.label())
                            && shape.pairs().len() == map.len()
                            && shape.pairs().iter().zip(map.pairs()).all(
                                |((key, value), (stored_key, stored_value))| {
                                    key.as_ref() == stored_key && value.admits(stored_value, labels)
                                },
                            )
                    }
                    Shape::Declared(label) => {
                        map.label() == Some(&**label)
                            && labels
                                .get(label)
                                .is_none_or(|declared| declared.admits(value, labels))
                    }
                })
            }),
            _ => true,
        }
    }

    /// Returns this type with its written-out shapes cut off at the given depth.
    ///
    /// A shape built from element types that are refined already may nest deeper than
    /// [DEPTH](Self::DEPTH), and this cuts it back.
    /// A declared label refers to its declaration instead of nesting it,
    /// so it stays at any depth.
    pub fn capped(&self, depth: usize) -> Self {
        Self {
            domains: self.domains,
            tuples: shape::capped(self.tuples.as_deref(), depth),
            maps: shape::capped(self.maps.as_deref(), depth),
        }
    }

    /// Returns this type with every declared label replaced by its declared shape,
    /// down to the given number of levels.
    ///
    /// [intersect](Self::intersect) does not read declarations,
    /// so it keeps a declared label that a written-out shape accepts.
    /// To show that two types share no value, unfold both first.
    /// A label without a declaration in `labels`, or one below the depth, stays a label.
    pub fn unfolded(&self, labels: &LabelTypes, depth: usize) -> Self {
        Self {
            domains: self.domains,
            tuples: shape::unfolded(self.tuples.as_deref(), labels, depth),
            maps: shape::unfolded(self.maps.as_deref(), labels, depth),
        }
    }

    /// Returns the possible domains.
    pub fn domains(&self) -> DomainSet {
        self.domains
    }

    /// Returns the shapes of which every possible tuple fits one,
    /// or `None` if the tuple domain is not possible or has no shapes.
    pub fn tuple_shapes(&self) -> Option<&[Shape<TupleShape>]> {
        self.tuples.as_deref()
    }

    /// Returns the shapes of which every possible map fits one,
    /// or `None` if the map domain is not possible or has no shapes.
    pub fn map_shapes(&self) -> Option<&[Shape<MapShape>]> {
        self.maps.as_deref()
    }

    /// Returns whether the type contains no value.
    pub fn is_empty(&self) -> bool {
        self.domains.is_empty()
    }

    /// Returns whether the two types may share a value, as [intersect](Self::intersect) finds.
    pub fn intersects(&self, other: &Self) -> bool {
        !self.intersect(other).is_empty()
    }

    /// Returns the type of the values in both types.
    ///
    /// Its domains are the domains of both types,
    /// except a structured domain whose shapes on one side all exclude those on the other.
    pub fn intersect(&self, other: &Self) -> Self {
        let mut domains = self.domains & other.domains;
        let tuples = shape::intersect(
            &mut domains,
            self.tuples.as_deref(),
            other.tuples.as_deref(),
        );
        let maps = shape::intersect(&mut domains, self.maps.as_deref(), other.maps.as_deref());

        Self {
            domains,
            tuples,
            maps,
        }
    }

    /// Returns the type of the values in either type.
    ///
    /// Its domains and shapes are those of either type,
    /// except that a structured domain without shapes in one type has no shapes in the result.
    pub fn union(&self, other: &Self) -> Self {
        let has = |datatype: &Self, domain| datatype.domains.contains(domain);
        Self {
            domains: self.domains | other.domains,
            tuples: shape::union(
                has(self, ValueDomain::Tuple),
                self.tuples.as_deref(),
                has(other, ValueDomain::Tuple),
                other.tuples.as_deref(),
            ),
            maps: shape::union(
                has(self, ValueDomain::Map),
                self.maps.as_deref(),
                has(other, ValueDomain::Map),
                other.maps.as_deref(),
            ),
        }
    }

    /// Returns whether every value of this type is shown to be a value of the other.
    ///
    /// Declared labels are read through `labels`.
    /// The result is conservative: it is `false` wherever the containment cannot be shown,
    /// as for a declared label that `labels` has no type for.
    pub fn is_subset(&self, other: &Self, labels: &LabelTypes) -> bool {
        self.domains.is_subset(other.domains)
            && (!self.domains.contains(ValueDomain::Tuple)
                || shape::within(self.tuples.as_deref(), other.tuples.as_deref(), labels))
            && (!self.domains.contains(ValueDomain::Map)
                || shape::within(self.maps.as_deref(), other.maps.as_deref(), labels))
    }
}

#[cfg(test)]
mod tests;
