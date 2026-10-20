//! Types: sets of domains refined by the shapes of structured values.
//!
//! A [Type] is an upper bound on the values a place can hold:
//! a [DomainSet], refined by what is known of the insides of its structured domains.
//! A structured domain is refined by a set of [shapes](Shape),
//! one of which every value of that domain the place can hold fits.
//! A shape is written out, as a [TupleShape] or a [MapShape],
//! or it is the type declared for a label, referred to by the label.
//!
//! Written-out shapes nest up to [Type::DEPTH];
//! below that, a structure's inside is just its domain,
//! which keeps the lattice of types finite while recursion builds ever deeper values.
//! A declared label is never unfolded, so a recursive declaration needs no depth;
//! [LabelTypes] holds what the labels stand for.

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

/// An upper bound on the values a place can hold:
/// the possible domains, with optional shape refinements for the structured ones.
///
/// The refinements over-approximate like the domains do.
/// Present shapes bound every tuple, or map, the place can hold,
/// and absent ones say nothing beyond the domain.
/// A domain that is not possible has no shapes.
/// The empty type describes a place no value can reach.
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
    /// The shapes every possible tuple fits one of, if known.
    tuples: Option<Box<[Shape<TupleShape>]>>,
    /// The shapes every possible map fits one of, if known.
    maps: Option<Box<[Shape<MapShape>]>>,
}

impl Type {
    /// How deep written-out shapes nest:
    /// a structure this many levels in is described by its domain alone.
    ///
    /// This bounds the height of the lattice,
    /// which is what lets a fixpoint over types terminate
    /// while recursion builds ever deeper structures.
    pub const DEPTH: usize = 3;

    /// The empty type: no value can be here.
    pub const EMPTY: Self = Self::of_domains(DomainSet::EMPTY);

    /// Every value: nothing is known.
    pub const ANY: Self = Self::of_domains(DomainSet::ANY);

    /// Creates the type of exactly one domain, unrefined.
    pub const fn of(domain: ValueDomain) -> Self {
        Self::of_domains(DomainSet::of(domain))
    }

    /// Creates the type of a domain set, unrefined.
    pub const fn of_domains(domains: DomainSet) -> Self {
        Self {
            domains,
            tuples: None,
            maps: None,
        }
    }

    /// Creates the type holding exactly the tuples of a shape.
    pub fn tuple(shape: TupleShape) -> Self {
        Self {
            domains: DomainSet::TUPLE,
            tuples: Some(Box::new([Shape::Structural(shape)])),
            maps: None,
        }
    }

    /// Creates the type holding exactly the maps of a shape.
    pub fn map(shape: MapShape) -> Self {
        Self {
            domains: DomainSet::MAP,
            tuples: None,
            maps: Some(Box::new([Shape::Structural(shape)])),
        }
    }

    /// Creates the type of the values of a label's declared type:
    /// the tuples, or the maps, carrying the label.
    ///
    /// # Panics
    ///
    /// Panics if the domain is neither the tuple nor the map domain.
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

    /// Computes the precise type of one value:
    /// its domain, refined through its structure down to [DEPTH](Self::DEPTH).
    pub fn of_value(value: ValueRef<'_>) -> Self {
        Self::of_value_within(value, Self::DEPTH)
    }

    /// Computes [of_value](Self::of_value) with the remaining refinement depth.
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
    /// The domain decides for a scalar.
    /// A tuple or map must also fit one of the shapes its domain is refined by, if it is,
    /// through every level the shape describes;
    /// for a declared label, through the declaration the labels give,
    /// or by the label alone if they lack it.
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

    /// Returns this type with its written-out shapes trimmed to the given depth.
    ///
    /// A shape composed from already refined element types
    /// is trimmed to stay inside [DEPTH](Self::DEPTH).
    /// Declared labels refer rather than nest, so they stay at any depth.
    pub fn capped(&self, depth: usize) -> Self {
        Self {
            domains: self.domains,
            tuples: shape::capped(self.tuples.as_deref(), depth),
            maps: shape::capped(self.maps.as_deref(), depth),
        }
    }

    /// Returns this type with every declared label replaced by its declaration,
    /// through the given number of levels of structure.
    ///
    /// Intersection meets a declared label with a written-out shape without reading the
    /// declaration, so a proof that two types share no value unfolds them first.
    /// A label the declarations lack, or one below the depth, stays a label.
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

    /// Returns the shapes every possible tuple fits one of,
    /// if the tuple domain is possible and refined.
    pub fn tuple_shapes(&self) -> Option<&[Shape<TupleShape>]> {
        self.tuples.as_deref()
    }

    /// Returns the shapes every possible map fits one of,
    /// if the map domain is possible and refined.
    pub fn map_shapes(&self) -> Option<&[Shape<MapShape>]> {
        self.maps.as_deref()
    }

    /// Returns whether no value can be here.
    pub fn is_empty(&self) -> bool {
        self.domains.is_empty()
    }

    /// Returns whether the types can share a value.
    ///
    /// If both refine a structured domain, incompatible shapes exclude it.
    pub fn intersects(&self, other: &Self) -> bool {
        !self.intersect(other).is_empty()
    }

    /// Returns the values in both types:
    /// the domains intersect,
    /// and a structured domain survives only if some of the shapes can agree.
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

    /// Returns the values in either type.
    ///
    /// The domains unite, and so do the shapes of a structured domain,
    /// unless one side holds the domain without shapes.
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

    /// Returns whether every value of this type is a value of the other,
    /// reading declared labels through the given declarations.
    ///
    /// The answer is a proof: `false` where containment is not shown,
    /// as for a declared label the labels lack.
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
