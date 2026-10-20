//! Shapes of structures, and the sets of them a [Type] refines a structured domain by.
//!
//! A set is kept canonical, so that equal sets compare equal:
//! - written-out shapes come first, ordered by outline (arity, or key set) and label,
//!   and no two share both;
//! - within one outline, a shape of unknown label takes in every other,
//!   since it admits what they admit;
//! - declared labels follow, ordered and once each,
//!   and a written-out shape carrying one of them is dropped,
//!   since every structure carrying a declared label fits its declaration.

use std::cmp::Ordering;

use crate::value::{
    DomainSet, ValueDomain,
    typing::{LabelBound, LabelTypes, Type},
};

/// One shape of the structures of a domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Shape<S> {
    /// The structures fitting a written-out shape.
    Structural(S),
    /// The structures carrying a label whose type is declared.
    ///
    /// Every such structure fits the declaration, which [LabelTypes] holds.
    Declared(Box<str>),
}

/// What a written-out shape of tuples or maps provides to the sets of shapes.
pub(crate) trait StructuralShape: Clone + PartialEq + Sized {
    /// The domain of the structures the shape describes.
    const DOMAIN: ValueDomain;

    /// Returns what is known of the label.
    fn label(&self) -> &LabelBound;

    /// Returns the shapes a type refines this domain by, if it does.
    fn of_type(datatype: &Type) -> Option<&[Shape<Self>]>;

    /// Orders by outline, labels aside: by arity for tuples, by key set for maps.
    fn cmp_outline(&self, other: &Self) -> Ordering;

    /// Returns the shape covering both, of the same outline, under this shape's label.
    fn merge(&self, other: &Self) -> Self;

    /// Returns the shape of the structures both admit,
    /// or `None` if no structure fits both.
    fn intersect(&self, other: &Self) -> Option<Self>;

    /// Returns whether every component of this shape lies within the other's,
    /// of the same outline.
    fn components_within(&self, other: &Self, labels: &LabelTypes) -> bool;

    /// Returns the shape with its components trimmed below the given depth, at least one.
    fn capped(&self, depth: usize) -> Self;

    /// Returns the shape with the declared labels in its components unfolded
    /// through the given depth; see [Type::unfolded].
    fn unfolded(&self, labels: &LabelTypes, depth: usize) -> Self;
}

/// Returns the canonical set of the given shapes; see the module documentation.
pub(super) fn normalized<S: StructuralShape>(
    shapes: impl IntoIterator<Item = Shape<S>>,
) -> Box<[Shape<S>]> {
    let mut declared: Vec<Box<str>> = Vec::new();
    let mut structural: Vec<S> = Vec::new();
    for shape in shapes {
        match shape {
            Shape::Declared(label) => declared.push(label),
            Shape::Structural(shape) => structural.push(shape),
        }
    }
    declared.sort_unstable();
    declared.dedup();
    structural.retain(|shape| match shape.label() {
        LabelBound::Exactly(Some(label)) => declared.binary_search(label).is_err(),
        _ => true,
    });
    structural.sort_by(|left, right| {
        left.cmp_outline(right)
            .then_with(|| left.label().cmp(right.label()))
    });

    let mut merged: Vec<S> = Vec::with_capacity(structural.len());
    for shape in structural {
        match merged.last_mut() {
            Some(last)
                if last.cmp_outline(&shape).is_eq()
                    && (*last.label() == LabelBound::Unknown || last.label() == shape.label()) =>
            {
                *last = last.merge(&shape);
            }
            _ => merged.push(shape),
        }
    }

    merged
        .into_iter()
        .map(Shape::Structural)
        .chain(declared.into_iter().map(Shape::Declared))
        .collect()
}

/// Returns the union of the shapes of one domain,
/// given whether each side holds the domain at all:
/// a side without the domain contributes nothing,
/// a side holding it without shapes takes in every shape.
pub(super) fn union<S: StructuralShape>(
    left_domain: bool,
    left: Option<&[Shape<S>]>,
    right_domain: bool,
    right: Option<&[Shape<S>]>,
) -> Option<Box<[Shape<S>]>> {
    match (left_domain, right_domain) {
        (true, false) => left.map(Box::from),
        (false, true) => right.map(Box::from),
        (true, true) => match (left, right) {
            (Some(left), Some(right)) if left == right => Some(Box::from(left)),
            (Some(left), Some(right)) => Some(normalized(left.iter().chain(right).cloned())),
            _ => None,
        },
        (false, false) => None,
    }
}

/// Returns the intersection of the shapes of one domain,
/// or `None` if `domains`, the domains both types hold, lack it:
/// a side without shapes contributes nothing,
/// and shapes that exclude each other remove the domain from `domains`.
pub(super) fn intersect<S: StructuralShape>(
    domains: &mut DomainSet,
    left: Option<&[Shape<S>]>,
    right: Option<&[Shape<S>]>,
) -> Option<Box<[Shape<S>]>> {
    if !domains.contains(S::DOMAIN) {
        return None;
    }

    match (left, right) {
        (None, None) => None,
        (Some(one), None) | (None, Some(one)) => Some(Box::from(one)),
        (Some(left), Some(right)) => {
            let met: Vec<Shape<S>> = left
                .iter()
                .flat_map(|left| right.iter().filter_map(move |right| meet(left, right)))
                .collect();
            if met.is_empty() {
                *domains = domains.without(S::DOMAIN);
                return None;
            }
            Some(normalized(met))
        }
    }
}

/// Returns the shape of the structures two shapes both admit, or `None` if none.
///
/// A declared label met with a written-out shape that admits it stays the declared label,
/// a bound on what both admit without reading the declaration.
fn meet<S: StructuralShape>(left: &Shape<S>, right: &Shape<S>) -> Option<Shape<S>> {
    match (left, right) {
        (Shape::Structural(left), Shape::Structural(right)) => {
            left.intersect(right).map(Shape::Structural)
        }
        (Shape::Declared(left), Shape::Declared(right)) => {
            (left == right).then(|| Shape::Declared(left.clone()))
        }
        (Shape::Declared(label), Shape::Structural(shape))
        | (Shape::Structural(shape), Shape::Declared(label)) => shape
            .label()
            .admits(Some(label))
            .then(|| Shape::Declared(label.clone())),
    }
}

/// Returns the shapes of one domain trimmed to the given depth.
///
/// At depth zero only declared labels remain, since they refer rather than nest;
/// a set holding a written-out shape is then just its domain.
pub(super) fn capped<S: StructuralShape>(
    shapes: Option<&[Shape<S>]>,
    depth: usize,
) -> Option<Box<[Shape<S>]>> {
    let shapes = shapes?;
    if depth == 0 {
        return shapes
            .iter()
            .all(|shape| matches!(shape, Shape::Declared(_)))
            .then(|| Box::from(shapes));
    }

    Some(
        shapes
            .iter()
            .map(|shape| match shape {
                Shape::Structural(shape) => Shape::Structural(shape.capped(depth)),
                Shape::Declared(label) => Shape::Declared(label.clone()),
            })
            .collect(),
    )
}

/// Returns the shapes of one domain with their declared labels unfolded through the given depth;
/// see [Type::unfolded].
pub(super) fn unfolded<S: StructuralShape>(
    shapes: Option<&[Shape<S>]>,
    labels: &LabelTypes,
    depth: usize,
) -> Option<Box<[Shape<S>]>> {
    let shapes = shapes?;
    if depth == 0 {
        return Some(Box::from(shapes));
    }
    Some(normalized(shapes.iter().map(|shape| match shape {
        Shape::Structural(shape) => Shape::Structural(shape.unfolded(labels, depth - 1)),
        Shape::Declared(label) => match labels.structural::<S>(label) {
            Some(declared) => Shape::Structural(declared.unfolded(labels, depth - 1)),
            None => Shape::Declared(label.clone()),
        },
    })))
}

/// Returns whether every structure the inner shapes admit, the outer ones admit too.
///
/// No shapes means any structure of the domain.
pub(super) fn within<S: StructuralShape>(
    inner: Option<&[Shape<S>]>,
    outer: Option<&[Shape<S>]>,
    labels: &LabelTypes,
) -> bool {
    match (inner, outer) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(inner), Some(outer)) => inner
            .iter()
            .all(|shape| outer.iter().any(|outer| shape_within(shape, outer, labels))),
    }
}

/// Returns whether every structure one shape admits, another admits too.
///
/// A declared label is compared by its declaration, which the labels give;
/// a label they lack has a declaration no written-out shape is known to contain.
/// Every step reads one level further into a written-out shape, so the comparison ends.
fn shape_within<S: StructuralShape>(
    inner: &Shape<S>,
    outer: &Shape<S>,
    labels: &LabelTypes,
) -> bool {
    match (inner, outer) {
        (Shape::Declared(inner), Shape::Declared(outer)) => inner == outer,
        (Shape::Structural(inner), Shape::Structural(outer)) => {
            inner.label().is_within(outer.label())
                && inner.cmp_outline(outer).is_eq()
                && inner.components_within(outer, labels)
        }
        (Shape::Structural(inner), Shape::Declared(label)) => {
            inner
                .label()
                .is_within(&LabelBound::Exactly(Some(label.clone())))
                && labels.structural::<S>(label).is_some_and(|declared| {
                    inner.cmp_outline(declared).is_eq() && inner.components_within(declared, labels)
                })
        }
        (Shape::Declared(label), Shape::Structural(outer)) => {
            outer.label().admits(Some(label))
                && labels.structural::<S>(label).is_some_and(|declared| {
                    declared.cmp_outline(outer).is_eq() && declared.components_within(outer, labels)
                })
        }
    }
}
