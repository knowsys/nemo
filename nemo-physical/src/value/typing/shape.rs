//! Shapes of tuples and maps,
//! and the sets of shapes by which a [Type] refines a structured domain.
//!
//! The *outline* of a written-out shape is its length for tuples, and its keys for maps.
//! A set of shapes is kept in canonical form, so that equal sets compare equal:
//! - Written-out shapes come first, ordered by outline and then by label.
//!   Shapes with the same outline and label are merged into one,
//!   and a shape with an unknown label absorbs the other shapes of its outline,
//!   since it accepts every structure that they accept.
//! - Declared labels come last, ordered and each once.
//!   A written-out shape with one of these labels is dropped,
//!   since every structure with a declared label fits its declaration.

use std::cmp::Ordering;

use crate::value::{
    DomainSet, ValueDomain,
    typing::{LabelBound, LabelTypes, Type},
};

/// A shape of the tuples or of the maps of a type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Shape<S> {
    /// The structures that fit a written-out shape.
    Structural(S),
    /// The structures with a label whose type is declared.
    ///
    /// Every such structure fits the declared type, which [LabelTypes] contains.
    Declared(Box<str>),
}

/// The operations that sets of shapes need from a written-out shape of tuples or maps.
pub(crate) trait StructuralShape: Clone + PartialEq + Sized {
    /// The domain of the structures the shape describes.
    const DOMAIN: ValueDomain;

    /// Returns what is known about the label.
    fn label(&self) -> &LabelBound;

    /// Returns the shapes by which a type refines this domain, or `None` if it has none.
    fn of_type(datatype: &Type) -> Option<&[Shape<Self>]>;

    /// Orders two shapes by outline, ignoring their labels.
    fn cmp_outline(&self, other: &Self) -> Ordering;

    /// Returns a shape that accepts what both shapes accept, with this shape's label.
    ///
    /// Both shapes must have the same outline.
    fn merge(&self, other: &Self) -> Self;

    /// Returns the shape of the structures that both shapes accept,
    /// or `None` if no structure fits both.
    fn intersect(&self, other: &Self) -> Option<Self>;

    /// Returns whether every component type of this shape is a subset of the other's.
    ///
    /// Both shapes must have the same outline.
    fn components_within(&self, other: &Self, labels: &LabelTypes) -> bool;

    /// Returns the shape with its components cut off below the given depth,
    /// which must be at least one.
    fn capped(&self, depth: usize) -> Self;

    /// Returns the shape with the declared labels in its components unfolded
    /// down to the given depth, as [Type::unfolded] does.
    fn unfolded(&self, labels: &LabelTypes, depth: usize) -> Self;
}

/// Returns the given shapes as a set in canonical form, as the module documentation describes.
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

/// Returns the union of the shapes of one domain.
///
/// `left_domain` and `right_domain` say whether each side contains the domain at all.
/// A side without the domain contributes nothing,
/// and a side that contains the domain without shapes leaves the union without shapes.
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

/// Returns the intersection of the shapes of one domain.
///
/// `domains` contains the domains of both types.
/// Returns `None` if `domains` lacks the domain, or if neither side has shapes for it.
/// A side without shapes leaves the shapes of the other side as they are.
/// If every shape of one side excludes every shape of the other,
/// the domain is removed from `domains`.
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

/// Returns the shape of the structures that two shapes both accept,
/// or `None` if there are none.
///
/// A declared label and a written-out shape that accepts the label give the declared label.
/// This bounds what both accept without reading the declaration.
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

/// Returns the shapes of one domain, cut off at the given depth.
///
/// At depth zero only declared labels remain,
/// since they refer to their declarations instead of nesting them.
/// A set with a written-out shape then has no shapes, and the domain alone describes it.
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

/// Returns the shapes of one domain with their declared labels unfolded down to the given
/// depth, as [Type::unfolded] does.
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

/// Returns whether the outer shapes accept every structure that the inner shapes accept.
///
/// `None` stands for no shapes, which accept every structure of the domain.
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

/// Returns whether `outer` accepts every structure that `inner` accepts.
///
/// A declared label is compared through its declared shape in `labels`.
/// A label without a declaration there is not known to lie within any written-out shape.
/// Each step reads one level further into a written-out shape, so the comparison ends.
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
