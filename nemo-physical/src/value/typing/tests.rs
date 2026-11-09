//! Tests of types: membership, intersection, union, containment, and declared labels.

use crate::value::{
    DataValue, DomainSet, LabelBound, LabelTypes, MapShape, MapValue, Shape, TupleShape,
    TupleValue, Type, ValueDomain,
};

/// Creates the type of one domain, without shapes.
fn of(domain: ValueDomain) -> Type {
    Type::of(domain)
}

/// Creates the type of the unlabeled tuples with the given element types.
fn tuple(elements: Vec<Type>) -> Type {
    Type::tuple(TupleShape {
        label: LabelBound::Exactly(None),
        elements,
    })
}

/// Creates the type of the tuples with the given label and element types.
fn labeled(label: &str, elements: Vec<Type>) -> Type {
    Type::tuple(TupleShape {
        label: LabelBound::Exactly(Some(label.into())),
        elements,
    })
}

/// Creates a labeled tuple value.
fn value(label: &str, elements: Vec<DataValue>) -> DataValue {
    DataValue::tuple(TupleValue::labeled(label, elements))
}

/// Returns the declared types of the labels of `list(int, list | nil)` and `nil()`.
fn lists() -> LabelTypes {
    let either = Type::declared("list", ValueDomain::Tuple)
        .union(&Type::declared("nil", ValueDomain::Tuple));
    let mut labels = LabelTypes::default();
    labels.declare(
        "list",
        labeled("list", vec![of(ValueDomain::Integer), either]),
    );
    labels.declare("nil", labeled("nil", vec![]));
    labels
}

/// The type of a tuple value has the tuple's label and the domains of its elements.
#[test]
fn values_type_at_depth() {
    let point = value("point", vec![DataValue::integer(1), DataValue::string("x")]);
    assert_eq!(
        Type::of_value(point.as_ref()),
        labeled(
            "point",
            vec![of(ValueDomain::Integer), of(ValueDomain::String)]
        )
    );
}

/// A value belongs to a type by its domain, and a tuple must also fit one of the type's shapes.
#[test]
fn admission_follows_domains_and_refined_shapes() {
    let none = &LabelTypes::NONE;
    let pair = tuple(vec![of(ValueDomain::Integer), of(ValueDomain::Integer)]);
    let declared = Type::of(ValueDomain::Integer).union(&pair);
    let point = |elements: Vec<DataValue>| DataValue::tuple(TupleValue::new(elements));

    // An integer belongs by its domain alone, and a tuple must also fit the shape.
    assert!(declared.admits(DataValue::integer(1).as_ref(), none));
    assert!(!declared.admits(DataValue::string("1").as_ref(), none));
    assert!(declared.admits(
        point(vec![DataValue::integer(1), DataValue::integer(2)]).as_ref(),
        none
    ));
    assert!(!declared.admits(point(vec![DataValue::integer(1)]).as_ref(), none));
    assert!(!declared.admits(
        point(vec![DataValue::string("x"), DataValue::integer(2)]).as_ref(),
        none
    ));
    // A shape with an exact label accepts only the tuples with that label.
    let labeled = labeled("p", vec![of(ValueDomain::Integer)]);
    assert!(labeled.admits(value("p", vec![DataValue::integer(1)]).as_ref(), none));
    assert!(!labeled.admits(point(vec![DataValue::integer(1)]).as_ref(), none));
    // A tuple domain without shapes accepts every tuple.
    assert!(of(ValueDomain::Tuple).admits(point(vec![]).as_ref(), none));
    assert!(Type::ANY.admits(DataValue::string("x").as_ref(), none));
    assert!(!Type::EMPTY.admits(DataValue::string("x").as_ref(), none));
}

/// A map fits a map shape if it has exactly the shape's keys and its values fit,
/// and a shape with a key given twice cannot be created.
#[test]
fn map_admission_matches_keys_positionally() {
    let none = &LabelTypes::NONE;
    let key = |name: &str| DataValue::string(name);
    let shape = MapShape::new(
        LabelBound::Unknown,
        vec![
            (key("b"), of(ValueDomain::Integer)),
            (key("a"), of(ValueDomain::String)),
        ],
    )
    .expect("keys are unique");
    assert_eq!(shape.pairs()[0].0, key("a"));
    let declared = Type::map(shape);
    let map = |pairs: Vec<(DataValue, DataValue)>| {
        DataValue::map(MapValue::new(pairs).expect("keys are unique"))
    };
    assert!(
        declared.admits(
            map(vec![
                (key("a"), DataValue::string("x")),
                (key("b"), DataValue::integer(1))
            ])
            .as_ref(),
            none
        )
    );
    assert!(!declared.admits(map(vec![(key("a"), DataValue::string("x"))]).as_ref(), none));
    assert!(
        !declared.admits(
            map(vec![
                (key("a"), DataValue::string("x")),
                (key("c"), DataValue::integer(1))
            ])
            .as_ref(),
            none
        )
    );
    assert!(
        !declared.admits(
            map(vec![
                (key("a"), DataValue::integer(1)),
                (key("b"), DataValue::integer(1))
            ])
            .as_ref(),
            none
        )
    );
    assert!(
        MapShape::new(
            LabelBound::Unknown,
            vec![(key("a"), Type::ANY), (key("a"), Type::ANY)]
        )
        .is_none()
    );
}

/// Types with conflicting shapes share no value, and compatible shapes intersect.
#[test]
fn intersection_proves_shape_conflicts() {
    let pair = tuple(vec![of(ValueDomain::Integer), of(ValueDomain::Integer)]);
    let triple = tuple(vec![
        of(ValueDomain::Integer),
        of(ValueDomain::Integer),
        of(ValueDomain::Integer),
    ]);
    // Shapes of different lengths share no tuple.
    assert!(pair.intersect(&triple).is_empty());
    // One conflicting element removes the whole tuple domain.
    let stringy = tuple(vec![of(ValueDomain::String), of(ValueDomain::Integer)]);
    assert!(pair.intersect(&stringy).is_empty());
    // A side without shapes keeps the shapes of the other side.
    assert_eq!(of(ValueDomain::Tuple).intersect(&pair), pair);
    // Compatible shapes intersect element by element.
    let numeric = tuple(vec![
        Type::of_domains(DomainSet::NUMERIC),
        of(ValueDomain::Integer),
    ]);
    assert_eq!(pair.intersect(&numeric), pair);
}

/// A union keeps every shape, and merges the shapes with the same label and length.
#[test]
fn union_keeps_every_shape() {
    let pair = tuple(vec![of(ValueDomain::Integer), of(ValueDomain::Integer)]);
    let stringy = tuple(vec![of(ValueDomain::String), of(ValueDomain::Integer)]);
    // Shapes with the same label and length merge into one, with the union of the elements.
    let both = pair.union(&stringy);
    let [Shape::Structural(shape)] = both.tuple_shapes().expect("refined") else {
        panic!("one shape: {both:?}");
    };
    assert_eq!(
        shape.elements[0].domains(),
        DomainSet::INTEGER.with(ValueDomain::String)
    );
    // Shapes with different lengths or labels stay apart.
    let triple = tuple(vec![of(ValueDomain::Integer); 3]);
    assert_eq!(pair.union(&triple).tuple_shapes().map(<[_]>::len), Some(2));
    let point = labeled("point", vec![of(ValueDomain::Integer); 2]);
    let size = labeled("size", vec![of(ValueDomain::Integer); 2]);
    let either = point.union(&size);
    assert_eq!(either.tuple_shapes().map(<[_]>::len), Some(2));
    // So a tuple with a third label is not in the union.
    let other = labeled("other", vec![of(ValueDomain::Integer); 2]);
    assert!(either.intersect(&other).is_empty());
    assert!(!other.is_subset(&either, &LabelTypes::NONE));
    // The order of the union does not matter.
    assert_eq!(either, size.union(&point));
    // A side without the tuple domain leaves the shapes as they are,
    // and a side with the tuple domain but without shapes leaves the union without shapes.
    assert!(
        pair.union(&of(ValueDomain::Integer))
            .tuple_shapes()
            .is_some()
    );
    assert!(pair.union(&of(ValueDomain::Tuple)).tuple_shapes().is_none());
}

/// In a union, a shape with an unknown label absorbs the other shapes of its length.
#[test]
fn an_unknown_label_takes_in_its_arity() {
    let anonymous = Type::tuple(TupleShape {
        label: LabelBound::Unknown,
        elements: vec![of(ValueDomain::Integer)],
    });
    let point = labeled("point", vec![of(ValueDomain::String)]);
    let united = anonymous.union(&point);
    let [Shape::Structural(shape)] = united.tuple_shapes().expect("refined") else {
        panic!("one shape: {united:?}");
    };
    assert_eq!(shape.label, LabelBound::Unknown);
    assert_eq!(
        shape.elements[0].domains(),
        DomainSet::INTEGER.with(ValueDomain::String)
    );
}

/// The type of a value is refined down to [Type::DEPTH] levels,
/// and below them it is the bare domain.
#[test]
fn refinement_stops_at_the_depth_cap() {
    let mut value = DataValue::integer(1);
    for _ in 0..Type::DEPTH + 2 {
        value = DataValue::tuple(TupleValue::new(vec![value]));
    }
    let mut typed = Type::of_value(value.as_ref());
    for _ in 0..Type::DEPTH {
        let [Shape::Structural(shape)] = typed.tuple_shapes().expect("refined to the cap") else {
            panic!("one shape: {typed:?}");
        };
        typed = shape.elements[0].clone();
    }
    // Below the cap, the type is the tuple domain without shapes.
    assert_eq!(typed.domains(), DomainSet::TUPLE);
    assert!(typed.tuple_shapes().is_none());
}

/// Cutting a type off at a depth removes written-out shapes below it, but keeps declared labels.
#[test]
fn declared_labels_stay_below_the_cap() {
    // Below the cap, a written-out shape is reduced to its domain,
    // but a declared label stays, since it refers to its declaration instead of nesting it.
    let declared = Type::declared("list", ValueDomain::Tuple);
    let nested = tuple(vec![declared.clone()]);
    assert_eq!(nested.capped(1), tuple(vec![declared.clone()]));
    assert_eq!(declared.capped(0), declared);
    let mixed = declared.union(&tuple(vec![]));
    assert!(mixed.capped(0).tuple_shapes().is_none());
}

/// Containment compares the domains, and the shapes by length, label and elements.
#[test]
fn containment_reads_shapes_and_domains() {
    let none = &LabelTypes::NONE;
    let int = of(ValueDomain::Integer);
    let number = Type::of_domains(DomainSet::NUMERIC);
    assert!(int.is_subset(&number, none));
    assert!(!number.is_subset(&int, none));
    assert!(Type::EMPTY.is_subset(&int, none));
    assert!(int.is_subset(&Type::ANY, none));

    let pair = tuple(vec![int.clone(), int.clone()]);
    let wide = tuple(vec![number.clone(), number]);
    assert!(pair.is_subset(&wide, none));
    assert!(!wide.is_subset(&pair, none));
    // Every tuple shape lies within the tuple domain without shapes,
    // which lies within no shape.
    assert!(pair.is_subset(&of(ValueDomain::Tuple), none));
    assert!(!of(ValueDomain::Tuple).is_subset(&pair, none));
    // An exact label lies within an unknown one, but not the other way round.
    let anonymous = Type::tuple(TupleShape {
        label: LabelBound::Unknown,
        elements: vec![int.clone(), int.clone()],
    });
    let point = labeled("point", vec![int.clone(), int]);
    assert!(point.is_subset(&anonymous, none));
    assert!(!anonymous.is_subset(&point, none));
}

/// A declared label is read through its declaration, for containment and for membership.
#[test]
fn a_declared_label_stands_for_its_declaration() {
    let labels = lists();
    let list = Type::declared("list", ValueDomain::Tuple);
    let nil = Type::declared("nil", ValueDomain::Tuple);
    let either = list.union(&nil);

    // A written-out list shape lies within the label if its elements lie within the declaration,
    // and the tail lies within it by its label alone.
    let built = labeled("list", vec![of(ValueDomain::Integer), either.clone()]);
    assert!(built.is_subset(&list, &labels));
    assert!(!built.is_subset(&nil, &labels));
    let wrong = labeled("list", vec![of(ValueDomain::String), either.clone()]);
    assert!(!wrong.is_subset(&list, &labels));
    // Without the declarations, the containment cannot be shown.
    assert!(!built.is_subset(&list, &LabelTypes::NONE));
    // The label lies within its own declared shape, written out.
    assert!(list.is_subset(&built, &labels));

    // Membership reads the declaration at every level of the value.
    let ok = value(
        "list",
        vec![
            DataValue::integer(1),
            value("list", vec![DataValue::integer(2), value("nil", vec![])]),
        ],
    );
    assert!(list.admits(ok.as_ref(), &labels));
    let bad = value(
        "list",
        vec![
            DataValue::integer(1),
            value("list", vec![DataValue::string("x"), value("nil", vec![])]),
        ],
    );
    assert!(!list.admits(bad.as_ref(), &labels));
    assert!(list.admits(bad.as_ref(), &LabelTypes::NONE));

    // A union absorbs a written-out shape with a declared label,
    // and an intersection keeps the label.
    assert_eq!(built.union(&list), list);
    assert_eq!(built.intersect(&list), list);
    assert!(list.intersect(&nil).is_empty());
}
