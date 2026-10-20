//! Types: admission, intersection, union, containment, and declared labels.

use crate::value::{
    DataValue, DomainSet, LabelBound, LabelTypes, MapShape, MapValue, Shape, TupleShape,
    TupleValue, Type, ValueDomain,
};

/// Creates an unrefined type.
fn of(domain: ValueDomain) -> Type {
    Type::of(domain)
}

/// Creates an unlabeled tuple type.
fn tuple(elements: Vec<Type>) -> Type {
    Type::tuple(TupleShape {
        label: LabelBound::Exactly(None),
        elements,
    })
}

/// Creates a labeled tuple type.
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

/// Returns the labels of `list(int, list | nil)` and `nil()`.
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

#[test]
fn admission_follows_domains_and_refined_shapes() {
    let none = &LabelTypes::NONE;
    let pair = tuple(vec![of(ValueDomain::Integer), of(ValueDomain::Integer)]);
    let declared = Type::of(ValueDomain::Integer).union(&pair);
    let point = |elements: Vec<DataValue>| DataValue::tuple(TupleValue::new(elements));

    // Scalars decide by domain alone, tuples by the refined shape.
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
    // A label bound holds the label to it.
    let labeled = labeled("p", vec![of(ValueDomain::Integer)]);
    assert!(labeled.admits(value("p", vec![DataValue::integer(1)]).as_ref(), none));
    assert!(!labeled.admits(point(vec![DataValue::integer(1)]).as_ref(), none));
    // An unrefined tuple domain takes every tuple.
    assert!(of(ValueDomain::Tuple).admits(point(vec![]).as_ref(), none));
    assert!(Type::ANY.admits(DataValue::string("x").as_ref(), none));
    assert!(!Type::EMPTY.admits(DataValue::string("x").as_ref(), none));
}

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

#[test]
fn intersection_proves_shape_conflicts() {
    let pair = tuple(vec![of(ValueDomain::Integer), of(ValueDomain::Integer)]);
    let triple = tuple(vec![
        of(ValueDomain::Integer),
        of(ValueDomain::Integer),
        of(ValueDomain::Integer),
    ]);
    // Different arities share no tuple.
    assert!(pair.intersect(&triple).is_empty());
    // A conflicting element empties the whole structured domain.
    let stringy = tuple(vec![of(ValueDomain::String), of(ValueDomain::Integer)]);
    assert!(pair.intersect(&stringy).is_empty());
    // An unrefined side keeps the refined shapes.
    assert_eq!(of(ValueDomain::Tuple).intersect(&pair), pair);
    // Compatible shapes intersect elementwise.
    let numeric = tuple(vec![
        Type::of_domains(DomainSet::NUMERIC),
        of(ValueDomain::Integer),
    ]);
    assert_eq!(pair.intersect(&numeric), pair);
}

#[test]
fn union_keeps_every_shape() {
    let pair = tuple(vec![of(ValueDomain::Integer), of(ValueDomain::Integer)]);
    let stringy = tuple(vec![of(ValueDomain::String), of(ValueDomain::Integer)]);
    // One label and arity: the elements unite in one shape.
    let both = pair.union(&stringy);
    let [Shape::Structural(shape)] = both.tuple_shapes().expect("refined") else {
        panic!("one shape: {both:?}");
    };
    assert_eq!(
        shape.elements[0].domains(),
        DomainSet::INTEGER.with(ValueDomain::String)
    );
    // Different arities, or labels, stay apart.
    let triple = tuple(vec![of(ValueDomain::Integer); 3]);
    assert_eq!(pair.union(&triple).tuple_shapes().map(<[_]>::len), Some(2));
    let point = labeled("point", vec![of(ValueDomain::Integer); 2]);
    let size = labeled("size", vec![of(ValueDomain::Integer); 2]);
    let either = point.union(&size);
    assert_eq!(either.tuple_shapes().map(<[_]>::len), Some(2));
    // So a third label is outside the union.
    let other = labeled("other", vec![of(ValueDomain::Integer); 2]);
    assert!(either.intersect(&other).is_empty());
    assert!(!other.is_subset(&either, &LabelTypes::NONE));
    // The order of the union does not matter.
    assert_eq!(either, size.union(&point));
    // A side without the structured domain leaves the shapes alone,
    // and a side holding it unrefined takes them in.
    assert!(
        pair.union(&of(ValueDomain::Integer))
            .tuple_shapes()
            .is_some()
    );
    assert!(pair.union(&of(ValueDomain::Tuple)).tuple_shapes().is_none());
}

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
    // Below the cap: the bare domain, no shapes.
    assert_eq!(typed.domains(), DomainSet::TUPLE);
    assert!(typed.tuple_shapes().is_none());
}

#[test]
fn declared_labels_stay_below_the_cap() {
    // A written-out shape below the cap is its domain,
    // but a declared label refers rather than nests.
    let declared = Type::declared("list", ValueDomain::Tuple);
    let nested = tuple(vec![declared.clone()]);
    assert_eq!(nested.capped(1), tuple(vec![declared.clone()]));
    assert_eq!(declared.capped(0), declared);
    let mixed = declared.union(&tuple(vec![]));
    assert!(mixed.capped(0).tuple_shapes().is_none());
}

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
    // Any tuple lies within the bare domain, and the bare domain within no shape.
    assert!(pair.is_subset(&of(ValueDomain::Tuple), none));
    assert!(!of(ValueDomain::Tuple).is_subset(&pair, none));
    // An exact label lies within an unknown one, not the other way.
    let anonymous = Type::tuple(TupleShape {
        label: LabelBound::Unknown,
        elements: vec![int.clone(), int.clone()],
    });
    let point = labeled("point", vec![int.clone(), int]);
    assert!(point.is_subset(&anonymous, none));
    assert!(!anonymous.is_subset(&point, none));
}

#[test]
fn a_declared_label_stands_for_its_declaration() {
    let labels = lists();
    let list = Type::declared("list", ValueDomain::Tuple);
    let nil = Type::declared("nil", ValueDomain::Tuple);
    let either = list.union(&nil);

    // A built list fits by its fields, the tail by its label alone.
    let built = labeled("list", vec![of(ValueDomain::Integer), either.clone()]);
    assert!(built.is_subset(&list, &labels));
    assert!(!built.is_subset(&nil, &labels));
    let wrong = labeled("list", vec![of(ValueDomain::String), either.clone()]);
    assert!(!wrong.is_subset(&list, &labels));
    // Without the declarations, nothing is shown.
    assert!(!built.is_subset(&list, &LabelTypes::NONE));
    // The label lies within its own declaration written out.
    assert!(list.is_subset(&built, &labels));

    // Admission reads the declaration through every level of the value.
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

    // A union absorbs written-out shapes of a declared label,
    // and an intersection keeps the label.
    assert_eq!(built.union(&list), list);
    assert_eq!(built.intersect(&list), list);
    assert!(list.intersect(&nil).is_empty());
}
