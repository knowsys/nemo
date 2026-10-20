use super::*;

use rand::{Rng, SeedableRng, rngs::StdRng};

use crate::{
    testing::assert_panics,
    value::{
        DataValue, DecimalRef, EvaluationError, LabelTypes, NumberRef, ValueRef, number::decimal,
    },
};

/// Builds a program, asserting well-formedness.
fn program(operations: Vec<OpCode>, constants: Vec<DataValue>, inputs: usize) -> StackProgram {
    StackProgram::new(operations, constants, inputs)
}

/// Evaluates a program over a row, returning an owned outcome.
fn outcome(program: &StackProgram, inputs: &[ValueRef<'_>]) -> Result<DataValue, EvaluationError> {
    let mut stack = Vec::new();
    let result = program.evaluate(EvaluationContext::now(), &mut stack, inputs);
    result.as_result().map(ValueRef::to_owned)
}

/// Applies one binary operation to two values.
fn apply(
    operation: OpCode,
    left: &DataValue,
    right: &DataValue,
) -> Result<DataValue, EvaluationError> {
    let compiled = program(
        vec![OpCode::Input(0), OpCode::Input(1), operation],
        vec![],
        2,
    );
    outcome(&compiled, &[left.as_ref(), right.as_ref()])
}

/// Shorthand for integer values.
fn int(value: i64) -> DataValue {
    DataValue::integer(value)
}

/// Shorthand for compact decimals.
fn dec(mantissa: i128, exponent: i64) -> DataValue {
    DataValue::decimal(mantissa, exponent).expect("value is representable")
}

#[test]
fn structures_are_built_from_the_stack() {
    use crate::value::{MapValue, TupleValue};

    // (?0, ?1)
    let compiled = program(
        vec![OpCode::Input(0), OpCode::Input(1), OpCode::Tuple(2)],
        vec![],
        2,
    );
    assert_eq!(
        outcome(&compiled, &[int(1).as_ref(), int(2).as_ref()]),
        Ok(DataValue::tuple(TupleValue::new(vec![int(1), int(2)])))
    );

    // point(?0), the label sitting below the elements it names.
    let compiled = program(
        vec![
            OpCode::Constant(0),
            OpCode::Input(0),
            OpCode::LabeledTuple(1),
        ],
        vec![DataValue::iri("point")],
        1,
    );
    assert_eq!(
        outcome(&compiled, &[int(7).as_ref()]),
        Ok(DataValue::tuple(TupleValue::labeled("point", vec![int(7)])))
    );

    // {?0 = ?1}
    let compiled = program(
        vec![OpCode::Input(0), OpCode::Input(1), OpCode::Map(1)],
        vec![],
        2,
    );
    assert_eq!(
        outcome(&compiled, &[DataValue::iri("k").as_ref(), int(1).as_ref()]),
        Ok(DataValue::map(
            MapValue::new([(DataValue::iri("k"), int(1))]).expect("the keys differ")
        ))
    );

    // {?0 = 1, ?0 = 2} — one key twice describes no map.
    let compiled = program(
        vec![
            OpCode::Input(0),
            OpCode::Constant(0),
            OpCode::Input(0),
            OpCode::Constant(1),
            OpCode::Map(2),
        ],
        vec![int(1), int(2)],
        1,
    );
    assert_eq!(
        outcome(&compiled, &[DataValue::iri("k").as_ref()]),
        Err(EvaluationError::Malformed)
    );

    // An error in a part is an error in the whole.
    let compiled = program(
        vec![
            OpCode::Input(0),
            OpCode::Input(1),
            OpCode::Divide,
            OpCode::Tuple(1),
        ],
        vec![],
        2,
    );
    assert_eq!(
        outcome(&compiled, &[int(1).as_ref(), int(0).as_ref()]),
        Err(EvaluationError::DivisionByZero)
    );
}

#[test]
fn accessors_read_components() {
    use crate::value::{DomainSet, MapValue, TupleValue};

    let tuple = DataValue::tuple(TupleValue::labeled(
        "point",
        vec![int(7), DataValue::string("x")],
    ));
    let map = DataValue::map(
        MapValue::new([(DataValue::iri("b"), int(2)), (DataValue::iri("a"), int(1))])
            .expect("the keys differ"),
    );

    // GET: a tuple's element at a 0-based index, a map's value at a
    // key; an absent component is Missing, a wrong operand a type
    // mismatch.
    let get = |structure: &DataValue, key: &DataValue| apply(OpCode::Get, structure, key);
    assert_eq!(get(&tuple, &int(0)), Ok(int(7)));
    assert_eq!(get(&tuple, &int(1)), Ok(DataValue::string("x")));
    assert_eq!(get(&tuple, &int(2)), Err(EvaluationError::Missing));
    assert_eq!(get(&tuple, &int(-1)), Err(EvaluationError::Missing));
    assert_eq!(
        get(&tuple, &DataValue::string("a")),
        Err(EvaluationError::TypeMismatch)
    );
    assert_eq!(get(&map, &DataValue::iri("a")), Ok(int(1)));
    assert_eq!(
        get(&map, &DataValue::iri("c")),
        Err(EvaluationError::Missing)
    );
    assert_eq!(get(&int(5), &int(0)), Err(EvaluationError::TypeMismatch));

    // GET reaches into a structure built earlier in the same
    // program — an owned intermediate, cloned rather than borrowed.
    let built = program(
        vec![
            OpCode::Input(0),
            OpCode::Input(1),
            OpCode::Tuple(2),
            OpCode::Constant(0),
            OpCode::Get,
        ],
        vec![int(1)],
        2,
    );
    assert_eq!(
        outcome(&built, &[int(4).as_ref(), int(5).as_ref()]),
        Ok(int(5))
    );

    // LENGTH counts elements or pairs.
    let length = program(vec![OpCode::Input(0), OpCode::Length], vec![], 1);
    assert_eq!(outcome(&length, &[tuple.as_ref()]), Ok(int(2)));
    assert_eq!(outcome(&length, &[map.as_ref()]), Ok(int(2)));
    assert_eq!(
        outcome(&length, &[int(5).as_ref()]),
        Err(EvaluationError::TypeMismatch)
    );

    // LABEL is total over structures: the label as an IRI, or the
    // empty string where there is none.
    let label = program(vec![OpCode::Input(0), OpCode::Label], vec![], 1);
    assert_eq!(
        outcome(&label, &[tuple.as_ref()]),
        Ok(DataValue::iri("point"))
    );
    assert_eq!(outcome(&label, &[map.as_ref()]), Ok(DataValue::string("")));
    assert_eq!(
        outcome(&label, &[int(5).as_ref()]),
        Err(EvaluationError::TypeMismatch)
    );

    // KEYS lists a map's keys in the canonical term order they are
    // stored in.
    let keys = program(vec![OpCode::Input(0), OpCode::Keys], vec![], 1);
    assert_eq!(
        outcome(&keys, &[map.as_ref()]),
        Ok(DataValue::tuple(TupleValue::new(vec![
            DataValue::iri("a"),
            DataValue::iri("b"),
        ])))
    );
    assert_eq!(
        outcome(&keys, &[tuple.as_ref()]),
        Err(EvaluationError::TypeMismatch)
    );

    // HASKEY and the domain tests.
    assert_eq!(
        apply(OpCode::HasKey, &map, &DataValue::iri("a")),
        Ok(DataValue::boolean(true))
    );
    assert_eq!(
        apply(OpCode::HasKey, &map, &DataValue::iri("c")),
        Ok(DataValue::boolean(false))
    );
    assert_eq!(
        apply(OpCode::HasKey, &tuple, &int(0)),
        Err(EvaluationError::TypeMismatch)
    );
    let is_tuple = program(
        vec![OpCode::Input(0), OpCode::InDomains(DomainSet::TUPLE)],
        vec![],
        1,
    );
    let is_map = program(
        vec![OpCode::Input(0), OpCode::InDomains(DomainSet::MAP)],
        vec![],
        1,
    );
    assert_eq!(
        outcome(&is_tuple, &[tuple.as_ref()]),
        Ok(DataValue::boolean(true))
    );
    assert_eq!(
        outcome(&is_tuple, &[map.as_ref()]),
        Ok(DataValue::boolean(false))
    );
    assert_eq!(
        outcome(&is_map, &[map.as_ref()]),
        Ok(DataValue::boolean(true))
    );
}

#[test]
fn accessor_inference_follows_the_operand_types() {
    use crate::value::{DomainSet, LabelBound, TupleShape, Type, ValueDomain};

    // A string key can never index a tuple; a possible unrefined
    // map takes any key. With a shape, an exact key projects the
    // element type.
    let get = |structure: Type, key: Type| {
        program(
            vec![OpCode::Input(0), OpCode::Input(1), OpCode::Get],
            vec![],
            2,
        )
        .infer(&[structure, key], &LabelTypes::NONE)
    };
    let tuple = Type::of(ValueDomain::Tuple);
    let string = Type::of(ValueDomain::String);
    let integer = Type::of(ValueDomain::Integer);
    assert!(get(tuple.clone(), string.clone()).bound().is_empty());
    assert_eq!(get(tuple.clone(), integer.clone()).into_bound(), Type::ANY);
    assert!(get(integer.clone(), integer.clone()).bound().is_empty());

    let pair = Type::tuple(TupleShape {
        label: LabelBound::Exactly(None),
        elements: vec![integer.clone(), string.clone()],
    });
    // A shape and a constant key give the element's own type.
    let projected = program(
        vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Get],
        vec![int(1)],
        1,
    )
    .infer(std::slice::from_ref(&pair), &LabelTypes::NONE);
    assert_eq!(projected.into_bound(), string);
    // A variable key gives the union of the elements.
    assert_eq!(
        get(pair.clone(), integer.clone()).domains(),
        DomainSet::INTEGER.with(ValueDomain::String)
    );

    // A known shape makes the length exact — which is what proves
    // `LENGTH(?t) = 3` false against an arity-2 column.
    let length = program(vec![OpCode::Input(0), OpCode::Length], vec![], 1);
    assert!(
        length
            .infer(std::slice::from_ref(&string), &LabelTypes::NONE)
            .bound()
            .is_empty()
    );
    assert_eq!(
        length
            .infer(
                &[Type::of_domains(DomainSet::STRUCTURED)],
                &LabelTypes::NONE
            )
            .into_bound(),
        integer
    );
    assert_eq!(
        length
            .infer(std::slice::from_ref(&pair), &LabelTypes::NONE)
            .exact()
            .cloned(),
        Some(int(2))
    );
    let dead = program(
        vec![
            OpCode::Input(0),
            OpCode::Length,
            OpCode::Constant(0),
            OpCode::Equal,
        ],
        vec![int(3)],
        1,
    )
    .infer(std::slice::from_ref(&pair), &LabelTypes::NONE);
    assert_eq!(dead.exact().cloned(), Some(DataValue::boolean(false)));

    // The guard's conjunction folds through: the whole condition is
    // exactly false, absorbing nothing real.
    let guard = program(
        vec![
            OpCode::Input(0),
            OpCode::InDomains(DomainSet::TUPLE),
            OpCode::Input(0),
            OpCode::Length,
            OpCode::Constant(0),
            OpCode::Equal,
            OpCode::And,
        ],
        vec![int(3)],
        1,
    )
    .infer(&[pair], &LabelTypes::NONE);
    assert_eq!(guard.exact().cloned(), Some(DataValue::boolean(false)));
}

#[test]
fn domain_tests_are_exact_where_the_kinds_decide() {
    use crate::value::{DomainSet, Type};

    let is_numeric = program(
        vec![OpCode::Input(0), OpCode::InDomains(DomainSet::NUMERIC)],
        vec![],
        1,
    );
    let exact = |kinds: DomainSet| {
        is_numeric
            .infer(&[Type::of_domains(kinds)], &LabelTypes::NONE)
            .exact()
            .cloned()
    };
    let (integer, double) = (DomainSet::INTEGER, DomainSet::DOUBLE);
    let iri = DomainSet::IRI;
    assert_eq!(exact(integer | double), Some(DataValue::boolean(true)));
    assert_eq!(exact(iri), Some(DataValue::boolean(false)));
    assert_eq!(exact(integer | iri), None);
}

#[test]
fn a_holding_condition_narrows_its_inputs() {
    use crate::value::{DomainSet, Type};

    let holds = |operations: Vec<OpCode>, constants: Vec<DataValue>, inputs: &[Type]| {
        program(operations, constants, inputs.len()).narrow(
            inputs,
            &LabelTypes::NONE,
            &Outcome::Holds,
        )
    };
    let any = || Type::ANY;
    let domains =
        |inputs: Vec<Type>| -> Vec<DomainSet> { inputs.iter().map(Type::domains).collect() };

    // A domain test holds for its domains, and its negation for the others.
    let is_integer = vec![OpCode::Input(0), OpCode::InDomains(DomainSet::INTEGER)];
    assert_eq!(
        domains(holds(is_integer.clone(), vec![], &[any()])),
        [DomainSet::INTEGER]
    );
    let mut is_not_integer = is_integer.clone();
    is_not_integer.push(OpCode::Not);
    let either = Type::of_domains(DomainSet::INTEGER | DomainSet::STRING);
    assert_eq!(
        domains(holds(is_not_integer, vec![], &[either])),
        [DomainSet::STRING]
    );

    // A comparison with a number holds only for numbers, by the signature.
    let greater = vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Greater];
    assert_eq!(
        domains(holds(greater, vec![int(3)], &[any()])),
        [DomainSet::NUMERIC]
    );

    // A conjunction holds where both hold, a disjunction where either does.
    let both = vec![
        OpCode::Input(0),
        OpCode::InDomains(DomainSet::INTEGER),
        OpCode::Input(1),
        OpCode::InDomains(DomainSet::STRING),
        OpCode::And,
    ];
    assert_eq!(
        domains(holds(both, vec![], &[any(), any()])),
        [DomainSet::INTEGER, DomainSet::STRING]
    );
    let either = vec![
        OpCode::Input(0),
        OpCode::InDomains(DomainSet::INTEGER),
        OpCode::Input(0),
        OpCode::InDomains(DomainSet::STRING),
        OpCode::Or,
    ];
    assert_eq!(
        domains(holds(either, vec![], &[any()])),
        [DomainSet::INTEGER | DomainSet::STRING]
    );

    // An identity narrows each side to the other.
    let same = vec![OpCode::Input(0), OpCode::Input(1), OpCode::Same];
    let integer = Type::of_domains(DomainSet::INTEGER);
    assert_eq!(
        domains(holds(same, vec![], &[any(), integer])),
        [DomainSet::INTEGER, DomainSet::INTEGER]
    );
}

#[test]
fn an_evaluation_narrows_its_inputs_to_the_result() {
    use crate::value::{DomainSet, Type};

    // A sum that is an integer has exact numbers as operands.
    let add = program(
        vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Add],
        vec![int(1)],
        1,
    );
    let narrowed = add.narrow(
        &[Type::ANY],
        &LabelTypes::NONE,
        &Outcome::Within(Type::of_domains(DomainSet::INTEGER)),
    );
    assert_eq!(narrowed[0].domains(), DomainSet::EXACT);
}

#[test]
fn structure_reads_narrow_by_label_and_length() {
    use crate::value::{DomainSet, LabelBound, Shape, TupleShape, Type, ValueDomain};

    let labeled = |label: &str, arity: usize| {
        Type::tuple(TupleShape {
            label: LabelBound::Exactly(Some(label.into())),
            elements: vec![Type::of_domains(DomainSet::INTEGER); arity],
        })
    };
    let label_is = program(
        vec![
            OpCode::Input(0),
            OpCode::Label,
            OpCode::Constant(0),
            OpCode::Same,
        ],
        vec![DataValue::iri("point")],
        1,
    );
    // Of the shapes, the one carrying the label stays.
    let either = labeled("point", 2).union(&labeled("size", 2));
    let narrowed = label_is.narrow(&[either], &LabelTypes::NONE, &Outcome::Holds);
    assert_eq!(narrowed[0], labeled("point", 2));

    // A label with a declared type gives that type, even to any value.
    let mut labels = LabelTypes::default();
    labels.declare("point", labeled("point", 2));
    let narrowed = label_is.narrow(&[Type::ANY], &labels, &Outcome::Holds);
    assert_eq!(narrowed[0], Type::declared("point", ValueDomain::Tuple));

    // A length gives an unrefined tuple domain its arity.
    let length_is = program(
        vec![
            OpCode::Input(0),
            OpCode::Length,
            OpCode::Constant(0),
            OpCode::Same,
        ],
        vec![int(2)],
        1,
    );
    let narrowed = length_is.narrow(
        &[Type::of(ValueDomain::Tuple)],
        &LabelTypes::NONE,
        &Outcome::Holds,
    );
    let Some([Shape::Structural(shape)]) = narrowed[0].tuple_shapes() else {
        panic!("one shape: {narrowed:?}");
    };
    assert_eq!(shape.elements.len(), 2);
    assert_eq!(shape.label, LabelBound::Unknown);
}

#[test]
fn extrema_are_numeric() {
    use crate::value::{DomainSet, Type, ValueDomain};

    let maximum = program(
        vec![OpCode::Input(0), OpCode::Input(1), OpCode::Maximum(2)],
        vec![],
        2,
    );
    assert_eq!(
        outcome(&maximum, &[int(1).as_ref(), dec(15, -1).as_ref()]),
        Ok(dec(15, -1))
    );
    assert_eq!(
        outcome(&maximum, &[ValueRef::String("a"), ValueRef::String("b")]),
        Err(EvaluationError::TypeMismatch)
    );
    // The inference agrees: strings have no extremum, and the
    // extremum of exact numbers is one of them.
    let string = Type::of(ValueDomain::String);
    assert!(
        maximum
            .infer(&[string.clone(), string], &LabelTypes::NONE)
            .bound()
            .is_empty()
    );
    let integer = Type::of(ValueDomain::Integer);
    let decimal = Type::of(ValueDomain::Decimal);
    assert_eq!(
        maximum
            .infer(&[integer, decimal], &LabelTypes::NONE)
            .domains(),
        DomainSet::EXACT
    );
}

#[test]
fn extrema_promote_to_the_smallest_common_type() {
    use crate::value::{DomainSet, Type};

    let (maximum, minimum) = (OpCode::Maximum(2), OpCode::Minimum(2));
    let (float, double) = (DataValue::float, DataValue::double);

    // The extreme operand takes the common type, as in `fn:max((1, 0.5e0))`;
    // exact numbers keep their domain.
    assert_eq!(apply(maximum, &int(1), &double(0.5)), Ok(double(1.0)));
    assert_eq!(apply(maximum, &int(0), &double(-1.5)), Ok(double(0.0)));
    assert_eq!(apply(minimum, &int(1), &float(0.5)), Ok(float(0.5)));
    assert_eq!(apply(maximum, &float(1.0), &double(0.5)), Ok(double(1.0)));
    assert_eq!(apply(maximum, &int(2), &dec(15, -1)), Ok(int(2)));

    // The order of the operands never matters,
    // neither for equal values of two domains nor for the two zeros.
    for (left, right) in [
        (int(1), double(1.0)),
        (int(0), double(-0.0)),
        (double(0.0), double(-0.0)),
    ] {
        for operation in [maximum, minimum] {
            assert_eq!(
                apply(operation, &left, &right),
                apply(operation, &right, &left)
            );
        }
    }
    assert_eq!(apply(maximum, &double(-0.0), &double(0.0)), Ok(double(0.0)));
    assert_eq!(
        apply(minimum, &double(0.0), &double(-0.0)),
        Ok(double(-0.0))
    );

    // The inference follows the ladder.
    let promoted = |left: DomainSet, right: DomainSet| {
        program(vec![OpCode::Input(0), OpCode::Input(1), maximum], vec![], 2)
            .infer(
                &[Type::of_domains(left), Type::of_domains(right)],
                &LabelTypes::NONE,
            )
            .domains()
    };
    let integer = DomainSet::INTEGER;
    let floats = DomainSet::FLOAT;
    let doubles = DomainSet::DOUBLE;
    assert_eq!(promoted(integer, doubles), doubles);
    assert_eq!(promoted(floats, doubles), doubles);
    assert_eq!(promoted(integer, floats | doubles), floats | doubles);
    assert_eq!(promoted(integer | doubles, integer), integer | doubles);
}

#[test]
fn the_nan_is_both_extrema() {
    let nan = DataValue::double(f64::NAN);

    assert_eq!(apply(OpCode::Maximum(2), &int(1), &nan), Ok(nan.clone()));
    assert_eq!(apply(OpCode::Minimum(2), &nan, &int(1)), Ok(nan.clone()));
    // The NaN takes the common type too.
    assert_eq!(
        apply(
            OpCode::Minimum(2),
            &DataValue::float(f32::NAN),
            &DataValue::double(1.0)
        ),
        Ok(nan.clone())
    );
    // An operand that is not a number is still an error.
    assert_eq!(
        apply(OpCode::Maximum(2), &nan, &DataValue::string("a")),
        Err(EvaluationError::TypeMismatch)
    );
}

#[test]
fn malformed_programs_are_rejected() {
    assert_panics("underflows", || {
        StackProgram::new(vec![OpCode::Add], vec![], 0)
    });
    assert_panics("exactly one result", || {
        StackProgram::new(vec![OpCode::Input(0), OpCode::Input(1)], vec![], 2)
    });
    assert_panics("constant", || {
        StackProgram::new(vec![OpCode::Constant(0)], vec![], 0)
    });
    assert_panics("input beyond", || {
        StackProgram::new(vec![OpCode::Input(0)], vec![], 0)
    });
    assert_panics("exactly one result", || {
        StackProgram::new(vec![], vec![], 0)
    });
}

#[test]
fn integer_arithmetic_is_exact() {
    assert_eq!(apply(OpCode::Add, &int(2), &int(3)), Ok(int(5)));
    assert_eq!(apply(OpCode::Multiply, &int(-4), &int(3)), Ok(int(-12)));
    assert_eq!(apply(OpCode::Divide, &int(6), &int(3)), Ok(int(2)));
    assert_eq!(apply(OpCode::Divide, &int(7), &int(2)), Ok(dec(35, -1)));

    // Results beyond i64 become wide integers instead of overflowing.
    let above = apply(OpCode::Add, &int(i64::MAX), &int(1)).expect("no error");
    assert_eq!(
        above,
        DataValue::decimal_from_lexical("9223372036854775808").expect("well-formed")
    );

    let negated = program(vec![OpCode::Input(0), OpCode::Negate], vec![], 1);
    assert_eq!(
        outcome(
            &negated,
            &[ValueRef::Number(NumberRef::Decimal(i64::MIN.into()))]
        ),
        Ok(DataValue::decimal_from_lexical("9223372036854775808").expect("well-formed"))
    );
}

#[test]
fn division_of_exact_numbers_yields_decimals() {
    // A non-terminating quotient is rounded at eighteen digits.
    assert_eq!(
        apply(OpCode::Divide, &int(1), &int(3)),
        Ok(dec(333_333_333_333_333_333, -18))
    );
    assert_eq!(
        apply(OpCode::Divide, &int(1), &int(0)),
        Err(EvaluationError::DivisionByZero)
    );
    // Floats divide and overflow as IEEE does, without error.
    assert_eq!(
        apply(
            OpCode::Divide,
            &DataValue::double(1.0),
            &DataValue::double(0.0)
        ),
        Ok(DataValue::double(f64::INFINITY))
    );
    assert_eq!(
        apply(
            OpCode::Divide,
            &DataValue::double(1e300),
            &DataValue::double(1e-300)
        ),
        Ok(DataValue::double(f64::INFINITY))
    );
    assert_eq!(
        apply(
            OpCode::Multiply,
            &DataValue::double(f64::MAX),
            &DataValue::double(2.0)
        ),
        Ok(DataValue::double(f64::INFINITY))
    );
}

#[test]
fn promotion_takes_the_smallest_common_type() {
    assert_eq!(apply(OpCode::Add, &int(1), &dec(15, -1)), Ok(dec(25, -1)));
    assert_eq!(
        apply(OpCode::Add, &int(1), &DataValue::double(0.5)),
        Ok(DataValue::double(1.5))
    );
    assert_eq!(
        apply(OpCode::Add, &dec(15, -1), &DataValue::float(1.0)),
        Ok(DataValue::float(2.5))
    );
    assert_eq!(
        apply(
            OpCode::Add,
            &DataValue::float(1.0),
            &DataValue::double(0.25)
        ),
        Ok(DataValue::double(1.25))
    );
}

#[test]
fn decimal_arithmetic_stays_exact() {
    assert_eq!(apply(OpCode::Add, &dec(15, -1), &dec(25, -1)), Ok(int(4)));
    assert_eq!(
        apply(OpCode::Multiply, &dec(1, -10), &dec(1, -10)),
        Ok(dec(1, -20))
    );
    // A product needing more than 16 mantissa digits becomes a wide
    // decimal, exactly.
    let product = apply(
        OpCode::Multiply,
        &dec(1_234_567_890_123_456, -4),
        &dec(1_000_000_000_000_003, -4),
    )
    .expect("no error");
    assert!(matches!(
        product.as_ref(),
        ValueRef::Number(NumberRef::Decimal(DecimalRef(decimal::ReprRef::Wide(_))))
    ));
}

/// A wide number from its canonical lexical.
fn big(lexical: &str) -> DataValue {
    let value = DataValue::decimal_from_lexical(lexical).expect("well-formed");
    assert!(
        matches!(
            value.as_ref(),
            ValueRef::Number(NumberRef::Decimal(DecimalRef(decimal::ReprRef::Wide(_))))
        ),
        "{lexical} is compact"
    );
    value
}

/// Applies a unary operation to one value.
fn apply_unary(operation: OpCode, value: &DataValue) -> Result<DataValue, EvaluationError> {
    let program = program(vec![OpCode::Input(0), operation], vec![], 1);
    outcome(&program, &[value.as_ref()])
}

#[test]
fn wide_number_arithmetic_is_exact() {
    let big_int = big("123456789012345678901");
    let big_dec = big("0.0000000000000000000012345678901234567890123");

    assert_eq!(
        apply(OpCode::Add, &big_int, &int(1)),
        Ok(big("123456789012345678902"))
    );
    // A result back within 64 bits is the integer term.
    assert_eq!(
        apply(OpCode::Subtract, &big_int, &big("123456789012345678900")),
        Ok(int(1))
    );
    assert_eq!(
        apply(OpCode::Multiply, &big_int, &big_int),
        Ok(big("15241578753238836750437433565526596567801"))
    );
    assert_eq!(
        apply(OpCode::Add, &big_int, &big_dec),
        Ok(big(
            "123456789012345678901.0000000000000000000012345678901234567890123"
        ))
    );
    assert_eq!(
        apply(OpCode::Divide, &big_int, &int(100)),
        Ok(big("1234567890123456789.01"))
    );
    // A non-terminating quotient never loses integer digits.
    assert_eq!(
        apply(
            OpCode::Divide,
            &big("1000000000000000000000000000000"),
            &int(3)
        ),
        Ok(big("333333333333333333333333333333.333333333333333333"))
    );
    assert_eq!(
        apply(OpCode::Divide, &int(1), &big_int),
        Ok(dec(810_000_007_290_000_066, -38))
    );
    assert_eq!(
        apply(OpCode::Divide, &big_int, &int(0)),
        Err(EvaluationError::DivisionByZero)
    );
    assert_eq!(apply(OpCode::Remainder, &big_int, &int(1000)), Ok(int(901)));
    assert_eq!(
        apply(OpCode::Remainder, &big_int, &big("100000000000000000000")),
        Ok(big("23456789012345678901"))
    );
    assert_eq!(
        apply(OpCode::Remainder, &big_dec, &int(2)),
        Err(EvaluationError::TypeMismatch)
    );

    // Floating operands approximate, as for every exact number.
    assert_eq!(
        apply(OpCode::Add, &big_int, &DataValue::double(0.5)),
        Ok(DataValue::double(1.2345678901234568e20))
    );
    assert_eq!(
        apply(OpCode::Multiply, &big_int, &DataValue::float(2.0)),
        Ok(DataValue::float(2.469_135_8e20))
    );
    assert_eq!(
        apply_unary(
            OpCode::SquareRoot,
            &big("100000000000000000000000000000000000000000")
        ),
        Ok(DataValue::double(3.1622776601683794e20))
    );
}

#[test]
fn compact_workspace_overflow_upgrades_to_wide_numbers() {
    // Aligning far-apart decimals outgrows the i128 workspace.
    assert_eq!(
        apply(OpCode::Add, &dec(1, -1), &dec(1, -40)),
        Ok(big("0.1000000000000000000000000000000000000001"))
    );
    assert_eq!(
        apply(OpCode::Subtract, &dec(1, -1), &dec(1, -40)),
        Ok(big("0.0999999999999999999999999999999999999999"))
    );
    // Scaling a numerator for its integer digits does too.
    assert_eq!(
        apply(OpCode::Divide, &int(7_000_000_000_000_000_000), &dec(3, -1)),
        Ok(big("23333333333333333333.333333333333333333"))
    );
    assert_eq!(
        apply(OpCode::Divide, &int(1), &dec(3, -50)),
        Ok(big(
            "33333333333333333333333333333333333333333333333333.33333333333333333333333333333333333333333333333333"
        ))
    );
    // Powers grow past the workspace.
    assert_eq!(
        apply(OpCode::Power, &int(2), &int(200)),
        Ok(big(
            "1606938044258990275541962092341162602522202993782792835301376"
        ))
    );
    assert_eq!(
        apply(OpCode::Power, &big("100000000000000000000"), &int(2)),
        Ok(big("10000000000000000000000000000000000000000"))
    );
    // A negative exponent leaves the exact path for the doubles.
    assert_eq!(
        apply(OpCode::Power, &big("100000000000000000000"), &int(-1)),
        Ok(DataValue::double(1e-20))
    );
    // The big workspace is bounded.
    let huge = apply(OpCode::Power, &int(10), &int(100_000)).expect("within the limit");
    assert_eq!(
        apply(OpCode::Power, &huge, &int(20)),
        Err(EvaluationError::Overflow)
    );
}

#[test]
fn wide_numbers_round_and_cast_like_compact_ones() {
    use crate::value::DomainSet;

    let half = big("123456789012345678901.5");
    assert_eq!(
        apply_unary(OpCode::Round, &half),
        Ok(big("123456789012345678902"))
    );
    assert_eq!(
        apply_unary(OpCode::Round, &big("-123456789012345678901.5")),
        Ok(big("-123456789012345678901"))
    );
    assert_eq!(apply_unary(OpCode::Round, &dec(25, -1)), Ok(int(3)));
    assert_eq!(
        apply_unary(OpCode::Ceiling, &half),
        Ok(big("123456789012345678902"))
    );
    assert_eq!(
        apply_unary(OpCode::Floor, &half),
        Ok(big("123456789012345678901"))
    );
    assert_eq!(
        apply_unary(OpCode::Floor, &big("-123456789012345678901.5")),
        Ok(big("-123456789012345678902"))
    );
    assert_eq!(
        apply_unary(OpCode::Absolute, &big("-123456789012345678901.5")),
        Ok(half.clone())
    );
    assert_eq!(
        apply_unary(OpCode::Negate, &big("9223372036854775808")),
        Ok(int(i64::MIN))
    );

    let big_int = big("123456789012345678901");
    assert_eq!(
        apply_unary(OpCode::CastInteger, &big_int),
        Ok(big_int.clone())
    );
    assert_eq!(
        apply_unary(
            OpCode::CastInteger,
            &DataValue::string("123456789012345678901")
        ),
        Ok(big_int.clone())
    );
    assert_eq!(apply_unary(OpCode::CastInteger, &half), Ok(big_int.clone()));
    assert_eq!(
        apply_unary(OpCode::CastDouble, &big_int),
        Ok(DataValue::double(1.2345678901234568e20))
    );
    assert_eq!(
        apply_unary(OpCode::InDomains(DomainSet::INTEGER), &big_int),
        Ok(DataValue::boolean(true))
    );
    assert_eq!(
        apply_unary(OpCode::InDomains(DomainSet::INTEGER), &half),
        Ok(DataValue::boolean(false))
    );
}

#[test]
fn big_constants_fold() {
    let program = program(
        vec![OpCode::Constant(0), OpCode::Constant(1), OpCode::Add],
        vec![big("123456789012345678901"), int(1)],
        0,
    );
    let inferred = program.infer(&[], &LabelTypes::NONE);
    assert_eq!(
        inferred.exact().cloned(),
        Some(big("123456789012345678902"))
    );
}

#[test]
fn identity_is_total_and_per_domain() {
    let truth = |result: Result<DataValue, EvaluationError>| {
        result.map(|value| value == DataValue::boolean(true))
    };

    // Numerically equal values of different domains are distinct
    // terms; value equality (`==`) is where they meet.
    assert_eq!(
        truth(apply(OpCode::Same, &int(1), &DataValue::double(1.0))),
        Ok(false)
    );
    assert_eq!(truth(apply(OpCode::Same, &int(1), &int(1))), Ok(true));
    // Identity never errors — a mixed literal pair is simply
    // distinct, where `==` reports an error.
    assert_eq!(
        truth(apply(OpCode::Same, &int(1), &DataValue::string("1"))),
        Ok(false)
    );
    // The canonical NaN is one term identical to itself, though `==`
    // follows IEEE and denies it.
    let nan = DataValue::double(f64::NAN);
    assert_eq!(truth(apply(OpCode::Same, &nan, &nan)), Ok(true));
}

#[test]
fn comparisons_follow_value_semantics() {
    let truth = |result: Result<DataValue, EvaluationError>| {
        result.map(|value| value == DataValue::boolean(true))
    };

    assert_eq!(truth(apply(OpCode::Less, &int(1), &dec(15, -1))), Ok(true));
    assert_eq!(
        truth(apply(OpCode::Equal, &int(1), &DataValue::double(1.0))),
        Ok(true)
    );
    assert_eq!(
        truth(apply(
            OpCode::Less,
            &DataValue::string("a"),
            &DataValue::string("b")
        )),
        Ok(true)
    );
    assert_eq!(
        truth(apply(
            OpCode::Less,
            &DataValue::boolean(false),
            &DataValue::boolean(true)
        )),
        Ok(true)
    );

    // NaN compares false under every predicate, including equality.
    let nan = DataValue::double(f64::NAN);
    assert_eq!(truth(apply(OpCode::Equal, &nan, &nan)), Ok(false));
    assert_eq!(
        truth(apply(OpCode::Less, &nan, &DataValue::double(1.0))),
        Ok(false)
    );

    // Wide numbers compare exactly.
    let big = DataValue::decimal_from_lexical("123456789012345678901").expect("well-formed");
    assert_eq!(
        truth(apply(OpCode::Greater, &big, &int(i64::MAX))),
        Ok(true)
    );

    // Terms without an order or comparable values.
    let iri = DataValue::iri("http://example.org/a");
    assert_eq!(
        apply(OpCode::Less, &iri, &int(1)),
        Err(EvaluationError::Incomparable)
    );
    assert_eq!(
        apply(OpCode::Equal, &int(1), &DataValue::string("1")),
        Err(EvaluationError::Incomparable)
    );
    // A non-literal never equals a value of another domain.
    assert_eq!(truth(apply(OpCode::Equal, &iri, &int(1))), Ok(false));
}

#[test]
fn datetime_comparison_uses_the_instant_only() {
    // The same instant under different offsets: equal values (XSD),
    // distinct terms (identity).
    let utc = DataValue::datetime_from_lexical("1970-01-01T00:00:00Z").expect("valid");
    let shifted = DataValue::datetime_from_lexical("1970-01-01T02:00:00+02:00").expect("valid");
    assert_ne!(utc, shifted);
    assert_eq!(
        apply(OpCode::Equal, &utc, &shifted),
        Ok(DataValue::boolean(true))
    );
}

#[test]
fn temporal_arithmetic_evaluates_and_infers_its_kinds() {
    use crate::value::{DomainSet, Type, ValueDomain};

    let typed = |datatype: &str, lexical: &str| {
        DataValue::typed_literal(
            &format!("http://www.w3.org/2001/XMLSchema#{datatype}"),
            lexical,
        )
        .expect(lexical)
    };
    let (start, end) = (typed("date", "2024-03-01"), typed("date", "2024-03-15"));
    let two_weeks = typed("dayTimeDuration", "P14D");

    assert_eq!(apply(OpCode::Subtract, &end, &start), Ok(two_weeks.clone()));
    assert_eq!(apply(OpCode::Add, &start, &two_weeks), Ok(end.clone()));
    assert_eq!(
        apply(OpCode::Less, &start, &end),
        Ok(DataValue::boolean(true))
    );
    assert_eq!(
        apply(OpCode::Adjust, &start, &typed("dayTimeDuration", "PT1H")),
        Ok(typed("date", "2024-03-01+01:00"))
    );

    let kinds = |operation, left: ValueDomain, right: ValueDomain| {
        program(
            vec![OpCode::Input(0), OpCode::Input(1), operation],
            vec![],
            2,
        )
        .infer(&[Type::of(left), Type::of(right)], &LabelTypes::NONE)
        .domains()
    };
    assert_eq!(
        kinds(OpCode::Subtract, ValueDomain::Date, ValueDomain::Date),
        DomainSet::DAY_TIME_DURATION
    );
    assert_eq!(
        kinds(
            OpCode::Add,
            ValueDomain::YearMonthDuration,
            ValueDomain::DateTime
        ),
        DomainSet::DATE_TIME
    );
    assert_eq!(
        kinds(
            OpCode::Multiply,
            ValueDomain::Decimal,
            ValueDomain::DayTimeDuration
        ),
        DomainSet::ORDERED_DURATION
    );
    assert_eq!(
        kinds(
            OpCode::Divide,
            ValueDomain::DayTimeDuration,
            ValueDomain::DayTimeDuration
        ),
        DomainSet::EXACT
    );
    assert_eq!(
        kinds(
            OpCode::Adjust,
            ValueDomain::Time,
            ValueDomain::DayTimeDuration
        ),
        DomainSet::TIME
    );
    assert!(kinds(OpCode::Add, ValueDomain::String, ValueDomain::Date).is_empty());
    assert!(kinds(OpCode::Less, ValueDomain::Date, ValueDomain::Time).is_empty());
    assert!(
        !kinds(
            OpCode::Less,
            ValueDomain::DayTimeDuration,
            ValueDomain::YearMonthDuration
        )
        .is_empty()
    );
}

#[test]
fn logic_is_three_valued() {
    let filter = |operations: Vec<OpCode>, inputs: &[ValueRef<'_>]| {
        let compiled = program(operations, vec![], inputs.len());
        let mut stack = Vec::new();
        compiled.filter(EvaluationContext::now(), &mut stack, inputs)
    };
    let error_producing = vec![OpCode::Input(0), OpCode::Input(1), OpCode::Less];
    let iri = DataValue::iri("http://example.org/a");

    // Or(true, error) is true; And(false, error) is false.
    let mut or_program = error_producing.clone();
    or_program.extend([OpCode::Input(2), OpCode::Or]);
    assert_eq!(
        filter(
            or_program,
            &[
                iri.as_ref(),
                ValueRef::Number(NumberRef::Decimal(1.into())),
                ValueRef::Boolean(true)
            ]
        ),
        Ok(true)
    );

    let mut and_program = error_producing.clone();
    and_program.extend([OpCode::Input(2), OpCode::And]);
    assert_eq!(
        filter(
            and_program.clone(),
            &[
                iri.as_ref(),
                ValueRef::Number(NumberRef::Decimal(1.into())),
                ValueRef::Boolean(false)
            ]
        ),
        Ok(false)
    );
    // And(error, true) stays the error.
    assert_eq!(
        filter(
            and_program,
            &[
                iri.as_ref(),
                ValueRef::Number(NumberRef::Decimal(1.into())),
                ValueRef::Boolean(true)
            ]
        ),
        Err(EvaluationError::Incomparable)
    );

    // Not propagates errors; != via Not(Equal) makes NaN unequal to
    // itself.
    let not_equal = vec![
        OpCode::Input(0),
        OpCode::Input(1),
        OpCode::Equal,
        OpCode::Not,
    ];
    assert_eq!(
        filter(
            not_equal,
            &[
                DataValue::double(f64::NAN).as_ref(),
                DataValue::double(f64::NAN).as_ref()
            ]
        ),
        Ok(true)
    );
}

#[test]
fn effective_boolean_values_follow_sparql() {
    let filter_one = |value: ValueRef<'_>| {
        let compiled = program(vec![OpCode::Input(0)], vec![], 1);
        let mut stack = Vec::new();
        compiled.filter(EvaluationContext::now(), &mut stack, &[value])
    };
    assert_eq!(filter_one(ValueRef::Boolean(true)), Ok(true));
    assert_eq!(
        filter_one(ValueRef::Number(NumberRef::Decimal(0.into()))),
        Ok(false)
    );
    assert_eq!(filter_one(DataValue::double(f64::NAN).as_ref()), Ok(false));
    assert_eq!(filter_one(DataValue::double(-0.0).as_ref()), Ok(false));
    assert_eq!(filter_one(ValueRef::String("")), Ok(false));
    assert_eq!(filter_one(ValueRef::String("x")), Ok(true));
    let iri = DataValue::iri("http://example.org/a");
    assert_eq!(filter_one(iri.as_ref()), Err(EvaluationError::TypeMismatch));
}

#[test]
fn string_operations_work_on_characters() {
    let length = program(vec![OpCode::Input(0), OpCode::StringLength], vec![], 1);
    assert_eq!(outcome(&length, &[ValueRef::String("häuser")]), Ok(int(6)));

    let tagged = DataValue::language_string("hello", "en").expect("tag is nonempty");
    assert_eq!(outcome(&length, &[tagged.as_ref()]), Ok(int(5)));

    let upper = program(vec![OpCode::Input(0), OpCode::UpperCase], vec![], 1);
    assert_eq!(
        outcome(&upper, &[tagged.as_ref()]),
        Ok(DataValue::language_string("HELLO", "en").expect("tag is nonempty"))
    );

    // Language compatibility: a plain pattern always fits, a tagged
    // pattern only under the same tag.
    assert_eq!(
        apply(OpCode::Contains, &tagged, &DataValue::string("ell")),
        Ok(DataValue::boolean(true))
    );
    let german = DataValue::language_string("ell", "de").expect("tag is nonempty");
    assert_eq!(
        apply(OpCode::Contains, &tagged, &german),
        Err(EvaluationError::Malformed)
    );

    let concat = program(
        vec![
            OpCode::Input(0),
            OpCode::Constant(0),
            OpCode::Input(1),
            OpCode::Concat(3),
        ],
        vec![DataValue::string(", ")],
        2,
    );
    assert_eq!(
        outcome(&concat, &[ValueRef::String("a"), ValueRef::String("b")]),
        Ok(DataValue::string("a, b"))
    );
    assert_eq!(
        outcome(
            &concat,
            &[
                ValueRef::Number(NumberRef::Decimal(1.into())),
                ValueRef::String("b")
            ]
        ),
        Err(EvaluationError::TypeMismatch)
    );
}

#[test]
fn string_pairs_infer_as_the_evaluator_takes_them() {
    use crate::value::{DomainSet, Type, ValueDomain};

    let kinds = |operation, left: ValueDomain, right: ValueDomain| {
        program(
            vec![OpCode::Input(0), OpCode::Input(1), operation],
            vec![],
            2,
        )
        .infer(&[Type::of(left), Type::of(right)], &LabelTypes::NONE)
        .domains()
    };
    let (plain, tagged) = (ValueDomain::String, ValueDomain::LanguageString);
    let text = DataValue::string("motor car");
    let english = DataValue::language_string("motor car", "en").expect("tag is nonempty");

    // The second string of a pair is plain or carries the tag of the first.
    assert_eq!(kinds(OpCode::Contains, tagged, plain), DomainSet::BOOLEAN);
    assert!(kinds(OpCode::Contains, plain, tagged).is_empty());
    assert_eq!(
        apply(OpCode::Contains, &text, &english),
        Err(EvaluationError::TypeMismatch)
    );
    assert!(kinds(OpCode::StringBefore, ValueDomain::Integer, plain).is_empty());
    assert_eq!(
        apply(OpCode::StringBefore, &int(1), &text),
        Err(EvaluationError::TypeMismatch)
    );

    // A part of a tagged string is tagged, and a miss is a plain empty string.
    assert_eq!(
        kinds(OpCode::StringAfter, tagged, tagged),
        DomainSet::STRINGLIKE
    );
    assert_eq!(
        kinds(OpCode::StringAfter, plain, plain),
        DomainSet::of(plain)
    );

    // A tag and a range are plain strings.
    assert!(kinds(OpCode::StringLanguage, tagged, plain).is_empty());
    assert_eq!(
        apply(OpCode::StringLanguage, &english, &DataValue::string("de")),
        Err(EvaluationError::TypeMismatch)
    );
    assert!(kinds(OpCode::LanguageMatches, plain, tagged).is_empty());
}

#[test]
fn integer_specialization_matches_the_generic_interpreter() {
    // ((a + b) * 2 < c) or (a = 5)
    let compiled = program(
        vec![
            OpCode::Input(0),
            OpCode::Input(1),
            OpCode::Add,
            OpCode::Constant(0),
            OpCode::Multiply,
            OpCode::Input(2),
            OpCode::Less,
            OpCode::Input(0),
            OpCode::Constant(1),
            OpCode::Equal,
            OpCode::Or,
        ],
        vec![int(2), int(5)],
        3,
    );
    let specialized = compiled
        .integer_specialization()
        .expect("the program is in the integer fragment");

    let mut stack = Vec::new();
    let mut integer_stack = Vec::new();
    let mut rng = StdRng::seed_from_u64(13);
    for _ in 0..500 {
        let row: Vec<i64> = (0..3).map(|_| rng.gen_range(-100..100)).collect();
        let values: Vec<ValueRef<'_>> = row
            .iter()
            .map(|&value| ValueRef::Number(NumberRef::Decimal(value.into())))
            .collect();

        let generic = compiled
            .filter(EvaluationContext::now(), &mut stack, &values)
            .expect("no errors");
        let fast = specialized
            .filter(&mut integer_stack, &row)
            .expect("no overflow in this range");
        assert_eq!(generic, fast, "row {row:?}");
    }

    // If an intermediate outgrows i64, the integer form asks for the
    // generic fallback, which carries the sum on as a wide integer.
    let overflowing = [i64::MAX, 1, 0];
    assert_eq!(specialized.filter(&mut integer_stack, &overflowing), None);
    let values: Vec<ValueRef<'_>> = overflowing
        .iter()
        .map(|&value| ValueRef::Number(NumberRef::Decimal(value.into())))
        .collect();
    assert_eq!(
        compiled.filter(EvaluationContext::now(), &mut stack, &values),
        Ok(false)
    );
}

#[test]
fn identity_specialization_recognizes_bare_identities() {
    let iri = DataValue::iri("a");
    let recognize = |operations: Vec<OpCode>, inputs: usize| {
        program(operations, vec![iri.clone()], inputs).identity_specialization()
    };

    let between = recognize(vec![OpCode::Input(1), OpCode::Input(0), OpCode::Same], 2)
        .expect("an identity of two inputs");
    assert_eq!(between.input, 1);
    assert!(matches!(between.operand, IdentityOperand::Input(0)));
    assert!(!between.negated);

    let against = recognize(
        vec![
            OpCode::Constant(0),
            OpCode::Input(0),
            OpCode::Same,
            OpCode::Not,
        ],
        1,
    )
    .expect("a negated identity with a constant");
    assert_eq!(against.input, 0);
    assert!(matches!(against.operand, IdentityOperand::Constant(ref value) if *value == iri));
    assert!(against.negated);

    // Value equality, and identity with a computed operand, are no bare identities.
    assert!(recognize(vec![OpCode::Input(0), OpCode::Input(1), OpCode::Equal], 2).is_none());
    let computed = vec![
        OpCode::Input(0),
        OpCode::Input(1),
        OpCode::Concat(1),
        OpCode::Same,
    ];
    assert!(recognize(computed, 2).is_none());
}

#[test]
fn programs_outside_the_integer_fragment_do_not_specialize() {
    let division = program(
        vec![OpCode::Input(0), OpCode::Input(1), OpCode::Divide],
        vec![],
        2,
    );
    assert!(division.integer_specialization().is_none());

    let stringly = program(
        vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Equal],
        vec![DataValue::string("x")],
        1,
    );
    assert!(stringly.integer_specialization().is_none());
}

#[test]
fn bindings_are_recognized_on_either_side() {
    // c = a + 1 over inputs (c, a), and a + 1 = c over inputs (a, c),
    // both bind the compared input to the expression over the other.
    let increment = |first: u16| vec![OpCode::Input(first), OpCode::Constant(0), OpCode::Add];
    let one = vec![DataValue::integer(1)];

    let mut left = vec![OpCode::Input(0)];
    left.extend(increment(1));
    left.push(OpCode::Same);
    let (compared, expression) = program(left, one.clone(), 2).binding().expect("a binding");
    assert_eq!(compared, 0);
    assert_eq!(expression, program(increment(0), one.clone(), 1));

    let mut right = increment(0);
    right.extend([OpCode::Input(1), OpCode::Same]);
    let (compared, expression) = program(right, one.clone(), 2).binding().expect("a binding");
    assert_eq!(compared, 1);
    assert_eq!(expression, program(increment(0), one.clone(), 1));

    // The inverse of shifting.
    let shifted = program(increment(0), one.clone(), 1).shifted_equality();
    assert_eq!(shifted.binding().map(|(compared, _)| compared), Some(0));
}

#[test]
fn constant_bindings_are_recognized_on_either_side() {
    let one = vec![DataValue::integer(1)];
    let binding = |operations: Vec<OpCode>, inputs: usize| {
        program(operations, one.clone(), inputs).constant_binding()
    };

    let left = binding(vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Same], 1);
    assert_eq!(left, Some((0, DataValue::integer(1))));
    let right = binding(vec![OpCode::Constant(0), OpCode::Input(1), OpCode::Same], 2);
    assert_eq!(right, Some((1, DataValue::integer(1))));

    // Difference cannot be looked up, nor can value equality or another input.
    let negated = vec![
        OpCode::Input(0),
        OpCode::Constant(0),
        OpCode::Same,
        OpCode::Not,
    ];
    assert_eq!(binding(negated, 1), None);
    let equal = vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Equal];
    assert_eq!(binding(equal, 1), None);
    let inputs = vec![OpCode::Input(0), OpCode::Input(1), OpCode::Same];
    assert_eq!(binding(inputs, 2), None);
}

#[test]
fn identities_and_draws_are_no_bindings() {
    // A bare input is the identity form's, and a constant a constant binding;
    // an expression reading the compared input, reading no input, or drawing is no binding;
    // value equality cannot be looked up.
    let none = |operations: Vec<OpCode>, inputs: usize| {
        assert!(
            program(operations.clone(), vec![DataValue::integer(1)], inputs)
                .binding()
                .is_none(),
            "{operations:?}"
        );
    };
    none(vec![OpCode::Input(0), OpCode::Input(1), OpCode::Same], 2);
    none(vec![OpCode::Input(0), OpCode::Constant(0), OpCode::Same], 1);
    none(
        vec![
            OpCode::Input(0),
            OpCode::Input(0),
            OpCode::Constant(0),
            OpCode::Add,
            OpCode::Same,
        ],
        1,
    );
    none(
        vec![
            OpCode::Input(0),
            OpCode::Constant(0),
            OpCode::Constant(0),
            OpCode::Add,
            OpCode::Same,
        ],
        1,
    );
    none(
        vec![
            OpCode::Input(0),
            OpCode::Input(1),
            OpCode::Random,
            OpCode::Add,
            OpCode::Same,
        ],
        2,
    );
    none(
        vec![
            OpCode::Input(0),
            OpCode::Input(1),
            OpCode::Constant(0),
            OpCode::Add,
            OpCode::Equal,
        ],
        2,
    );
}
