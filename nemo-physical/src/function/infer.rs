//! Inference: the evaluation of stack programs over types.
//!
//! Inference evaluates a program on abstract values.
//! An abstract value, an [Inferred], describes the values an evaluation yields if it succeeds:
//! - exactly, as the one value every success yields;
//! - or by a bound, a [Type] every such value lies in.
//!
//! The description over-approximates,
//! so an empty bound proves that the evaluation fails on every input.
//!
//! [transfer] answers one instruction over abstract operands in up to three steps:
//! - Folding: a deterministic instruction over exact operands runs the evaluator itself.
//! - The signature: the instruction's [signature](super::OpCode::signature)
//!   bounds the result by the domains the operands can lie in.
//! - Refinement: the connectives, the identity and domain tests, and the structure instructions
//!   narrow that bound through the shapes and exact values of their operands.

use std::borrow::Cow;

use crate::{
    function::{EvaluationContext, OpCode, StackProgram},
    value::{
        DataValue, DomainSet, LabelBound, LabelTypes, MapShape, Shape, TupleShape, TupleValue,
        Type, ValueDomain, ValueRef,
    },
};

#[cfg(debug_assertions)]
use crate::function::StackValue;

/// What inference knows of the values an evaluation yields if it succeeds.
///
/// The knowledge over-approximates,
/// so an empty bound proves that the evaluation fails on every input.
#[derive(Debug, Clone)]
pub enum Inferred {
    /// The one value every successful evaluation yields.
    Exact(DataValue),
    /// The type every value a successful evaluation yields lies in.
    Bound(Type),
}

impl Inferred {
    /// Returns the type every value lies in:
    /// the bound, or the precise type of the exact value.
    pub fn bound(&self) -> Cow<'_, Type> {
        match self {
            Inferred::Exact(value) => Cow::Owned(Type::of_value(value.as_ref())),
            Inferred::Bound(bound) => Cow::Borrowed(bound),
        }
    }

    /// Returns the type every value lies in. See [bound](Self::bound).
    pub fn into_bound(self) -> Type {
        match self {
            Inferred::Exact(value) => Type::of_value(value.as_ref()),
            Inferred::Bound(bound) => bound,
        }
    }

    /// Returns the domains every value lies in.
    pub fn domains(&self) -> DomainSet {
        match self {
            Inferred::Exact(value) => DomainSet::of(value.domain()),
            Inferred::Bound(bound) => bound.domains(),
        }
    }

    /// Returns the one value every successful evaluation yields, if known.
    pub fn exact(&self) -> Option<&DataValue> {
        match self {
            Inferred::Exact(value) => Some(value),
            Inferred::Bound(_) => None,
        }
    }

    /// Returns whether a value is one this knowledge allows:
    /// the same term as the exact value, or a value within the bound.
    #[cfg(debug_assertions)]
    fn admits(&self, value: ValueRef<'_>) -> bool {
        match self {
            Inferred::Exact(exact) => exact.as_ref() == value,
            Inferred::Bound(bound) => bound.admits(value, &LabelTypes::NONE),
        }
    }

    /// Returns this knowledge narrowed to values in the given domains.
    fn within(self, domains: DomainSet) -> Self {
        match self {
            Inferred::Exact(value) if domains.contains(value.domain()) => Inferred::Exact(value),
            Inferred::Exact(_) => Inferred::Bound(Type::EMPTY),
            Inferred::Bound(bound) => Inferred::Bound(bound.intersect(&Type::of_domains(domains))),
        }
    }
}

/// Returns what an instruction yields if it succeeds,
/// given what its operands are, in operand order,
/// and the types declared for the labels their types refer to;
/// see the module documentation for the steps.
pub(crate) fn transfer(operation: OpCode, arguments: &[Inferred], labels: &LabelTypes) -> Inferred {
    if !operation.is_nondeterministic()
        && !arguments.is_empty()
        && let Some(folded) = fold(operation, arguments)
    {
        return folded;
    }

    let domains: Vec<DomainSet> = arguments.iter().map(Inferred::domains).collect();
    let bound = operation.signature().apply(&domains);
    if bound.is_empty() {
        return Inferred::Bound(Type::EMPTY);
    }

    match refined(operation, arguments, labels) {
        Some(refined) => refined.within(bound),
        None => Inferred::Bound(Type::of_domains(bound)),
    }
}

/// Returns what a deterministic instruction yields over exact operands,
/// by running the evaluator on them: constant folding.
///
/// An error on these operands is an error on every row, which the empty bound states.
/// Returns `None` if an operand is not exact,
/// or if there are more operands than an instruction addresses.
fn fold(operation: OpCode, arguments: &[Inferred]) -> Option<Inferred> {
    let constants: Vec<DataValue> = arguments
        .iter()
        .map(|argument| argument.exact().cloned())
        .collect::<Option<_>>()?;
    let mut operations = Vec::with_capacity(constants.len() + 1);
    for index in 0..constants.len() {
        operations.push(OpCode::Constant(u16::try_from(index).ok()?));
    }
    operations.push(operation);
    // The arguments are the operands the instruction pops, so the program is valid.
    let program = StackProgram::new(operations, constants, 0);

    let mut stack = Vec::new();
    let folded = match program
        .evaluate(EvaluationContext::now(), &mut stack, &[])
        .as_result()
    {
        Ok(value) => Inferred::Exact(value.to_owned()),
        Err(_) => Inferred::Bound(Type::EMPTY),
    };

    Some(folded)
}

/// Returns what an instruction yields beyond its signature,
/// from the shapes and exact values of its operands;
/// `None` if it knows nothing more.
///
/// The answer can be wider than the signature's; [transfer] narrows it.
fn refined(operation: OpCode, arguments: &[Inferred], labels: &LabelTypes) -> Option<Inferred> {
    match operation {
        OpCode::And => connective(arguments, false),
        OpCode::Or => connective(arguments, true),
        OpCode::Same => same_term(&arguments[0], &arguments[1]),
        OpCode::InDomains(domains) => domain_test(&arguments[0], domains),
        OpCode::Tuple(_) => built_tuple(None, arguments),
        OpCode::LabeledTuple(_) => built_tuple(Some(&arguments[0]), &arguments[1..]),
        OpCode::Map(_) => built_map(None, arguments),
        OpCode::LabeledMap(_) => built_map(Some(&arguments[0]), &arguments[1..]),
        OpCode::Get => accessed(&arguments[0], &arguments[1], labels),
        OpCode::Length => length(&arguments[0], labels),
        OpCode::Label => label(&arguments[0]),
        OpCode::Keys => keys(&arguments[0], labels),
        OpCode::HasKey => has_key(&arguments[0], &arguments[1], labels),
        OpCode::Concat(_) => concatenated(arguments),
        _ => None,
    }
}

/// Returns what a connective of three-valued logic yields beyond its signature:
/// exactly the winning value if an operand is exactly that value,
/// whatever the other operand holds.
///
/// False wins a conjunction, and true a disjunction.
fn connective(arguments: &[Inferred], winner: bool) -> Option<Inferred> {
    arguments
        .iter()
        .any(|argument| exactly_boolean(argument, winner))
        .then(|| Inferred::Exact(DataValue::boolean(winner)))
}

/// Returns whether an operand is exactly a value with the given effective boolean value;
/// `false` if it is not exact or has no effective boolean value.
fn exactly_boolean(argument: &Inferred, wanted: bool) -> bool {
    argument
        .exact()
        .is_some_and(|value| value.as_ref().effective_boolean_value() == Ok(wanted))
}

/// Returns what an identity test yields beyond its signature:
/// exactly false if the operands share no domain.
///
/// The test is total, so this holds without a shape.
fn same_term(left: &Inferred, right: &Inferred) -> Option<Inferred> {
    (!left.domains().intersects(right.domains()))
        .then(|| Inferred::Exact(DataValue::boolean(false)))
}

/// Returns what a domain test yields beyond its signature,
/// exact if the operand's domains decide it:
/// - true if every domain the operand may hold is tested for;
/// - false if none is.
///
/// The test is total, so this holds without a shape.
fn domain_test(argument: &Inferred, domains: DomainSet) -> Option<Inferred> {
    let kinds = argument.domains();
    if kinds.is_subset(domains) {
        Some(Inferred::Exact(DataValue::boolean(true)))
    } else if !kinds.intersects(domains) {
        Some(Inferred::Exact(DataValue::boolean(false)))
    } else {
        None
    }
}

/// Returns what a concatenation yields beyond its signature:
/// a plain string, if every operand is one.
fn concatenated(arguments: &[Inferred]) -> Option<Inferred> {
    arguments
        .iter()
        .all(|argument| argument.domains().is_subset(DomainSet::STRING))
        .then(|| Inferred::Bound(Type::of_domains(DomainSet::STRING)))
}

/// Returns what is known of a built structure's label.
///
/// Returns `None` if the label operand is exactly a value that cannot name a structure,
/// an IRI or a plain string, so that the build fails on every row.
fn label_bound(label: Option<&Inferred>) -> Option<LabelBound> {
    let Some(label) = label else {
        return Some(LabelBound::Exactly(None));
    };
    let Some(exact) = label.exact() else {
        return Some(LabelBound::Unknown);
    };

    match exact.as_ref() {
        ValueRef::Iri(iri) => Some(LabelBound::Exactly(Some(iri.to_string().into()))),
        ValueRef::String(text) => Some(LabelBound::Exactly(Some(text.into()))),
        _ => None,
    }
}

/// Returns what a tuple build yields beyond its signature: the shape of its operands.
fn built_tuple(label: Option<&Inferred>, elements: &[Inferred]) -> Option<Inferred> {
    let Some(label) = label_bound(label) else {
        return Some(Inferred::Bound(Type::EMPTY));
    };
    let shape = TupleShape {
        label,
        elements: elements
            .iter()
            .map(|element| element.bound().into_owned())
            .collect(),
    };
    Some(Inferred::Bound(Type::tuple(shape).capped(Type::DEPTH)))
}

/// Returns what a map build yields beyond its signature:
/// the shape of its operands if every key is exact.
///
/// Exact keys naming one key twice fail on every row.
fn built_map(label: Option<&Inferred>, pairs: &[Inferred]) -> Option<Inferred> {
    let Some(label) = label_bound(label) else {
        return Some(Inferred::Bound(Type::EMPTY));
    };
    let mut known = Vec::with_capacity(pairs.len() / 2);
    for pair in pairs.as_chunks::<2>().0 {
        known.push((pair[0].exact()?.clone(), pair[1].bound().into_owned()));
    }

    let Some(shape) = MapShape::new(label, known) else {
        return Some(Inferred::Bound(Type::EMPTY));
    };
    Some(Inferred::Bound(Type::map(shape).capped(Type::DEPTH)))
}

/// Returns the written-out shapes of a structured domain a type refines,
/// declared labels read through their declarations,
/// or `None` if any shape is unknown: the domain unrefined, or a label the declarations lack.
fn written_out<'a, S>(
    shapes: Option<&'a [Shape<S>]>,
    declared: impl Fn(&str) -> Option<&'a S>,
) -> Option<Vec<&'a S>> {
    shapes?
        .iter()
        .map(|shape| match shape {
            Shape::Structural(shape) => Some(shape),
            Shape::Declared(label) => declared(label),
        })
        .collect()
}

/// Returns the written-out tuple shapes of a type; see [written_out].
fn tuple_shapes<'a>(bound: &'a Type, labels: &'a LabelTypes) -> Option<Vec<&'a TupleShape>> {
    written_out(bound.tuple_shapes(), |label| labels.tuple_shape(label))
}

/// Returns the written-out map shapes of a type; see [written_out].
fn map_shapes<'a>(bound: &'a Type, labels: &'a LabelTypes) -> Option<Vec<&'a MapShape>> {
    written_out(bound.map_shapes(), |label| labels.map_shape(label))
}

/// Returns what a component access yields beyond its signature:
/// the union of the components over the structure domains the operand may hold.
fn accessed(structure: &Inferred, key: &Inferred, labels: &LabelTypes) -> Option<Inferred> {
    let kinds = structure.domains();
    let mut possible = Type::EMPTY;
    if kinds.contains(ValueDomain::Tuple) && key.domains().contains(ValueDomain::Integer) {
        possible = possible.union(&tuple_component(structure, key, labels));
    }
    if kinds.contains(ValueDomain::Map) {
        possible = possible.union(&map_component(structure, key, labels));
    }
    Some(Inferred::Bound(possible))
}

/// Returns the type of an element read from a tuple.
///
/// With shapes, it is the union over the shapes
/// of the one element under an exact index, or of every element otherwise,
/// and nothing past a shape's arity.
fn tuple_component(structure: &Inferred, key: &Inferred, labels: &LabelTypes) -> Type {
    let bound = structure.bound();
    let Some(shapes) = tuple_shapes(&bound, labels) else {
        return Type::ANY;
    };

    // `Some(None)` for an exact key that is no index: no element lies under it.
    let index = match key.exact() {
        Some(exact) => match exact.as_ref() {
            ValueRef::Number(index) => {
                Some(index.to_i64().and_then(|index| usize::try_from(index).ok()))
            }
            _ => return Type::EMPTY,
        },
        None => None,
    };
    shapes.iter().fold(Type::EMPTY, |sum, shape| match index {
        Some(index) => sum.union(
            index
                .and_then(|index| shape.elements.get(index))
                .unwrap_or(&Type::EMPTY),
        ),
        None => shape
            .elements
            .iter()
            .fold(sum, |sum, element| sum.union(element)),
    })
}

/// Returns the type of a value read from a map.
///
/// With shapes, it is the union over the shapes
/// of the value under an exact key, or of every value otherwise,
/// and nothing under a key a shape lacks.
fn map_component(structure: &Inferred, key: &Inferred, labels: &LabelTypes) -> Type {
    let bound = structure.bound();
    let Some(shapes) = map_shapes(&bound, labels) else {
        return Type::ANY;
    };

    shapes
        .iter()
        .fold(Type::EMPTY, |sum, shape| match key.exact() {
            Some(exact) => sum.union(shape.value_at(exact.as_ref()).unwrap_or(&Type::EMPTY)),
            None => shape
                .pairs()
                .iter()
                .fold(sum, |sum, (_, value)| sum.union(value)),
        })
}

/// Returns what a length read yields beyond its signature:
/// exactly the length every structure the operand may hold has, if they agree.
fn length(structure: &Inferred, labels: &LabelTypes) -> Option<Inferred> {
    let kinds = structure.domains();
    let bound = structure.bound();
    let mut lengths = Vec::new();
    if kinds.contains(ValueDomain::Tuple) {
        lengths.extend(
            tuple_shapes(&bound, labels)?
                .iter()
                .map(|shape| shape.elements.len()),
        );
    }
    if kinds.contains(ValueDomain::Map) {
        lengths.extend(
            map_shapes(&bound, labels)?
                .iter()
                .map(|shape| shape.pairs().len()),
        );
    }

    let (&length, rest) = lengths.split_first()?;
    if rest.iter().any(|&other| other != length) {
        return None;
    }
    let length = i64::try_from(length).ok()?;
    Some(Inferred::Exact(DataValue::integer(length)))
}

/// Returns the label bound of every shape of a structured domain,
/// or one unknown bound if the domain is unrefined.
fn label_bounds<S>(
    shapes: Option<&[Shape<S>]>,
    label: impl Fn(&S) -> &LabelBound,
) -> Vec<LabelBound> {
    match shapes {
        None => vec![LabelBound::Unknown],
        Some(shapes) => shapes
            .iter()
            .map(|shape| match shape {
                Shape::Structural(shape) => label(shape).clone(),
                Shape::Declared(declared) => LabelBound::Exactly(Some(declared.clone())),
            })
            .collect(),
    }
}

/// Returns what a label read yields beyond its signature:
/// exactly the label every structure the operand may hold carries, if they agree.
fn label(structure: &Inferred) -> Option<Inferred> {
    let kinds = structure.domains();
    let bound = structure.bound();
    let mut bounds = Vec::new();
    if kinds.contains(ValueDomain::Tuple) {
        bounds.extend(label_bounds(bound.tuple_shapes(), |shape| &shape.label));
    }
    if kinds.contains(ValueDomain::Map) {
        bounds.extend(label_bounds(bound.map_shapes(), |shape| &shape.label));
    }

    let (first, rest) = bounds.split_first()?;
    let LabelBound::Exactly(label) = first else {
        return None;
    };
    if rest.iter().any(|other| other != first) {
        return None;
    }
    let value = match label {
        Some(label) => DataValue::iri(label.clone()),
        None => DataValue::string(""),
    };
    Some(Inferred::Exact(value))
}

/// Returns what a key enumeration yields beyond its signature:
/// exactly the keys every possible map holds, if they agree,
/// since a shape's keys are values.
fn keys(structure: &Inferred, labels: &LabelTypes) -> Option<Inferred> {
    let bound = structure.bound();
    let shapes = map_shapes(&bound, labels)?;
    let (first, rest) = shapes.split_first()?;
    let keys_of = |shape: &MapShape| -> Vec<DataValue> {
        shape.pairs().iter().map(|(key, _)| key.clone()).collect()
    };
    let keys = keys_of(first);
    if rest.iter().any(|shape| keys_of(shape) != keys) {
        return None;
    }
    Some(Inferred::Exact(DataValue::tuple(TupleValue::new(keys))))
}

/// Returns what a key membership test yields beyond its signature:
/// the exact answer, if the key is known and every possible map agrees on it.
fn has_key(structure: &Inferred, key: &Inferred, labels: &LabelTypes) -> Option<Inferred> {
    let bound = structure.bound();
    let shapes = map_shapes(&bound, labels)?;
    let key = key.exact()?;
    let (first, rest) = shapes.split_first()?;
    let held = first.value_at(key.as_ref()).is_some();
    if rest
        .iter()
        .any(|shape| shape.value_at(key.as_ref()).is_some() != held)
    {
        return None;
    }
    Some(Inferred::Exact(DataValue::boolean(held)))
}

/// What inference promises for one instruction over the operands it pops,
/// which debug builds hold every evaluated instruction to.
#[cfg(debug_assertions)]
#[derive(Debug)]
pub(crate) struct Promise {
    /// The instruction.
    operation: OpCode,
    /// The operands, each by the precise type of its value, an error by the empty type.
    operands: Vec<Inferred>,
    /// What inference answers for the instruction over those operands.
    result: Inferred,
}

#[cfg(debug_assertions)]
impl Promise {
    /// Creates the promise for an instruction over the operands on top of the stack.
    ///
    /// The operands go in as types, never as exact values.
    /// Over exact values inference folds, which runs the evaluator,
    /// so the check would hold the evaluator to itself.
    pub(crate) fn of(operation: OpCode, stack: &[StackValue<'_>]) -> Self {
        let operands: Vec<Inferred> = stack[stack.len() - operation.pops()..]
            .iter()
            .map(|operand| match operand.as_result() {
                Ok(value) => Inferred::Bound(Type::of_value(value)),
                Err(_) => Inferred::Bound(Type::EMPTY),
            })
            .collect();
        let result = transfer(operation, &operands, &LabelTypes::NONE);

        Self {
            operation,
            operands,
            result,
        }
    }

    /// Panics if the instruction's result, on top of the stack,
    /// is a value the promise excludes.
    pub(crate) fn check(&self, stack: &[StackValue<'_>]) {
        let Some(Ok(value)) = stack.last().map(StackValue::as_result) else {
            return;
        };

        assert!(
            self.result.admits(value),
            "{:?} over {:?} yielded {value:?}, outside the inferred {:?}",
            self.operation,
            self.operands,
            self.result
        );
    }
}
