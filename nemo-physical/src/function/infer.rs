//! Inference, the evaluation of stack programs on types instead of values.
//!
//! Inference evaluates a program on abstract values.
//! An abstract value, an [Inferred], describes the values that an evaluation yields
//! if it succeeds, either exactly as one value, or by a bound, a [Type] that contains them.
//! The description over-approximates,
//! so an empty bound proves that the evaluation fails on every input.
//! [transfer] infers the result of one instruction.

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

/// What is known of the values that an evaluation yields if it succeeds.
///
/// The knowledge over-approximates,
/// so an empty bound proves that the evaluation fails on every input.
#[derive(Debug, Clone)]
pub enum Inferred {
    /// The one value that every successful evaluation yields.
    Exact(DataValue),
    /// A type that contains every value that a successful evaluation yields.
    Bound(Type),
}

impl Inferred {
    /// Returns a type that contains every value,
    /// the bound itself or the precise type of the exact value.
    pub fn bound(&self) -> Cow<'_, Type> {
        match self {
            Inferred::Exact(value) => Cow::Owned(Type::of_value(value.as_ref())),
            Inferred::Bound(bound) => Cow::Borrowed(bound),
        }
    }

    /// Returns the type that [bound](Self::bound) returns, without cloning a bound.
    pub fn into_bound(self) -> Type {
        match self {
            Inferred::Exact(value) => Type::of_value(value.as_ref()),
            Inferred::Bound(bound) => bound,
        }
    }

    /// Returns the domains that the values can lie in.
    pub fn domains(&self) -> DomainSet {
        match self {
            Inferred::Exact(value) => DomainSet::of(value.domain()),
            Inferred::Bound(bound) => bound.domains(),
        }
    }

    /// Returns the one value that every successful evaluation yields, if it is known.
    pub fn exact(&self) -> Option<&DataValue> {
        match self {
            Inferred::Exact(value) => Some(value),
            Inferred::Bound(_) => None,
        }
    }

    /// Returns whether the knowledge allows a value,
    /// which must be the same term as the exact value or lie within the bound.
    #[cfg(debug_assertions)]
    fn admits(&self, value: ValueRef<'_>) -> bool {
        match self {
            Inferred::Exact(exact) => exact.as_ref() == value,
            Inferred::Bound(bound) => bound.admits(value, &LabelTypes::NONE),
        }
    }

    /// Returns the knowledge narrowed to values in the given domains.
    fn within(self, domains: DomainSet) -> Self {
        match self {
            Inferred::Exact(value) if domains.contains(value.domain()) => Inferred::Exact(value),
            Inferred::Exact(_) => Inferred::Bound(Type::EMPTY),
            Inferred::Bound(bound) => Inferred::Bound(bound.intersect(&Type::of_domains(domains))),
        }
    }
}

/// Returns what is known of the result of an instruction if it succeeds.
///
/// `arguments` is what is known of the operands, in operand order,
/// and `labels` gives the types declared for the labels that their types refer to.
///
/// The result is inferred in up to three steps.
/// 1. A deterministic instruction with exact operands is evaluated on them, see [fold].
/// 2. Otherwise, the instruction's [signature](OpCode::signature) bounds the result
///    by the domains that the operands can lie in.
/// 3. The connectives, the identity and domain tests, the concatenation
///    and the structure instructions refine that bound
///    by the shapes and exact values of their operands, see [refined].
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

/// Returns the result of a deterministic instruction on exact operands,
/// computed by evaluating the instruction on them, which is constant folding.
///
/// An error on these operands is an error on every row, which an empty bound states.
/// Returns `None` if an operand is not exact,
/// or if there are more operands than a program can have constants.
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

/// Returns what the shapes and exact values of the operands tell of an instruction's result
/// beyond its signature, or `None` if they tell nothing more.
///
/// The result can be wider than the signature's bound, and [transfer] narrows it to that bound.
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

/// Returns the result of a connective of three-valued logic if one operand decides it.
///
/// `winner` is the value that decides the connective,
/// `false` for a conjunction and `true` for a disjunction.
/// If an operand is exactly a value with that effective boolean value,
/// the result is exactly `winner`, whatever the other operand is.
fn connective(arguments: &[Inferred], winner: bool) -> Option<Inferred> {
    arguments
        .iter()
        .any(|argument| exactly_boolean(argument, winner))
        .then(|| Inferred::Exact(DataValue::boolean(winner)))
}

/// Returns whether an operand is exactly a value with the effective boolean value `wanted`.
///
/// An operand that is not exact, or that has no effective boolean value, gives `false`.
fn exactly_boolean(argument: &Inferred, wanted: bool) -> bool {
    argument
        .exact()
        .is_some_and(|value| value.as_ref().effective_boolean_value() == Ok(wanted))
}

/// Returns that an identity test is exactly false if its operands share no domain,
/// and `None` otherwise.
///
/// The test never fails, so the domains alone decide this.
fn same_term(left: &Inferred, right: &Inferred) -> Option<Inferred> {
    (!left.domains().intersects(right.domains()))
        .then(|| Inferred::Exact(DataValue::boolean(false)))
}

/// Returns the result of a domain test if the operand's domains decide it, and `None` otherwise.
///
/// The result is
/// - exactly true if every domain that the operand may lie in is in `domains`;
/// - exactly false if none is.
///
/// The test never fails, so the domains alone decide this.
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

/// Returns that a concatenation yields a plain string if every operand is one,
/// and `None` otherwise.
fn concatenated(arguments: &[Inferred]) -> Option<Inferred> {
    arguments
        .iter()
        .all(|argument| argument.domains().is_subset(DomainSet::STRING))
        .then(|| Inferred::Bound(Type::of_domains(DomainSet::STRING)))
}

/// Returns what is known of the label of a structure that an instruction builds.
///
/// `label` is the label operand, or `None` for an unlabeled structure.
/// Returns `None` if the label operand is exactly a value that is neither an IRI
/// nor a plain string, which makes the build fail on every row.
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

/// Returns the type of a tuple built from operands, with the shape that the operands give.
///
/// The shape is nested up to [DEPTH](Type::DEPTH).
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

/// Returns the type of a map built from operands, with the shape that the operands give,
/// or `None` if a key is not exact.
///
/// The shape is nested up to [DEPTH](Type::DEPTH).
/// If two exact keys are the same, the build fails on every row, and the type is empty.
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

/// Returns the written-out shapes of a structured domain,
/// with each declared shape replaced by the shape declared for its label.
///
/// `shapes` are the shapes of the domain in a type,
/// and `declared` returns the shape declared for a label.
/// Returns `None` if the domain has no shapes, or if a label has no declared shape.
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

/// Returns the written-out tuple shapes of a type, as [written_out] does.
fn tuple_shapes<'a>(bound: &'a Type, labels: &'a LabelTypes) -> Option<Vec<&'a TupleShape>> {
    written_out(bound.tuple_shapes(), |label| labels.tuple_shape(label))
}

/// Returns the written-out map shapes of a type, as [written_out] does.
fn map_shapes<'a>(bound: &'a Type, labels: &'a LabelTypes) -> Option<Vec<&'a MapShape>> {
    written_out(bound.map_shapes(), |label| labels.map_shape(label))
}

/// Returns the type of the component that [Get](OpCode::Get) reads,
/// the union over the structured domains that the operand may lie in.
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
/// If the tuple has shapes, the type is the union over the shapes
/// of the element at an exact index, or of every element if the index is not exact.
/// A shape adds nothing for an index past its arity.
/// Without shapes, the element may be any value.
fn tuple_component(structure: &Inferred, key: &Inferred, labels: &LabelTypes) -> Type {
    let bound = structure.bound();
    let Some(shapes) = tuple_shapes(&bound, labels) else {
        return Type::ANY;
    };

    // `Some(None)` stands for an exact number that is no valid index, where no element lies.
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
/// If the map has shapes, the type is the union over the shapes
/// of the value at an exact key, or of every value if the key is not exact.
/// A shape adds nothing for a key that it does not have.
/// Without shapes, the value may be any value.
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

/// Returns the exact length that [Length](OpCode::Length) reads,
/// if all structures that the operand may be have the same length.
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
/// or one unknown bound if the domain has no shapes.
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

/// Returns the exact label that [Label](OpCode::Label) reads,
/// if all structures that the operand may be have the same known label.
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

/// Returns the exact keys that [Keys](OpCode::Keys) reads,
/// if all maps that the operand may be have the same keys.
///
/// The keys of a map shape are values, so they can be known exactly.
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

/// Returns the exact result of [HasKey](OpCode::HasKey),
/// if the key is exact and all maps that the operand may be agree on it.
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

/// What inference predicts for the result of one instruction from the operands that it pops.
///
/// Debug builds check the result of every evaluated instruction against it.
#[cfg(debug_assertions)]
#[derive(Debug)]
pub(crate) struct Promise {
    /// The instruction.
    operation: OpCode,
    /// What is known of each operand,
    /// the precise type of its value, or the empty type for an error.
    operands: Vec<Inferred>,
    /// What inference returns for the instruction on these operands.
    result: Inferred,
}

#[cfg(debug_assertions)]
impl Promise {
    /// Creates the promise of an instruction from its operands on top of the stack.
    ///
    /// The operands are given to inference as types, never as exact values.
    /// With exact values, inference would evaluate the instruction itself,
    /// and the check would compare the evaluator with itself.
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

    /// Panics if the instruction's result on top of the stack is a value that the promise
    /// excludes.
    ///
    /// An error result is not checked.
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
