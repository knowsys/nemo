//! Narrowing, which derives what the outcome of an evaluation tells of its inputs.
//!
//! Given a required outcome, a condition that holds or a value of a given type,
//! narrowing restricts the types of the inputs to the values that make the outcome possible.
//! It works back from the outcome through the program's expression tree,
//! as [Tree::narrow] describes.
//!
//! Every step keeps what may succeed, so narrowing over-approximates like inference,
//! and an input narrowed to the empty type proves that no row yields the outcome.

use crate::{
    function::{
        OpCode,
        infer::{self, Inferred},
    },
    value::{
        DataValue, DomainSet, LabelBound, LabelTypes, MapShape, Shape, TupleShape, Type,
        ValueDomain, ValueRef,
    },
};

/// The outcome that an evaluation is required to have,
/// which [narrowing](super::StackProgram::narrow) works back from.
#[derive(Debug, Clone)]
pub enum Outcome {
    /// A value of the type.
    Within(Type),
    /// A value whose effective boolean value is true, as for a condition that holds.
    Holds,
}

/// What one node of the expression tree must yield.
#[derive(Debug, Clone)]
enum Required {
    /// A value that the knowledge allows.
    Value(Inferred),
    /// A value whose effective boolean value is true.
    True,
    /// A value whose effective boolean value is false.
    False,
}

impl Required {
    /// Returns the requirement of a value with the given effective boolean value.
    fn of_truth(truth: bool) -> Self {
        if truth {
            Required::True
        } else {
            Required::False
        }
    }

    /// Returns the requirement of a value of a type.
    fn within(datatype: Type) -> Self {
        Required::Value(Inferred::Bound(datatype))
    }

    /// Returns the type of the values that the requirement allows.
    fn bound(&self) -> Type {
        match self {
            Required::Value(known) => known.bound().into_owned(),
            Required::True | Required::False => Type::of_domains(DomainSet::TRUTHY),
        }
    }

    /// Returns the effective boolean value that the requirement fixes, if it fixes one.
    fn truth(&self) -> Option<bool> {
        match self {
            Required::True => Some(true),
            Required::False => Some(false),
            Required::Value(known) => match known.exact()?.as_ref() {
                ValueRef::Boolean(truth) => Some(truth),
                _ => None,
            },
        }
    }
}

/// The expression tree of a program, with what inference knows of each node.
///
/// A node is an instruction, identified by its position in the program,
/// and its operands are the nodes that pushed them.
struct Tree<'a> {
    /// The instruction of each node.
    operations: &'a [OpCode],
    /// The operand nodes of each node, in operand order.
    operands: Vec<Vec<usize>>,
    /// What inference knows of the result of each node.
    known: Vec<Inferred>,
    /// The types declared for labels.
    labels: &'a LabelTypes,
}

/// Returns the input types narrowed to the values for which the program can yield `outcome`.
///
/// `operations` and `constants` make up the program.
/// The other parameters are those of [StackProgram::narrow](super::StackProgram::narrow).
pub(super) fn narrow(
    operations: &[OpCode],
    constants: &[DataValue],
    inputs: &[Type],
    labels: &LabelTypes,
    outcome: &Outcome,
) -> Vec<Type> {
    let mut operands = Vec::with_capacity(operations.len());
    let mut known: Vec<Inferred> = Vec::with_capacity(operations.len());
    let mut stack: Vec<usize> = Vec::new();
    for (node, &operation) in operations.iter().enumerate() {
        // The program is validated, so the stack has the operands.
        let popped = stack.split_off(stack.len() - operation.pops());
        known.push(match operation {
            OpCode::Constant(index) => Inferred::Exact(constants[usize::from(index)].clone()),
            OpCode::Input(index) => {
                Inferred::Bound(inputs.get(usize::from(index)).cloned().unwrap_or(Type::ANY))
            }
            other => {
                let arguments: Vec<Inferred> = popped
                    .iter()
                    .map(|&operand| known[operand].clone())
                    .collect();
                infer::transfer(other, &arguments, labels)
            }
        });
        operands.push(popped);
        stack.push(node);
    }
    let root = stack.pop().expect("a validated program leaves one result");

    let tree = Tree {
        operations,
        operands,
        known,
        labels,
    };
    let required = match outcome {
        Outcome::Within(datatype) => Required::within(datatype.clone()),
        Outcome::Holds => Required::True,
    };
    tree.narrow(root, &required, inputs.to_vec())
}

impl Tree<'_> {
    /// Returns the inputs narrowed to the values that let a node meet a requirement.
    ///
    /// - An input node narrows its input to the type that the requirement allows.
    /// - The negation, the connectives, the domain test and the identity narrow their operands
    ///   by the truth that the requirement fixes, if it fixes one.
    /// - Every other node narrows its operands to the overloads of its
    ///   [signature](OpCode::signature) that can meet the requirement.
    ///   A node that builds a structure also narrows each operand to the matching field
    ///   of the required shapes, see [fields](Self::fields).
    ///   A node that reads the label, the length or the key membership of a structure
    ///   narrows the structure further, see [refined](Self::refined).
    fn narrow(&self, node: usize, required: &Required, inputs: Vec<Type>) -> Vec<Type> {
        let operands = &self.operands[node];
        match self.operations[node] {
            OpCode::Input(index) => {
                let mut inputs = inputs;
                if let Some(input) = inputs.get_mut(usize::from(index)) {
                    *input = input.intersect(&required.bound());
                }
                inputs
            }
            OpCode::Constant(_) => inputs,
            OpCode::Not => {
                let operand = match required.truth() {
                    Some(truth) => Required::of_truth(!truth),
                    None => Required::within(Type::of_domains(DomainSet::TRUTHY)),
                };
                self.narrow(operands[0], &operand, inputs)
            }
            // A connective can succeed whatever one operand is, so only a fixed truth narrows.
            // All operands of a true conjunction are true, and one operand of a true disjunction.
            OpCode::And => match required.truth() {
                Some(true) => operands.iter().fold(inputs, |inputs, &operand| {
                    self.narrow(operand, &Required::True, inputs)
                }),
                _ => inputs,
            },
            OpCode::Or => match required.truth() {
                Some(true) => operands
                    .iter()
                    .map(|&operand| self.narrow(operand, &Required::True, inputs.clone()))
                    .reduce(|left, right| {
                        left.iter()
                            .zip(&right)
                            .map(|(left, right)| left.union(right))
                            .collect()
                    })
                    .unwrap_or(inputs),
                Some(false) => operands.iter().fold(inputs, |inputs, &operand| {
                    self.narrow(operand, &Required::False, inputs)
                }),
                None => inputs,
            },
            OpCode::InDomains(domains) => match required.truth() {
                Some(truth) => {
                    let within = if truth { domains } else { domains.complement() };
                    self.narrow(
                        operands[0],
                        &Required::within(Type::of_domains(within)),
                        inputs,
                    )
                }
                None => inputs,
            },
            // Identical terms have the same type, and an exact operand gives the other its value.
            OpCode::Same => match required.truth() {
                Some(true) => {
                    let (left, right) = (operands[0], operands[1]);
                    let inputs =
                        self.narrow(left, &Required::Value(self.known[right].clone()), inputs);
                    self.narrow(right, &Required::Value(self.known[left].clone()), inputs)
                }
                _ => inputs,
            },
            operation => {
                let inputs = self.generic(node, operation, required, inputs);
                if let Some(fields) = self.fields(node, operation, required) {
                    return fields.into_iter().fold(inputs, |inputs, (operand, field)| {
                        self.narrow(operand, &Required::within(field), inputs)
                    });
                }
                match self.refined(node, operation, required) {
                    Some(structure) => {
                        self.narrow(operands[0], &Required::within(structure), inputs)
                    }
                    None => inputs,
                }
            }
        }
    }

    /// Returns the type that each field operand of a node that builds a structure must have
    /// for the structure to meet the requirement.
    ///
    /// The type of a field is the union of the field's types
    /// over the required shapes that the structure can fit.
    /// A tuple fits a shape of its arity whose label allows the tuple's label.
    /// A map fits a shape with exactly its keys whose label allows the map's label.
    ///
    /// Returns `None` for a node that builds no structure,
    /// if the requirement has no shapes, and if the label or a key of a map is not exact.
    fn fields(
        &self,
        node: usize,
        operation: OpCode,
        required: &Required,
    ) -> Option<Vec<(usize, Type)>> {
        let operands = &self.operands[node];
        let label = |operand: usize| -> Option<Option<Box<str>>> {
            match self.known[operand].exact()?.as_ref() {
                ValueRef::Iri(iri) => Some(Some(iri.to_string().into())),
                ValueRef::String(text) => Some(Some(text.into())),
                _ => None,
            }
        };
        let required = required.bound();
        match operation {
            OpCode::Tuple(_) | OpCode::LabeledTuple(_) => {
                let (label, elements) = match operation {
                    OpCode::Tuple(_) => (None, &operands[..]),
                    _ => (label(operands[0])?, &operands[1..]),
                };
                let shapes = required.tuple_shapes()?;
                let mut fields = vec![Type::EMPTY; elements.len()];
                for shape in shapes {
                    let shape = match shape {
                        Shape::Structural(shape) => shape,
                        Shape::Declared(declared) if label.as_deref() == Some(&**declared) => {
                            self.labels.tuple_shape(declared)?
                        }
                        Shape::Declared(_) => continue,
                    };
                    if !shape.label.admits(label.as_deref())
                        || shape.elements.len() != elements.len()
                    {
                        continue;
                    }
                    for (field, element) in fields.iter_mut().zip(&shape.elements) {
                        *field = field.union(element);
                    }
                }
                Some(elements.iter().copied().zip(fields).collect())
            }
            OpCode::Map(_) | OpCode::LabeledMap(_) => {
                let (label, pairs) = match operation {
                    OpCode::Map(_) => (None, &operands[..]),
                    _ => (label(operands[0])?, &operands[1..]),
                };
                let keys: Vec<&DataValue> = pairs
                    .chunks(2)
                    .map(|pair| self.known[pair[0]].exact())
                    .collect::<Option<_>>()?;
                let shapes = required.map_shapes()?;
                let mut fields = vec![Type::EMPTY; keys.len()];
                for shape in shapes {
                    let shape = match shape {
                        Shape::Structural(shape) => shape,
                        Shape::Declared(declared) if label.as_deref() == Some(&**declared) => {
                            self.labels.map_shape(declared)?
                        }
                        Shape::Declared(_) => continue,
                    };
                    if !shape.label.admits(label.as_deref()) || shape.pairs().len() != keys.len() {
                        continue;
                    }
                    let values: Option<Vec<&Type>> = keys
                        .iter()
                        .map(|key| shape.value_at(key.as_ref()))
                        .collect();
                    let Some(values) = values else { continue };
                    for (field, value) in fields.iter_mut().zip(values) {
                        *field = field.union(value);
                    }
                }
                Some(pairs.chunks(2).map(|pair| pair[1]).zip(fields).collect())
            }
            _ => None,
        }
    }

    /// Returns the inputs narrowed by the signature of a node's instruction.
    ///
    /// Each operand is narrowed to the domains of the overloads that can meet the requirement.
    fn generic(
        &self,
        node: usize,
        operation: OpCode,
        required: &Required,
        inputs: Vec<Type>,
    ) -> Vec<Type> {
        let operands = &self.operands[node];
        let domains: Vec<DomainSet> = operands
            .iter()
            .map(|&operand| self.known[operand].domains())
            .collect();
        let within = operation
            .signature()
            .narrow(&domains, required.bound().domains());
        operands
            .iter()
            .zip(within)
            .fold(inputs, |inputs, (&operand, within)| {
                self.narrow(operand, &Required::within(Type::of_domains(within)), inputs)
            })
    }

    /// Returns the type that the structure operand of a [Label](OpCode::Label),
    /// [Length](OpCode::Length) or [HasKey](OpCode::HasKey) node narrows to beyond the signature.
    ///
    /// The requirement must fix the label, the length or the key membership exactly.
    /// Returns `None` for any other node, and if the requirement fixes nothing.
    fn refined(&self, node: usize, operation: OpCode, required: &Required) -> Option<Type> {
        if !matches!(operation, OpCode::Label | OpCode::Length | OpCode::HasKey) {
            return None;
        }
        let operands = &self.operands[node];
        let structure = self.known[operands[0]].bound();
        match operation {
            OpCode::Label => {
                let Required::Value(known) = required else {
                    return None;
                };
                Some(self.labeled(&structure, known.exact()?))
            }
            OpCode::Length => {
                let Required::Value(known) = required else {
                    return None;
                };
                let ValueRef::Number(length) = known.exact()?.as_ref() else {
                    return Some(Type::EMPTY);
                };
                let length = length.to_i64()?;
                Some(
                    usize::try_from(length)
                        .map_or(Type::EMPTY, |length| self.of_length(&structure, length)),
                )
            }
            OpCode::HasKey => {
                let held = required.truth()?;
                let key = self.known[operands[1]].exact()?;
                Some(self.keyed(&structure, key.as_ref(), held))
            }
            _ => None,
        }
    }

    /// Returns the structures of a type that have the label that [Label](OpCode::Label) yields.
    ///
    /// `label` is an IRI that names the label, or the empty string for no label,
    /// and any other value selects no structure.
    /// A label with a declared type gives that declared type, whatever was known before.
    fn labeled(&self, structure: &Type, label: &DataValue) -> Type {
        let label: Option<Box<str>> = match label.as_ref() {
            ValueRef::Iri(iri) => Some(iri.to_string().into()),
            ValueRef::String("") => None,
            _ => return Type::EMPTY,
        };
        if let Some(label) = &label {
            if self.labels.tuple_shape(label).is_some() {
                return Type::declared(label, ValueDomain::Tuple);
            }
            if self.labels.map_shape(label).is_some() {
                return Type::declared(label, ValueDomain::Map);
            }
        }

        let exactly = LabelBound::Exactly(label.clone());
        let carries = |bound: &LabelBound| bound.admits(label.as_deref());
        self.select(
            structure,
            |shape| {
                carries(&shape.label).then(|| TupleShape {
                    label: exactly.clone(),
                    elements: shape.elements.clone(),
                })
            },
            |shape| {
                carries(&shape.label).then(|| {
                    MapShape::new(exactly.clone(), shape.pairs().to_vec())
                        .expect("the shape holds each key once")
                })
            },
            |declared| label.as_deref() == Some(declared),
        )
    }

    /// Returns the structures of a type with the given number of elements or pairs.
    ///
    /// If the type has tuples without shapes, the length gives them one,
    /// the shape of any tuple of that arity.
    fn of_length(&self, structure: &Type, length: usize) -> Type {
        let selected = self.select(
            structure,
            |shape| (shape.elements.len() == length).then(|| shape.clone()),
            |shape| (shape.pairs().len() == length).then(|| shape.clone()),
            |declared| {
                self.labels
                    .tuple_shape(declared)
                    .map(|shape| shape.elements.len())
                    .or_else(|| {
                        self.labels
                            .map_shape(declared)
                            .map(|shape| shape.pairs().len())
                    })
                    .is_none_or(|declared| declared == length)
            },
        );
        if structure.domains().contains(ValueDomain::Tuple) && structure.tuple_shapes().is_none() {
            let others = Type::of_domains(DomainSet::ANY.without(ValueDomain::Tuple));
            return selected.intersect(&others).union(&Type::tuple(TupleShape {
                label: LabelBound::Unknown,
                elements: vec![Type::ANY; length],
            }));
        }
        selected
    }

    /// Returns the structures of a type whose maps have the key if `held` is true,
    /// or lack it otherwise.
    ///
    /// Tuples are kept.
    fn keyed(&self, structure: &Type, key: ValueRef<'_>, held: bool) -> Type {
        self.select(
            structure,
            |shape| Some(shape.clone()),
            |shape| (shape.value_at(key).is_some() == held).then(|| shape.clone()),
            |declared| {
                self.labels
                    .map_shape(declared)
                    .is_none_or(|shape| shape.value_at(key).is_some() == held)
            },
        )
    }

    /// Returns the structures of a type whose shapes the selection keeps, possibly changed.
    ///
    /// `tuple` and `map` return the kept form of a written-out shape, or `None` to drop it,
    /// and `declared` returns whether to keep the declared shape of a label.
    /// A structured domain without shapes is kept whole,
    /// since the selection cannot be applied to it.
    fn select(
        &self,
        structure: &Type,
        tuple: impl Fn(&TupleShape) -> Option<TupleShape>,
        map: impl Fn(&MapShape) -> Option<MapShape>,
        declared: impl Fn(&str) -> bool,
    ) -> Type {
        let domains = structure.domains();
        let mut selected = Type::EMPTY;
        if domains.contains(ValueDomain::Tuple) {
            match structure.tuple_shapes() {
                None => selected = selected.union(&Type::of(ValueDomain::Tuple)),
                Some(shapes) => {
                    for shape in shapes {
                        selected = selected.union(&match shape {
                            Shape::Structural(shape) => {
                                tuple(shape).map_or(Type::EMPTY, Type::tuple)
                            }
                            Shape::Declared(label) if declared(label) => {
                                Type::declared(label, ValueDomain::Tuple)
                            }
                            Shape::Declared(_) => Type::EMPTY,
                        });
                    }
                }
            }
        }
        if domains.contains(ValueDomain::Map) {
            match structure.map_shapes() {
                None => selected = selected.union(&Type::of(ValueDomain::Map)),
                Some(shapes) => {
                    for shape in shapes {
                        selected = selected.union(&match shape {
                            Shape::Structural(shape) => map(shape).map_or(Type::EMPTY, Type::map),
                            Shape::Declared(label) if declared(label) => {
                                Type::declared(label, ValueDomain::Map)
                            }
                            Shape::Declared(_) => Type::EMPTY,
                        });
                    }
                }
            }
        }
        selected
    }
}
