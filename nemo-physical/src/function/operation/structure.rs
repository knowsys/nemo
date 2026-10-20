//! Construction and destructuring of structured values.
//!
//! A tuple or a map is built from the values on the stack,
//! so an error in any part is an error in the whole:
//! a structure containing an error is not a structure.

use crate::{
    function::StackValue,
    value::{DataValue, EvaluationError, IriRef, MapValue, NumberRef, TupleValue, ValueRef},
};

/// Builds a tuple from its elements, in the order given.
pub(crate) fn tuple<'a>(
    label: Option<&StackValue<'_>>,
    elements: &[StackValue<'_>],
) -> StackValue<'a> {
    let elements = match owned(elements) {
        Ok(elements) => elements,
        Err(error) => return StackValue::Error(error),
    };

    match label {
        None => StackValue::Owned(DataValue::tuple(TupleValue::new(elements))),
        Some(label) => match name(label) {
            Ok(label) => StackValue::Owned(DataValue::tuple(TupleValue::labeled(label, elements))),
            Err(error) => StackValue::Error(error),
        },
    }
}

/// Builds a map from alternating keys and values.
pub(crate) fn map<'a>(label: Option<&StackValue<'_>>, pairs: &[StackValue<'_>]) -> StackValue<'a> {
    let flat = match owned(pairs) {
        Ok(flat) => flat,
        Err(error) => return StackValue::Error(error),
    };
    let entries: Vec<(DataValue, DataValue)> = flat
        .as_chunks::<2>()
        .0
        .iter()
        .map(|[key, value]| (key.clone(), value.clone()))
        .collect();

    let built = match label {
        None => MapValue::new(entries),
        Some(label) => match name(label) {
            Ok(label) => MapValue::labeled(label, entries),
            Err(error) => return StackValue::Error(error),
        },
    };

    match built {
        Some(map) => StackValue::Owned(DataValue::map(map)),
        // Pairs sharing a key describe no map: which value the key has
        // would be a matter of which pair was written first.
        None => StackValue::Error(EvaluationError::Malformed),
    }
}

/// Returns the component of a structure:
/// a tuple's element at a 0-based integer index, a map's value at a key.
///
/// An absent component is [Missing](EvaluationError::Missing);
/// a non-structure, or a tuple indexed by anything but an integer, is a type mismatch.
///
/// Takes the stack entries rather than views,
/// so that a component of a borrowed structure comes back borrowed.
/// Only a component of an owned intermediate, a structure built earlier in the same program,
/// is cloned, since its storage does not outlive the stack slot.
pub(crate) fn get<'a>(structure: &StackValue<'a>, key: &StackValue<'_>) -> StackValue<'a> {
    let key = match structure.as_result().and(key.as_result()) {
        Ok(key) => key,
        Err(error) => return StackValue::Error(error),
    };

    match structure {
        StackValue::Value(value) => match component(*value, key) {
            Ok(component) => StackValue::Value(component),
            Err(error) => StackValue::Error(error),
        },
        StackValue::Owned(value) => match component(value.as_ref(), key) {
            Ok(component) => StackValue::Owned(component.to_owned()),
            Err(error) => StackValue::Error(error),
        },
        StackValue::Error(error) => StackValue::Error(*error),
    }
}

/// Returns the component of a structure at a key, as [get] defines it.
fn component<'a>(
    structure: ValueRef<'a>,
    key: ValueRef<'_>,
) -> Result<ValueRef<'a>, EvaluationError> {
    match (structure, key) {
        // A wide integer lies beyond any tuple's length.
        (ValueRef::Tuple(tuple), ValueRef::Number(index)) if index.is_integral() => index
            .to_i64()
            .and_then(|index| usize::try_from(index).ok())
            .and_then(|index| tuple.element(index))
            .ok_or(EvaluationError::Missing),
        (ValueRef::Map(map), key) => map.get(key).ok_or(EvaluationError::Missing),
        _ => Err(EvaluationError::TypeMismatch),
    }
}

/// Returns the label of a structure: an IRI, or the empty string if it has none.
///
/// The operation is total over structures, like the language of a string,
/// so that a label test in a condition is a comparison and never an error.
pub(crate) fn label<'a>(structure: &StackValue<'a>) -> StackValue<'a> {
    let as_value = |label: Option<&'a str>| match label {
        Some(label) => StackValue::Value(ValueRef::Iri(IriRef::new(label))),
        None => StackValue::Value(ValueRef::String("")),
    };
    match structure {
        StackValue::Value(ValueRef::Tuple(tuple)) => as_value(tuple.label()),
        StackValue::Value(ValueRef::Map(map)) => as_value(map.label()),
        StackValue::Owned(value) => match value.as_ref() {
            ValueRef::Tuple(tuple) => match tuple.label() {
                Some(label) => StackValue::Owned(DataValue::iri(label)),
                None => StackValue::Value(ValueRef::String("")),
            },
            ValueRef::Map(map) => match map.label() {
                Some(label) => StackValue::Owned(DataValue::iri(label)),
                None => StackValue::Value(ValueRef::String("")),
            },
            _ => StackValue::Error(EvaluationError::TypeMismatch),
        },
        StackValue::Value(_) => StackValue::Error(EvaluationError::TypeMismatch),
        StackValue::Error(error) => StackValue::Error(*error),
    }
}

/// Returns the number of elements or pairs of a structure.
pub(crate) fn length(value: ValueRef<'_>) -> StackValue<'static> {
    match value {
        ValueRef::Tuple(tuple) => count(tuple.len()),
        ValueRef::Map(map) => count(map.len()),
        _ => StackValue::Error(EvaluationError::TypeMismatch),
    }
}

/// Returns a count as an integer value.
fn count(length: usize) -> StackValue<'static> {
    match i64::try_from(length) {
        Ok(length) => StackValue::Value(ValueRef::Number(NumberRef::Decimal(length.into()))),
        Err(_) => StackValue::Error(EvaluationError::Overflow),
    }
}

/// Returns the keys of a map as a tuple, in the canonical term order the map
/// stores them in.
pub(crate) fn keys(value: ValueRef<'_>) -> StackValue<'static> {
    match value {
        ValueRef::Map(map) => {
            let keys: Vec<DataValue> = map.pairs().map(|(key, _)| key.to_owned()).collect();
            StackValue::Owned(DataValue::tuple(TupleValue::new(keys)))
        }
        _ => StackValue::Error(EvaluationError::TypeMismatch),
    }
}

/// Joins a tuple of plain strings with a separator.
pub(crate) fn join_strings(tuple: ValueRef<'_>, separator: ValueRef<'_>) -> StackValue<'static> {
    let (ValueRef::Tuple(tuple), ValueRef::String(separator)) = (tuple, separator) else {
        return StackValue::Error(EvaluationError::TypeMismatch);
    };

    let mut joined = String::new();
    for (index, element) in tuple.elements().enumerate() {
        let ValueRef::String(text) = element else {
            return StackValue::Error(EvaluationError::TypeMismatch);
        };
        if index > 0 {
            joined.push_str(separator);
        }
        joined.push_str(text);
    }

    StackValue::Owned(DataValue::string(joined))
}

/// Returns whether a map holds a key.
pub(crate) fn has_key(map: ValueRef<'_>, key: ValueRef<'_>) -> StackValue<'static> {
    match map {
        ValueRef::Map(map) => map.get(key).is_some().into(),
        _ => StackValue::Error(EvaluationError::TypeMismatch),
    }
}

/// Returns the values behind the stack entries, or the first error among them.
fn owned(entries: &[StackValue<'_>]) -> Result<Vec<DataValue>, EvaluationError> {
    entries
        .iter()
        .map(|entry| entry.as_result().map(|value| value.to_owned()))
        .collect()
}

/// Returns the label a value stands for:
/// an IRI or a plain string names a structure, nothing else does.
fn name(value: &StackValue<'_>) -> Result<String, EvaluationError> {
    match value.as_result()? {
        ValueRef::Iri(iri) => Ok(iri.to_string()),
        ValueRef::String(text) => Ok(text.to_owned()),
        _ => Err(EvaluationError::TypeMismatch),
    }
}
