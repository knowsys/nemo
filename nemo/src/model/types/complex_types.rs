//! This module defines a model for nested type constructs

use std::{iter::from_fn, ops::Deref, sync::Arc};

use crate::model::PrimitiveType;

/// A nested type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NestedType {
    /// A tuple of nested types
    Tuple(TupleType),
    /// A primitive type
    Primitive(PrimitiveType),
}

impl NestedType {
    /// Returns the [PrimitiveType] contained within, if any.
    pub fn as_primitive(&self) -> Option<&PrimitiveType> {
        match self {
            Self::Primitive(inner) => Some(inner),
            _ => None,
        }
    }

    /// Returns the [TupleType] contained within, if any.
    pub fn as_tuple(&self) -> Option<&TupleType> {
        match self {
            Self::Tuple(inner) => Some(inner),
            _ => None,
        }
    }
}

impl Default for NestedType {
    fn default() -> Self {
        Self::Primitive(PrimitiveType::default())
    }
}

/// A tuple of nested types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TupleType {
    field_types: Arc<[NestedType]>,
}

impl TupleType {
    /// Returns the arity (width) the tuple type.
    pub fn arity(&self) -> usize {
        self.field_types.len()
    }

    /// Returns `true` if the tuple type does not contain nested tuples.
    pub fn is_flat(&self) -> bool {
        self.field_types
            .iter()
            .all(|t| matches!(t, NestedType::Primitive(_)))
    }

    /// Returns the underlying [primitive types][PrimitiveType],
    /// provided that this is a flat type.
    pub fn into_flat(&self) -> Option<Vec<PrimitiveType>> {
        let mut result = Vec::new();

        for field_type in self.field_types.iter() {
            if let Some(primitive_type) = field_type.as_primitive() {
                result.push(primitive_type.clone())
            } else {
                // found a non-primitive type, so we're not flat.
                return None;
            }
        }

        Some(result)
    }
}

impl Deref for TupleType {
    type Target = [NestedType];

    fn deref(&self) -> &Self::Target {
        &self.field_types
    }
}

impl FromIterator<PrimitiveType> for TupleType {
    fn from_iter<T: IntoIterator<Item = PrimitiveType>>(iter: T) -> Self {
        Self {
            field_types: iter.into_iter().map(NestedType::Primitive).collect(),
        }
    }
}

impl FromIterator<NestedType> for TupleType {
    fn from_iter<T: IntoIterator<Item = NestedType>>(iter: T) -> Self {
        Self {
            field_types: iter.into_iter().collect(),
        }
    }
}

impl From<TypeConstraint> for NestedType {
    fn from(value: TypeConstraint) -> Self {
        match value {
            TypeConstraint::None => NestedType::Primitive(PrimitiveType::Any),
            TypeConstraint::Exact(p) => NestedType::Primitive(p),
            TypeConstraint::AtLeast(p) => NestedType::Primitive(p),
            TypeConstraint::Tuple(t) => NestedType::Tuple(t.into()),
        }
    }
}

impl From<TupleConstraint> for TupleType {
    fn from(value: TupleConstraint) -> Self {
        value.fields.iter().cloned().map(NestedType::from).collect()
    }
}

impl TryFrom<TypeConstraint> for TupleType {
    type Error = ();

    fn try_from(value: TypeConstraint) -> Result<Self, Self::Error> {
        match value {
            TypeConstraint::None => Err(()),
            TypeConstraint::Exact(_) => Err(()),
            TypeConstraint::AtLeast(_) => Err(()),
            TypeConstraint::Tuple(t) => Ok(t.into()),
        }
    }
}

/// A constraint on the type of an item
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(variant_size_differences)]
pub enum TypeConstraint {
    /// No constraints
    None,
    /// An exact constraint
    Exact(PrimitiveType),
    /// A soft constraint
    AtLeast(PrimitiveType),
    /// A constraint on a tuple type
    Tuple(TupleConstraint),
}

/// A constraint on a tuple type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TupleConstraint {
    fields: Arc<[TypeConstraint]>,
}

impl TupleConstraint {
    /// Returns the arity specified in the constraint.
    pub fn arity(&self) -> usize {
        self.fields.len()
    }

    /// Creates a [TupleConstraint], which only constrains the arity.
    pub(crate) fn from_arity(arity: usize) -> Self {
        from_fn(|| Some(TypeConstraint::None)).take(arity).collect()
    }

    /// Returns the underlying [primitive types][PrimitiveType], provided that this is an exact
    pub fn into_flat_primitive(&self) -> Option<Vec<PrimitiveType>> {
        let mut result = Vec::new();

        for type_constraint in self.fields.iter() {
            match type_constraint {
                TypeConstraint::None => {
                    result.push(PrimitiveType::Any);
                }
                TypeConstraint::Exact(inner) | TypeConstraint::AtLeast(inner) => {
                    result.push(*inner);
                }
                TypeConstraint::Tuple(_) => {
                    return None;
                }
            }
        }

        Some(result)
    }
}

impl Deref for TupleConstraint {
    type Target = [TypeConstraint];

    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

impl FromIterator<TypeConstraint> for TupleConstraint {
    fn from_iter<T: IntoIterator<Item = TypeConstraint>>(iter: T) -> Self {
        Self {
            fields: iter.into_iter().collect(),
        }
    }
}
