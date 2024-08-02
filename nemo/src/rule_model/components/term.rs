//! This module defines [Term].

pub mod value_type;

#[macro_use]
pub mod aggregate;
#[macro_use]
pub mod function;
#[macro_use]
pub mod map;
#[macro_use]
pub mod operation;
#[macro_use]
pub mod primitive;
#[macro_use]
pub mod tuple;

use std::fmt::{Debug, Display};

use aggregate::Aggregate;
use function::FunctionTerm;
use map::Map;
use nemo_physical::datavalues::AnyDataValue;
use operation::Operation;
use primitive::{
    ground::GroundTerm,
    variable::{existential::ExistentialVariable, universal::UniversalVariable, Variable},
    Primitive,
};
use tuple::Tuple;
use value_type::ValueType;

use crate::rule_model::{
    error::{ValidationError, ValidationErrorBuilder},
    origin::Origin,
};

use super::{IterableVariables, ProgramComponent};

/// Term
///
/// Basic building block for expressions like atoms or facts.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum Term {
    /// Unstructured, primitive term
    Primitive(Primitive),
    /// Aggregate
    Aggregate(Aggregate),
    /// Abstract function over a list of terms
    FunctionTerm(FunctionTerm),
    /// Map of terms
    Map(Map),
    /// Operation applied to a list of terms
    Operation(Operation),
    /// Tuple
    Tuple(Tuple),
}

impl Term {
    /// Create a universal variable term.
    pub fn universal_variable(name: &str) -> Self {
        Self::Primitive(Primitive::Variable(Variable::universal(name)))
    }

    /// Create a anynmous variable term.
    pub fn anonymous_variable() -> Self {
        Self::Primitive(Primitive::Variable(Variable::anonymous()))
    }

    /// Create a existential variable term.
    pub fn existential_variable(name: &str) -> Self {
        Self::Primitive(Primitive::Variable(Variable::existential(name)))
    }

    /// Create a groud term.
    pub fn ground(value: AnyDataValue) -> Self {
        Self::Primitive(Primitive::Ground(GroundTerm::new(value)))
    }

    /// Return the value type of this term.
    pub fn value_type(&self) -> ValueType {
        match self {
            Term::Primitive(term) => term.value_type(),
            Term::Aggregate(term) => term.value_type(),
            Term::FunctionTerm(term) => term.value_type(),
            Term::Map(term) => term.value_type(),
            Term::Operation(term) => term.value_type(),
            Term::Tuple(term) => term.value_type(),
        }
    }

    /// Return whether this term is a primitive term.
    pub fn is_primitive(&self) -> bool {
        matches!(self, Term::Primitive(_))
    }

    /// Return whether this term is an aggregate.
    pub fn is_aggregate(&self) -> bool {
        matches!(self, Term::Aggregate(_))
    }

    /// Return whether this term is a function term.
    pub fn is_function(&self) -> bool {
        matches!(self, Term::FunctionTerm(_))
    }

    /// Return whether this term is a map.
    pub fn is_map(&self) -> bool {
        matches!(self, Term::Map(_))
    }

    /// Return whether this term is a operation.
    pub fn is_operation(&self) -> bool {
        matches!(self, Term::Operation(_))
    }

    /// Return whether this term is a tuple.
    pub fn is_tuple(&self) -> bool {
        matches!(self, Term::Tuple(_))
    }

    /// Return an iterator over the arguments to this term.
    pub fn arguments(&self) -> Box<dyn Iterator<Item = &Term> + '_> {
        match self {
            Term::Primitive(_) => Box::new(None.into_iter()),
            Term::Aggregate(term) => Box::new(Some(term.aggregate_term()).into_iter()),
            Term::FunctionTerm(term) => Box::new(term.arguments()),
            Term::Map(term) => Box::new(term.key_value().flat_map(|(key, value)| vec![key, value])),
            Term::Operation(term) => Box::new(term.arguments()),
            Term::Tuple(term) => Box::new(term.arguments()),
        }
    }
}

impl From<Variable> for Term {
    fn from(value: Variable) -> Self {
        Self::Primitive(Primitive::from(value))
    }
}

impl From<UniversalVariable> for Term {
    fn from(value: UniversalVariable) -> Self {
        Self::Primitive(Primitive::from(value))
    }
}

impl From<ExistentialVariable> for Term {
    fn from(value: ExistentialVariable) -> Self {
        Self::Primitive(Primitive::from(value))
    }
}

impl From<Primitive> for Term {
    fn from(value: Primitive) -> Self {
        Self::Primitive(value)
    }
}

impl From<AnyDataValue> for Term {
    fn from(value: AnyDataValue) -> Self {
        Self::Primitive(Primitive::from(value))
    }
}

impl From<i64> for Term {
    fn from(value: i64) -> Self {
        Self::Primitive(Primitive::from(value))
    }
}

impl From<i32> for Term {
    fn from(value: i32) -> Self {
        Self::Primitive(Primitive::from(value))
    }
}

impl From<u64> for Term {
    fn from(value: u64) -> Self {
        Self::Primitive(Primitive::from(value))
    }
}

impl From<String> for Term {
    fn from(value: String) -> Self {
        Self::Primitive(Primitive::from(value))
    }
}

impl From<&str> for Term {
    fn from(value: &str) -> Self {
        Self::Primitive(Primitive::from(value))
    }
}

impl From<FunctionTerm> for Term {
    fn from(value: FunctionTerm) -> Self {
        Self::FunctionTerm(value)
    }
}

impl From<Map> for Term {
    fn from(value: Map) -> Self {
        Self::Map(value)
    }
}

impl From<Operation> for Term {
    fn from(value: Operation) -> Self {
        Self::Operation(value)
    }
}

impl From<Tuple> for Term {
    fn from(value: Tuple) -> Self {
        Self::Tuple(value)
    }
}

impl From<Aggregate> for Term {
    fn from(value: Aggregate) -> Self {
        Self::Aggregate(value)
    }
}

impl Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Term::Primitive(term) => write!(f, "{}", term),
            Term::FunctionTerm(term) => write!(f, "{}", term),
            Term::Map(term) => write!(f, "{}", term),
            Term::Operation(term) => write!(f, "{}", term),
            Term::Tuple(term) => write!(f, "{}", term),
            Term::Aggregate(term) => write!(f, "{}", term),
        }
    }
}

impl ProgramComponent for Term {
    fn parse(_string: &str) -> Result<Self, ValidationError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        match self {
            Term::Primitive(primitive) => primitive.origin(),
            Term::FunctionTerm(function) => function.origin(),
            Term::Map(map) => map.origin(),
            Term::Operation(operation) => operation.origin(),
            Term::Tuple(tuple) => tuple.origin(),
            Term::Aggregate(aggregate) => aggregate.origin(),
        }
    }

    fn set_origin(self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        match self {
            Term::Primitive(primitive) => Term::Primitive(primitive.set_origin(origin)),
            Term::FunctionTerm(function) => Term::FunctionTerm(function.set_origin(origin)),
            Term::Map(map) => Term::Map(map.set_origin(origin)),
            Term::Operation(operation) => Term::Operation(operation.set_origin(origin)),
            Term::Tuple(tuple) => Term::Tuple(tuple.set_origin(origin)),
            Term::Aggregate(aggregate) => Term::Aggregate(aggregate.set_origin(origin)),
        }
    }

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Result<(), ()>
    where
        Self: Sized,
    {
        match self {
            Term::Primitive(term) => term.validate(builder),
            Term::Aggregate(term) => term.validate(builder),
            Term::FunctionTerm(term) => term.validate(builder),
            Term::Map(term) => term.validate(builder),
            Term::Operation(term) => term.validate(builder),
            Term::Tuple(term) => term.validate(builder),
        }
    }
}

impl IterableVariables for Term {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        let mut iter_primitive = None;
        let mut iter_function = None;
        let mut iter_map = None;
        let mut iter_operation = None;
        let mut iter_tuple = None;
        let mut iter_aggregate = None;

        match self {
            Term::Primitive(primitive) => iter_primitive = Some(primitive.variables()),
            Term::FunctionTerm(function) => iter_function = Some(function.variables()),
            Term::Map(map) => iter_map = Some(map.variables()),
            Term::Operation(operation) => iter_operation = Some(operation.variables()),
            Term::Tuple(tuple) => iter_tuple = Some(tuple.variables()),
            Term::Aggregate(aggregate) => iter_aggregate = Some(aggregate.variables()),
        }

        Box::new(
            iter_primitive
                .into_iter()
                .flatten()
                .chain(iter_function.into_iter().flatten())
                .chain(iter_map.into_iter().flatten())
                .chain(iter_operation.into_iter().flatten())
                .chain(iter_tuple.into_iter().flatten())
                .chain(iter_aggregate.into_iter().flatten()),
        )
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        let mut iter_primitive = None;
        let mut iter_function = None;
        let mut iter_map = None;
        let mut iter_operation = None;
        let mut iter_tuple = None;
        let mut iter_aggregate = None;

        match self {
            Term::Primitive(primitive) => iter_primitive = Some(primitive.variables_mut()),
            Term::FunctionTerm(function) => iter_function = Some(function.variables_mut()),
            Term::Map(map) => iter_map = Some(map.variables_mut()),
            Term::Operation(operation) => iter_operation = Some(operation.variables_mut()),
            Term::Tuple(tuple) => iter_tuple = Some(tuple.variables_mut()),
            Term::Aggregate(aggregate) => iter_aggregate = Some(aggregate.variables_mut()),
        }

        Box::new(
            iter_primitive
                .into_iter()
                .flatten()
                .chain(iter_function.into_iter().flatten())
                .chain(iter_map.into_iter().flatten())
                .chain(iter_operation.into_iter().flatten())
                .chain(iter_tuple.into_iter().flatten())
                .chain(iter_aggregate.into_iter().flatten()),
        )
    }
}
