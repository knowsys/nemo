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

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
};

use aggregate::Aggregate;
use function::FunctionTerm;
use map::Map;
use nemo_physical::datavalues::{AnyDataValue, IriDataValue, MapDataValue, TupleDataValue};
use operation::Operation;
use primitive::{
    ground::GroundTerm,
    variable::{
        existential::ExistentialVariable, global::GlobalVariable, universal::UniversalVariable,
        Variable,
    },
    Primitive,
};
use tuple::Tuple;
use value_type::ValueType;

use crate::rule_model::{
    error::ValidationErrorBuilder, origin::Origin, substitution::Substitution,
};

use super::{IterablePrimitives, IterableVariables, ProgramComponent};

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

    /// Return whether this term is ground,
    /// i.e. whether it does not contain any variables.
    pub fn is_ground(&self) -> bool {
        match self {
            Term::Primitive(term) => term.is_ground(),
            Term::Aggregate(term) => term.is_ground(),
            Term::FunctionTerm(term) => term.is_ground(),
            Term::Map(term) => term.is_ground(),
            Term::Operation(term) => term.is_ground(),
            Term::Tuple(term) => term.is_ground(),
        }
    }

    pub fn reduce(&self) -> Term {
        self.reduce_with_substitution(&Substitution::default())
    }

    /// Reduce (potential) constant expressions contained and return a copy of the resulting [Term].
    pub fn reduce_with_substitution(&self, bindings: &Substitution) -> Term {
        match self {
            Term::Operation(operation) => operation.reduce_with_substitution(bindings),
            Term::Aggregate(term) => Term::Aggregate(term.reduce_with_substitution(bindings)),
            Term::FunctionTerm(term) => Term::FunctionTerm(term.reduce_with_substitution(bindings)),
            Term::Map(term) => Term::Map(term.reduce_with_substitution(bindings)),
            Term::Tuple(term) => Term::Tuple(term.reduce(bindings)),
            Term::Primitive(_) => {
                let mut res = self.clone();
                bindings.apply(&mut res);
                res
            }
        }
    }

    pub fn try_into_ground(
        self,
        bindings: &HashMap<GlobalVariable, GroundTerm>,
    ) -> Result<GroundTerm, Term> {
        let subst = Substitution::new(bindings.clone());

        fn rec(term: Term, subst: &Substitution) -> Result<GroundTerm, Term> {
            match term {
                Term::Map(map) => {
                    let origin = *map.origin();
                    let label = map
                        .tag()
                        .map(|tag| IriDataValue::new(tag.name().to_string()));

                    let mut buffer = Vec::new();
                    for (key, value) in map.into_iter() {
                        let key = rec(key, subst)?.value();
                        let value = rec(value, subst)?.value();
                        buffer.push((key, value));
                    }

                    let res = AnyDataValue::from(MapDataValue::new(label, buffer));
                    Ok(GroundTerm::new(res).set_origin(origin))
                }
                Term::Tuple(tuple) => {
                    let origin = *tuple.origin();

                    let mut buffer = Vec::new();
                    for value in tuple.into_iter() {
                        let value = rec(value, subst)?.value();
                        buffer.push(value);
                    }

                    let res = AnyDataValue::from(TupleDataValue::from_iter(buffer));
                    Ok(GroundTerm::new(res).set_origin(origin))
                }
                Term::FunctionTerm(function_term) => {
                    let origin = *function_term.origin();
                    let label = IriDataValue::new(function_term.tag().name().to_string());

                    let mut buffer = Vec::new();
                    for value in function_term.into_iter() {
                        let value = rec(value, subst)?.value();
                        buffer.push(value);
                    }

                    let res = AnyDataValue::from(TupleDataValue::new(Some(label), buffer));
                    Ok(GroundTerm::new(res).set_origin(origin))
                }
                Term::Operation(operation) => {
                    let reduced = operation.reduce_with_substitution(subst);

                    if matches!(reduced, Term::Operation(_)) {
                        Err(reduced)
                    } else {
                        rec(reduced, subst)
                    }
                }
                Term::Aggregate(_) => Err(term),
                Term::Primitive(_) => {
                    let Term::Primitive(primitive) = term.reduce_with_substitution(subst) else {
                        unreachable!("primitive reduces to primitive")
                    };

                    match primitive {
                        Primitive::Variable(_) => Err(term),
                        Primitive::Ground(ground_term) => Ok(ground_term),
                    }
                }
            }
        }

        rec(self, &subst)
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

impl From<GroundTerm> for Term {
    fn from(value: GroundTerm) -> Self {
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
            Term::Primitive(term) => write!(f, "{term}"),
            Term::FunctionTerm(term) => write!(f, "{term}"),
            Term::Map(term) => write!(f, "{term}"),
            Term::Operation(term) => write!(f, "{term}"),
            Term::Tuple(term) => write!(f, "{term}"),
            Term::Aggregate(term) => write!(f, "{term}"),
        }
    }
}

impl ProgramComponent for Term {
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

    fn validate(&self, builder: &mut ValidationErrorBuilder) -> Option<()>
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

    fn kind(&self) -> super::ProgramComponentKind {
        match self {
            Term::Primitive(term) => term.kind(),
            Term::Aggregate(term) => term.kind(),
            Term::FunctionTerm(term) => term.kind(),
            Term::Map(term) => term.kind(),
            Term::Operation(term) => term.kind(),
            Term::Tuple(term) => term.kind(),
        }
    }
}

impl IterableVariables for Term {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        match self {
            Term::Primitive(term) => term.variables(),
            Term::Aggregate(term) => term.variables(),
            Term::FunctionTerm(term) => term.variables(),
            Term::Map(term) => term.variables(),
            Term::Operation(term) => term.variables(),
            Term::Tuple(term) => term.variables(),
        }
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        match self {
            Term::Primitive(term) => term.variables_mut(),
            Term::Aggregate(term) => term.variables_mut(),
            Term::FunctionTerm(term) => term.variables_mut(),
            Term::Map(term) => term.variables_mut(),
            Term::Operation(term) => term.variables_mut(),
            Term::Tuple(term) => term.variables_mut(),
        }
    }
}

impl IterablePrimitives for Term {
    fn primitive_terms<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Primitive> + 'a> {
        match self {
            Term::Primitive(term) => Box::new(Some(term).into_iter()),
            Term::Aggregate(term) => term.primitive_terms(),
            Term::FunctionTerm(term) => term.primitive_terms(),
            Term::Map(term) => term.primitive_terms(),
            Term::Operation(term) => term.primitive_terms(),
            Term::Tuple(term) => term.primitive_terms(),
        }
    }

    fn primitive_terms_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Term> + 'a> {
        match self {
            Term::Primitive(_) => Box::new(Some(self).into_iter()),
            Term::Aggregate(term) => term.primitive_terms_mut(),
            Term::FunctionTerm(term) => term.primitive_terms_mut(),
            Term::Map(term) => term.primitive_terms_mut(),
            Term::Operation(term) => term.primitive_terms_mut(),
            Term::Tuple(term) => term.primitive_terms_mut(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::rule_model::{
        components::term::primitive::{ground::GroundTerm, variable::global::GlobalVariable},
        substitution::Substitution,
        translation::TranslationComponent,
    };

    use super::Term;

    #[test]
    fn term_reduce_ground() {
        let constant = Term::parse("2 * ($global + 7)").unwrap();
        let term = Term::from(tuple!(5, constant));

        let reduced = term.reduce_with_substitution(&Substitution::new([(
            GlobalVariable::new("global"),
            GroundTerm::parse("3").unwrap(),
        )]));
        let expected_term = Term::from(tuple!(5, 20));

        assert_eq!(reduced, expected_term);
    }

    #[test]
    fn term_reduce_nonground() {
        let expression = Term::parse("?x * (3 + 7)").unwrap();
        let term = Term::from(tuple!(5, expression));

        let reduced = term.reduce();
        let expected_term = Term::parse("(5, ?x * 10)").unwrap();

        assert_eq!(reduced, expected_term);
    }
}
