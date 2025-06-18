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

use delegate::delegate;

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
    error::ValidationReport, origin::Origin, pipeline::id::ProgramComponentId,
};

use super::{
    import_export::io_type::IOType, ComponentBehavior, ComponentIdentity, ComponentSource,
    IterableComponent, IterablePrimitives, IterableVariables, ProgramComponent,
    ProgramComponentKind,
};

/// Term
///
/// Basic building block for expressions like atoms or facts.
#[derive(Debug, Clone, PartialEq, Hash, PartialOrd)]
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

    /// Create an IRI term.
    pub fn constant(iri: &str) -> Self {
        Self::Primitive(Primitive::Ground(GroundTerm::constant(iri)))
    }

    /// Create a language tagged string term.
    pub fn language_tagged(value: &str, tag: &str) -> Self {
        Self::Primitive(Primitive::Ground(GroundTerm::language_tagged(value, tag)))
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
    pub fn terms(&self) -> Box<dyn Iterator<Item = &Term> + '_> {
        match self {
            Term::Primitive(_) => Box::new(None.into_iter()),
            Term::Aggregate(term) => Box::new(Some(term.aggregate_term()).into_iter()),
            Term::FunctionTerm(term) => Box::new(term.terms()),
            Term::Map(term) => Box::new(term.key_value().flat_map(|(key, value)| vec![key, value])),
            Term::Operation(term) => Box::new(term.terms()),
            Term::Tuple(term) => Box::new(term.terms()),
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

    /// Reduce this term by evaluating all contained expressions,
    /// and return a new [Term] with the same [Origin] as `self`.
    ///
    /// This function does nothing if `self` is not ground.
    pub fn reduce(&self) -> Option<Self> {
        Some(match self {
            Term::Operation(operation) => operation.reduce()?,
            Term::Aggregate(term) => Term::Aggregate(term.reduce()?),
            Term::FunctionTerm(term) => Term::FunctionTerm(term.reduce()?),
            Term::Map(term) => Term::Map(term.reduce()?),
            Term::Tuple(term) => Term::Tuple(term.reduce()?),
            Term::Primitive(term) => Term::Primitive(term.clone()),
        })
    }

    /// Check wether this term can be reduced to a ground value,
    /// except for global variables that need to be resolved.
    ///
    /// This is the case if
    ///     * This term does not contain non-global variables.
    ///     * This term does not contain undefined intermediate values.
    pub fn is_resolvable(&self) -> bool {
        match self {
            Term::Primitive(term) => term.is_resolvable(),
            Term::Aggregate(term) => term.is_resolvable(),
            Term::FunctionTerm(term) => term.is_resolvable(),
            Term::Map(term) => term.is_resolvable(),
            Term::Operation(term) => term.is_resolvable(),
            Term::Tuple(term) => term.is_resolvable(),
        }
    }
}

impl ComponentBehavior for Term {
    delegate! {
        to match self {
            Self::Aggregate(term) => term,
            Self::FunctionTerm(term) => term,
            Self::Map(term) => term,
            Self::Operation(term) => term,
            Self::Primitive(term) => term,
            Self::Tuple(term) => term,
        } {
            fn kind(&self) -> ProgramComponentKind;
            fn validate(&self) -> Result<(), ValidationReport>;
            fn boxed_clone(&self) -> Box<dyn ProgramComponent>;
        }
    }
}

impl ComponentSource for Term {
    type Source = Origin;

    delegate! {
        to match self {
            Self::Aggregate(term) => term,
            Self::FunctionTerm(term) => term,
            Self::Map(term) => term,
            Self::Operation(term) => term,
            Self::Primitive(term) => term,
            Self::Tuple(term) => term,
        } {
            fn origin(&self) -> Origin;
            fn set_origin(&mut self, origin: Origin);
        }
    }
}

impl ComponentIdentity for Term {
    delegate! {
        to match self {
            Self::Aggregate(term) => term,
            Self::FunctionTerm(term) => term,
            Self::Map(term) => term,
            Self::Operation(term) => term,
            Self::Primitive(term) => term,
            Self::Tuple(term) => term,
        } {
            fn id(&self) -> ProgramComponentId;
            fn set_id(&mut self, id: ProgramComponentId);
        }
    }
}

impl IterableComponent for Term {
    delegate! {
        to match self {
            Self::Aggregate(term) => term,
            Self::FunctionTerm(term) => term,
            Self::Map(term) => term,
            Self::Operation(term) => term,
            Self::Primitive(term) => term,
            Self::Tuple(term) => term,
        } {
            #[allow(late_bound_lifetime_arguments)]
            fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a dyn ProgramComponent> + 'a>;
            #[allow(late_bound_lifetime_arguments)]
            fn children_mut<'a>(
                &'a mut self,
            ) -> Box<dyn Iterator<Item = &'a mut dyn ProgramComponent> + 'a>;
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

impl From<GroundTerm> for Term {
    fn from(value: GroundTerm) -> Self {
        Self::Primitive(Primitive::from(value))
    }
}

impl From<bool> for Term {
    fn from(value: bool) -> Self {
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

impl From<IOType> for Term {
    fn from(value: IOType) -> Self {
        Self::from(value.name().to_owned())
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
        let mut constant = Term::parse("2 * ($global + 7)").unwrap();
        let substitution = Substitution::new([(
            GlobalVariable::new("global"),
            GroundTerm::parse("3").unwrap(),
        )]);
        substitution.apply(&mut constant);

        let term = Term::from(tuple!(5, constant));
        let reduced = term.reduce().unwrap();

        let expected_term = Term::from(tuple!(5, 20));

        assert_eq!(reduced, expected_term);
    }
}
