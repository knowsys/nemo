use std::fmt::{Debug, Display};

use nemo_physical::{
    columnar::operations::arithmetic::traits::{CheckedPow, CheckedSquareRoot},
    datatypes::{DataValueT, Double},
};
use num::{traits::CheckedNeg, Zero};

use crate::model::{
    types::primitive_logical_value::LOGICAL_NULL_PREFIX, PrimitiveType, VariableAssignment,
};

use super::{Aggregate, Identifier, NumericLiteral, RdfLiteral};

/// Variable that can be bound to a specific value
#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub enum Variable {
    /// A universally quantified variable.
    Universal(Identifier),
    /// An existentially quantified variable.
    Existential(Identifier),
}

impl Variable {
    /// Return the name of the variable.
    pub fn name(&self) -> String {
        match self {
            Self::Universal(identifier) | Self::Existential(identifier) => identifier.name(),
        }
    }

    /// Return whether this is a universal variable.
    pub fn is_universal(&self) -> bool {
        matches!(self, Variable::Universal(_))
    }

    /// Return whether this is an existential variable.
    pub fn is_existential(&self) -> bool {
        matches!(self, Variable::Existential(_))
    }
}

impl Display for Variable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let prefix = match self {
            Variable::Universal(_) => "?",
            Variable::Existential(_) => "!",
        };

        write!(f, "{}{}", prefix, &self.name())
    }
}

/// A term with a specific constant value
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum Constant {
    /// An (abstract) constant.
    Abstract(Identifier),
    /// A numeric literal.
    NumericLiteral(NumericLiteral),
    /// A string literal.
    StringLiteral(String),
    /// An RDF-literal.
    RdfLiteral(RdfLiteral),
}

impl Display for Constant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Constant::Abstract(Identifier(abstract_string)) => {
                // Nulls on logical level start with __Null# and shall be wrapped in angle brackets
                // blank nodes and anything that starts with an ascii letter (like bare names)
                // should not be wrapped in angle brackets
                if !abstract_string.starts_with(LOGICAL_NULL_PREFIX)
                    && abstract_string.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
                {
                    write!(f, "{abstract_string}")
                }
                // everything else (including nulls) shall be wrapped in angle_brackets
                else {
                    write!(f, "<{abstract_string}>")
                }
            }
            Constant::NumericLiteral(literal) => write!(f, "{}", literal),
            Constant::StringLiteral(literal) => write!(f, "\"{}\"", literal),
            Constant::RdfLiteral(literal) => write!(f, "{}", literal),
        }
    }
}

/// Simple term that is either a constant or a variable
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum PrimitiveTerm {
    /// A constant.
    Constant(Constant),
    /// A variable.
    Variable(Variable),
}

impl From<Constant> for PrimitiveTerm {
    fn from(value: Constant) -> Self {
        Self::Constant(value)
    }
}

impl PrimitiveTerm {
    /// Return `true` if term is not a variable.
    pub fn is_ground(&self) -> bool {
        !matches!(self, PrimitiveTerm::Variable(_))
    }
}

impl Display for PrimitiveTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrimitiveTerm::Constant(term) => write!(f, "{}", term),
            PrimitiveTerm::Variable(term) => write!(f, "{}", term),
        }
    }
}

/// Represents an abstract logical function
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub struct AbstractFunction {
    /// Name of the function
    pub name: Identifier,
    /// Its subterms
    pub subterms: Vec<Term>,
}

/// Binary operation between two [`Term`]
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum BinaryOperation {
    /// Sum between terms.
    Addition(Box<Term>, Box<Term>),
    /// Difference between two terms.
    Subtraction(Box<Term>, Box<Term>),
    /// Product between terms.
    Multiplication(Box<Term>, Box<Term>),
    /// Ratio between the two terms.
    Division(Box<Term>, Box<Term>),
    /// First term raised to the power of the second term.
    Exponent(Box<Term>, Box<Term>),
}

impl BinaryOperation {
    /// Returns the left and the right [`Term`] of this binary operation.
    pub fn terms(&self) -> (&Term, &Term) {
        match self {
            BinaryOperation::Addition(left, right)
            | BinaryOperation::Subtraction(left, right)
            | BinaryOperation::Multiplication(left, right)
            | BinaryOperation::Division(left, right)
            | BinaryOperation::Exponent(left, right) => (left, right),
        }
    }

    /// Returns a mutable reference to the left and the right [`Term`] of this binary operation.
    pub fn terms_mut(&mut self) -> (&mut Term, &mut Term) {
        match self {
            BinaryOperation::Addition(left, right)
            | BinaryOperation::Subtraction(left, right)
            | BinaryOperation::Multiplication(left, right)
            | BinaryOperation::Division(left, right)
            | BinaryOperation::Exponent(left, right) => (left, right),
        }
    }

    /// Returns the left [`Term`] of this binary operation.
    pub fn left(&self) -> &Term {
        self.terms().0
    }

    /// Returns the right [`Term`] of this binary operation.
    pub fn right(&self) -> &Term {
        self.terms().1
    }

    /// Return the name of the operation.
    pub fn name(&self) -> String {
        let name = match self {
            BinaryOperation::Addition(_, _) => "Addition",
            BinaryOperation::Subtraction(_, _) => "Subtraction",
            BinaryOperation::Multiplication(_, _) => "Multiplication",
            BinaryOperation::Division(_, _) => "Division",
            BinaryOperation::Exponent(_, _) => "Exponent",
        };

        String::from(name)
    }
}

/// Unary operation applied to a [`Term`]
#[derive(Debug, Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum UnaryOperation {
    /// Squareroot of the given term
    SquareRoot(Box<Term>),
    /// Additive inverse of the given term.
    UnaryMinus(Box<Term>),
    /// Absolute value of the given term.
    Abs(Box<Term>),
}

impl UnaryOperation {
    /// Return a function which is able to construct the respective term based on the function name.
    /// Returns `None` if the provided function name does not correspond to a know unary function.
    pub fn construct_from_name(name: &str) -> Option<Box<dyn Fn(Term) -> Term>> {
        match name {
            "Abs" => Some(Box::new(|t| Term::Unary(UnaryOperation::Abs(Box::new(t))))),
            "Sqrt" => Some(Box::new(|t| {
                Term::Unary(UnaryOperation::SquareRoot(Box::new(t)))
            })),
            _ => None,
        }
    }

    /// Return the [`Term`] to which the unary operation is applied.
    pub fn term(&self) -> &Term {
        match self {
            UnaryOperation::SquareRoot(term)
            | UnaryOperation::UnaryMinus(term)
            | UnaryOperation::Abs(term) => term,
        }
    }

    /// Return the [`Term`] to which the unary operation is applied.
    pub fn term_mut(&mut self) -> &mut Term {
        match self {
            UnaryOperation::SquareRoot(term)
            | UnaryOperation::UnaryMinus(term)
            | UnaryOperation::Abs(term) => term,
        }
    }

    /// Return the name of the operation.
    pub fn name(&self) -> String {
        let name = match self {
            UnaryOperation::SquareRoot(_) => "SquareRoot",
            UnaryOperation::UnaryMinus(_) => "UnaryMinus",
            UnaryOperation::Abs(_) => "Abs",
        };

        String::from(name)
    }
}

/// Possibly complex term that may occur within an [`super::Atom`]
#[derive(Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum Term {
    /// Primitive term.
    Primitive(PrimitiveTerm),
    /// Binary operation.
    Binary(BinaryOperation),
    /// Unary operation.
    Unary(UnaryOperation),
    /// Aggregation.
    Aggregation(Aggregate),
    /// Abstract Function.
    Function(AbstractFunction),
}

impl Term {
    /// If the term is a simple [`PrimitiveTerm`] then return it.
    /// Otherwise return `None`.
    pub fn as_primitive(&self) -> Option<PrimitiveTerm> {
        match self {
            Term::Primitive(primitive) => Some(primitive.clone()),
            _ => None,
        }
    }

    /// Returns `true` if term is primitive.
    /// Returns `false` if term is composite.
    pub fn is_primitive(&self) -> bool {
        self.as_primitive().is_some()
    }

    /// Return all [`PrimitiveTerm`]s that make up this term.
    pub fn primitive_terms(&self) -> Vec<&PrimitiveTerm> {
        match self {
            Term::Primitive(primitive) => {
                vec![primitive]
            }
            Term::Binary(binary) => {
                let (left, right) = binary.terms();

                let mut terms = left.primitive_terms();
                terms.extend(right.primitive_terms());

                terms
            }
            Term::Unary(unary) => unary.term().primitive_terms(),
            Term::Function(f) => f
                .subterms
                .iter()
                .flat_map(|t| t.primitive_terms())
                .collect(),
            Term::Aggregation(aggregate) => aggregate.terms.iter().collect(),
        }
    }

    /// Return all variables in the atom.
    pub fn variables(&self) -> impl Iterator<Item = &Variable> {
        self.primitive_terms()
            .into_iter()
            .filter_map(|term| match term {
                PrimitiveTerm::Variable(var) => Some(var),
                _ => None,
            })
    }

    /// Return all universally quantified variables in the atom.
    pub fn universal_variables(&self) -> impl Iterator<Item = &Variable> {
        self.variables()
            .filter(|var| matches!(var, Variable::Universal(_)))
    }

    /// Return all existentially quantified variables in the atom.
    pub fn existential_variables(&self) -> impl Iterator<Item = &Variable> {
        self.variables()
            .filter(|var| matches!(var, Variable::Existential(_)))
    }

    /// Replaces [`Variable`]s with [`Term`]s according to the provided assignment.
    pub fn apply_assignment(&mut self, assignment: &VariableAssignment) {
        match self {
            Term::Primitive(primitive) => {
                if let PrimitiveTerm::Variable(variable) = primitive {
                    if let Some(value) = assignment.get(variable) {
                        *self = value.clone();
                    }
                }
            }
            Term::Binary(binary) => {
                let (left, right) = binary.terms_mut();

                left.apply_assignment(assignment);
                right.apply_assignment(assignment);
            }
            Term::Unary(unary) => unary.term_mut().apply_assignment(assignment),
            Term::Aggregation(aggregate) => aggregate.apply_assignment(assignment),
            Term::Function(sub) => sub
                .subterms
                .iter_mut()
                .for_each(|t| t.apply_assignment(assignment)),
        }
    }

    fn aggregate_subterm_recursive(term: &Term) -> bool {
        match term {
            Term::Primitive(_primitive) => false,
            Term::Binary(binary) => {
                let (left, right) = binary.terms();

                Self::aggregate_subterm_recursive(left) || Self::aggregate_subterm(right)
            }
            Term::Unary(unary) => Self::aggregate_subterm_recursive(unary.term()),
            Term::Aggregation(_aggregate) => true,
            Term::Function(sub) => sub.subterms.iter().any(Self::aggregate_subterm_recursive),
        }
    }

    /// Checks if this term contains an aggregate as a sub term.
    /// This is currently not allowed.
    pub fn aggregate_subterm(&self) -> bool {
        match self {
            Term::Primitive(_primitive) => false,
            Term::Binary(binary) => {
                let (left, right) = binary.terms();

                Self::aggregate_subterm_recursive(left) || Self::aggregate_subterm(right)
            }
            Term::Unary(unary) => Self::aggregate_subterm_recursive(unary.term()),
            Term::Aggregation(_aggregate) => false, // We allow aggregation on the top level
            Term::Function(sub) => sub.subterms.iter().any(Self::aggregate_subterm_recursive),
        }
    }

    /// Evaluates a constant (numeric) term
    /// We do this by casting everything to `f64` as it seems like the most general number type.
    /// Returns `None` if this not possible.
    pub fn evaluate_constant_numeric(&self) -> Option<Double> {
        match self {
            Term::Primitive(primitive) => match primitive {
                PrimitiveTerm::Constant(constant) => {
                    if let DataValueT::Double(value) = PrimitiveType::Float64
                        .ground_term_to_data_value_t(constant.clone())
                        .ok()?
                    {
                        Some(value)
                    } else {
                        None
                    }
                }
                PrimitiveTerm::Variable(_) => todo!(),
            },
            Term::Binary(binary) => {
                let (left, right) = binary.terms();
                let value_left = left.evaluate_constant_numeric()?;
                let value_right = right.evaluate_constant_numeric()?;

                match binary {
                    BinaryOperation::Addition(_, _) => Some(value_left + value_right),
                    BinaryOperation::Subtraction(_, _) => Some(value_left - value_right),
                    BinaryOperation::Multiplication(_, _) => Some(value_left * value_right),
                    BinaryOperation::Division(_, _) => Some(value_left / value_right),
                    BinaryOperation::Exponent(_, _) => Some(value_left.checked_pow(value_right)?),
                }
            }
            Term::Unary(unary) => {
                let sub = unary.term();
                let mut value_sub = sub.evaluate_constant_numeric()?;

                match unary {
                    UnaryOperation::SquareRoot(_) => Some(value_sub.checked_sqrt()?),
                    UnaryOperation::UnaryMinus(_) => Some(value_sub.checked_neg()?),
                    UnaryOperation::Abs(_) => {
                        if value_sub < Double::zero() {
                            value_sub *= Double::new(-1.0).unwrap();
                        }

                        Some(value_sub)
                    }
                }
            }
            Term::Aggregation(_) => None,
            Term::Function(_) => None,
        }
    }
}

impl From<PrimitiveTerm> for Term {
    fn from(value: PrimitiveTerm) -> Self {
        Term::Primitive(value)
    }
}

impl Term {
    fn ascii_tree(&self) -> ascii_tree::Tree {
        match self {
            Term::Primitive(primitive) => ascii_tree::Tree::Leaf(vec![format!("{:?}", primitive)]),
            Term::Binary(binary) => ascii_tree::Tree::Node(
                binary.name(),
                vec![binary.left().ascii_tree(), binary.right().ascii_tree()],
            ),
            Term::Unary(unary) => {
                ascii_tree::Tree::Node(unary.name(), vec![unary.term().ascii_tree()])
            }
            Term::Aggregation(aggregate) => {
                ascii_tree::Tree::Leaf(vec![format!("{:?}", aggregate)])
            }
            Term::Function(function) => ascii_tree::Tree::Node(
                function.name.to_string(),
                function.subterms.iter().map(|s| s.ascii_tree()).collect(),
            ),
        }
    }

    /// Defines the precedence of the term operations.
    /// This is only relevant for the [`Display`] implementation.
    fn precedence(&self) -> usize {
        match self {
            Term::Primitive(_) => 0,
            Term::Binary(BinaryOperation::Addition(_, _)) => 1,
            Term::Binary(BinaryOperation::Subtraction(_, _)) => 1,
            Term::Binary(BinaryOperation::Multiplication(_, _)) => 2,
            Term::Binary(BinaryOperation::Division(_, _)) => 2,
            Term::Binary(BinaryOperation::Exponent(_, _)) => 3,
            Term::Unary(UnaryOperation::SquareRoot(_)) => 5,
            Term::Unary(UnaryOperation::UnaryMinus(_)) => 5,
            Term::Unary(UnaryOperation::Abs(_)) => 5,
            Term::Aggregation(_) => 5,
            Term::Function(_) => 5,
        }
    }

    fn format_braces_priority(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        term: &Term,
    ) -> std::fmt::Result {
        let need_braces = self.precedence() > term.precedence() && !term.is_primitive();

        if need_braces {
            self.format_braces(f, term)
        } else {
            write!(f, "{}", term)
        }
    }

    fn format_braces(&self, f: &mut std::fmt::Formatter<'_>, term: &Term) -> std::fmt::Result {
        f.write_str("(")?;
        write!(f, "{}", term)?;
        f.write_str(")")
    }

    fn format_multinary_operation(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        terms: &[Term],
        delimiter: &str,
    ) -> std::fmt::Result {
        for (index, term) in terms.iter().enumerate() {
            self.format_braces_priority(f, term)?;

            if index < terms.len() - 1 {
                f.write_str(delimiter)?;
            }
        }

        Ok(())
    }

    fn format_binary_operation(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        left: &Term,
        right: &Term,
        delimiter: &str,
    ) -> std::fmt::Result {
        self.format_braces_priority(f, left)?;
        f.write_str(delimiter)?;
        self.format_braces_priority(f, right)
    }
}

impl Debug for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        ascii_tree::write_tree(f, &self.ascii_tree())
    }
}

impl Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Term::Primitive(primitive) => write!(f, "{}", primitive),
            Term::Binary(BinaryOperation::Addition(left, right)) => {
                self.format_binary_operation(f, left, right, " + ")
            }
            Term::Binary(BinaryOperation::Subtraction(left, right)) => {
                self.format_binary_operation(f, left, right, " - ")
            }
            Term::Binary(BinaryOperation::Multiplication(left, right)) => {
                self.format_binary_operation(f, left, right, " * ")
            }
            Term::Binary(BinaryOperation::Division(left, right)) => {
                self.format_binary_operation(f, left, right, " / ")
            }
            Term::Binary(BinaryOperation::Exponent(left, right)) => {
                self.format_binary_operation(f, left, right, " ^ ")
            }
            Term::Unary(UnaryOperation::SquareRoot(sub)) => {
                f.write_str("sqrt(")?;
                write!(f, "{}", sub)?;
                f.write_str(")")
            }
            Term::Unary(UnaryOperation::UnaryMinus(sub)) => {
                f.write_str("-")?;

                self.format_braces_priority(f, sub)
            }
            Term::Unary(UnaryOperation::Abs(sub)) => {
                f.write_str("|")?;
                write!(f, "{}", sub)?;
                f.write_str("|")
            }
            Term::Aggregation(aggregate) => write!(f, "{}", aggregate),
            Term::Function(function) => {
                f.write_str(&function.name.to_string())?;
                f.write_str("(")?;
                self.format_multinary_operation(f, &function.subterms, ", ")?;
                f.write_str(")")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::assert_eq;

    use nemo_physical::datatypes::Double;

    use crate::model::{
        InvalidRdfLiteral, NumericLiteral, RdfLiteral, XSD_DECIMAL, XSD_DOUBLE, XSD_INTEGER,
        XSD_STRING,
    };

    use super::*;

    #[test]
    fn rdf_literal_normalization() {
        let language_string_literal = RdfLiteral::LanguageString {
            value: "language string".to_string(),
            tag: "en".to_string(),
        };
        let random_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "some random datavalue".to_string(),
            datatype: "a datatype that I totally did not just make up".to_string(),
        };
        let string_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "string datavalue".to_string(),
            datatype: XSD_STRING.to_string(),
        };
        let integer_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "73".to_string(),
            datatype: XSD_INTEGER.to_string(),
        };
        let decimal_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "1.23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };
        let signed_decimal_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "+1.23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };
        let negative_decimal_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "-1.23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };
        let pointless_decimal_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };
        let signed_pointless_decimal_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "+23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };
        let negative_pointless_decimal_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "-23".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };
        let double_datavalue_literal = RdfLiteral::DatatypeValue {
            value: "3.33".to_string(),
            datatype: XSD_DOUBLE.to_string(),
        };
        let large_integer_literal = RdfLiteral::DatatypeValue {
            value: "9950000000000000000".to_string(),
            datatype: XSD_INTEGER.to_string(),
        };
        let large_decimal_literal = RdfLiteral::DatatypeValue {
            value: "9950000000000000001".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };
        let invalid_integer_literal = RdfLiteral::DatatypeValue {
            value: "123.45".to_string(),
            datatype: XSD_INTEGER.to_string(),
        };
        let invalid_decimal_literal = RdfLiteral::DatatypeValue {
            value: "123.45a".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        };

        let expected_language_string_literal =
            Constant::RdfLiteral(language_string_literal.clone());
        let expected_random_datavalue_literal =
            Constant::RdfLiteral(random_datavalue_literal.clone());
        let expected_string_datavalue_literal =
            Constant::StringLiteral("string datavalue".to_string());
        let expected_integer_datavalue_literal =
            Constant::NumericLiteral(NumericLiteral::Integer(73));
        let expected_decimal_datavalue_literal =
            Constant::NumericLiteral(NumericLiteral::Decimal(1, 23));
        let expected_signed_decimal_datavalue_literal =
            Constant::NumericLiteral(NumericLiteral::Decimal(1, 23));
        let expected_negative_decimal_datavalue_literal =
            Constant::NumericLiteral(NumericLiteral::Decimal(-1, 23));
        let expected_pointless_decimal_datavalue_literal =
            Constant::NumericLiteral(NumericLiteral::Decimal(23, 0));
        let expected_signed_pointless_decimal_datavalue_literal =
            Constant::NumericLiteral(NumericLiteral::Decimal(23, 0));
        let expected_negative_pointless_decimal_datavalue_literal =
            Constant::NumericLiteral(NumericLiteral::Decimal(-23, 0));
        let expected_double_datavalue_literal =
            Constant::NumericLiteral(NumericLiteral::Double(Double::new(3.33).unwrap()));
        let expected_large_integer_literal = Constant::RdfLiteral(RdfLiteral::DatatypeValue {
            value: "9950000000000000000".to_string(),
            datatype: XSD_INTEGER.to_string(),
        });
        let expected_large_decimal_literal = Constant::RdfLiteral(RdfLiteral::DatatypeValue {
            value: "9950000000000000001".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        });
        let expected_invalid_integer_literal = InvalidRdfLiteral::new(RdfLiteral::DatatypeValue {
            value: "123.45".to_string(),
            datatype: XSD_INTEGER.to_string(),
        });
        let expected_invalid_decimal_literal = InvalidRdfLiteral::new(RdfLiteral::DatatypeValue {
            value: "123.45a".to_string(),
            datatype: XSD_DECIMAL.to_string(),
        });

        assert_eq!(
            Constant::try_from(language_string_literal).unwrap(),
            expected_language_string_literal
        );
        assert_eq!(
            Constant::try_from(random_datavalue_literal).unwrap(),
            expected_random_datavalue_literal
        );
        assert_eq!(
            Constant::try_from(string_datavalue_literal).unwrap(),
            expected_string_datavalue_literal
        );
        assert_eq!(
            Constant::try_from(integer_datavalue_literal).unwrap(),
            expected_integer_datavalue_literal
        );
        assert_eq!(
            Constant::try_from(decimal_datavalue_literal).unwrap(),
            expected_decimal_datavalue_literal
        );
        assert_eq!(
            Constant::try_from(signed_decimal_datavalue_literal).unwrap(),
            expected_signed_decimal_datavalue_literal
        );
        assert_eq!(
            Constant::try_from(negative_decimal_datavalue_literal).unwrap(),
            expected_negative_decimal_datavalue_literal
        );
        assert_eq!(
            Constant::try_from(pointless_decimal_datavalue_literal).unwrap(),
            expected_pointless_decimal_datavalue_literal
        );
        assert_eq!(
            Constant::try_from(signed_pointless_decimal_datavalue_literal).unwrap(),
            expected_signed_pointless_decimal_datavalue_literal
        );
        assert_eq!(
            Constant::try_from(negative_pointless_decimal_datavalue_literal).unwrap(),
            expected_negative_pointless_decimal_datavalue_literal
        );
        assert_eq!(
            Constant::try_from(double_datavalue_literal).unwrap(),
            expected_double_datavalue_literal
        );
        assert_eq!(
            Constant::try_from(large_integer_literal).unwrap(),
            expected_large_integer_literal
        );
        assert_eq!(
            Constant::try_from(large_decimal_literal).unwrap(),
            expected_large_decimal_literal
        );
        assert_eq!(
            Constant::try_from(invalid_integer_literal).unwrap_err(),
            expected_invalid_integer_literal
        );
        assert_eq!(
            Constant::try_from(invalid_decimal_literal).unwrap_err(),
            expected_invalid_decimal_literal
        );
    }
}
