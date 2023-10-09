use std::fmt::{Debug, Display};

use crate::model::{types::primitive_logical_value::LOGICAL_NULL_PREFIX, VariableAssignment};

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
            Constant::StringLiteral(literal) => write!(f, "{}", literal),
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

/// Possibly complex term that may occur within an [`super::Atom`]
#[derive(Eq, PartialEq, Clone, PartialOrd, Ord)]
pub enum Term {
    /// Primitive term.
    Primitive(PrimitiveTerm),
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
    /// Squareroot of the given term
    SquareRoot(Box<Term>),
    /// Additive inverse of the given term.
    UnaryMinus(Box<Term>),
    /// Absolute value of the given term.
    Abs(Box<Term>),
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
            Term::Addition(left, right)
            | Term::Subtraction(left, right)
            | Term::Multiplication(left, right)
            | Term::Division(left, right)
            | Term::Exponent(left, right) => {
                let mut terms = left.primitive_terms();
                terms.extend(right.primitive_terms());

                terms
            }
            Term::SquareRoot(sub) | Term::UnaryMinus(sub) | Term::Abs(sub) => sub.primitive_terms(),
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
            Term::Addition(left, right)
            | Term::Subtraction(left, right)
            | Term::Multiplication(left, right)
            | Term::Division(left, right)
            | Term::Exponent(left, right) => {
                left.apply_assignment(assignment);
                right.apply_assignment(assignment);
            }
            Term::SquareRoot(sub) | Term::UnaryMinus(sub) | Term::Abs(sub) => {
                sub.apply_assignment(assignment)
            }
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
            Term::Addition(left, right)
            | Term::Subtraction(left, right)
            | Term::Multiplication(left, right)
            | Term::Division(left, right)
            | Term::Exponent(left, right) => {
                Self::aggregate_subterm_recursive(left) || Self::aggregate_subterm(right)
            }
            Term::SquareRoot(sub) | Term::UnaryMinus(sub) | Term::Abs(sub) => {
                Self::aggregate_subterm_recursive(sub)
            }
            Term::Aggregation(_aggregate) => true,
            Term::Function(sub) => sub.subterms.iter().any(Self::aggregate_subterm_recursive),
        }
    }

    /// Checks if this term contains an aggregate as a sub term.
    /// This is currently not allowed.
    pub fn aggregate_subterm(&self) -> bool {
        match self {
            Term::Primitive(_primitive) => false,
            Term::Addition(left, right)
            | Term::Subtraction(left, right)
            | Term::Multiplication(left, right)
            | Term::Division(left, right)
            | Term::Exponent(left, right) => {
                Self::aggregate_subterm_recursive(left) || Self::aggregate_subterm(right)
            }
            Term::SquareRoot(sub) | Term::UnaryMinus(sub) | Term::Abs(sub) => {
                Self::aggregate_subterm_recursive(sub)
            }
            Term::Aggregation(_aggregate) => false, // We allow aggregation on the top level
            Term::Function(sub) => sub.subterms.iter().any(Self::aggregate_subterm_recursive),
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
            Term::Addition(left, right) => ascii_tree::Tree::Node(
                "Addition".to_string(),
                vec![left.ascii_tree(), right.ascii_tree()],
            ),
            Term::Subtraction(left, right) => ascii_tree::Tree::Node(
                "Subtraction".to_string(),
                vec![left.ascii_tree(), right.ascii_tree()],
            ),
            Term::Multiplication(left, right) => ascii_tree::Tree::Node(
                "Multiplication".to_string(),
                vec![left.ascii_tree(), right.ascii_tree()],
            ),
            Term::Division(left, right) => ascii_tree::Tree::Node(
                "Division".to_string(),
                vec![left.ascii_tree(), right.ascii_tree()],
            ),
            Term::Exponent(left, right) => ascii_tree::Tree::Node(
                "Exponent".to_string(),
                vec![left.ascii_tree(), right.ascii_tree()],
            ),
            Term::SquareRoot(sub) => {
                ascii_tree::Tree::Node("Squareroot".to_string(), vec![sub.ascii_tree()])
            }
            Term::UnaryMinus(sub) => {
                ascii_tree::Tree::Node("Negation".to_string(), vec![sub.ascii_tree()])
            }
            Term::Abs(sub) => ascii_tree::Tree::Node("Abs".to_string(), vec![sub.ascii_tree()]),
            Term::Aggregation(aggregate) => {
                ascii_tree::Tree::Leaf(vec![format!("{:?}", aggregate)])
            }
            Term::Function(_) => todo!(),
        }
    }

    /// Defines the relative priority between term operations.
    /// This is only relevant for the [`Display`] implementation.
    fn priority(&self) -> usize {
        match self {
            Term::Primitive(_) => 0,
            Term::Addition(_, _) => 1,
            Term::Subtraction(_, _) => 1,
            Term::Multiplication(_, _) => 2,
            Term::Division(_, _) => 2,
            Term::Exponent(_, _) => 3,
            Term::SquareRoot(_) => 5,
            Term::UnaryMinus(_) => 5,
            Term::Abs(_) => 5,
            Term::Aggregation(_) => 5,
            Term::Function(_) => 5,
        }
    }

    fn format_braces_priority(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        term: &Term,
    ) -> std::fmt::Result {
        let need_braces = self.priority() > term.priority() && !term.is_primitive();

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
            Term::Addition(left, right) => self.format_binary_operation(f, left, right, " + "),
            Term::Subtraction(left, right) => self.format_binary_operation(f, left, right, " - "),
            Term::Multiplication(left, right) => {
                self.format_binary_operation(f, left, right, " * ")
            }
            Term::Division(left, right) => self.format_binary_operation(f, left, right, " / "),
            Term::Exponent(left, right) => self.format_binary_operation(f, left, right, " ^ "),
            Term::SquareRoot(sub) => {
                f.write_str("sqrt(")?;
                write!(f, "{}", sub)?;
                f.write_str(")")
            }
            Term::UnaryMinus(sub) => {
                f.write_str("-")?;

                self.format_braces_priority(f, sub)
            }
            Term::Abs(sub) => {
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
