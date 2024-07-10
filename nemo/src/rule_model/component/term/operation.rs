//! This module defines [Operation].

use std::{fmt::Display, hash::Hash};

use crate::rule_model::{
    component::{IteratableVariables, ProgramComponent},
    origin::Origin,
};

use super::{primitive::variable::Variable, Term};

/// Supported operations
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum OperationKind {
    /// Equality
    Equal,
    /// Inequality
    Unequals,
    /// Sum of numeric values
    NumericSum,
    /// Subtraction between two numeric values
    NumericSubtraction,
    /// Product of numeric values
    NumericProduct,
    /// Division between two numeric values
    NumericDivision,
    /// Logarithm of a numeric value to some numeric base
    NumericLogarithm,
    /// Numeric value raised to another numeric value
    NumericPower,
    /// Remainder of a division between two numeric values
    NumericRemainder,
    /// Numeric greater than comparison
    NumericGreaterthan,
    /// Numeric greater than or equals comparison
    NumericGreaterthaneq,
    /// Numeric less than comparison
    NumericLessthan,
    /// Numeric less than or equals comparison
    NumericLessthaneq,
    /// Lexicographic comparison between strings
    StringCompare,
    /// Check whether string is contained in another, correspondng to SPARQL function CONTAINS.
    StringContains,
    /// String starting at some start position
    StringSubstring,
    /// First part of a string split by some other string
    StringBefore,
    /// Second part of a string split by some other string
    StringAfter,
    /// Whether string starts with a certain string
    StringStarts,
    /// Whether string ends with a certain string
    StringEnds,
    /// Boolean negation
    BooleanNegation,
    /// Cast to double
    CastToDouble,
    /// Cast to float
    CastToFloat,
    /// Cast to integer
    CastToInteger,
    /// Canonical string representation of a value
    CanonicalString,
    /// Check if value is an integer
    CheckIsInteger,
    /// Check if value is a float
    CheckIsFloat,
    /// Check if value is a double
    CheckIsDouble,
    /// Check if value is an iri
    CheckIsIri,
    /// Check if value is numeric
    CheckIsNumeric,
    /// Check if value is a null
    CheckIsNull,
    /// Check if value is a string
    CheckIsString,
    /// Get datatype of a value
    Datatype,
    /// Get language tag of a languaged tagged string
    LanguageTag,
    /// Lexical value
    LexicalValue,
    /// Absolute value of a numeric value
    NumericAbsolute,
    /// Cosine of a numeric value
    NumericCosine,
    /// Rounding up of a numeric value
    NumericCeil,
    /// Rounding down of a numeric value
    NumericFloor,
    /// Additive inverse of a numeric value
    NumericNegation,
    /// Rounding of a numeric value
    NumericRound,
    /// Sine of a numeric value
    NumericSine,
    /// Square root of a numeric value
    NumericSquareroot,
    /// Tangent of a numeric value
    NumericTangent,
    /// Length of a string value
    StringLength,
    /// Reverse of a string value
    StringReverse,
    /// String converted to lowercase letters
    StringLowercase,
    /// String converted to uppercase letters
    StringUppercase,
    /// Bitwise and operation
    BitAnd,
    /// Bitwise or operation
    BitOr,
    /// Bitwise xor operation
    BitXor,
    /// Conjunction of boolean values
    BooleanConjunction,
    /// Disjunction of boolean values
    BooleanDisjunction,
    /// Minimum of numeric values
    NumericMinimum,
    /// Maximum of numeric values
    NumericMaximum,
    /// Lukasiewicz norm of numeric values
    NumericLukasiewicz,
    /// Concatentation of two string values, correspondng to SPARQL function CONCAT.
    StringConcatenation,
}

impl OperationKind {
    /// Return the [OperationKind] corresponding to the given operation name or `None` if there is no such operation.
    pub fn from_name(name: &str) -> Option<OperationKind> {
        Some(match name.to_uppercase().as_str() {
            "+" => Self::NumericSum,
            "-" => Self::NumericSubtraction,
            "/" => Self::NumericDivision,
            "*" => Self::NumericProduct,
            "<" => Self::NumericLessthan,
            ">" => Self::NumericGreaterthan,
            "<=" => Self::NumericLessthaneq,
            ">=" => Self::NumericGreaterthaneq,
            "isInteger" => Self::CheckIsInteger,
            "isFloat" => Self::CheckIsFloat,
            "isDouble" => Self::CheckIsDouble,
            "isIri" => Self::CheckIsIri,
            "isNumeric" => Self::CheckIsNumeric,
            "isNull" => Self::CheckIsNull,
            "isString" => Self::CheckIsString,
            "ABS" => Self::NumericAbsolute,
            "SQRT" => Self::NumericSquareroot,
            "NOT" => Self::BooleanNegation,
            "fullStr" => Self::CanonicalString,
            "STR" => Self::LexicalValue,
            "SIN" => Self::NumericSine,
            "COS" => Self::NumericCosine,
            "TAN" => Self::NumericTangent,
            "STRLEN" => Self::StringLength,
            "STRREV" => Self::StringReverse,
            "UCASE" => Self::StringLowercase,
            "LCASE" => Self::StringUppercase,
            "ROUND" => Self::NumericRound,
            "CEIL" => Self::NumericCeil,
            "FLOOR" => Self::NumericFloor,
            "DATATYPE" => Self::Datatype,
            "LANG" => Self::LanguageTag,
            "INT" => Self::CastToInteger,
            "DOUBLE" => Self::CastToDouble,
            "FLOAT" => Self::CastToFloat,
            "LOG" => Self::NumericLogarithm,
            "POW" => Self::NumericPower,
            "COMPARE" => Self::StringCompare,
            "CONTAINS" => Self::StringContains,
            "SUBSTR" => Self::StringSubstring,
            "STRSTARTS" => Self::StringStarts,
            "STRENDS" => Self::StringEnds,
            "STRBEFORE" => Self::StringBefore,
            "STRAFTER" => Self::StringAfter,
            "REM" => Self::NumericRemainder,
            "BITAND" => Self::BitAnd,
            "BITOR" => Self::BitOr,
            "BITXOR" => Self::BitXor,
            "MAX" => Self::NumericMaximum,
            "MIN" => Self::NumericMinimum,
            "LUKA" => Self::NumericLukasiewicz,
            "SUM" => Self::NumericSum,
            "PROD" => Self::NumericProduct,
            "AND" => Self::BooleanConjunction,
            "OR" => Self::BooleanDisjunction,
            "CONCAT" => Self::StringConcatenation,
            _ => return None,
        })
    }

    /// Precendence of operations for display purposes.
    pub(crate) fn precedence(&self) -> usize {
        match &self {
            Self::NumericSum => 1,
            Self::NumericSubtraction => 1,
            Self::NumericProduct => 2,
            Self::NumericDivision => 2,
            _ => 3,
        }
    }
}

impl Display for OperationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            OperationKind::Equal => "EQUAL",
            OperationKind::Unequals => "UNEQUAL",
            OperationKind::NumericSum => "SUM",
            OperationKind::NumericSubtraction => "MINUS",
            OperationKind::NumericProduct => "PROD",
            OperationKind::NumericDivision => "DIV",
            OperationKind::NumericLogarithm => "LOG",
            OperationKind::NumericPower => "POW",
            OperationKind::NumericRemainder => "REM",
            OperationKind::NumericGreaterthan => "GT",
            OperationKind::NumericGreaterthaneq => "GTE",
            OperationKind::NumericLessthan => "LT",
            OperationKind::NumericLessthaneq => "LTE",
            OperationKind::StringCompare => "COMPARE",
            OperationKind::StringContains => "CONTAINS",
            OperationKind::StringSubstring => "SUBSTR",
            OperationKind::StringBefore => "STRBEFORE",
            OperationKind::StringAfter => "STRAFTER",
            OperationKind::StringStarts => "STRSTARTS",
            OperationKind::StringEnds => "STRENDS",
            OperationKind::BooleanNegation => "NOT",
            OperationKind::CastToDouble => "DOUBLE",
            OperationKind::CastToFloat => "FLOAT",
            OperationKind::CastToInteger => "INT",
            OperationKind::CanonicalString => "fullStr",
            OperationKind::CheckIsInteger => "isInteger",
            OperationKind::CheckIsFloat => "isFloat",
            OperationKind::CheckIsDouble => "isDouble",
            OperationKind::CheckIsIri => "isIri",
            OperationKind::CheckIsNumeric => "isNumeric",
            OperationKind::CheckIsNull => "isNull",
            OperationKind::CheckIsString => "isString",
            OperationKind::Datatype => "DATATYPE",
            OperationKind::LanguageTag => "LANG",
            OperationKind::LexicalValue => "STR",
            OperationKind::NumericAbsolute => "ABS",
            OperationKind::NumericCosine => "COS",
            OperationKind::NumericCeil => "CEIL",
            OperationKind::NumericFloor => "FLOOR",
            OperationKind::NumericNegation => "MINUS",
            OperationKind::NumericRound => "ROUND",
            OperationKind::NumericSine => "SIN",
            OperationKind::NumericSquareroot => "SQRT",
            OperationKind::NumericTangent => "TAN",
            OperationKind::StringLength => "STRLEN",
            OperationKind::StringReverse => "STRREV",
            OperationKind::StringLowercase => "LCASE",
            OperationKind::StringUppercase => "UCASE",
            OperationKind::BitAnd => "BITAND",
            OperationKind::BitOr => "BITOR",
            OperationKind::BitXor => "BITXOR",
            OperationKind::BooleanConjunction => "AND",
            OperationKind::BooleanDisjunction => "OR",
            OperationKind::NumericMinimum => "MIN",
            OperationKind::NumericMaximum => "MAX",
            OperationKind::NumericLukasiewicz => "LUKA",
            OperationKind::StringConcatenation => "CONCAT",
        };

        write!(f, "{}", string)
    }
}

/// Operation
///
/// An action or computation performed on [Term]s.
/// This can include for example arithmetic or string operations.
#[derive(Debug, Clone, Eq)]
pub struct Operation {
    /// Origin of this component
    origin: Origin,

    /// The kind of operation
    kind: OperationKind,
    /// The input arguments for the operation
    subterms: Vec<Term>,
}

impl Operation {
    /// Create a new [Operation]
    pub fn new(kind: OperationKind, subterms: Vec<Term>) -> Self {
        Self {
            origin: Origin::default(),
            kind,
            subterms,
        }
    }

    /// Create a new [Operation] giving the string name of the operation.
    pub fn new_from_name(operation: &str, subterms: Vec<Term>) -> Option<Self> {
        Some(Self::new(OperationKind::from_name(operation)?, subterms))
    }
}

// Helper functions related to the display implementation
impl Operation {
    /// Puts braces around `term` if it has a lower precendence than `self`.
    fn format_braces_priority(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        term: &Term,
    ) -> std::fmt::Result {
        let need_braces = if let Term::Operation(other) = term {
            self.kind.precedence() > other.kind.precedence()
        } else {
            false
        };

        if need_braces {
            self.format_braces(f, term)
        } else {
            write!(f, "{}", term)
        }
    }

    /// Put braces around the input term.
    fn format_braces(&self, f: &mut std::fmt::Formatter<'_>, term: &Term) -> std::fmt::Result {
        write!(f, "({})", term)
    }

    /// Formats the arguments of an operation as a delimiter separated list.
    fn format_operation_arguments(
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

    /// Returns the infix symbol corresponding to the operation
    /// or `None` if this operation should never be displayed as an infix operation.
    fn infix_representation(&self) -> Option<&str> {
        Some(match &self.kind {
            OperationKind::NumericSum => "+",
            OperationKind::NumericSubtraction => "-",
            OperationKind::NumericProduct => "*",
            &OperationKind::NumericDivision => "/",
            _ => return None,
        })
    }

    /// Format operation in the usual <name>(<arg1>, <arg2>, ...) style
    fn format_operation(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(", self.kind)?;
        self.format_operation_arguments(f, &self.subterms, ",")?;
        f.write_str(")")
    }

    /// Format operation that is more naturally written in an infix style <left> <op> <right>.
    fn format_infix_operation(
        &self,
        f: &mut std::fmt::Formatter<'_>,
        operation: &str,
        left: &Term,
        right: &Term,
    ) -> std::fmt::Result {
        self.format_braces_priority(f, left)?;
        write!(f, " {} ", operation)?;
        self.format_braces_priority(f, right)
    }
}

impl Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(infix) = self.infix_representation() {
            if self.subterms.len() == 2 {
                return self.format_infix_operation(f, infix, &self.subterms[0], &self.subterms[1]);
            }
        }

        self.format_operation(f)
    }
}

impl PartialEq for Operation {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.subterms == other.subterms
    }
}

impl PartialOrd for Operation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.kind.partial_cmp(&other.kind) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.subterms.partial_cmp(&other.subterms)
    }
}

impl Hash for Operation {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.subterms.hash(state);
    }
}

impl ProgramComponent for Operation {
    fn parse(_string: &str) -> Result<Self, crate::rule_model::error::ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }

    fn origin(&self) -> &Origin {
        &self.origin
    }

    fn set_origin(mut self, origin: Origin) -> Self
    where
        Self: Sized,
    {
        self.origin = origin;
        self
    }

    fn validate(&self) -> Result<(), crate::rule_model::error::ProgramConstructionError>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl IteratableVariables for Operation {
    fn variables<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Variable> + 'a> {
        Box::new(self.subterms.iter().flat_map(|term| term.variables()))
    }

    fn variables_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut Variable> + 'a> {
        Box::new(
            self.subterms
                .iter_mut()
                .flat_map(|term| term.variables_mut()),
        )
    }
}
