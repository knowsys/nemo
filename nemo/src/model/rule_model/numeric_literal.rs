use nemo_physical::datatypes::Double;

use crate::model::PrimitiveType;

/// A numerical literal.
#[derive(Eq, PartialEq, Copy, Clone, PartialOrd, Ord)]
pub enum NumericLiteral {
    /// An integer literal.
    Integer(i64),
    /// A decimal literal.
    Decimal(i64, u64),
    /// A double literal.
    Double(Double),
}

impl NumericLiteral {
    /// Get primitive type that fits the literal
    pub fn primitive_type(&self) -> PrimitiveType {
        match self {
            Self::Integer(_) => PrimitiveType::Integer,
            Self::Double(_) => PrimitiveType::Float64,
            Self::Decimal(_, 0) => PrimitiveType::Integer,
            Self::Decimal(_, _) => PrimitiveType::Any,
        }
    }
}

impl std::fmt::Debug for NumericLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumericLiteral::Integer(value) => write!(f, "{value}"),
            NumericLiteral::Decimal(left, right) => write!(f, "{left}.{right}"),
            NumericLiteral::Double(value) => write!(f, "{:E}", f64::from(*value)),
        }
    }
}

impl std::fmt::Display for NumericLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumericLiteral::Integer(value) => write!(f, "{value}"),
            NumericLiteral::Decimal(left, right) => write!(f, "{left}.{right}"),
            NumericLiteral::Double(value) => {
                f.write_str(format!("{:.4}", f64::from(*value)).trim_end_matches(['.', '0']))
            }
        }
    }
}
