//! This module provides implementations [`super::DataValue`]s that represent floating point numbers.
//! By convention (following XML Schema), we consider the value spaces of floats of different precisions
//! to be disjoint, and also dijoint with any integer domain.

use super::{DataValue,ValueDomain};

/// Physical representation of a 64bit floating point number as an f64.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Double(f64);

impl DataValue for Double {
    fn datatype_iri(&self) -> String {
        self.value_domain().type_iri()
    }

    fn lexical_value(&self) -> String {
        self.0.to_string()
    }

    fn value_domain(&self) -> ValueDomain {
        ValueDomain::Double
    }

    fn to_f64_unchecked(&self) -> f64 {
        self.0
    }
}

#[cfg(test)]
mod test {
    use super::Double;
    use crate::datavalues::{DataValue,ValueDomain};

    #[test]
    fn test_double() {
        let value: f64 = 2.34e3;
        let long1 = Double(value);

        assert_eq!(long1.lexical_value(), value.to_string());
        assert_eq!(long1.datatype_iri(), "http://www.w3.org/2001/XMLSchema#double".to_string());
        assert_eq!(long1.value_domain(), ValueDomain::Double);

        assert_eq!(long1.to_f64(), Some(value));
        assert_eq!(long1.to_f64_unchecked(), value);
    }
}