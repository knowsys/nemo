//! The semantic domains of data values.

use crate::value::datatype::{rdf, xsd};

/// The domain of a data value.
///
/// Domains partition the value space:
/// values of different domains are always distinct terms.
/// The classification is semantic, not representational.
/// For example, a number might be stored as a decimal internally,
/// but if it is integral it belongs to the [Self::Integer] domain,
/// not [Self::Decimal].
///
/// The domains are ordered as declared, which is their order in the term order:
/// every value of an earlier domain is below every value of a later one.
/// Nulls come first and IRIs before literals,
/// as in SPARQL's [ORDER BY](https://www.w3.org/TR/sparql11-query/#modOrderBy),
/// whose order `#min` and `#max` take.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ValueDomain {
    /// Labeled nulls.
    Null,
    /// [IRIs](https://www.w3.org/TR/rdf11-concepts/#section-IRIs), the RDF resources.
    Iri,
    /// Plain strings, the [`xsd:string`](https://www.w3.org/TR/xmlschema11-2/#string) value space.
    String,
    /// [Language-tagged strings](https://www.w3.org/TR/rdf11-concepts/#dfn-language-tagged-string),
    /// the `rdf:langString` value space.
    LanguageString,
    /// Booleans, the [`xsd:boolean`](https://www.w3.org/TR/xmlschema11-2/#boolean) value space.
    Boolean,
    /// The integral values of the [`xsd:decimal`](https://www.w3.org/TR/xmlschema11-2/#decimal) value space,
    /// which is the [`xsd:integer`](https://www.w3.org/TR/xmlschema11-2/#integer) value space.
    Integer,
    /// The non-integral values of the [`xsd:decimal`](https://www.w3.org/TR/xmlschema11-2/#decimal) value space.
    Decimal,
    /// 32-bit floating point numbers, the [`xsd:float`](https://www.w3.org/TR/xmlschema11-2/#float) value space.
    Float,
    /// 64-bit floating point numbers, the [`xsd:double`](https://www.w3.org/TR/xmlschema11-2/#double) value space.
    Double,
    /// Instants with an optional timezone, the [`xsd:dateTime`](https://www.w3.org/TR/xmlschema11-2/#dateTime) value space.
    DateTime,
    /// Calendar days with an optional timezone, the [`xsd:date`](https://www.w3.org/TR/xmlschema11-2/#date) value space.
    Date,
    /// Times of day with an optional timezone, the [`xsd:time`](https://www.w3.org/TR/xmlschema11-2/#time) value space.
    Time,
    /// The durations without months,
    /// the [`xsd:dayTimeDuration`](https://www.w3.org/TR/xmlschema11-2/#dayTimeDuration) value space.
    DayTimeDuration,
    /// The durations of months only, the zero duration excluded,
    /// the rest of the [`xsd:yearMonthDuration`](https://www.w3.org/TR/xmlschema11-2/#yearMonthDuration) value space.
    YearMonthDuration,
    /// The durations with both months and seconds,
    /// the rest of the [`xsd:duration`](https://www.w3.org/TR/xmlschema11-2/#duration) value space.
    Duration,
    /// Tuples, optionally labeled.
    Tuple,
    /// Maps, optionally labeled.
    Map,
    /// [Literals](https://www.w3.org/TR/rdf11-concepts/#section-Graph-Literal)
    /// of a datatype this crate has no native representation for.
    Other,
}

impl ValueDomain {
    /// Every domain, in declaration order.
    pub const ALL: [Self; 18] = [
        Self::Null,
        Self::Iri,
        Self::String,
        Self::LanguageString,
        Self::Boolean,
        Self::Integer,
        Self::Decimal,
        Self::Float,
        Self::Double,
        Self::DateTime,
        Self::Date,
        Self::Time,
        Self::DayTimeDuration,
        Self::YearMonthDuration,
        Self::Duration,
        Self::Tuple,
        Self::Map,
        Self::Other,
    ];

    /// Returns the datatype IRI of the literals in this domain
    /// and `None` for a domain without a fixed one.
    pub fn datatype_iri(self) -> Option<&'static str> {
        match self {
            ValueDomain::String => Some(xsd::STRING),
            ValueDomain::LanguageString => Some(rdf::LANG_STRING),
            ValueDomain::Boolean => Some(xsd::BOOLEAN),
            ValueDomain::Integer => Some(xsd::INTEGER),
            ValueDomain::Decimal => Some(xsd::DECIMAL),
            ValueDomain::Float => Some(xsd::FLOAT),
            ValueDomain::Double => Some(xsd::DOUBLE),
            ValueDomain::DateTime => Some(xsd::DATE_TIME),
            ValueDomain::Date => Some(xsd::DATE),
            ValueDomain::Time => Some(xsd::TIME),
            ValueDomain::DayTimeDuration => Some(xsd::DAY_TIME_DURATION),
            ValueDomain::YearMonthDuration => Some(xsd::YEAR_MONTH_DURATION),
            ValueDomain::Duration => Some(xsd::DURATION),
            ValueDomain::Iri
            | ValueDomain::Null
            | ValueDomain::Tuple
            | ValueDomain::Map
            | ValueDomain::Other => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_lists_every_domain_at_its_discriminant() {
        for (position, domain) in ValueDomain::ALL.into_iter().enumerate() {
            assert_eq!(domain as usize, position, "{domain:?}");
        }

        let listed = |domain: ValueDomain| match domain {
            ValueDomain::Iri
            | ValueDomain::String
            | ValueDomain::LanguageString
            | ValueDomain::Boolean
            | ValueDomain::Integer
            | ValueDomain::Decimal
            | ValueDomain::Float
            | ValueDomain::Double
            | ValueDomain::DateTime
            | ValueDomain::Date
            | ValueDomain::Time
            | ValueDomain::DayTimeDuration
            | ValueDomain::YearMonthDuration
            | ValueDomain::Duration
            | ValueDomain::Null
            | ValueDomain::Tuple
            | ValueDomain::Map
            | ValueDomain::Other => ValueDomain::ALL.contains(&domain),
        };
        assert!(ValueDomain::ALL.into_iter().all(listed));
    }
}
