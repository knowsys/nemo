//! Sets of value domains.

use std::{
    fmt,
    ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign},
};

use crate::value::ValueDomain;

/// A set of value [domains](ValueDomain).
///
/// As part of a [Type](crate::value::Type), it bounds the domains of the values
/// that a place, such as a column or the result of an expression, can hold.
/// The empty set describes a place that no value can reach.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct DomainSet(u32);

/// Returns the bit of a domain in the set.
const fn bit(domain: ValueDomain) -> u32 {
    1 << (domain as u32)
}

/// The number of domains.
///
/// Every bit at this position or above stays zero.
#[allow(clippy::cast_possible_truncation)]
const DOMAINS: u32 = ValueDomain::ALL.len() as u32;

/// Checks at compile time that the bit of every domain lies below [DOMAINS],
/// so that [ANY](DomainSet::ANY) contains every domain.
const _: () = {
    let mut index = 0;
    while index < ValueDomain::ALL.len() {
        assert!((ValueDomain::ALL[index] as u32) < DOMAINS);
        index += 1;
    }
};

impl DomainSet {
    /// The empty set, of a place that no value can reach.
    pub const EMPTY: Self = Self(0);

    /// The set of all domains, of a place about which nothing is known.
    pub const ANY: Self = Self((1 << DOMAINS) - 1);

    // One set for each domain.
    /// The IRI domain.
    pub const IRI: Self = Self::of(ValueDomain::Iri);
    /// The domain of plain strings.
    pub const STRING: Self = Self::of(ValueDomain::String);
    /// The domain of language-tagged strings.
    pub const LANGUAGE_STRING: Self = Self::of(ValueDomain::LanguageString);
    /// The boolean domain.
    pub const BOOLEAN: Self = Self::of(ValueDomain::Boolean);
    /// The integer domain.
    pub const INTEGER: Self = Self::of(ValueDomain::Integer);
    /// The domain of the decimals that are not integral.
    pub const DECIMAL: Self = Self::of(ValueDomain::Decimal);
    /// The domain of 32-bit floats.
    pub const FLOAT: Self = Self::of(ValueDomain::Float);
    /// The domain of 64-bit floats.
    pub const DOUBLE: Self = Self::of(ValueDomain::Double);
    /// The dateTime domain.
    pub const DATE_TIME: Self = Self::of(ValueDomain::DateTime);
    /// The date domain.
    pub const DATE: Self = Self::of(ValueDomain::Date);
    /// The time domain.
    pub const TIME: Self = Self::of(ValueDomain::Time);
    /// The dayTimeDuration domain.
    pub const DAY_TIME_DURATION: Self = Self::of(ValueDomain::DayTimeDuration);
    /// The yearMonthDuration domain.
    pub const YEAR_MONTH_DURATION: Self = Self::of(ValueDomain::YearMonthDuration);
    /// The domain of the durations with both months and seconds,
    /// [Duration](ValueDomain::Duration).
    ///
    /// [DURATION](Self::DURATION) is the set of all three duration domains.
    pub const MIXED_DURATION: Self = Self::of(ValueDomain::Duration);
    /// The domain of labeled nulls.
    pub const NULL: Self = Self::of(ValueDomain::Null);
    /// The tuple domain.
    pub const TUPLE: Self = Self::of(ValueDomain::Tuple);
    /// The map domain.
    pub const MAP: Self = Self::of(ValueDomain::Map);
    /// The domain of the literals of datatypes without a domain of their own.
    pub const OTHER: Self = Self::of(ValueDomain::Other);

    /// The domains of exact numbers, integers and decimals.
    pub const EXACT: Self = Self(bit(ValueDomain::Integer) | bit(ValueDomain::Decimal));

    /// The numeric domains: integers, decimals, floats and doubles.
    pub const NUMERIC: Self =
        Self(Self::EXACT.0 | bit(ValueDomain::Float) | bit(ValueDomain::Double));

    /// The domains whose values have an
    /// [effective boolean value](crate::value::ValueRef::effective_boolean_value):
    /// booleans, numbers, and plain and language-tagged strings.
    pub const TRUTHY: Self = Self(
        bit(ValueDomain::Boolean)
            | bit(ValueDomain::String)
            | bit(ValueDomain::LanguageString)
            | Self::NUMERIC.0,
    );

    /// The string domains, plain and language-tagged.
    pub const STRINGLIKE: Self = Self(bit(ValueDomain::String) | bit(ValueDomain::LanguageString));

    /// The structured domains, tuples and maps.
    pub const STRUCTURED: Self = Self(bit(ValueDomain::Tuple) | bit(ValueDomain::Map));

    /// The domains of dateTimes, dates and times.
    pub const TEMPORAL: Self =
        Self(bit(ValueDomain::DateTime) | bit(ValueDomain::Date) | bit(ValueDomain::Time));

    /// The three duration domains, which the values of [Duration](crate::value::Duration) span.
    pub const DURATION: Self = Self(
        bit(ValueDomain::DayTimeDuration)
            | bit(ValueDomain::YearMonthDuration)
            | bit(ValueDomain::Duration),
    );

    /// The domains of the durations of one kind, dayTimeDurations and yearMonthDurations.
    ///
    /// The durations of each of these domains are totally ordered.
    pub const ORDERED_DURATION: Self =
        Self(bit(ValueDomain::DayTimeDuration) | bit(ValueDomain::YearMonthDuration));

    /// The domains of RDF literals.
    ///
    /// These are the plain and language-tagged strings, booleans, numbers, dateTimes, dates,
    /// times, durations, and the literals of datatypes without a domain of their own.
    pub const LITERAL: Self = Self(
        Self::STRINGLIKE.0
            | bit(ValueDomain::Boolean)
            | Self::NUMERIC.0
            | Self::TEMPORAL.0
            | Self::DURATION.0
            | bit(ValueDomain::Other),
    );

    /// Creates the set of one domain.
    pub const fn of(domain: ValueDomain) -> Self {
        Self(bit(domain))
    }

    /// Returns this set with one domain added.
    pub const fn with(self, domain: ValueDomain) -> Self {
        Self(self.0 | bit(domain))
    }

    /// Returns this set with one domain removed.
    pub const fn without(self, domain: ValueDomain) -> Self {
        Self(self.0 & !bit(domain))
    }

    /// Returns the domains in either set.
    ///
    /// This is `|` for constant expressions, where operators cannot be called.
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Returns the domains not in the set.
    pub const fn complement(self) -> Self {
        Self(!self.0 & Self::ANY.0)
    }

    /// Returns whether no domain is in the set.
    pub const fn is_empty(&self) -> bool {
        self.0 == 0
    }

    /// Returns whether the domain is in the set.
    pub const fn contains(&self, domain: ValueDomain) -> bool {
        self.0 & bit(domain) != 0
    }

    /// Returns whether the sets share a domain.
    pub const fn intersects(&self, other: Self) -> bool {
        self.0 & other.0 != 0
    }

    /// Returns whether every domain of this set is in the other.
    pub const fn is_subset(&self, other: Self) -> bool {
        self.0 & !other.0 == 0
    }
}

impl BitOr for DomainSet {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        self.union(other)
    }
}

impl BitOrAssign for DomainSet {
    fn bitor_assign(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

impl BitAnd for DomainSet {
    type Output = Self;

    fn bitand(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }
}

impl BitAndAssign for DomainSet {
    fn bitand_assign(&mut self, other: Self) {
        self.0 &= other.0;
    }
}

impl From<ValueDomain> for DomainSet {
    fn from(domain: ValueDomain) -> Self {
        Self::of(domain)
    }
}

impl fmt::Debug for DomainSet {
    /// Writes the domains of the set in declaration order, such as `{Integer, Decimal}`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set()
            .entries(
                ValueDomain::ALL
                    .into_iter()
                    .filter(|&domain| self.contains(domain)),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sets combine with `|` and `&`, and report membership, overlap and inclusion.
    #[test]
    fn sets_combine_and_answer_membership() {
        let numbers = DomainSet::NUMERIC;
        assert!(numbers.contains(ValueDomain::Integer));
        assert!(!numbers.contains(ValueDomain::String));
        assert!(numbers.intersects(DomainSet::TRUTHY));
        assert!(numbers.is_subset(DomainSet::TRUTHY));
        assert!((numbers & DomainSet::of(ValueDomain::Iri)).is_empty());
        assert_eq!(
            DomainSet::of(ValueDomain::Integer) | DomainSet::of(ValueDomain::String),
            DomainSet::of(ValueDomain::String).with(ValueDomain::Integer)
        );
    }

    /// [DomainSet::LITERAL] contains exactly the domains whose values have a datatype.
    #[test]
    fn literals_are_the_domains_with_a_datatype() {
        for domain in ValueDomain::ALL {
            // A literal of the other domain has a datatype, which the domain does not fix.
            let typed = domain.datatype_iri().is_some() || domain == ValueDomain::Other;
            assert_eq!(DomainSet::LITERAL.contains(domain), typed, "{domain:?}");
        }
    }
}
