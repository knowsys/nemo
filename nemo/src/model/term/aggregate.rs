//! Aggregate expressions of rule heads.

use std::fmt::{self, Display, Formatter};

use crate::model::{Span, Term, Variable, fmt_list, span_ignoring_eq};

/// The aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateKind {
    /// The number of distinct contributions in the group, or of body matches for `#count()`.
    Count,
    /// Minimum value.
    Min,
    /// Maximum value.
    Max,
    /// Sum of all values.
    Sum,
    /// Mean of all values: their sum divided by their count, as `/` divides.
    Average,
    /// The group's values as a tuple, ascending in term order.
    Tuple,
    /// The group's key–value pairs as a map: `#map(?k = ?v)`.
    Map,
}

impl AggregateKind {
    /// Every aggregate, in the order they are listed.
    ///
    /// [from_name](Self::from_name) and [names](Self::names) derive from this list.
    pub const ALL: [Self; 7] = [
        AggregateKind::Count,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::Sum,
        AggregateKind::Average,
        AggregateKind::Tuple,
        AggregateKind::Map,
    ];

    /// Returns the name of the aggregate, without the `#` marker.
    pub fn name(&self) -> &'static str {
        match self {
            AggregateKind::Count => "count",
            AggregateKind::Min => "min",
            AggregateKind::Max => "max",
            AggregateKind::Sum => "sum",
            // SPARQL's spelling, which is what this set follows.
            AggregateKind::Average => "avg",
            // The structure builders are named after what they build.
            AggregateKind::Tuple => "tuple",
            AggregateKind::Map => "map",
        }
    }

    /// Resolves an aggregate name (without the `#` marker),
    /// case-insensitively.
    pub fn from_name(name: &str) -> Option<Self> {
        let name = name.to_ascii_lowercase();
        Self::ALL.into_iter().find(|kind| kind.name() == name)
    }

    /// Returns every aggregate name, separated by commas.
    pub fn names() -> String {
        Self::ALL
            .iter()
            .map(|kind| kind.name())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl Display for AggregateKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.name())
    }
}

/// An aggregate expression like `#sum(?v)`, usable in rule heads.
///
/// Under set semantics, an aggregate ranges over the distinct combinations
/// of its aggregated terms and its scope variables within each group:
/// `#sum(?v)` over the distinct values of `?v`, `#sum(?v per ?w)` over distinct pairs.
/// The exception is `#count()`, which names neither and counts the body matches.
#[derive(Debug, Clone)]
pub struct Aggregate {
    /// The aggregate function.
    pub kind: AggregateKind,
    /// The term whose values are aggregated, or the keys of a `#map`.
    ///
    /// `None` is a `#count` without one:
    /// `#count()` counting the body matches,
    /// or `#count(per ?w…)` counting the distinct combinations of its scope variables.
    pub input: Option<Box<Term>>,
    /// The value term of a `#map`, as in `#map(?k = ?v)`, or `None` for every other kind.
    pub value: Option<Box<Term>>,
    /// The scope variables: those after `per`, empty for the bare form.
    pub scope: Vec<Variable>,
    /// Whether the scope was written as the legacy comma list `#sum(?v, ?w)`,
    /// which means the same as `per` and draws a warning.
    ///
    /// Equality and hashing ignore it.
    pub legacy_list: bool,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(Aggregate: kind, input, value, scope);

impl Aggregate {
    /// Returns whether the aggregate counts the body matches: `#count()`.
    pub(crate) fn counts_matches(&self) -> bool {
        self.input.is_none() && self.scope.is_empty()
    }
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.kind)?;
        if let Some(input) = &self.input {
            write!(f, "{input}")?;
        }
        if let Some(value) = &self.value {
            write!(f, " = {value}")?;
        }
        if !self.scope.is_empty() {
            // The keyword is the canonical form; a legacy comma list
            // prints as it, which is how formatting migrates a file.
            if self.input.is_some() {
                f.write_str(" ")?;
            }
            f.write_str("per ")?;
            fmt_list(f, &self.scope)?;
        }
        f.write_str(")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The legacy comma list and the `per` keyword build the same scope.
    #[test]
    fn scope_equality_ignores_the_spelling() {
        let listed = Term::parse("#sum(?y, ?z)").expect("parses");
        let marked = Term::parse("#sum(?y per ?z)").expect("parses");
        assert_eq!(listed, marked);
        assert_eq!(marked.to_string(), "#sum(?y per ?z)");
        assert_eq!(listed.to_string(), marked.to_string());
    }

    #[test]
    fn every_aggregate_resolves_by_its_own_name() {
        // The list, the names, and the resolution are three statements
        // of one fact; a new aggregate reaches all of them or none.
        for kind in AggregateKind::ALL {
            assert_eq!(AggregateKind::from_name(kind.name()), Some(kind));
            assert_eq!(
                AggregateKind::from_name(&kind.name().to_ascii_uppercase()),
                Some(kind),
                "aggregates resolve case-insensitively"
            );
            assert!(
                AggregateKind::names().contains(kind.name()),
                "`{}` is named in the list a diagnostic shows",
                kind.name()
            );
        }
        assert_eq!(AggregateKind::from_name("median"), None);
    }

    #[test]
    fn an_aggregate_prints_the_way_it_is_written() {
        assert_eq!(AggregateKind::Average.to_string(), "#avg");
        assert_eq!(AggregateKind::Count.to_string(), "#count");

        let scope = [Variable::universal("x"), Variable::universal("y")];
        assert_eq!(
            Aggregate::count(Variable::universal("v"))
                .per(scope.clone())
                .to_string(),
            "#count(?v per ?x, ?y)"
        );
        assert_eq!(
            Aggregate::matches().per(scope).to_string(),
            "#count(per ?x, ?y)"
        );
    }
}
