//! Aggregates, the terms of rule heads that combine the values of a group.

use std::fmt::{self, Display, Formatter};

use crate::model::{Span, Term, Variable, fmt_list, span_ignoring_eq};

/// The aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateKind {
    /// The number of the group's distinct combinations, as [Aggregate] describes them,
    /// or the number of body matches for `#count()`.
    Count,
    /// The minimum of the values.
    Min,
    /// The maximum of the values.
    Max,
    /// The sum of the values.
    Sum,
    /// The mean of the values, which is their sum divided by their count, as `/` divides.
    Average,
    /// The values of the group as a tuple, in ascending term order.
    Tuple,
    /// The key-value pairs of the group as a map, as in `#map(?k = ?v)`.
    Map,
}

impl AggregateKind {
    /// Every aggregate kind, in declaration order.
    ///
    /// [from_name](Self::from_name) and [names](Self::names) are derived from this list.
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
            // The aggregate names follow SPARQL, which calls the mean `AVG`.
            AggregateKind::Average => "avg",
            // The aggregates that build structures are named after what they build.
            AggregateKind::Tuple => "tuple",
            AggregateKind::Map => "map",
        }
    }

    /// Returns the aggregate with the given name, written without the `#` marker and in any
    /// case, or `None` if there is none.
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

/// An aggregate term like `#sum(?v)`, which may appear in a rule head.
///
/// The body matches of the rule are grouped by the values of the head's other variables.
/// Under set semantics, an aggregate ranges over the distinct combinations
/// of its aggregated terms and its scope variables within each group.
/// So `#sum(?v)` sums the distinct values of `?v`,
/// and `#sum(?v per ?w)` sums `?v` over the distinct pairs of `?v` and `?w`.
/// The exception is `#count()`, which has neither and counts the body matches.
#[derive(Debug, Clone)]
pub struct Aggregate {
    /// The aggregate function.
    pub kind: AggregateKind,
    /// The term whose values are aggregated, which for a `#map` is the key term.
    ///
    /// It is `None` only for a `#count` without a term.
    /// `#count()` counts the body matches,
    /// and `#count(per ?w, …)` counts the distinct combinations of its scope variables.
    pub input: Option<Box<Term>>,
    /// The value term of a `#map`, as in `#map(?k = ?v)`, or `None` for every other kind.
    pub value: Option<Box<Term>>,
    /// The scope variables, which are written after `per`, or empty if there are none.
    pub scope: Vec<Variable>,
    /// Whether the scope was written as a comma list, as in `#sum(?v, ?w)`.
    ///
    /// This form is deprecated, and means the same as `per`.
    /// Equality and hashing ignore this field.
    pub legacy_list: bool,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(Aggregate: kind, input, value, scope);

impl Aggregate {
    /// Returns whether the aggregate counts the body matches, as `#count()` does.
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
            // `per` is the canonical form, so a scope written as a comma list
            // is displayed with `per` too, and formatting a program replaces the comma list.
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

    /// A scope written as a comma list equals the same scope written with `per`,
    /// and both are displayed with `per`.
    #[test]
    fn scope_equality_ignores_the_spelling() {
        let listed = Term::parse("#sum(?y, ?z)").expect("parses");
        let marked = Term::parse("#sum(?y per ?z)").expect("parses");
        assert_eq!(listed, marked);
        assert_eq!(marked.to_string(), "#sum(?y per ?z)");
        assert_eq!(listed.to_string(), marked.to_string());
    }

    /// Every aggregate is found by its name in any case,
    /// and [names](AggregateKind::names) lists it.
    #[test]
    fn every_aggregate_resolves_by_its_own_name() {
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

    /// An aggregate is displayed in program syntax, with `per` before its scope.
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
            Aggregate::count_matches().per(scope).to_string(),
            "#count(per ?x, ?y)"
        );
    }
}
