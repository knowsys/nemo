//! Macros that build program components from syntax close to Nemo's.
//!
//! [term!](crate::term), [atom!](crate::atom), [fact!](crate::fact) and [rule!](crate::rule)
//! accept a subset of Nemo syntax in Rust source,
//! and expand to code that builds the components:
//!
//! ```
//! use nemo::rule;
//!
//! let transitive = rule!(ancestor(?x, ?z) :- ancestor(?x, ?y), parent(?y, ?z));
//! let filtered = rule!(adult(?x) :- person(?x, ?age), ?age >= 18);
//! let negated = rule!(orphan(?x) :- person(?x), ~has_parent(?x));
//! ```
//!
//! # Limits
//!
//! * **Global variables `$x` cannot be written**, since `$` starts a metavariable in a macro.
//!   Interpolate them instead, as in `term!({ Term::global("x") })`.
//! * **Full IRIs cannot be written in angle brackets**,
//!   since `//` starts a Rust comment before any macro runs.
//!   Write `term!(iri "https://…")` instead.
//! * **Arithmetic has no operator precedence.**
//!   It nests to the right, so `?a * ?b + ?c` is `?a * (?b + ?c)`,
//!   and any other grouping needs parentheses.
//!   The left operand of an infix operator must be a variable, a literal,
//!   or a term in parentheses.
//! * **Rust float literals are doubles.** `term!(3.5)` is a double, not an exact decimal,
//!   since a Rust float literal cannot express the difference.
//!   Exact decimals are interpolated from [Term::decimal](crate::model::Term::decimal),
//!   or come from parsing.
//! * **Only `#count`, `#min`, `#max` and `#sum` can be written, and a scope follows a comma.**
//!   `term!(#sum(?v, ?w))` builds `#sum(?v per ?w)`, and `per` cannot be written.
//! * **Some checks happen at run time.**
//!   The names of builtins are recognized by [Term::apply](crate::model::Term::apply),
//!   and a scope argument of an aggregate that is not a variable panics.
//! * **Predicate names are Rust identifiers.**
//!   A name that is a Rust keyword or contains a hyphen needs the builders or parsing.
//!
//! The expansions use only `$crate::…`, `::std::…` and `::core::…` paths,
//! so that names at the call site cannot shadow them.

use crate::model::{Aggregate, AggregateKind, Term, TermKind};

impl Aggregate {
    /// Creates an aggregate from the arguments that it is written with in a macro.
    ///
    /// The first term is the aggregated input,
    /// and all further terms must be variables, which become the scope.
    /// Without terms, the aggregate is `#count()`.
    ///
    /// # Panics
    ///
    /// Panics for a scope argument that is not a variable,
    /// and for an aggregate other than `#count` without terms,
    /// since a `macro_rules` macro cannot check these when it expands.
    #[doc(hidden)]
    pub fn from_terms(kind: AggregateKind, mut terms: Vec<Term>) -> Aggregate {
        if terms.is_empty() {
            assert!(
                kind == AggregateKind::Count,
                "only `#count()` takes no arguments"
            );
            return Aggregate::count_matches();
        }

        let input = terms.remove(0);
        let scope = terms.into_iter().map(|term| match term.kind {
            TermKind::Variable(variable) => variable,
            other => panic!("aggregate scope arguments must be variables, found `{other:?}`"),
        });
        Aggregate::new(kind, input).per(scope)
    }
}

/// Splits a token stream at its top-level commas,
/// and returns a `Vec` of the given macro applied to each piece.
///
/// It is internal to the macros of this module.
#[doc(hidden)]
#[macro_export]
macro_rules! __split_commas {
    // A piece is complete: apply the callback to it, and start the next piece.
    (@go $callback:ident [$($result:expr,)*] [$($piece:tt)+] , $($rest:tt)*) => {
        $crate::__split_commas!(@go $callback [$($result,)* $crate::$callback!($($piece)+),] [] $($rest)*)
    };
    // Any other token extends the current piece.
    (@go $callback:ident [$($result:expr,)*] [$($piece:tt)*] $next:tt $($rest:tt)*) => {
        $crate::__split_commas!(@go $callback [$($result,)*] [$($piece)* $next] $($rest)*)
    };
    // The end of the input, after a final piece.
    (@go $callback:ident [$($result:expr,)*] [$($piece:tt)+]) => {
        ::std::vec![$($result,)* $crate::$callback!($($piece)+)]
    };
    // The end of the input without a final piece, after a trailing comma or for empty input.
    (@go $callback:ident [$($result:expr,)*] []) => {
        ::std::vec![$($result,)*]
    };
    // The entry point: the name of the callback macro, followed by the tokens to split.
    ($callback:ident $($tokens:tt)*) => {
        $crate::__split_commas!(@go $callback [] [] $($tokens)*)
    };
}

/// Builds an aggregate term from arguments separated by commas.
///
/// It is internal to the macros of this module.
#[doc(hidden)]
#[macro_export]
macro_rules! __aggregate {
    ($kind:ident; $($arguments:tt)+) => {{
        let arguments: ::std::vec::Vec<$crate::model::Term> =
            $crate::__split_commas!(term $($arguments)+);
        $crate::model::Term::from($crate::model::Aggregate::from_terms(
            $crate::model::AggregateKind::$kind,
            arguments,
        ))
    }};
}

/// Builds the term of an arithmetic operation.
///
/// It is internal to the macros of this module.
#[doc(hidden)]
#[macro_export]
macro_rules! __arithmetic {
    ($operator:ident; $left:expr; $($right:tt)+) => {
        $crate::model::Term::from($crate::model::Operation::new(
            $crate::model::Operator::$operator,
            ::std::vec![$left, $crate::term!($($right)+)],
        ))
    };
}

/// Builds a [Term] from syntax close to Nemo's.
///
/// The supported forms are:
/// * variables: `term!(?x)`, `term!(!v)`
/// * bare names: `term!(alice)`, and prefixed names: `term!(foaf:Person)`
/// * full IRIs, as string literals: `term!(iri "https://example.org/p")`
/// * numbers and strings: `term!(42)`, `term!(-1)`, `term!(3.5)`, `term!("abc")`
/// * calls: `term!(pair(?x, 3))`, where the name of a builtin like `STRLEN` gives an operation
/// * aggregates: `term!(#sum(?v))`, and `term!(#count(?x, ?d))` for `#count(?x per ?d)`
/// * arithmetic, nested to the right without precedence: `term!(?s + 100)`
/// * interpolation of any `Into<Term>` value: `term!({ computed })`
#[macro_export]
macro_rules! term {
    // Interpolates any value that converts into a term.
    ({ $value:expr }) => {
        $crate::model::Term::from($value)
    };
    // Aggregates.
    (# count ( $($arguments:tt)+ )) => { $crate::__aggregate!(Count; $($arguments)+) };
    (# min ( $($arguments:tt)+ )) => { $crate::__aggregate!(Min; $($arguments)+) };
    (# max ( $($arguments:tt)+ )) => { $crate::__aggregate!(Max; $($arguments)+) };
    (# sum ( $($arguments:tt)+ )) => { $crate::__aggregate!(Sum; $($arguments)+) };
    // A full IRI as a string literal, since Rust cannot lex an IRI in angle brackets.
    (iri $iri:literal) => {
        $crate::model::Term::iri($iri)
    };
    // Arithmetic with a variable on the left.
    (? $name:ident + $($rest:tt)+) => {
        $crate::__arithmetic!(Add; $crate::model::Term::var(stringify!($name)); $($rest)+)
    };
    (? $name:ident - $($rest:tt)+) => {
        $crate::__arithmetic!(Subtract; $crate::model::Term::var(stringify!($name)); $($rest)+)
    };
    (? $name:ident * $($rest:tt)+) => {
        $crate::__arithmetic!(Multiply; $crate::model::Term::var(stringify!($name)); $($rest)+)
    };
    (? $name:ident / $($rest:tt)+) => {
        $crate::__arithmetic!(Divide; $crate::model::Term::var(stringify!($name)); $($rest)+)
    };
    // Arithmetic with a literal on the left.
    ($value:literal + $($rest:tt)+) => {
        $crate::__arithmetic!(Add; $crate::model::Term::from($value); $($rest)+)
    };
    ($value:literal - $($rest:tt)+) => {
        $crate::__arithmetic!(Subtract; $crate::model::Term::from($value); $($rest)+)
    };
    ($value:literal * $($rest:tt)+) => {
        $crate::__arithmetic!(Multiply; $crate::model::Term::from($value); $($rest)+)
    };
    ($value:literal / $($rest:tt)+) => {
        $crate::__arithmetic!(Divide; $crate::model::Term::from($value); $($rest)+)
    };
    // Arithmetic with a parenthesized expression on the left.
    (( $($inner:tt)+ ) + $($rest:tt)+) => {
        $crate::__arithmetic!(Add; $crate::term!($($inner)+); $($rest)+)
    };
    (( $($inner:tt)+ ) - $($rest:tt)+) => {
        $crate::__arithmetic!(Subtract; $crate::term!($($inner)+); $($rest)+)
    };
    (( $($inner:tt)+ ) * $($rest:tt)+) => {
        $crate::__arithmetic!(Multiply; $crate::term!($($inner)+); $($rest)+)
    };
    (( $($inner:tt)+ ) / $($rest:tt)+) => {
        $crate::__arithmetic!(Divide; $crate::term!($($inner)+); $($rest)+)
    };
    // Parenthesized term.
    (( $($inner:tt)+ )) => {
        $crate::term!($($inner)+)
    };
    // Universal variable `?name`.
    (? $name:ident) => {
        $crate::model::Term::var(stringify!($name))
    };
    // Existential variable `!name`.
    (! $name:ident) => {
        $crate::model::Term::existential(stringify!($name))
    };
    // Prefixed name `prefix:name`.
    ($prefix:ident : $name:ident) => {
        $crate::model::Term::prefixed(stringify!($prefix), stringify!($name))
    };
    // A call, in which the name of a builtin gives an operation at run time.
    ($name:ident ( $($arguments:tt)* )) => {{
        let arguments: ::std::vec::Vec<$crate::model::Term> =
            $crate::__split_commas!(term $($arguments)*);
        $crate::model::Term::apply(stringify!($name), arguments)
    }};
    // Bare name: a constant.
    ($name:ident) => {
        $crate::model::Term::constant(stringify!($name))
    };
    // Number or string literal.
    ($value:literal) => {
        $crate::model::Term::from($value)
    };
}

/// Builds an [Atom](crate::model::Atom) from syntax close to Nemo's,
/// such as `atom!(parent(?x, ?y))` or `atom!(ex:knows(?x, ?y))`.
#[macro_export]
macro_rules! atom {
    // Prefixed predicate name.
    ($prefix:ident : $name:ident ( $($terms:tt)* )) => {{
        let terms: ::std::vec::Vec<$crate::model::Term> =
            $crate::__split_commas!(term $($terms)*);
        $crate::model::Atom::new(
            $crate::model::Identifier::Prefixed {
                prefix: ::std::string::String::from(stringify!($prefix)),
                name: ::std::string::String::from(stringify!($name)),
            },
            terms,
        )
    }};
    // Plain predicate name.
    ($name:ident ( $($terms:tt)* )) => {{
        let terms: ::std::vec::Vec<$crate::model::Term> =
            $crate::__split_commas!(term $($terms)*);
        $crate::model::Atom::new(stringify!($name), terms)
    }};
}

/// Builds a [Fact](crate::model::Fact) from syntax close to Nemo's,
/// such as `fact!(parent(alice, bob))`.
#[macro_export]
macro_rules! fact {
    ($($tokens:tt)+) => {
        $crate::model::Fact::from($crate::atom!($($tokens)+))
    };
}

/// Builds one literal of a rule's body, which is an atom, a negated atom or a comparison.
///
/// It is internal to the macros of this module.
#[doc(hidden)]
#[macro_export]
macro_rules! __literal {
    // Negated atom, prefixed and plain predicate name.
    (~ $prefix:ident : $name:ident ( $($terms:tt)* )) => {
        $crate::model::Literal::Negative($crate::atom!($prefix : $name ( $($terms)* )))
    };
    (~ $name:ident ( $($terms:tt)* )) => {
        $crate::model::Literal::Negative($crate::atom!($name ( $($terms)* )))
    };
    // Positive atom, prefixed and plain predicate name.
    ($prefix:ident : $name:ident ( $($terms:tt)* )) => {
        $crate::model::Literal::Positive($crate::atom!($prefix : $name ( $($terms)* )))
    };
    ($name:ident ( $($terms:tt)* )) => {
        $crate::model::Literal::Positive($crate::atom!($name ( $($terms)* )))
    };
    // A comparison is split at its leftmost comparison operator.
    // Rust lexes `!==` as `!=` followed by `=`, so its arm comes before the arm of `!=`.
    (@compare [$($left:tt)+] == $($right:tt)+) => {
        $crate::__literal!(@operation ValueEqual [$($left)+] [$($right)+])
    };
    (@compare [$($left:tt)+] != = $($right:tt)+) => {
        $crate::__literal!(@operation ValueUnequal [$($left)+] [$($right)+])
    };
    (@compare [$($left:tt)+] = $($right:tt)+) => {
        $crate::__literal!(@operation Equal [$($left)+] [$($right)+])
    };
    (@compare [$($left:tt)+] != $($right:tt)+) => {
        $crate::__literal!(@operation Unequal [$($left)+] [$($right)+])
    };
    (@compare [$($left:tt)+] >= $($right:tt)+) => {
        $crate::__literal!(@operation GreaterEq [$($left)+] [$($right)+])
    };
    (@compare [$($left:tt)+] <= $($right:tt)+) => {
        $crate::__literal!(@operation LessEq [$($left)+] [$($right)+])
    };
    (@compare [$($left:tt)+] > $($right:tt)+) => {
        $crate::__literal!(@operation Greater [$($left)+] [$($right)+])
    };
    (@compare [$($left:tt)+] < $($right:tt)+) => {
        $crate::__literal!(@operation Less [$($left)+] [$($right)+])
    };
    // No operator is found yet, so the left side is extended by one token.
    (@compare [$($left:tt)*] $next:tt $($rest:tt)*) => {
        $crate::__literal!(@compare [$($left)* $next] $($rest)*)
    };
    // The tokens ended without a comparison operator.
    (@compare [$($left:tt)*]) => {
        ::core::compile_error!("body literal is neither an atom nor a comparison")
    };
    // Builds the comparison literal.
    (@operation $operator:ident [$($left:tt)+] [$($right:tt)+]) => {
        $crate::model::Literal::Operation($crate::model::Operation::new(
            $crate::model::Operator::$operator,
            ::std::vec![$crate::term!($($left)+), $crate::term!($($right)+)],
        ))
    };
    // Anything that is not an atom is tried as a comparison.
    ($($tokens:tt)+) => {
        $crate::__literal!(@compare [] $($tokens)+)
    };
}

/// Builds a [Rule](crate::model::Rule) from syntax close to Nemo's.
///
/// It covers several head atoms, negation, comparisons, aggregates,
/// and arithmetic nested to the right:
///
/// ```
/// use nemo::rule;
///
/// rule!(ancestor(?x, ?z) :- ancestor(?x, ?y), parent(?y, ?z));
/// rule!(adult(?x) :- person(?x, ?age), ?age >= 18);
/// rule!(total(?g, #sum(?v)) :- data(?g, ?v));
/// ```
#[macro_export]
macro_rules! rule {
    // The separator `:-` is found, so the rule is built.
    //
    // Head and body cannot be captured in one pattern,
    // since `$($head:tt)+ :- …` is a local ambiguity at every `:`.
    // So these arms scan the tokens one by one.
    (@split [$($head:tt)+] :- $($body:tt)+) => {
        $crate::model::Rule::new(
            $crate::__split_commas!(atom $($head)+),
            $crate::__split_commas!(__literal $($body)+),
        )
    };
    // No separator is found yet, so the head is extended by one token.
    (@split [$($head:tt)*] $next:tt $($rest:tt)*) => {
        $crate::rule!(@split [$($head)* $next] $($rest)*)
    };
    // The tokens ended without the separator.
    (@split [$($head:tt)*]) => {
        ::core::compile_error!("rules require `:-` between head and body")
    };
    // The entry point, which scans for the leftmost `:-`.
    ($($tokens:tt)+) => {
        $crate::rule!(@split [] $($tokens)+)
    };
}

#[cfg(test)]
mod tests {
    use nemo_physical::value::Number;

    use crate::model::{Atom, Fact, Operation, Rule, Term, TermKind};

    /// Each form of [term!](crate::term) builds the term that the matching constructor builds.
    #[test]
    fn every_term_form_builds_its_term() {
        assert_eq!(term!(?x), Term::var("x"));
        assert_eq!(term!(!v), Term::existential("v"));
        assert_eq!(term!(alice), Term::constant("alice"));
        assert_eq!(term!(foaf: Person), Term::prefixed("foaf", "Person"));
        assert_eq!(
            term!(iri "https://example.org/p"),
            Term::iri("https://example.org/p")
        );
        assert_eq!(term!(42), Term::integer(42));
        assert_eq!(term!(-1), Term::integer(-1));
        assert_eq!(term!("abc"), Term::string("abc"));
        assert_eq!(term!(3.5), Term::double(3.5));
    }

    /// A Rust value in braces is converted into a term.
    #[test]
    fn braces_interpolate_rust_values() {
        let value = 7;
        assert_eq!(term!({ value }), Term::integer(7));
        let global = Term::global("threshold");
        assert_eq!(term!({ global.clone() }), global);
    }

    /// A call of a builtin's name builds an operation, and any other call a function term.
    #[test]
    fn builtins_resolve_functions_stay() {
        assert!(matches!(term!(pair(?x, 3)).kind, TermKind::Function { .. }));
        assert!(matches!(term!(STRLEN(?x)).kind, TermKind::Operation(_)));
    }

    /// A component written with the macros equals the same component parsed from text.
    #[test]
    fn macros_agree_with_the_parser() {
        // Equality ignores spans.
        let cases: Vec<(Rule, &str)> = vec![
            (
                rule!(ancestor(?x, ?z) :- ancestor(?x, ?y), parent(?y, ?z)),
                "ancestor(?x, ?z) :- ancestor(?x, ?y), parent(?y, ?z) .",
            ),
            (
                rule!(adult(?x) :- person(?x, ?age), ?age >= 18),
                "adult(?x) :- person(?x, ?age), ?age >= 18 .",
            ),
            (
                rule!(orphan(?x) :- person(?x), ~has_parent(?x)),
                "orphan(?x) :- person(?x), ~has_parent(?x) .",
            ),
            (
                // The comma list of the macro is the scope, which source text writes with `per`.
                rule!(total(?g, #sum(?v, ?g)) :- data(?g, ?v)),
                "total(?g, #sum(?v per ?g)) :- data(?g, ?v) .",
            ),
            (rule!(a(?x), b(?x) :- c(?x)), "a(?x), b(?x) :- c(?x) ."),
            (
                rule!(big(?x) :- value(?x, ?v), ?v > (?y * 2) + 1),
                "big(?x) :- value(?x, ?v), ?v > ?y * 2 + 1 .",
            ),
        ];
        for (constructed, source) in cases {
            let parsed = Rule::parse(source).expect("the source parses");
            assert_eq!(constructed, parsed, "{source:?}");
        }

        assert_eq!(
            fact!(parent(alice, bob)),
            Fact::parse("parent(alice, bob) .").expect("the fact parses")
        );
        for (constructed, source) in [
            (term!(foaf: Person), "foaf:Person"),
            (term!(pair(?x, 3)), "pair(?x, 3)"),
            (term!((?a + ?b) * ?c), "(?a + ?b) * ?c"),
            (term!(#count(?x, ?d)), "#count(?x per ?d)"),
        ] {
            assert_eq!(
                constructed,
                Term::parse(source).expect("the term parses"),
                "{source:?}"
            );
        }
    }

    /// A rule built with a [RuleBuilder](crate::model::RuleBuilder) equals the same rule parsed
    /// from text.
    #[test]
    fn builders_agree_with_the_parser() {
        let built = Rule::builder()
            .head(Atom::new("adult", vec![Term::var("x")]))
            .body(Atom::new("person", vec![Term::var("x"), Term::var("age")]))
            .body(Operation::greater_eq(Term::var("age"), 18))
            .negated(Atom::new("minor", vec![Term::var("x")]))
            .build();
        let parsed = Rule::parse("adult(?x) :- person(?x, ?age), ?age >= 18, ~minor(?x) .")
            .expect("the source parses");
        assert_eq!(built, parsed);
    }

    /// A Rust float literal in a macro gives a double,
    /// while the same number in source text is parsed as an exact decimal.
    #[test]
    fn float_literals_are_doubles_not_decimals() {
        // A Rust float literal cannot express the difference between a double and a decimal.
        assert!(matches!(
            term!(3.5).kind,
            TermKind::Number(Number::Double(_))
        ));
        assert!(matches!(
            Term::parse("3.5").expect("parses").kind,
            TermKind::Number(Number::Decimal(_))
        ));
    }
}
