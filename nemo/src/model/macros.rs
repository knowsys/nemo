//! Macros constructing program components from Nemo-like syntax.
//!
//! [term!](crate::term), [atom!](crate::atom), [fact!](crate::fact), and [rule!](crate::rule)
//! accept a subset of Nemo syntax directly in Rust source,
//! and expand to plain component construction:
//!
//! ```
//! use nemo::rule;
//!
//! let transitive = rule!(ancestor(?x, ?z) :- ancestor(?x, ?y), parent(?y, ?z));
//! let filtered = rule!(adult(?x) :- person(?x, ?age), ?age >= 18);
//! let negated = rule!(orphan(?x) :- person(?x), ~has_parent(?x));
//! ```
//!
//! # Limits of `macro_rules` here
//!
//! * **Global variables `$x` cannot be written**, since `$` introduces metavariables.
//!   Interpolate them: `term!({ Term::global("x") })`.
//! * **Full IRIs cannot appear in angle brackets**,
//!   since `//` starts a Rust comment before any macro runs.
//!   Write `term!(iri "https://…")`.
//! * **No operator precedence**: arithmetic nests to the right,
//!   so `?a * ?b + ?c` is `?a * (?b + ?c)`. Parenthesize instead.
//!   The left operand of an infix expression must be a variable, a literal, or parenthesized.
//! * **Rust float literals are doubles**: `term!(3.5)` is a double, not an exact decimal,
//!   since a float literal cannot carry the distinction.
//!   Exact decimals interpolate via [Term::decimal](crate::model::Term::decimal),
//!   or come from `parse`.
//! * **Builtin names resolve at runtime** ([Term::apply](crate::model::Term::apply)),
//!   and aggregate scope arguments are checked at runtime: a non-variable panics.
//! * **Predicate names are Rust identifiers**:
//!   keywords and hyphenated names need the builders or `parse`.
//!
//! Expansions use only `$crate::…` and `::std::…` paths,
//! so they are immune to shadowing at the call site.

use crate::model::{Aggregate, AggregateKind, Term, TermKind};

impl Aggregate {
    /// Creates an aggregate from macro arguments.
    ///
    /// The first term is the aggregated input,
    /// and all further terms must be variables, which become the `per` scope.
    /// No terms at all make the match count `#count()`.
    ///
    /// # Panics
    ///
    /// Panics on scope arguments that are not variables, and on an empty aggregate other than
    /// `#count`, since `macro_rules` cannot check these at expansion time
    /// (see the module documentation).
    #[doc(hidden)]
    pub fn from_terms(kind: AggregateKind, mut terms: Vec<Term>) -> Aggregate {
        if terms.is_empty() {
            assert!(
                kind == AggregateKind::Count,
                "only `#count()` takes no arguments"
            );
            return Aggregate::matches();
        }

        let input = terms.remove(0);
        let scope = terms.into_iter().map(|term| match term.kind {
            TermKind::Variable(variable) => variable,
            other => panic!("aggregate scope arguments must be variables, found `{other:?}`"),
        });
        Aggregate::new(kind, input).per(scope)
    }
}

/// Splits a token stream on top-level commas and applies the given macro
/// to each piece, producing a `Vec` of the results. Internal.
#[doc(hidden)]
#[macro_export]
macro_rules! __split_commas {
    // Piece complete: apply the callback, then start the next piece.
    (@go $callback:ident [$($result:expr,)*] [$($piece:tt)+] , $($rest:tt)*) => {
        $crate::__split_commas!(@go $callback [$($result,)* $crate::$callback!($($piece)+),] [] $($rest)*)
    };
    // Any other token: extend the current piece.
    (@go $callback:ident [$($result:expr,)*] [$($piece:tt)*] $next:tt $($rest:tt)*) => {
        $crate::__split_commas!(@go $callback [$($result,)*] [$($piece)* $next] $($rest)*)
    };
    // End of input with a final piece.
    (@go $callback:ident [$($result:expr,)*] [$($piece:tt)+]) => {
        ::std::vec![$($result,)* $crate::$callback!($($piece)+)]
    };
    // End of input without a final piece (empty input or trailing comma).
    (@go $callback:ident [$($result:expr,)*] []) => {
        ::std::vec![$($result,)*]
    };
    // Entry point: callback macro name followed by the tokens to split.
    ($callback:ident $($tokens:tt)*) => {
        $crate::__split_commas!(@go $callback [] [] $($tokens)*)
    };
}

/// Builds an aggregate term from comma-separated arguments. Internal.
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

/// Builds an arithmetic operation term. Internal.
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

/// Constructs a [Term] from Nemo-like syntax.
///
/// Supported forms:
/// * variables: `term!(?x)`, `term!(!v)`
/// * bare names: `term!(alice)`, and prefixed names: `term!(foaf:Person)`
/// * full IRIs, as string literals: `term!(iri "https://example.org/p")`
/// * numbers and strings: `term!(42)`, `term!(-1)`, `term!(3.5)`, `term!("abc")`
/// * function applications: `term!(pair(?x, 3))`,
///   in which builtin names like `STRLEN` resolve to operations at runtime
/// * aggregates: `term!(#sum(?v))`, `term!(#count(?x, ?d))`
/// * arithmetic, right-nested without precedence: `term!(?s + 100)`
/// * interpolation of any `Into<Term>` value: `term!({ computed })`
#[macro_export]
macro_rules! term {
    // Escape hatch: interpolate any value convertible into a term.
    ({ $value:expr }) => {
        $crate::model::Term::from($value)
    };
    // Aggregates.
    (# count ( $($arguments:tt)+ )) => { $crate::__aggregate!(Count; $($arguments)+) };
    (# min ( $($arguments:tt)+ )) => { $crate::__aggregate!(Min; $($arguments)+) };
    (# max ( $($arguments:tt)+ )) => { $crate::__aggregate!(Max; $($arguments)+) };
    (# sum ( $($arguments:tt)+ )) => { $crate::__aggregate!(Sum; $($arguments)+) };
    // Full IRI as a string literal (angle brackets are unlexable).
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
    // Function application: builtin names resolve at runtime.
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

/// Constructs an [Atom](crate::model::Atom) from Nemo-like syntax,
/// e.g. `atom!(parent(?x, ?y))` or `atom!(ex:knows(?x, ?y))`.
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

/// Constructs a [Fact](crate::model::Fact) from Nemo-like syntax,
/// e.g. `fact!(parent(alice, bob))`.
#[macro_export]
macro_rules! fact {
    ($($tokens:tt)+) => {
        $crate::model::Fact::from($crate::atom!($($tokens)+))
    };
}

/// Parses one body element of a rule: an atom, a negated atom, or a
/// comparison. Internal.
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
    // Comparison: split at the leftmost comparison operator. Rust lexes
    // `!==` as `!=` followed by `=`, so its arm precedes the `!=` arm.
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
    // No operator found yet: extend the left side by one token.
    (@compare [$($left:tt)*] $next:tt $($rest:tt)*) => {
        $crate::__literal!(@compare [$($left)* $next] $($rest)*)
    };
    // Exhausted without finding a comparison operator.
    (@compare [$($left:tt)*]) => {
        ::core::compile_error!("body literal is neither an atom nor a comparison")
    };
    // Assemble the comparison literal.
    (@operation $operator:ident [$($left:tt)+] [$($right:tt)+]) => {
        $crate::model::Literal::Operation($crate::model::Operation::new(
            $crate::model::Operator::$operator,
            ::std::vec![$crate::term!($($left)+), $crate::term!($($right)+)],
        ))
    };
    // Everything that is not an atom: try as a comparison.
    ($($tokens:tt)+) => {
        $crate::__literal!(@compare [] $($tokens)+)
    };
}

/// Constructs a [Rule](crate::model::Rule) from Nemo-like syntax.
///
/// Covers multiple head atoms, negation, comparisons, aggregates, and
/// (right-nested) arithmetic:
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
    // Separator found: assemble the rule.
    //
    // Head and body cannot be captured in one pattern
    // (`$($head:tt)+ :- …` is a local ambiguity at every `:`), hence
    // the token-by-token scan below.
    (@split [$($head:tt)+] :- $($body:tt)+) => {
        $crate::model::Rule::new(
            $crate::__split_commas!(atom $($head)+),
            $crate::__split_commas!(__literal $($body)+),
        )
    };
    // No separator yet: extend the head by one token.
    (@split [$($head:tt)*] $next:tt $($rest:tt)*) => {
        $crate::rule!(@split [$($head)* $next] $($rest)*)
    };
    // Exhausted without finding the separator.
    (@split [$($head:tt)*]) => {
        ::core::compile_error!("rules require `:-` between head and body")
    };
    // Entry point: scan for the leftmost `:-`.
    ($($tokens:tt)+) => {
        $crate::rule!(@split [] $($tokens)+)
    };
}

#[cfg(test)]
mod tests {
    use nemo_physical::value::Number;

    use crate::model::{Atom, Fact, Operation, Rule, Term, TermKind};

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

    #[test]
    fn braces_interpolate_rust_values() {
        let value = 7;
        assert_eq!(term!({ value }), Term::integer(7));
        let global = Term::global("threshold");
        assert_eq!(term!({ global.clone() }), global);
    }

    #[test]
    fn builtins_resolve_functions_stay() {
        assert!(matches!(term!(pair(?x, 3)).kind, TermKind::Function { .. }));
        assert!(matches!(term!(STRLEN(?x)).kind, TermKind::Operation(_)));
    }

    #[test]
    fn macros_agree_with_the_parser() {
        // The same statement written as a macro and as source text must
        // construct equal components (equality ignores spans).
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
                // The macro's comma list is the `per` scope, spelled
                // with the keyword in source text.
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

    #[test]
    fn float_literals_are_doubles_not_decimals() {
        // A Rust float literal cannot carry the double/decimal
        // distinction; the macro takes it as a double, while the same
        // source text parses as an exact decimal.
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
