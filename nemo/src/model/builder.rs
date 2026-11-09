//! Constructors, conversions and fluent builders for program components.
//!
//! They are meant for programs whose predicate names or rule shapes are computed at run time.
//! For a program fixed in the Rust source, the macros in [macros](crate::model::macros)
//! are shorter, and [Program::parse] reads a program from text.
//!
//! The conversions from Rust values are:
//! - `&str` and [String] convert to plain string terms,
//!   so that a conversion never guesses whether an IRI was meant.
//!   IRIs and names are created explicitly, with [Term::iri], [Term::prefixed] and
//!   [Term::constant].
//!   As an [Identifier], such as a predicate, a string is a bare name.
//! - `f64` converts to a double term.
//!   Exact decimals come from [Term::decimal] or from parsing,
//!   since a Rust float cannot express the difference.

use nemo_physical::value::{DataValue, Decimal, Double, Float, Number, ValueRef};

use crate::model::{
    Aggregate, AggregateKind, Atom, Attribute, Fact, Identifier, Literal, Operation, Operator,
    Program, Rule, Statement, Term, TermKind, Variable,
};

impl From<&str> for Identifier {
    fn from(name: &str) -> Self {
        Identifier::Bare(name.to_owned())
    }
}

impl From<String> for Identifier {
    fn from(name: String) -> Self {
        Identifier::Bare(name)
    }
}

impl Term {
    /// Creates a universal variable term `?name`.
    pub fn var(name: impl Into<String>) -> Self {
        Self::new(TermKind::Variable(Variable::universal(name)))
    }

    /// Creates an existential variable term `!name`.
    pub fn existential(name: impl Into<String>) -> Self {
        Self::new(TermKind::Variable(Variable::existential(name)))
    }

    /// Creates a global variable term `$name`.
    pub fn global(name: impl Into<String>) -> Self {
        Self::new(TermKind::Variable(Variable::global(name)))
    }

    /// Creates a constant term with a full IRI.
    pub fn iri(iri: impl Into<String>) -> Self {
        Self::new(TermKind::Constant(Identifier::Iri(iri.into())))
    }

    /// Creates a constant term with a prefixed name, like `foaf:Person`.
    pub fn prefixed(prefix: impl Into<String>, name: impl Into<String>) -> Self {
        Self::new(TermKind::Constant(Identifier::Prefixed {
            prefix: prefix.into(),
            name: name.into(),
        }))
    }

    /// Creates a double term.
    pub fn double(value: impl Into<Double>) -> Self {
        Self::new(TermKind::Number(Number::Double(value.into())))
    }

    /// Creates a float term.
    pub fn float(value: impl Into<Float>) -> Self {
        Self::new(TermKind::Number(Number::Float(value.into())))
    }

    /// Returns the term of the exact number `mantissa * 10^exponent`.
    ///
    /// An integral value gives an integer term.
    /// Returns `None` if the canonical form of the value exceeds the
    /// [length limit of numbers](Decimal).
    pub fn decimal(mantissa: i128, exponent: i64) -> Option<Self> {
        Decimal::new(mantissa, exponent)
            .map(|value| Self::new(TermKind::Number(Number::Decimal(value))))
    }

    /// Creates an uninterpreted function term.
    pub fn function(
        name: impl Into<Identifier>,
        arguments: impl IntoIterator<Item = impl Into<Term>>,
    ) -> Self {
        Self::new(TermKind::Function {
            name: name.into(),
            arguments: arguments.into_iter().map(Into::into).collect(),
            labels: None,
        })
    }

    /// Creates a call of the given name with the given arguments.
    ///
    /// The name of a builtin, as [Operator::from_name] recognizes it, gives an operation,
    /// and any other name gives an uninterpreted function term.
    pub fn apply(name: &str, arguments: impl IntoIterator<Item = impl Into<Term>>) -> Self {
        match Operator::from_name(name) {
            Some(operator) => {
                Operation::new(operator, arguments.into_iter().map(Into::into).collect()).into()
            }
            None => Self::function(name, arguments),
        }
    }

    /// Returns the term that denotes a data value.
    ///
    /// This puts values computed in Rust, or read from engine results, into a program.
    /// IRIs and structure labels become full IRIs, which are not resolved by prefixes or a base.
    /// Returns `None` for a null and for a structure that contains one,
    /// since no term denotes a null.
    pub fn value(value: &DataValue) -> Option<Self> {
        Self::of_value(value.as_ref())
    }

    /// Returns the term that denotes a borrowed data value, as [value](Self::value) does.
    fn of_value(value: ValueRef<'_>) -> Option<Self> {
        /// Returns a typed literal whose datatype is a full IRI.
        fn literal(lexical: String, datatype: String) -> Term {
            Term::new(TermKind::RdfLiteral {
                lexical,
                datatype: Identifier::Iri(datatype),
            })
        }
        /// Returns the typed literal of a value of a native datatype,
        /// with the given lexical form and the datatype of the value's domain.
        fn typed(value: ValueRef<'_>, lexical: String) -> Term {
            let datatype = value
                .datatype_iri()
                .expect("a value of a native datatype has one");
            literal(lexical, datatype.to_owned())
        }
        /// Returns the terms of a structure's elements, or `None` if any of them contains a null.
        fn elements<'a>(values: impl Iterator<Item = ValueRef<'a>>) -> Option<Vec<Term>> {
            values.map(Term::of_value).collect()
        }
        Some(match value {
            ValueRef::Iri(iri) => Self::iri(iri.to_string()),
            ValueRef::String(text) => Self::string(text),
            ValueRef::LanguageString(value) => Self::new(TermKind::LanguageString {
                text: value.text().to_owned(),
                language: value.language().to_owned(),
            }),
            ValueRef::Boolean(value) => Self::new(TermKind::Boolean(value)),
            ValueRef::Number(number) => Self::new(TermKind::Number(number.to_owned())),
            ValueRef::DateTime(lexical) => typed(value, lexical.to_string()),
            ValueRef::Date(lexical) => typed(value, lexical.to_string()),
            ValueRef::Time(lexical) => typed(value, lexical.to_string()),
            ValueRef::Duration(lexical) => typed(value, lexical.to_string()),
            ValueRef::Null(_) => return None,
            ValueRef::Tuple(tuple) => match tuple.label() {
                Some(label) => Self::new(TermKind::Function {
                    name: Identifier::Iri(label.to_owned()),
                    arguments: elements(tuple.elements())?,
                    labels: None,
                }),
                None => Self::new(TermKind::Tuple(elements(tuple.elements())?)),
            },
            ValueRef::Map(map) => Self::new(TermKind::Map {
                label: map.label().map(|label| Identifier::Iri(label.to_owned())),
                pairs: map
                    .pairs()
                    .map(|(key, value)| Some((Self::of_value(key)?, Self::of_value(value)?)))
                    .collect::<Option<_>>()?,
            }),
            ValueRef::Other(value) => {
                literal(value.lexical().to_owned(), value.datatype().to_owned())
            }
        })
    }
}

/// A string converts to a plain string term, never to an IRI,
/// so that a conversion never guesses whether an IRI was meant.
///
/// [Term::iri], [Term::prefixed] and [Term::constant] create names.
impl From<&str> for Term {
    fn from(value: &str) -> Self {
        Term::string(value)
    }
}

/// A string converts to a plain string term, never to an IRI,
/// so that a conversion never guesses whether an IRI was meant.
///
/// [Term::iri], [Term::prefixed] and [Term::constant] create names.
impl From<String> for Term {
    fn from(value: String) -> Self {
        Term::string(value)
    }
}

impl From<i64> for Term {
    fn from(value: i64) -> Self {
        Term::integer(value)
    }
}

impl From<f64> for Term {
    fn from(value: f64) -> Self {
        Term::double(value)
    }
}

impl From<bool> for Term {
    fn from(value: bool) -> Self {
        Term::new(TermKind::Boolean(value))
    }
}

impl From<Variable> for Term {
    fn from(value: Variable) -> Self {
        Term::new(TermKind::Variable(value))
    }
}

/// The term gets the operation's span, since both cover the same source text.
impl From<Operation> for Term {
    fn from(value: Operation) -> Self {
        let span = value.span;
        Term {
            kind: TermKind::Operation(value),
            span,
        }
    }
}

/// The term gets the aggregate's span, since both cover the same source text.
impl From<Aggregate> for Term {
    fn from(value: Aggregate) -> Self {
        let span = value.span;
        Term {
            kind: TermKind::Aggregate(value),
            span,
        }
    }
}

/// Generates a constructor of a binary [Operation] for each listed operator.
macro_rules! binary_constructors {
    ($($(#[$documentation:meta])* $function:ident => $operator:ident),+ $(,)?) => {
        impl Operation {
            $(
                $(#[$documentation])*
                pub fn $function(left: impl Into<Term>, right: impl Into<Term>) -> Self {
                    Self::new(Operator::$operator, vec![left.into(), right.into()])
                }
            )+
        }
    };
}

binary_constructors! {
    /// Creates the comparison `left = right`, by term identity.
    equal => Equal,
    /// Creates the comparison `left != right`, by term distinctness.
    unequal => Unequal,
    /// Creates the comparison `left == right`, by value equality.
    value_equal => ValueEqual,
    /// Creates the comparison `left !== right`, by value inequality.
    value_unequal => ValueUnequal,
    /// Creates the comparison `left > right`.
    greater => Greater,
    /// Creates the comparison `left >= right`.
    greater_eq => GreaterEq,
    /// Creates the comparison `left < right`.
    less => Less,
    /// Creates the comparison `left <= right`.
    less_eq => LessEq,
    /// Creates the addition `left + right`.
    add => Add,
    /// Creates the subtraction `left - right`.
    subtract => Subtract,
    /// Creates the multiplication `left * right`.
    multiply => Multiply,
    /// Creates the division `left / right`.
    divide => Divide,
}

impl Aggregate {
    /// Creates an aggregate over the distinct values of the given input term.
    pub fn new(kind: AggregateKind, input: impl Into<Term>) -> Self {
        Self {
            kind,
            input: Some(Box::new(input.into())),
            value: None,
            scope: Vec::new(),
            legacy_list: false,
            span: None,
        }
    }

    /// Creates the aggregate `#count()`, which counts the body matches.
    pub fn matches() -> Self {
        Self {
            kind: AggregateKind::Count,
            input: None,
            value: None,
            scope: Vec::new(),
            legacy_list: false,
            span: None,
        }
    }

    /// Returns the aggregate with the given scope variables, as in `#sum(?v per ?w, …)`.
    ///
    /// The variables replace any earlier scope.
    /// On [Aggregate::matches], this gives `#count(per ?w, …)`, which has no value.
    pub fn per(mut self, variables: impl IntoIterator<Item = Variable>) -> Self {
        self.scope = variables.into_iter().collect();
        self.legacy_list = false;
        self
    }

    /// Creates a `#count` aggregate.
    pub fn count(input: impl Into<Term>) -> Self {
        Self::new(AggregateKind::Count, input)
    }

    /// Creates a `#min` aggregate.
    pub fn min(input: impl Into<Term>) -> Self {
        Self::new(AggregateKind::Min, input)
    }

    /// Creates a `#max` aggregate.
    pub fn max(input: impl Into<Term>) -> Self {
        Self::new(AggregateKind::Max, input)
    }

    /// Creates a `#sum` aggregate.
    pub fn sum(input: impl Into<Term>) -> Self {
        Self::new(AggregateKind::Sum, input)
    }
}

impl Atom {
    /// Returns the atom with the given argument terms appended.
    pub fn terms(mut self, terms: impl IntoIterator<Item = impl Into<Term>>) -> Self {
        self.terms.extend(terms.into_iter().map(Into::into));
        self
    }

    /// Returns the atom with one argument term appended.
    pub fn term(mut self, term: impl Into<Term>) -> Self {
        self.terms.push(term.into());
        self
    }
}

impl From<Atom> for Fact {
    fn from(value: Atom) -> Self {
        Self {
            predicate: value.predicate,
            terms: value.terms,
            labels: value.labels,
            span: value.span,
        }
    }
}

/// An atom converts to a positive literal, since a negated literal must be created explicitly.
impl From<Atom> for Literal {
    fn from(value: Atom) -> Self {
        Self::Positive(value)
    }
}

impl From<Operation> for Literal {
    fn from(value: Operation) -> Self {
        Self::Operation(value)
    }
}

impl Rule {
    /// Creates a fluent builder for a rule.
    pub fn builder() -> RuleBuilder {
        RuleBuilder {
            rule: Rule::new(Vec::new(), Vec::new()),
        }
    }
}

/// A fluent builder for [Rule]s.
///
/// # Example
///
/// ```
/// use nemo::{atom, model::Rule};
///
/// let rule = Rule::builder()
///     .head(atom!(orphan(?x)))
///     .body(atom!(person(?x)))
///     .negated(atom!(parent(?y, ?x)))
///     .build();
/// assert_eq!(rule.to_string(), "orphan(?x) :- person(?x), ~parent(?y, ?x) .");
/// ```
#[derive(Debug, Clone)]
pub struct RuleBuilder {
    /// The rule under construction.
    rule: Rule,
}

impl RuleBuilder {
    /// Adds an atom to the head.
    pub fn head(mut self, atom: Atom) -> Self {
        self.rule.head.push(atom);
        self
    }

    /// Adds a literal to the body.
    ///
    /// An atom is added as a positive literal, and an operation as an operation literal.
    pub fn body(mut self, literal: impl Into<Literal>) -> Self {
        self.rule.body.push(literal.into());
        self
    }

    /// Adds a negated atom to the body.
    pub fn negated(self, atom: Atom) -> Self {
        self.body(Literal::Negative(atom))
    }

    /// Attaches an attribute to the rule.
    pub fn attribute(mut self, attribute: Attribute) -> Self {
        self.rule.attributes.push(attribute);
        self
    }

    /// Returns the built rule.
    pub fn build(self) -> Rule {
        self.rule
    }
}

impl Program {
    /// Creates a fluent builder for a program.
    pub fn builder() -> ProgramBuilder {
        ProgramBuilder {
            statements: Vec::new(),
        }
    }
}

/// A fluent builder for [Program]s.
#[derive(Debug, Clone, Default)]
pub struct ProgramBuilder {
    /// The statements collected so far.
    statements: Vec<Statement>,
}

impl ProgramBuilder {
    /// Adds a fact, given as a fact or an atom.
    pub fn fact(mut self, fact: impl Into<Fact>) -> Self {
        self.statements.push(Statement::Fact(fact.into()));
        self
    }

    /// Adds a rule.
    pub fn rule(mut self, rule: Rule) -> Self {
        self.statements.push(Statement::Rule(rule));
        self
    }

    /// Adds a statement of any kind.
    pub fn statement(mut self, statement: impl Into<Statement>) -> Self {
        self.statements.push(statement.into());
        self
    }

    /// Returns the built program.
    pub fn build(self) -> Program {
        Program::new(self.statements)
    }
}
