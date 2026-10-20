//! Constructors, conversions, and fluent builders for program components.
//!
//! They suit programs whose predicate names or rule shapes are computed at runtime.
//! The macros in [macros](crate::model::macros) suit programs fixed in the Rust source,
//! and `parse` reads programs from text.
//!
//! Conversions of Rust primitives:
//! - `&str` and [String] convert to *plain string* terms,
//!   since a conversion never guesses whether an IRI was meant.
//!   IRIs and names are requested explicitly ([Term::iri], [Term::prefixed], [Term::constant]).
//! - Rust float literals are doubles.
//!   Exact decimals come from [Term::decimal] (or parsing),
//!   since a float literal cannot carry the distinction.

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

    /// Creates a full-IRI constant term.
    pub fn iri(iri: impl Into<String>) -> Self {
        Self::new(TermKind::Constant(Identifier::Iri(iri.into())))
    }

    /// Creates a prefixed-name constant term like `foaf:Person`.
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

    /// Returns the exact number term `mantissa * 10^exponent`.
    ///
    /// An integral value gives an integer term.
    /// Returns `None` if the value's canonical form exceeds the length limit of numbers.
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

    /// Creates a function-style call by name.
    ///
    /// A builtin name (see [Operator::from_name]) becomes an operation,
    /// and any other name an uninterpreted function term.
    pub fn apply(name: &str, arguments: impl IntoIterator<Item = impl Into<Term>>) -> Self {
        match Operator::from_name(name) {
            Some(operator) => {
                Operation::new(operator, arguments.into_iter().map(Into::into).collect()).into()
            }
            None => Self::function(name, arguments),
        }
    }

    /// Returns the term denoting a data value.
    ///
    /// It puts values computed in Rust, or read from engine results, into a program.
    /// IRIs and structure labels become full IRIs, not resolved against prefixes or a base.
    /// Returns `None` for nulls and for structures containing them, since no term denotes a null.
    pub fn value(value: &DataValue) -> Option<Self> {
        Self::of_value(value.as_ref())
    }

    /// Returns the term denoting a borrowed data value.
    fn of_value(value: ValueRef<'_>) -> Option<Self> {
        /// Returns a typed literal with a full-IRI datatype.
        fn literal(lexical: String, datatype: String) -> Term {
            Term::new(TermKind::RdfLiteral {
                lexical,
                datatype: Identifier::Iri(datatype),
            })
        }
        /// Returns the typed literal of a value of a native datatype:
        /// its canonical lexical and the datatype of its domain.
        fn typed(value: ValueRef<'_>, lexical: String) -> Term {
            let datatype = value
                .datatype_iri()
                .expect("a value of a native datatype has one");
            literal(lexical, datatype.to_owned())
        }
        /// Returns the terms of a structure's elements, or `None` if any holds a null.
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

/// Strings convert to *plain string* terms, never IRIs (see the module
/// documentation).
impl From<&str> for Term {
    fn from(value: &str) -> Self {
        Term::string(value)
    }
}

/// Strings convert to *plain string* terms, never IRIs (see the module
/// documentation).
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

/// The term takes the operation's span, since both cover the same source text.
impl From<Operation> for Term {
    fn from(value: Operation) -> Self {
        let span = value.span;
        Term {
            kind: TermKind::Operation(value),
            span,
        }
    }
}

/// The term takes the aggregate's span, since both cover the same source text.
impl From<Aggregate> for Term {
    fn from(value: Aggregate) -> Self {
        let span = value.span;
        Term {
            kind: TermKind::Aggregate(value),
            span,
        }
    }
}

/// Generates a binary [Operation] constructor per listed operator.
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
    /// A term-identity comparison `left = right`.
    equal => Equal,
    /// A term-distinctness comparison `left != right`.
    unequal => Unequal,
    /// A value-equality comparison `left == right`.
    value_equal => ValueEqual,
    /// A value-inequality comparison `left !== right`.
    value_unequal => ValueUnequal,
    /// A comparison `left > right`.
    greater => Greater,
    /// A comparison `left >= right`.
    greater_eq => GreaterEq,
    /// A comparison `left < right`.
    less => Less,
    /// A comparison `left <= right`.
    less_eq => LessEq,
    /// An addition `left + right`.
    add => Add,
    /// A subtraction `left - right`.
    subtract => Subtract,
    /// A multiplication `left * right`.
    multiply => Multiply,
    /// A division `left / right`.
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

    /// Returns the match count: `#count()`.
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

    /// Scopes the aggregate by the given variables: `?v per ?w…`.
    ///
    /// On [Aggregate::matches], it is the value-less `#count(per ?w…)`.
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
    /// Appends argument terms.
    pub fn terms(mut self, terms: impl IntoIterator<Item = impl Into<Term>>) -> Self {
        self.terms.extend(terms.into_iter().map(Into::into));
        self
    }

    /// Appends one argument term.
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

/// Atoms convert to positive literals, since negation is explicit.
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

    /// Adds a literal to the body:
    /// atoms come in as positive literals, and operations as constraints.
    pub fn body(mut self, literal: impl Into<Literal>) -> Self {
        self.rule.body.push(literal.into());
        self
    }

    /// Adds a negated atom to the body.
    pub fn negated(self, atom: Atom) -> Self {
        self.body(Literal::Negative(atom))
    }

    /// Attaches an attribute.
    pub fn attribute(mut self, attribute: Attribute) -> Self {
        self.rule.attributes.push(attribute);
        self
    }

    /// Finishes the rule.
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

    /// Adds any statement.
    pub fn statement(mut self, statement: impl Into<Statement>) -> Self {
        self.statements.push(statement.into());
        self
    }

    /// Finishes the program.
    pub fn build(self) -> Program {
        Program::new(self.statements)
    }
}
