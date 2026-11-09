//! Terms, the arguments of atoms, facts and other terms.

mod aggregate;
mod documentation;
mod operation;
mod variable;

pub use self::{
    aggregate::{Aggregate, AggregateKind},
    documentation::BuiltinDocumentation,
    operation::{Operation, Operator},
    variable::{Variable, VariableKind},
};

use std::fmt::{self, Display, Formatter};

use nemo_physical::value::{Decimal, Number, ValueRef};

use crate::model::{Identifier, Span, fmt_list, span_ignoring_eq};

/// The kinds of [Term]s, with their contents.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TermKind {
    /// A variable.
    Variable(Variable),
    /// A constant, which is a bare name, an IRI, a prefixed name or a module-qualified name.
    Constant(Identifier),
    /// A blank node `_:label`, which is a null local to the program.
    ///
    /// Loading a program creates one null per label,
    /// so two mentions of `_:a` in a program name the same unknown value,
    /// and `_:a` in another program names a different one.
    BlankNode(String),
    /// A plain string.
    String(String),
    /// A language-tagged string like `"hello"@en`.
    LanguageString {
        /// The text.
        text: String,
        /// The language tag, without the `@`.
        language: String,
    },
    /// A boolean.
    Boolean(bool),
    /// A number like `42`, `1.5`, `2.5f` or `2.5e3`.
    Number(Number),
    /// A literal with an explicit datatype, like `"P1Y"^^xsd:duration`.
    RdfLiteral {
        /// The lexical form.
        lexical: String,
        /// The datatype's name.
        datatype: Identifier,
    },
    /// An uninterpreted function term like `pair(?x, ?y)`.
    Function {
        /// The function's name.
        name: Identifier,
        /// The argument terms.
        arguments: Vec<Term>,
        /// The names of the arguments, one per argument,
        /// if the term is written with named arguments, as in `person(name: ?n)`.
        ///
        /// The parser parses an atom as a function term first, which is why the field exists.
        /// A function term used as a value may not have named arguments.
        labels: Option<Vec<String>>,
    },
    /// A tuple like `(1, 2)`.
    Tuple(Vec<Term>),
    /// A map like `{a = 1, b = 2}`, with an optional label, as in `point{x = 1}`.
    ///
    /// The label must directly precede the brace,
    /// since a name followed by a space and a brace, like `csv { … }`,
    /// starts the options of a directive.
    Map {
        /// The map's label, if it has one.
        label: Option<Identifier>,
        /// The key-value pairs, in the order in which they are written.
        pairs: Vec<(Term, Term)>,
    },
    /// An application of a builtin operator.
    Operation(Operation),
    /// An aggregate, like `#sum(?v)`.
    Aggregate(Aggregate),
}

/// A term, which is the content of one argument position
/// in an atom, a fact or an enclosing term.
#[derive(Debug, Clone)]
pub struct Term {
    /// The kind of the term, with its content.
    pub kind: TermKind,
    /// The source position, or `None` for a component built programmatically.
    pub span: Option<Span>,
}

span_ignoring_eq!(Term: kind);

impl Term {
    /// Creates a term without a source position.
    pub fn new(kind: TermKind) -> Self {
        Self { kind, span: None }
    }

    /// Creates a variable term.
    pub fn variable(variable: Variable) -> Self {
        Self::new(TermKind::Variable(variable))
    }

    /// Creates an integer term.
    pub fn integer(value: i64) -> Self {
        Self::new(TermKind::Number(Number::Decimal(Decimal::from(value))))
    }

    /// Creates a plain string term.
    pub fn string(value: impl Into<String>) -> Self {
        Self::new(TermKind::String(value.into()))
    }

    /// Creates a constant term with a bare name.
    pub fn constant(name: impl Into<String>) -> Self {
        Self::new(TermKind::Constant(Identifier::bare(name)))
    }
}

impl From<TermKind> for Term {
    fn from(kind: TermKind) -> Self {
        Self::new(kind)
    }
}

/// Writes a list of arguments, each preceded by its label if `labels` is given.
pub(crate) fn fmt_labeled(
    f: &mut Formatter<'_>,
    terms: &[Term],
    labels: Option<&[String]>,
) -> fmt::Result {
    for (index, term) in terms.iter().enumerate() {
        if index > 0 {
            f.write_str(", ")?;
        }
        if let Some(labels) = labels {
            write!(f, "{}: ", labels[index])?;
        }
        write!(f, "{term}")?;
    }
    Ok(())
}

impl Display for Term {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.kind {
            TermKind::Variable(variable) => write!(f, "{variable}"),
            TermKind::Constant(name) => write!(f, "{name}"),
            TermKind::BlankNode(label) => write!(f, "_:{label}"),
            TermKind::String(string) => fmt_string(f, string),
            TermKind::LanguageString { text, language } => {
                fmt_string(f, text)?;
                write!(f, "@{language}")
            }
            TermKind::Boolean(value) => write!(f, "{value}"),
            // A number is written in the term syntax of its value:
            // a float with its suffix,
            // and a value without the form of a number literal as a typed literal.
            TermKind::Number(number) => write!(f, "{}", ValueRef::Number(number.as_ref())),
            TermKind::RdfLiteral { lexical, datatype } => {
                fmt_string(f, lexical)?;
                write!(f, "^^{datatype}")
            }
            TermKind::Function {
                name,
                arguments,
                labels,
            } => {
                write!(f, "{name}(")?;
                fmt_labeled(f, arguments, labels.as_deref())?;
                f.write_str(")")
            }
            TermKind::Tuple(terms) => {
                f.write_str("(")?;
                fmt_list(f, terms)?;
                if terms.len() == 1 {
                    f.write_str(",")?;
                }
                f.write_str(")")
            }
            TermKind::Map { label, pairs } => {
                if let Some(label) = label {
                    write!(f, "{label}")?;
                }
                f.write_str("{")?;
                for (index, (key, value)) in pairs.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{key} = {value}")?;
                }
                f.write_str("}")
            }
            TermKind::Operation(operation) => write!(f, "{operation}"),
            TermKind::Aggregate(aggregate) => write!(f, "{aggregate}"),
        }
    }
}

/// Writes a string in quotes, with its backslashes and quotes escaped.
fn fmt_string(f: &mut Formatter<'_>, string: &str) -> fmt::Result {
    write!(
        f,
        "\"{}\"",
        string.replace('\\', "\\\\").replace('"', "\\\"")
    )
}
