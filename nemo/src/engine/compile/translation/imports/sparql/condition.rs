//! Pushed conditions as SPARQL expressions:
//! the row condition the filter pass decided for a SPARQL-fed predicate,
//! translated for the endpoint to apply,
//! if every operator in it has a SPARQL meaning that excludes no row the condition keeps,
//! under the assumptions the rewrite makes of an endpoint
//! (see [sparql_rewrite](crate::engine::transform::sparql_rewrite)).
//!
//! The local filter applies to the delivered rows as well, so an expression may keep more:
//! wherever Nemo keeps a row, the expression must hold, neither false nor an error.
//!
//! An identity goes out in one of three forms:
//! - With a constant at the top of the condition, as a `VALUES` block
//!   ([sparql_values](Translator::sparql_values)), as bindings do: the constant's terms
//!   ([ground_terms](SparqlQuery::ground_terms)), which the endpoint matches as terms,
//!   through its indexes.
//! - Elsewhere in a positive position, as SPARQL's `=`, never `sameTerm`, which QLever does not
//!   implement. There `=` holds wherever Nemo's identity does, and finds every spelling of a
//!   number, boolean or dateTime, except at a NaN, which is never `=` to itself.
//! - Under a negation, only with an IRI, where `=` is the identity and never fails.
//!   With a literal, `=` would hold for more (the double `3.0E0` is `=` to the integer 3, a
//!   different value to Nemo) and fail on literals it cannot compare.
//!
//! Where Nemo reads a value differently from its term:
//! - Regardless of the data, the translation follows Nemo: it reads language tags in lower
//!   case.
//! - For a number, which Nemo and stores, or stores among themselves, read differently,
//!   the expression keeps the rows for the local filter.
//!   Nemo reads Wikidata's `"3.0"^^xsd:decimal`, and `"03"^^xsd:int`, as the integer 3, whose
//!   `STR` is `"3"` and whose `DATATYPE` is `xsd:integer`, and the lexical form of a double is
//!   the one stored for some stores (`"2.5E0"`) and the store's own writing of the number for
//!   others (Oxigraph writes `"2.5"`).

use nemo_io::sparql::{
    SparqlQuery, ValuesBlock,
    spargebra::{
        algebra::{Expression, Function},
        term::{GroundTerm, Literal, NamedNode, Variable},
    },
};
use nemo_physical::value::DataValue;

use crate::{
    engine::compile::translation::Translator,
    model::{Operation, Operator, Term, TermKind, VariableKind},
};

/// Where a subexpression sits: what the expression around it needs of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Polarity {
    /// Under an even number of negations: it must hold wherever Nemo's holds.
    Positive,
    /// Under an odd number of negations: it must be false wherever Nemo's is,
    /// never an error.
    Negative,
    /// Inside another operator's argument: it must hold exactly where Nemo's holds,
    /// and be false exactly where Nemo's is.
    Exact,
}

impl Polarity {
    /// Returns the polarity under a negation.
    fn flip(self) -> Self {
        match self {
            Self::Positive => Self::Negative,
            Self::Negative => Self::Positive,
            Self::Exact => Self::Exact,
        }
    }
}

impl Translator<'_> {
    /// Returns a condition that is the identity of a delivered column with a constant,
    /// at the top of a conjunct, as a `VALUES` block over the column's projected variable,
    /// or `None` for any other condition.
    ///
    /// The block binds the column to the constant's terms, as bindings do
    /// ([with_values](SparqlQuery::with_values)).
    /// `delivered`, one flag per projected variable, maps delivered columns to them.
    pub(super) fn sparql_values(
        &self,
        condition: &Operation,
        delivered: &[bool],
    ) -> Option<ValuesBlock> {
        if condition.operator != Operator::Equal {
            return None;
        }
        let [left, right] = condition.arguments.as_slice() else {
            return None;
        };
        let (column, constant) = match (column_number(left), column_number(right)) {
            (Some(column), None) => (column, right),
            (None, Some(column)) => (column, left),
            _ => return None,
        };
        let value = self.ground_value(constant)?;
        if SparqlQuery::ground_terms(&value).is_empty() {
            return None;
        }
        let (position, _) = delivered
            .iter()
            .enumerate()
            .filter(|(_, delivered)| **delivered)
            .nth(column)?;
        Some(ValuesBlock {
            positions: vec![position],
            rows: vec![vec![value]],
        })
    }

    /// Returns the condition as a SPARQL expression over the query's projected variables,
    /// or `None` if some part of it has no SPARQL form (then only the local filter applies).
    ///
    /// A condition reading the lexical form or the datatype of a value (`STR`, `DATATYPE`)
    /// also keeps the rows where that value is a number (see the module documentation).
    ///
    /// The variable `column N` refers to delivered column `N`.
    /// `delivered`, one flag per projected variable, maps it back to its projected variable.
    pub(super) fn sparql_condition(
        &self,
        condition: &Operation,
        variables: &[String],
        delivered: &[bool],
    ) -> Option<Expression> {
        // Delivered position → projected variable.
        let delivered: Vec<&String> = variables
            .iter()
            .zip(delivered)
            .filter(|(_, delivered)| **delivered)
            .map(|(variable, _)| variable)
            .collect();
        let mut lexical_reads = Vec::new();
        let expression = self.sparql_expression(
            condition,
            &delivered,
            &mut lexical_reads,
            Polarity::Positive,
        )?;
        Some(
            lexical_reads
                .into_iter()
                .fold(expression, |expression, value| {
                    let numeric = Expression::FunctionCall(Function::IsNumeric, vec![value]);
                    Expression::Or(Box::new(expression), Box::new(numeric))
                }),
        )
    }

    /// Returns an operation in the given position as an expression,
    /// adding to `lexical_reads` the arguments whose lexical form or datatype it reads (see
    /// [sparql_condition](Self::sparql_condition)).
    fn sparql_expression(
        &self,
        operation: &Operation,
        delivered: &[&String],
        lexical_reads: &mut Vec<Expression>,
        polarity: Polarity,
    ) -> Option<Expression> {
        let inner = match operation.operator {
            Operator::And | Operator::Or => polarity,
            Operator::Not => polarity.flip(),
            _ => Polarity::Exact,
        };
        let arguments: Vec<Expression> = operation
            .arguments
            .iter()
            .map(|argument| self.sparql_argument(argument, delivered, lexical_reads, inner))
            .collect::<Option<_>>()?;
        let binary = |build: fn(Box<Expression>, Box<Expression>) -> Expression,
                      arguments: Vec<Expression>| {
            let [left, right] = <[Expression; 2]>::try_from(arguments).ok()?;
            Some(build(Box::new(left), Box::new(right)))
        };
        let call = |function: Function, arity: usize, arguments: Vec<Expression>| {
            (arguments.len() == arity).then_some(Expression::FunctionCall(function, arguments))
        };
        match operation.operator {
            // Nemo's identity as SPARQL's `=`: with an IRI anywhere, with another constant
            // only where `=` may hold for more (see the module documentation).
            Operator::Equal => {
                let [left, right] = <[Expression; 2]>::try_from(arguments).ok()?;
                let exact = is_iri(&left) || is_iri(&right);
                let covering = polarity == Polarity::Positive
                    && (is_constant(&left) || is_constant(&right))
                    && !is_nan(&left)
                    && !is_nan(&right);
                (exact || covering).then(|| Expression::Equal(Box::new(left), Box::new(right)))
            }
            // A negated identity, which only an IRI makes exact.
            Operator::Unequal => {
                let [left, right] = <[Expression; 2]>::try_from(arguments).ok()?;
                (is_iri(&left) || is_iri(&right)).then(|| {
                    Expression::Not(Box::new(Expression::Equal(Box::new(left), Box::new(right))))
                })
            }
            Operator::ValueEqual => binary(Expression::Equal, arguments),
            Operator::ValueUnequal => {
                binary(Expression::Equal, arguments).map(|equal| Expression::Not(Box::new(equal)))
            }
            Operator::Greater => binary(Expression::Greater, arguments),
            Operator::GreaterEq => binary(Expression::GreaterOrEqual, arguments),
            Operator::Less => binary(Expression::Less, arguments),
            Operator::LessEq => binary(Expression::LessOrEqual, arguments),
            Operator::And => fold(Expression::And, arguments),
            Operator::Or => fold(Expression::Or, arguments),
            Operator::Not => {
                let [inner] = <[Expression; 1]>::try_from(arguments).ok()?;
                Some(Expression::Not(Box::new(inner)))
            }
            Operator::Add => binary(Expression::Add, arguments),
            Operator::Subtract => binary(Expression::Subtract, arguments),
            Operator::Multiply => binary(Expression::Multiply, arguments),
            Operator::Divide => binary(Expression::Divide, arguments),
            Operator::Negate => {
                let [inner] = <[Expression; 1]>::try_from(arguments).ok()?;
                Some(Expression::UnaryMinus(Box::new(inner)))
            }
            Operator::Str => {
                lexical_reads.extend(arguments.first().cloned());
                call(Function::Str, 1, arguments)
            }
            // Nemo reads language tags in lower case.
            Operator::Lang => call(Function::Lang, 1, arguments)
                .map(|tag| Expression::FunctionCall(Function::LCase, vec![tag])),
            Operator::LangMatches => call(Function::LangMatches, 2, arguments),
            Operator::Datatype => {
                lexical_reads.extend(arguments.first().cloned());
                call(Function::Datatype, 1, arguments)
            }
            Operator::IsIri => call(Function::IsIri, 1, arguments),
            Operator::IsNumeric => call(Function::IsNumeric, 1, arguments),
            // An unbound value is a null to Nemo, but `isBlank` fails on it.
            Operator::IsNull => match <[Expression; 1]>::try_from(arguments).ok()? {
                [Expression::Variable(variable)] => Some(Expression::Or(
                    Box::new(Expression::Not(Box::new(Expression::Bound(
                        variable.clone(),
                    )))),
                    Box::new(Expression::FunctionCall(
                        Function::IsBlank,
                        vec![Expression::Variable(variable)],
                    )),
                )),
                [value] => Some(Expression::FunctionCall(Function::IsBlank, vec![value])),
            },
            // A type test of literals is a test of their datatype,
            // guarded so that it is false rather than an error on other terms.
            Operator::IsString => datatype_test(arguments, &["string"]),
            Operator::IsDouble => datatype_test(arguments, &["double"]),
            Operator::IsFloat => datatype_test(arguments, &["float"]),
            Operator::IsBoolean => datatype_test(arguments, &["boolean"]),
            Operator::IsDateTime => datatype_test(arguments, &["dateTime", "dateTimeStamp"]),
            Operator::IsDate => datatype_test(arguments, &["date"]),
            Operator::IsTime => datatype_test(arguments, &["time"]),
            Operator::IsDuration => datatype_test(
                arguments,
                &["duration", "dayTimeDuration", "yearMonthDuration"],
            ),
            Operator::IsLangString => {
                let [value] = <[Expression; 1]>::try_from(arguments).ok()?;
                let language = Expression::FunctionCall(Function::Lang, vec![value.clone()]);
                let untagged = Expression::Equal(
                    Box::new(language),
                    Box::new(Expression::Literal(Literal::new_simple_literal(""))),
                );
                Some(Expression::And(
                    Box::new(Expression::FunctionCall(Function::IsLiteral, vec![value])),
                    Box::new(Expression::Not(Box::new(untagged))),
                ))
            }
            Operator::StringLength => call(Function::StrLen, 1, arguments),
            Operator::UpperCase => call(Function::UCase, 1, arguments),
            Operator::LowerCase => call(Function::LCase, 1, arguments),
            Operator::Contains => call(Function::Contains, 2, arguments),
            Operator::StartsWith => call(Function::StrStarts, 2, arguments),
            Operator::EndsWith => call(Function::StrEnds, 2, arguments),
            Operator::StrBefore => call(Function::StrBefore, 2, arguments),
            Operator::StrAfter => call(Function::StrAfter, 2, arguments),
            Operator::Abs => call(Function::Abs, 1, arguments),
            Operator::Round => call(Function::Round, 1, arguments),
            Operator::Ceil => call(Function::Ceil, 1, arguments),
            Operator::Floor => call(Function::Floor, 1, arguments),
            // Everything else (Nemo's own builtins, regular expressions,
            // whose syntax differs, structures, and the type tests whose
            // values come from several datatypes, such as an integer from
            // `xsd:int` or an integral `xsd:decimal`) has no SPARQL form
            // that excludes the same rows, so only the local filter
            // applies.
            _ => None,
        }
    }

    /// Returns an argument term as an expression: a column pseudo-variable, a
    /// nested operation, or a ground term.
    fn sparql_argument(
        &self,
        term: &Term,
        delivered: &[&String],
        lexical_reads: &mut Vec<Expression>,
        polarity: Polarity,
    ) -> Option<Expression> {
        if let Some(position) = column_number(term) {
            let name = delivered.get(position)?;
            return Variable::new(name.as_str()).ok().map(Expression::Variable);
        }
        match &term.kind {
            TermKind::Operation(operation) => {
                self.sparql_expression(operation, delivered, lexical_reads, polarity)
            }
            _ => term_expression(SparqlQuery::ground_term(&self.ground_value(term)?)?),
        }
    }

    /// Returns the value of a ground argument, a constant or a parameter,
    /// or `None` for any other.
    fn ground_value(&self, term: &Term) -> Option<DataValue> {
        match &term.kind {
            TermKind::Variable(variable) if variable.kind == VariableKind::Global => {
                self.global(&variable.name, term.span).ok()
            }
            TermKind::Variable(_) | TermKind::Aggregate(_) | TermKind::Operation(_) => None,
            _ => self.evaluate(term).ok(),
        }
    }
}

/// Returns the number of the delivered column a pseudo-variable `column N` refers to,
/// or `None` for any other term.
fn column_number(term: &Term) -> Option<usize> {
    match &term.kind {
        TermKind::Variable(variable) if variable.kind == VariableKind::Universal => {
            variable.name.strip_prefix("column ")?.parse().ok()
        }
        _ => None,
    }
}

/// Returns whether an expression is an IRI constant.
fn is_iri(expression: &Expression) -> bool {
    matches!(expression, Expression::NamedNode(_))
}

/// Returns whether an expression is a constant.
fn is_constant(expression: &Expression) -> bool {
    matches!(
        expression,
        Expression::NamedNode(_) | Expression::Literal(_)
    )
}

/// Returns whether an expression is a NaN constant, which SPARQL's `=` never holds for.
fn is_nan(expression: &Expression) -> bool {
    matches!(expression, Expression::Literal(literal) if literal.value() == "NaN")
}

/// Returns a ground term as an expression.
fn term_expression(term: GroundTerm) -> Option<Expression> {
    Some(match term {
        GroundTerm::NamedNode(node) => Expression::NamedNode(node),
        GroundTerm::Literal(literal) => Expression::Literal(literal),
        #[allow(unreachable_patterns)]
        _ => return None,
    })
}

/// Returns the test that a literal has one of the given XSD datatypes, by their local names:
/// `isLiteral(x) && (datatype(x) = xsd:t || …)`.
///
/// The literal test comes first, so that the test is false on an IRI or a blank node
/// rather than an error, as the local type test is.
/// `=` of two IRIs is their identity, and never an error, so the test is exact in any position.
fn datatype_test(arguments: Vec<Expression>, datatypes: &[&str]) -> Option<Expression> {
    let [value] = <[Expression; 1]>::try_from(arguments).ok()?;
    let datatype = Expression::FunctionCall(Function::Datatype, vec![value.clone()]);
    let alternatives = datatypes
        .iter()
        .map(|name| {
            let iri = format!("http://www.w3.org/2001/XMLSchema#{name}");
            Expression::Equal(
                Box::new(datatype.clone()),
                Box::new(Expression::NamedNode(NamedNode::new_unchecked(iri))),
            )
        })
        .collect();
    Some(Expression::And(
        Box::new(Expression::FunctionCall(Function::IsLiteral, vec![value])),
        Box::new(fold(Expression::Or, alternatives)?),
    ))
}

/// Returns a connective folded over its operands.
fn fold(
    build: fn(Box<Expression>, Box<Expression>) -> Expression,
    arguments: Vec<Expression>,
) -> Option<Expression> {
    let mut arguments = arguments.into_iter();
    let first = arguments.next()?;
    Some(arguments.fold(first, |left, right| build(Box::new(left), Box::new(right))))
}
