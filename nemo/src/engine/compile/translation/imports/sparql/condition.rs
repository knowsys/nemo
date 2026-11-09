//! The translation of an import's condition into a SPARQL expression, for the endpoint to
//! apply.
//!
//! A condition is translated only if every operator in it has a SPARQL meaning that excludes
//! no row that the condition keeps, under the assumptions that the rewrite makes of an
//! endpoint (see [sparql_rewrite](crate::engine::transform::sparql_rewrite)).
//! The load still applies the whole condition to the delivered rows,
//! so an expression may keep more rows than the condition does:
//! wherever Nemo keeps a row, the expression must hold, and be neither false nor an error.
//!
//! Nemo may read a value differently from its term:
//! - Nemo reads language tags in lower case, and the translation follows Nemo.
//! - Nemo and the stores read numbers differently, and so do the stores among themselves.
//!   Nemo reads `"3.0"^^xsd:decimal` and `"03"^^xsd:int` as the integer 3,
//!   whose `STR` is `"3"` and whose `DATATYPE` is `xsd:integer`.
//!   A store may give a double the lexical form that it stored (`"2.5E0"`),
//!   or its own writing of the number, as Oxigraph gives `"2.5"`.
//!   So an expression that reads the lexical form or the datatype of a number keeps the
//!   row, and leaves the decision to the load.

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

/// What the expression around a subexpression needs of it,
/// which depends on where the subexpression sits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Polarity {
    /// Under an even number of negations,
    /// where it must hold wherever Nemo's condition holds.
    Positive,
    /// Under an odd number of negations,
    /// where it must be false, and never an error, wherever Nemo's condition is false.
    Negative,
    /// Inside an argument of another operator,
    /// where it must hold exactly where Nemo's condition holds, and be false exactly where it
    /// is false.
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
    /// Returns a condition that requires a delivered column to be identical to a constant
    /// as a `VALUES` block over the column's projected variable,
    /// or `None` for any other condition.
    ///
    /// The block binds the variable to the constant's terms
    /// ([ground_terms](SparqlQuery::ground_terms)),
    /// as [with_values](SparqlQuery::with_values) does for bindings,
    /// and the endpoint matches them as terms, through its indexes.
    /// `delivered` has one flag per projected variable, which says whether it is delivered.
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
    /// or `None` if some part of it has no SPARQL form.
    ///
    /// Without an expression, only the load applies the condition.
    /// A condition that reads the lexical form or the datatype of a value (`STR`, `DATATYPE`)
    /// also keeps the rows where that value is a number, as the module documentation explains.
    ///
    /// The variable `column N` refers to delivered column `N`.
    /// `variables` are the projected variables,
    /// and `delivered` has one flag per projected variable, which says whether it is delivered.
    pub(super) fn sparql_condition(
        &self,
        condition: &Operation,
        variables: &[String],
        delivered: &[bool],
    ) -> Option<Expression> {
        // The projected variable of each delivered column.
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

    /// Returns an operation at a position of the given polarity as an expression,
    /// and adds to `lexical_reads` the arguments whose lexical form or datatype it reads
    /// (see [sparql_condition](Self::sparql_condition)).
    ///
    /// `delivered` contains the projected variable of each delivered column.
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
            // Nemo's identity becomes SPARQL's `=`, never `sameTerm`,
            // which some stores do not implement, such as QLever.
            // With an IRI, `=` is the identity, in any position.
            // With another constant, `=` holds wherever Nemo's identity does,
            // and matches every spelling of a number, boolean or dateTime,
            // except a NaN, which is never `=` to itself.
            // So it is used only in a positive position.
            Operator::Equal => {
                let [left, right] = <[Expression; 2]>::try_from(arguments).ok()?;
                let exact = is_iri(&left) || is_iri(&right);
                let covering = polarity == Polarity::Positive
                    && (is_constant(&left) || is_constant(&right))
                    && !is_nan(&left)
                    && !is_nan(&right);
                (exact || covering).then(|| Expression::Equal(Box::new(left), Box::new(right)))
            }
            // A negated identity is translated only with an IRI.
            // With a literal, `=` would hold for more, since the double `3.0E0` is `=` to
            // the integer 3, a different value to Nemo,
            // and `=` fails on literals that it cannot compare.
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
            // Every other operator has no SPARQL form that excludes the same rows,
            // so only the load applies it.
            // These are Nemo's own builtins, regular expressions, whose syntax differs,
            // structures, and the type tests of values that come from several datatypes,
            // such as an integer from `xsd:int` or from an integral `xsd:decimal`.
            _ => None,
        }
    }

    /// Returns an argument term as an expression,
    /// for the variable of a delivered column, a nested operation, or a ground term.
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

/// Returns the number `N` of the delivered column that a variable `column N` refers to,
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

/// Returns the test that a value is a literal with one of the given XSD datatypes,
/// which are given by their local names: `isLiteral(x) && (datatype(x) = xsd:t || …)`.
///
/// The literal test comes first, so that the test is false on an IRI or a blank node
/// rather than an error, like Nemo's own type test.
/// `=` of two IRIs is their identity and never an error, so the test is exact in any position.
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

/// Returns the operands combined with a binary connective from left to right,
/// or `None` if there are none.
fn fold(
    build: fn(Box<Expression>, Box<Expression>) -> Expression,
    arguments: Vec<Expression>,
) -> Option<Expression> {
    let mut arguments = arguments.into_iter();
    let first = arguments.next()?;
    Some(arguments.fold(first, |left, right| build(Box::new(left), Box::new(right))))
}
