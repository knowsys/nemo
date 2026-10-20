//! Pushed conditions as SPARQL expressions:
//! the row condition the filter pass decided for a SPARQL-fed predicate,
//! translated for the endpoint to apply,
//! if every operator in it has a SPARQL meaning that excludes the same rows.

use nemo_io::sparql::{
    SparqlQuery,
    spargebra::{
        algebra::{Expression, Function},
        term::{GroundTerm, Literal, NamedNode, Variable},
    },
};

use crate::{
    engine::compile::translation::Translator,
    model::{Operation, Operator, Term, TermKind, VariableKind},
};

impl Translator<'_> {
    /// Returns the condition as a SPARQL expression over the query's projected variables,
    /// or `None` if some part of it has no SPARQL form (then only the local filter applies).
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
        self.sparql_expression(condition, &delivered)
    }

    /// Returns an operation as an expression.
    fn sparql_expression(
        &self,
        operation: &Operation,
        delivered: &[&String],
    ) -> Option<Expression> {
        let arguments: Vec<Expression> = operation
            .arguments
            .iter()
            .map(|argument| self.sparql_argument(argument, delivered))
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
            // Term identity and value equality both exist in SPARQL.
            Operator::Equal => binary(Expression::SameTerm, arguments),
            Operator::Unequal => {
                binary(Expression::SameTerm, arguments).map(|same| Expression::Not(Box::new(same)))
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
            Operator::Str => call(Function::Str, 1, arguments),
            Operator::Lang => call(Function::Lang, 1, arguments),
            Operator::LangMatches => call(Function::LangMatches, 2, arguments),
            Operator::Datatype => call(Function::Datatype, 1, arguments),
            Operator::IsIri => call(Function::IsIri, 1, arguments),
            Operator::IsNumeric => call(Function::IsNumeric, 1, arguments),
            Operator::IsNull => call(Function::IsBlank, 1, arguments),
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
    fn sparql_argument(&self, term: &Term, delivered: &[&String]) -> Option<Expression> {
        match &term.kind {
            TermKind::Variable(variable) if variable.kind == VariableKind::Universal => {
                let position: usize = variable.name.strip_prefix("column ")?.parse().ok()?;
                let name = delivered.get(position)?;
                Variable::new(name.as_str()).ok().map(Expression::Variable)
            }
            TermKind::Variable(variable) if variable.kind == VariableKind::Global => {
                let value = self.global(&variable.name, term.span).ok()?;
                term_expression(SparqlQuery::ground_term(&value)?)
            }
            TermKind::Variable(_) | TermKind::Aggregate(_) => None,
            TermKind::Operation(operation) => self.sparql_expression(operation, delivered),
            _ => {
                let value = self.evaluate(term).ok()?;
                term_expression(SparqlQuery::ground_term(&value)?)
            }
        }
    }
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
/// `isLiteral(x) && (sameTerm(datatype(x), xsd:t) || …)`.
///
/// The literal test comes first, so that the test is false on an IRI or a blank node
/// rather than an error, as the local type test is.
fn datatype_test(arguments: Vec<Expression>, datatypes: &[&str]) -> Option<Expression> {
    let [value] = <[Expression; 1]>::try_from(arguments).ok()?;
    let datatype = Expression::FunctionCall(Function::Datatype, vec![value.clone()]);
    let alternatives = datatypes
        .iter()
        .map(|name| {
            let iri = format!("http://www.w3.org/2001/XMLSchema#{name}");
            Expression::SameTerm(
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
