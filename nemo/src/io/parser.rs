//! A parser for rulewerk-style rules.

use std::{cell::RefCell, collections::HashMap, fmt::Debug};

use nom::{
    branch::alt,
    bytes::complete::{is_not, tag},
    character::complete::{alpha1, digit1, multispace1, none_of, satisfy},
    combinator::{all_consuming, cut, map, map_res, opt, recognize, value},
    multi::{many0, many1, separated_list1},
    sequence::{delimited, pair, preceded, terminated, tuple},
    Err,
};

use crate::{error::Error, model::*, types::LogicalTypeEnum};

use macros::traced;

mod types;
use types::{IntermediateResult, Span};
pub(crate) mod iri;
pub(crate) mod ntriples;
pub(crate) mod rfc5234;
pub(crate) mod sparql;
pub(crate) mod turtle;
pub use types::{LocatedParseError, ParseError, ParseResult};

/// Parse a program in the given `input`-String and return a [`Program`].
///
/// The program will be parsed and checked for unsupported features.
///
/// # Error
/// Returns an appropriate [`Error`][crate::error::Error] variant on parsing and feature check issues.
pub fn parse_program(input: impl AsRef<str>) -> Result<Program, Error> {
    let program = all_input_consumed(RuleParser::new().parse_program())(input.as_ref())?;
    Ok(program)
}

/// A combinator to add tracing to the parser.
/// [fun] is an identifier for the parser and [parser] is the actual parser.
#[inline(always)]
fn traced<'a, T, P>(
    fun: &'static str,
    mut parser: P,
) -> impl FnMut(Span<'a>) -> IntermediateResult<'a, T>
where
    T: Debug,
    P: FnMut(Span<'a>) -> IntermediateResult<'a, T>,
{
    move |input| {
        log::trace!(target: "parser", "{fun}({input:?})");
        let result = parser(input);
        log::trace!(target: "parser", "{fun}({input:?}) -> {result:?}");
        result
    }
}

/// A combinator that makes sure all input has been consumed.
pub fn all_input_consumed<'a, T: 'a>(
    parser: impl FnMut(Span<'a>) -> IntermediateResult<'a, T> + 'a,
) -> impl FnMut(&'a str) -> Result<T, LocatedParseError> + 'a {
    let mut p = all_consuming(parser);
    move |input| {
        let input = Span::new(input);
        p(input).map(|(_, result)| result).map_err(|e| match e {
            Err::Incomplete(e) => ParseError::MissingInput(match e {
                nom::Needed::Unknown => "expected an unknown amount of further input".to_string(),
                nom::Needed::Size(size) => format!("expected at least {size} more bytes"),
            })
            .at(input),
            Err::Error(e) | Err::Failure(e) => e,
        })
    }
}

/// A combinator that recognises a comment, starting at a `%`
/// character and ending at the end of the line.
pub fn comment(input: Span) -> IntermediateResult<()> {
    alt((
        value((), pair(tag("%"), is_not("\n\r"))),
        // a comment that immediately precedes the end of the line –
        // this must come after the normal line comment above
        value((), tag("%")),
    ))(input)
}

/// A combinator that recognises an arbitrary amount of whitespace and
/// comments.
pub fn multispace_or_comment0(input: Span) -> IntermediateResult<()> {
    value((), many0(alt((value((), multispace1), comment))))(input)
}

/// A combinator that recognises any non-empty amount of whitespace
/// and comments.
pub fn multispace_or_comment1(input: Span) -> IntermediateResult<()> {
    value((), many1(alt((value((), multispace1), comment))))(input)
}

/// A combinator that modifies the associated error.
pub fn map_error<'a, T: 'a>(
    mut parser: impl FnMut(Span<'a>) -> IntermediateResult<'a, T> + 'a,
    mut error: impl FnMut() -> ParseError + 'a,
) -> impl FnMut(Span<'a>) -> IntermediateResult<'a, T> + 'a {
    move |input| {
        parser(input).map_err(|e| match e {
            Err::Incomplete(_) => e,
            Err::Error(context) => {
                let mut err = error().at(input);
                err.append(context);
                Err::Error(err)
            }
            Err::Failure(context) => {
                let mut err = error().at(input);
                err.append(context);
                Err::Failure(err)
            }
        })
    }
}

/// A combinator that creates a parser for a specific token.
pub fn token<'a>(token: &'a str) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
    map_error(tag(token), || ParseError::ExpectedToken(token.to_string()))
}

/// A combinator that creates a parser for a specific token,
/// surrounded by whitespace or comments.
pub fn space_delimited_token<'a>(
    token: &'a str,
) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
    map_error(
        delimited(multispace_or_comment0, tag(token), multispace_or_comment0),
        || ParseError::ExpectedToken(token.to_string()),
    )
}

/// Expand a prefix.
fn resolve_prefix<'a>(
    prefixes: &'a HashMap<&'a str, &'a str>,
    prefix: &'a str,
) -> Result<&'a str, ParseError> {
    prefixes
        .get(prefix)
        .copied()
        .ok_or_else(|| ParseError::UndeclaredPrefix(prefix.to_string()))
}

/// Expand a prefixed name.
fn resolve_prefixed_name(
    prefixes: &HashMap<&str, &str>,
    name: sparql::Name,
) -> Result<String, ParseError> {
    match name {
        sparql::Name::IriReference(iri) => Ok(iri.to_string()),
        sparql::Name::PrefixedName { prefix, local } => {
            resolve_prefix(prefixes, prefix).map(|iri| format!("{iri}{local}"))
        }
        sparql::Name::BlankNode(label) => Ok(format!("_:{label}")),
    }
}

/// Resolve prefixes in a [`turtle::RdfLiteral`].
#[must_use]
fn resolve_prefixed_rdf_literal(
    prefixes: &HashMap<&str, &str>,
    literal: turtle::RdfLiteral,
) -> RdfLiteral {
    match literal {
        turtle::RdfLiteral::LanguageString { value, tag } => RdfLiteral::LanguageString {
            value: value.to_string(),
            tag: tag.to_string(),
        },
        turtle::RdfLiteral::DatatypeValue { value, datatype } => RdfLiteral::DatatypeValue {
            value: value.to_string(),
            datatype: resolve_prefixed_name(prefixes, datatype)
                .expect("prefix should have been registered during parsing"),
        },
    }
}

#[traced("parser")]
fn parse_bare_name(input: Span<'_>) -> IntermediateResult<Span<'_>> {
    map_error(
        recognize(pair(
            alpha1,
            opt(preceded(
                many0(tag(" ")),
                separated_list1(
                    many1(tag(" ")),
                    many1(satisfy(|c| {
                        ['0'..='9', 'a'..='z', 'A'..='Z', '-'..='-', '_'..='_']
                            .iter()
                            .any(|range| range.contains(&c))
                    })),
                ),
            )),
        )),
        || ParseError::ExpectedBareName,
    )(input)
}

/// Parse an IRI representing a constant.
fn parse_iri_constant<'a>(
    prefixes: &'a RefCell<HashMap<&'a str, &'a str>>,
) -> impl FnMut(Span<'a>) -> IntermediateResult<'a, Identifier> {
    map_error(
        move |input| {
            let (remainder, name) = traced(
                "parse_iri_constant",
                alt((
                    map(sparql::iriref, |name| sparql::Name::IriReference(&name)),
                    sparql::prefixed_name,
                    sparql::blank_node_label,
                    map(parse_bare_name, |name| sparql::Name::IriReference(&name)),
                )),
            )(input)?;

            let resolved = resolve_prefixed_name(&prefixes.borrow(), name)
                .map_err(|e| Err::Failure(e.at(input)))?;

            Ok((remainder, Identifier(resolved)))
        },
        || ParseError::ExpectedIriConstant,
    )
}

/// Parse a ground term.
pub fn parse_ground_term<'a>(
    prefixes: &'a RefCell<HashMap<&'a str, &'a str>>,
) -> impl FnMut(Span<'a>) -> IntermediateResult<'a, Term> {
    traced(
        "parse_ground_term",
        map_error(
            alt((
                map(parse_iri_constant(prefixes), Term::Constant),
                map(turtle::numeric_literal, Term::NumericLiteral),
                map(turtle::rdf_literal, move |literal| {
                    Term::RdfLiteral(resolve_prefixed_rdf_literal(&prefixes.borrow(), literal))
                }),
                map(turtle::string, move |literal| {
                    Term::StringLiteral(literal.to_string())
                }),
            )),
            || ParseError::ExpectedGroundTerm,
        ),
    )
}

/// The main parser. Holds a hash map for
/// prefixes, as well as the base IRI.
#[derive(Debug, Default)]
pub struct RuleParser<'a> {
    /// The base IRI, if set.
    base: RefCell<Option<&'a str>>,
    /// A map from Prefixes to IRIs.
    prefixes: RefCell<HashMap<&'a str, &'a str>>,
    /// The external data sources.
    sources: RefCell<Vec<DataSourceDeclaration>>,
    /// Declarations of predicates with their types.
    predicate_declarations: RefCell<HashMap<Identifier, Vec<LogicalTypeEnum>>>,
}

/// Body may contain literals or filter expressions
#[derive(Debug, Clone)]
pub enum BodyExpression {
    /// Literal
    Literal(Literal),
    /// Filter
    Filter(Filter),
}

impl<'a> RuleParser<'a> {
    /// Construct a new [`RuleParser`].
    pub fn new() -> Self {
        Default::default()
    }

    /// Parse the dot that ends declarations, optionally surrounded by spaces.
    fn parse_dot(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced("parse_dot", space_delimited_token("."))
    }

    /// Parse a comma, optionally surrounded by spaces.
    fn parse_comma(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced("parse_comma", space_delimited_token(","))
    }

    /// Parse a negation sign (`~`), optionally surrounded by spaces.
    fn parse_not(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced("parse_not", space_delimited_token("~"))
    }

    /// Parse an arrow (`:-`), optionally surrounded by spaces.
    fn parse_arrow(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced("parse_arrow", space_delimited_token(":-"))
    }

    /// Parse an opening parenthesis, optionally surrounded by spaces.
    fn parse_open_parenthesis(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced("parse_open_parenthesis", space_delimited_token("("))
    }

    /// Parse a closing parenthesis, optionally surrounded by spaces.
    fn parse_close_parenthesis(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced("parse_close_parenthesis", space_delimited_token(")"))
    }

    /// Parse a base declaration.
    fn parse_base(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Identifier> {
        traced(
            "parse_base",
            map_error(
                move |input| {
                    let (remainder, base) = delimited(
                        terminated(token("@base"), cut(multispace_or_comment1)),
                        cut(sparql::iriref),
                        cut(self.parse_dot()),
                    )(input)?;

                    log::debug!(target: "parser", r#"parse_base: set new base: "{base}""#);
                    *self.base.borrow_mut() = Some(&base);

                    Ok((remainder, Identifier(base.to_string())))
                },
                || ParseError::ExpectedBaseDeclaration,
            ),
        )
    }

    fn parse_prefix(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced(
            "parse_prefix",
            map_error(
                move |input| {
                    let (remainder, (prefix, iri)) = delimited(
                        terminated(token("@prefix"), cut(multispace_or_comment1)),
                        cut(tuple((
                            cut(terminated(sparql::pname_ns, multispace_or_comment1)),
                            cut(sparql::iriref),
                        ))),
                        cut(self.parse_dot()),
                    )(input)?;

                    log::debug!(target: "parser", r#"parse_prefix: got prefix "{prefix}" for iri "{iri}""#);
                    if self.prefixes.borrow_mut().insert(&prefix, &iri).is_some() {
                        Err(Err::Failure(
                            ParseError::RedeclaredPrefix(prefix.to_string()).at(input),
                        ))
                    } else {
                        Ok((remainder, prefix))
                    }
                },
                || ParseError::ExpectedPrefixDeclaration,
            ),
        )
    }

    fn parse_predicate_declaration(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<(Identifier, Vec<LogicalTypeEnum>)> {
        traced(
            "parse_predicate_declaration",
            map_error(
                move |input| {
                    let (remainder, (predicate, types)) = delimited(
                        terminated(token("@declare"), cut(multispace_or_comment1)),
                        cut(pair(
                            self.parse_predicate_name(),
                            delimited(
                                cut(self.parse_open_parenthesis()),
                                cut(separated_list1(self.parse_comma(), self.parse_type_name())),
                                cut(self.parse_close_parenthesis()),
                            ),
                        )),
                        cut(self.parse_dot()),
                    )(input)?;

                    self.predicate_declarations
                        .borrow_mut()
                        .entry(predicate.clone())
                        .or_insert(types.clone());
                    Ok((remainder, (predicate, types)))
                },
                || ParseError::ExpectedPredicateDeclaration,
            ),
        )
    }

    fn parse_type_name(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<LogicalTypeEnum> {
        traced("parse_type_name", move |input| {
            let (remainder, type_name) = map_error(
                map_res(
                    recognize(pair(alpha1, many0(none_of(",)] ")))),
                    |name: Span<'a>| name.parse(),
                    // NOTE: type names may not contain commata but any
                    // other character (they should start with [a-zA-Z]
                    // though)
                ),
                || ParseError::ExpectedLogicalTypeName,
            )(input)?;

            Ok((remainder, type_name))
        })
    }

    /// Parses a data source declaration.
    pub fn parse_source(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<DataSourceDeclaration> {
        traced(
            "parse_source",
            map_error(
                move |input| {
                    let (remainder, (predicate, arity_or_types)) = preceded(
                        terminated(token("@source"), cut(multispace_or_comment1)),
                        cut(self.parse_qualified_predicate_name()),
                    )(input)?;

                    let (remainder, datasource) = cut(delimited(
                        delimited(multispace_or_comment0, token(":"), multispace_or_comment1),
                        alt((
                            map(
                                delimited(
                                    preceded(token("load-csv"), cut(self.parse_open_parenthesis())),
                                    turtle::string,
                                    self.parse_close_parenthesis(),
                                ),
                                |filename| DataSource::csv_file(&filename),
                            ),
                            map(
                                delimited(
                                    preceded(token("load-tsv"), cut(self.parse_open_parenthesis())),
                                    turtle::string,
                                    self.parse_close_parenthesis(),
                                ),
                                |filename| DataSource::tsv_file(&filename),
                            ),
                            map(
                                delimited(
                                    preceded(token("load-rdf"), cut(self.parse_open_parenthesis())),
                                    turtle::string,
                                    self.parse_close_parenthesis(),
                                ),
                                |filename| DataSource::rdf_file(&filename),
                            ),
                            map(
                                delimited(
                                    preceded(token("sparql"), cut(self.parse_open_parenthesis())),
                                    tuple((
                                        self.parse_iri_pred(),
                                        delimited(
                                            self.parse_comma(),
                                            turtle::string,
                                            self.parse_comma(),
                                        ),
                                        turtle::string,
                                    )),
                                    self.parse_close_parenthesis(),
                                ),
                                |(endpoint, projection, query)| {
                                    DataSource::sparql_query(SparqlQuery::new(
                                        endpoint.name(),
                                        projection.to_string(),
                                        query.to_string(),
                                    ))
                                },
                            ),
                        )),
                        cut(self.parse_dot()),
                    ))(remainder)?;

                    let source = DataSourceDeclaration::new_validated(
                        predicate,
                        arity_or_types,
                        datasource.map_err(|e| Err::Failure(e.at(input)))?,
                    )
                    .map_err(|e| Err::Failure(e.at(input)))?;

                    log::trace!("Found external data source {source:?}");
                    self.sources.borrow_mut().push(source.clone());

                    Ok((remainder, source))
                },
                || ParseError::ExpectedDataSourceDeclaration,
            ),
        )
    }

    /// Parses an output directive.
    pub fn parse_output(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<QualifiedPredicateName> {
        traced(
            "parse_output",
            map_error(
                delimited(
                    terminated(token("@output"), cut(multispace_or_comment0)),
                    cut(alt((
                        map_res::<_, _, _, _, Error, _, _>(
                            self.parse_qualified_predicate_name(),
                            |(identifier, arity_or_types)| {
                                Ok(QualifiedPredicateName::with_arity_or_types(
                                    identifier,
                                    arity_or_types,
                                ))
                            },
                        ),
                        map_res::<_, _, _, _, Error, _, _>(
                            self.parse_predicate_name(),
                            |identifier| Ok(identifier.into()),
                        ),
                    ))),
                    cut(self.parse_dot()),
                ),
                || ParseError::ExpectedOutputDeclaration,
            ),
        )
    }

    /// Parses a statement.
    pub fn parse_statement(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Statement> {
        traced(
            "parse_statement",
            map_error(
                alt((
                    map(self.parse_fact(), Statement::Fact),
                    map(self.parse_rule(), Statement::Rule),
                )),
                || ParseError::ExpectedStatement,
            ),
        )
    }

    /// Parse a fact.
    pub fn parse_fact(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Fact> {
        traced(
            "parse_fact",
            map_error(
                move |input| {
                    let (remainder, (predicate, terms)) = terminated(
                        pair(
                            self.parse_predicate_name(),
                            delimited(
                                self.parse_open_parenthesis(),
                                separated_list1(
                                    self.parse_comma(),
                                    parse_ground_term(&self.prefixes),
                                ),
                                self.parse_close_parenthesis(),
                            ),
                        ),
                        self.parse_dot(),
                    )(input)?;

                    let predicate_name = predicate.name();
                    log::trace!(target: "parser", "found fact {predicate_name}({terms:?})");

                    Ok((remainder, Fact(Atom::new(predicate, terms))))
                },
                || ParseError::ExpectedFact,
            ),
        )
    }

    /// Parse an IRI which can be a predicate name.
    pub fn parse_iri_pred(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Identifier> {
        map_error(
            move |input| {
                let (remainder, name) = traced(
                    "parse_iri_pred",
                    alt((
                        map(sparql::iriref, |name| sparql::Name::IriReference(&name)),
                        sparql::prefixed_name,
                        sparql::blank_node_label,
                    )),
                )(input)?;

                Ok((
                    remainder,
                    Identifier(
                        resolve_prefixed_name(&self.prefixes.borrow(), name)
                            .map_err(|e| Err::Failure(e.at(input)))?,
                    ),
                ))
            },
            || ParseError::ExpectedIriPred,
        )
    }

    /// Parse a predicate name.
    pub fn parse_predicate_name(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<Identifier> {
        traced(
            "parse_predicate_name",
            map_error(alt((self.parse_iri_pred(), self.parse_pred_name())), || {
                ParseError::ExpectedPredicateName
            }),
        )
    }

    /// Parse a qualified predicate name – currently, this is a
    /// predicate name together with its arity.
    fn parse_qualified_predicate_name(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<(Identifier, ArityOrTypes)> {
        traced(
            "parse_qualified_predicate_name",
            pair(
                self.parse_predicate_name(),
                preceded(
                    multispace_or_comment0,
                    delimited(
                        token("["),
                        cut(alt((
                            map_res(digit1, |number: Span<'a>| {
                                number.parse::<usize>().map(ArityOrTypes::Arity)
                            }),
                            map(
                                separated_list1(self.parse_comma(), self.parse_type_name()),
                                ArityOrTypes::Types,
                            ),
                        ))),
                        cut(token("]")),
                    ),
                ),
            ),
        )
    }

    /// Parse a PREDNAME, i.e., a predicate name that is not an IRI.
    pub fn parse_pred_name(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Identifier> {
        traced("parse_pred_name", move |input| {
            let (remainder, name) = parse_bare_name(input)?;

            Ok((remainder, Identifier(name.to_string())))
        })
    }

    /// Parse a rule.
    pub fn parse_rule(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Rule> {
        traced(
            "parse_rule",
            map_error(
                move |input| {
                    let (remainder, (head, body)) = pair(
                        terminated(
                            separated_list1(self.parse_comma(), self.parse_atom()),
                            self.parse_arrow(),
                        ),
                        cut(terminated(
                            separated_list1(self.parse_comma(), self.parse_body_expression()),
                            self.parse_dot(),
                        )),
                    )(input)?;

                    log::trace!(target: "parser", r#"found rule "{head:?}" :- "{body:?}""#);

                    let literals = body
                        .iter()
                        .filter_map(|expr| match expr {
                            BodyExpression::Literal(l) => Some(l.clone()),
                            _ => None,
                        })
                        .collect();
                    let filters = body
                        .into_iter()
                        .filter_map(|expr| match expr {
                            BodyExpression::Filter(f) => Some(f),
                            _ => None,
                        })
                        .collect();
                    Ok((
                        remainder,
                        Rule::new_validated(head, literals, filters)
                            .map_err(|e| Err::Failure(e.at(input)))?,
                    ))
                },
                || ParseError::ExpectedRule,
            ),
        )
    }

    /// Parse an atom.
    pub fn parse_atom(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Atom> {
        traced(
            "parse_atom",
            map_error(
                move |input| {
                    let (remainder, predicate) = self.parse_predicate_name()(input)?;
                    let (remainder, terms) = delimited(
                        self.parse_open_parenthesis(),
                        cut(separated_list1(self.parse_comma(), self.parse_term())),
                        cut(self.parse_close_parenthesis()),
                    )(remainder)?;

                    let predicate_name = predicate.name();
                    log::trace!(target: "parser", "found atom {predicate_name}({terms:?})");

                    Ok((remainder, Atom::new(predicate, terms)))
                },
                || ParseError::ExpectedAtom,
            ),
        )
    }

    /// Parse a term.
    pub fn parse_term(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Term> {
        traced(
            "parse_term",
            map_error(
                alt((parse_ground_term(&self.prefixes), self.parse_variable())),
                || ParseError::ExpectedTerm,
            ),
        )
    }

    /// Parse a variable.
    pub fn parse_variable(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Term> {
        traced(
            "parse_variable",
            map_error(
                map(
                    alt((
                        self.parse_universal_variable(),
                        self.parse_existential_variable(),
                    )),
                    Term::Variable,
                ),
                || ParseError::ExpectedVariable,
            ),
        )
    }

    /// Parse a universally quantified variable.
    pub fn parse_universal_variable(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<Variable> {
        traced(
            "parse_universal_variable",
            map_error(
                map(
                    preceded(token("?"), cut(self.parse_variable_name())),
                    Variable::Universal,
                ),
                || ParseError::ExpectedUniversalVariable,
            ),
        )
    }

    /// Parse an existentially quantified variable.
    pub fn parse_existential_variable(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<Variable> {
        traced(
            "parse_existential_variable",
            map_error(
                map(
                    preceded(token("!"), cut(self.parse_variable_name())),
                    Variable::Existential,
                ),
                || ParseError::ExpectedExistentialVariable,
            ),
        )
    }

    /// Parse a variable name.
    pub fn parse_variable_name(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Identifier> {
        traced(
            "parse_variable",
            map_error(
                move |input| {
                    let (remainder, name) = parse_bare_name(input)?;

                    Ok((remainder, Identifier(name.to_string())))
                },
                || ParseError::ExpectedVariableName,
            ),
        )
    }

    /// Parse a literal (i.e., a possibly negated atom).
    pub fn parse_literal(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Literal> {
        traced(
            "parse_literal",
            map_error(
                alt((self.parse_negative_literal(), self.parse_positive_literal())),
                || ParseError::ExpectedLiteral,
            ),
        )
    }

    /// Parse a non-negated literal.
    pub fn parse_positive_literal(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Literal> {
        traced(
            "parse_positive_literal",
            map_error(map(self.parse_atom(), Literal::Positive), || {
                ParseError::ExpectedPositiveLiteral
            }),
        )
    }

    /// Parse a negated literal.
    pub fn parse_negative_literal(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Literal> {
        traced(
            "parse_negative_literal",
            map_error(
                map(
                    preceded(self.parse_not(), cut(self.parse_atom())),
                    Literal::Negative,
                ),
                || ParseError::ExpectedNegativeLiteral,
            ),
        )
    }

    /// Parse operation that is filters a variable
    pub fn parse_filter_operator(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<FilterOperation> {
        traced(
            "parse_filter_operator",
            map_error(
                delimited(
                    multispace_or_comment0,
                    alt((
                        value(FilterOperation::LessThanEq, token("<=")),
                        value(FilterOperation::LessThan, token("<")),
                        value(FilterOperation::Equals, token("=")),
                        value(FilterOperation::GreaterThanEq, token(">=")),
                        value(FilterOperation::GreaterThan, token(">")),
                    )),
                    multispace_or_comment0,
                ),
                || ParseError::ExpectedFilterOperator,
            ),
        )
    }

    /// Parse expression of the form `<variable> <operation> <term>`
    /// or `<term> <operation> <variable>`.
    pub fn parse_filter_expression(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Filter> {
        traced(
            "parse_filter_expression",
            map_error(
                alt((
                    map(
                        tuple((
                            self.parse_universal_variable(),
                            self.parse_filter_operator(),
                            cut(self.parse_term()),
                        )),
                        |(lhs, operation, rhs)| Filter::new(operation, lhs, rhs),
                    ),
                    map(
                        tuple((
                            self.parse_term(),
                            self.parse_filter_operator(),
                            cut(self.parse_universal_variable()),
                        )),
                        |(lhs, operation, rhs)| Filter::flipped(operation, lhs, rhs),
                    ),
                )),
                || ParseError::ExpectedFilterExpression,
            ),
        )
    }

    /// Parse body expression
    pub fn parse_body_expression(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<BodyExpression> {
        traced(
            "parse_body_expression",
            map_error(
                alt((
                    map(self.parse_filter_expression(), BodyExpression::Filter),
                    map(self.parse_literal(), BodyExpression::Literal),
                )),
                || ParseError::ExpectedBodyExpression,
            ),
        )
    }

    /// Parses a program in the rules language.
    pub fn parse_program(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Program> {
        traced("parse_program", move |input| {
            let (remainder, _) = multispace_or_comment0(input)?;
            let (remainder, _) = opt(self.parse_base())(remainder)?;
            let (remainder, _) = many0(self.parse_prefix())(remainder)?;
            let (remainder, _) = many0(self.parse_predicate_declaration())(remainder)?;
            let (remainder, _) = many0(self.parse_source())(remainder)?;
            let (remainder, statements) = many0(self.parse_statement())(remainder)?;
            let (remainder, output_predicates) = many0(self.parse_output())(remainder)?;

            let base = self.base().map(String::from);
            let prefixes = self
                .prefixes
                .borrow()
                .iter()
                .map(|(&prefix, &iri)| (prefix.to_string(), iri.to_string()))
                .collect();
            let mut rules = Vec::new();
            let mut facts = Vec::new();

            statements.iter().for_each(|statement| match statement {
                Statement::Fact(value) => facts.push(value.clone()),
                Statement::Rule(value) => rules.push(value.clone()),
            });

            Ok((
                remainder,
                Program::new(
                    base,
                    prefixes,
                    self.sources.borrow().clone(),
                    rules,
                    facts,
                    self.predicate_declarations.borrow().clone(),
                    output_predicates.into(),
                ),
            ))
        })
    }

    /// Return the declared base, if set, or None.
    #[must_use]
    pub fn base(&self) -> Option<&'a str> {
        *self.base.borrow()
    }

    /// Try to expand an IRI into an absolute IRI.
    #[must_use]
    pub fn absolutize_iri(&self, iri: Span) -> String {
        if iri::is_absolute(iri) {
            iri.to_string()
        } else {
            format!("{}{iri}", self.base().unwrap_or_default())
        }
    }

    /// Try to abbreviate an IRI given declared prefixes and base.
    #[must_use]
    pub fn unresolve_absolute_iri(iri: Span) -> String {
        if iri::is_relative(iri) {
            iri.to_string()
        } else {
            todo!()
        }
    }
}

#[cfg(test)]
mod test {
    use std::assert_matches::assert_matches;

    use test_log::test;

    use nemo_physical::datatypes::Double;

    use super::*;

    macro_rules! assert_parse {
        ($parser:expr, $left:expr, $right:expr $(,) ?) => {
            assert_eq!(
                all_input_consumed($parser)($left).expect(
                    format!("failed to parse `{:?}`\nexpected `{:?}`", $left, $right).as_str()
                ),
                $right
            );
        };
    }

    macro_rules! assert_fails {
        ($parser:expr, $left:expr, $right:pat $(,) ?) => {
            assert_matches!(all_input_consumed($parser)($left), Err($right));
        };
    }

    macro_rules! assert_parse_error {
        ($parser:expr, $left:expr, $right:pat $(,) ?) => {
            assert_fails!($parser, $left, LocatedParseError { source: $right, .. });
        };
    }

    macro_rules! assert_expected_token {
        ($parser:expr, $left:expr, $right:expr $(,) ?) => {
            let _token = String::from($right);
            assert_parse_error!($parser, $left, ParseError::ExpectedToken(_token),);
        };
    }

    #[test]
    fn base_directive() {
        let base = "http://example.org/foo";
        let input = format!("@base <{base}> .");
        let parser = RuleParser::new();
        let b = Identifier(base.to_string());
        assert!(parser.base().is_none());
        assert_parse!(parser.parse_base(), input.as_str(), b);
        assert_eq!(parser.base(), Some(base));
    }

    #[test]
    fn prefix_directive() {
        let prefix = unsafe { Span::new_from_raw_offset(8, 1, "foo", ()) };
        let iri = "http://example.org/foo";
        let input = format!("@prefix {prefix}: <{iri}> .");
        let parser = RuleParser::new();
        assert!(resolve_prefix(&parser.prefixes.borrow(), &prefix).is_err());
        assert_parse!(parser.parse_prefix(), input.as_str(), prefix);
        assert_eq!(
            resolve_prefix(&parser.prefixes.borrow(), &prefix).map_err(|_| ()),
            Ok(iri)
        );
    }

    #[test]
    fn source() {
        let parser = RuleParser::new();
        let file = "drinks.csv";
        let predicate_name = "drink";
        let predicate = Identifier(predicate_name.to_string());
        let default_source = DataSourceDeclaration::new(
            predicate.clone(),
            ArityOrTypes::Arity(1),
            DataSource::csv_file(file).unwrap(),
        );
        let any_and_int_source = DataSourceDeclaration::new(
            predicate,
            ArityOrTypes::Types(vec![LogicalTypeEnum::Any, LogicalTypeEnum::Integer]),
            DataSource::csv_file(file).unwrap(),
        );
        // rulewerk accepts all of these variants
        let input = format!(r#"@source {predicate_name}[1]: load-csv("{file}") ."#);
        assert_parse!(parser.parse_source(), &input, default_source);
        let input = format!(r#"@source {predicate_name}[1] : load-csv("{file}") ."#);
        assert_parse!(parser.parse_source(), &input, default_source);
        let input = format!(r#"@source {predicate_name}[1] : load-csv ( "{file}" ) ."#);
        assert_parse!(parser.parse_source(), &input, default_source);
        let input = format!(r#"@source {predicate_name} [1] : load-csv ( "{file}" ) ."#);
        assert_parse!(parser.parse_source(), &input, default_source);
        let input = format!(r#"@source {predicate_name}[string]: load-csv ( "{file}" ) ."#);
        assert_parse!(parser.parse_source(), &input, default_source);
        let input = format!(r#"@source {predicate_name} [string] : load-csv ( "{file}" ) ."#);
        assert_parse!(parser.parse_source(), &input, default_source);
        let input = format!(r#"@source {predicate_name}[any, integer]: load-csv ( "{file}" ) ."#);
        assert_parse!(parser.parse_source(), &input, any_and_int_source);
        let input =
            format!(r#"@source {predicate_name} [any  ,  integer] : load-csv ( "{file}" ) ."#);
        assert_parse!(parser.parse_source(), &input, any_and_int_source);
    }

    #[test]
    fn fact() {
        let parser = RuleParser::new();
        let predicate = "p";
        let value = "foo";
        let datatype = "bar";
        let p = Identifier(predicate.to_string());
        let v = value.to_string();
        let t = datatype.to_string();
        let fact = format!(r#"{predicate}("{value}"^^<{datatype}>) ."#);

        let expected_fact = Fact(Atom::new(
            p,
            vec![Term::RdfLiteral(RdfLiteral::DatatypeValue {
                value: v,
                datatype: t,
            })],
        ));

        assert_parse!(parser.parse_fact(), &fact, expected_fact,);
    }

    #[test]
    fn fact_namespaced() {
        let parser = RuleParser::new();
        let predicate = "p";
        let name = "foo";
        let prefix = unsafe { Span::new_from_raw_offset(8, 1, "eg", ()) };
        let iri = "http://example.org/foo";
        let prefix_declaration = format!("@prefix {prefix}: <{iri}> .");
        let p = Identifier(predicate.to_string());
        let pn = format!("{prefix}:{name}");
        let v = Identifier(format!("{iri}{name}"));
        let fact = format!(r#"{predicate}({pn}) ."#);

        assert_parse!(parser.parse_prefix(), &prefix_declaration, prefix);

        let expected_fact = Fact(Atom::new(p, vec![Term::Constant(v)]));

        assert_parse!(parser.parse_fact(), &fact, expected_fact,);
    }

    #[test]
    fn fact_bnode() {
        let parser = RuleParser::new();
        let predicate = "p";
        let name = "foo";
        let p = Identifier(predicate.to_string());
        let pn = format!("_:{name}");
        let fact = format!(r#"{predicate}({pn}) ."#);
        let v = Identifier(pn);

        let expected_fact = Fact(Atom::new(p, vec![Term::Constant(v)]));

        assert_parse!(parser.parse_fact(), &fact, expected_fact,);
    }

    #[test]
    fn fact_numbers() {
        let parser = RuleParser::new();
        let predicate = "p";
        let p = Identifier(predicate.to_string());
        let int = 23_i64;
        let dbl = Double::new(42.0).expect("is not NaN");
        let dec = 13.37;
        let fact = format!(r#"{predicate}({int}, {dbl:.1}E0, {dec:.2}) ."#);

        let expected_fact = Fact(Atom::new(
            p,
            vec![
                Term::NumericLiteral(NumericLiteral::Integer(int)),
                Term::NumericLiteral(NumericLiteral::Double(dbl)),
                Term::NumericLiteral(NumericLiteral::Decimal(13, 37)),
            ],
        ));

        assert_parse!(parser.parse_fact(), &fact, expected_fact,);
    }

    #[test]
    fn fact_rdf_literal_xsd_string() {
        let parser = RuleParser::new();

        let prefix = unsafe { Span::new_from_raw_offset(8, 1, "xsd", ()) };
        let iri = "http://www.w3.org/2001/XMLSchema#";
        let prefix_declaration = format!("@prefix {prefix}: <{iri}> .");

        assert_parse!(parser.parse_prefix(), &prefix_declaration, prefix);

        let predicate = "p";
        let value = "my nice string";
        let datatype = "xsd:string";

        let p = Identifier(predicate.to_string());
        let v = value.to_string();
        let fact = format!(r#"{predicate}("{value}"^^{datatype}) ."#);

        let expected_fact = Fact(Atom::new(
            p,
            vec![Term::RdfLiteral(RdfLiteral::DatatypeValue {
                value: v,
                datatype: format!("{iri}string"),
            })],
        ));

        assert_parse!(parser.parse_fact(), &fact, expected_fact,);
    }

    #[test]
    fn fact_string_literal() {
        let parser = RuleParser::new();
        let predicate = "p";
        let value = "my nice string";
        let p = Identifier(predicate.to_string());
        let v = value.to_string();
        let fact = format!(r#"{predicate}("{value}") ."#);

        let expected_fact = Fact(Atom::new(p, vec![Term::StringLiteral(v)]));

        assert_parse!(parser.parse_fact(), &fact, expected_fact,);
    }

    #[test]
    fn fact_abstract() {
        let parser = RuleParser::new();
        let predicate = "p";
        let name = "a";
        let p = Identifier(predicate.to_string());
        let a = Identifier(name.to_string());
        let fact = format!(r#"{predicate}({name}) ."#);

        let expected_fact = Fact(Atom::new(p, vec![Term::Constant(a)]));

        assert_parse!(parser.parse_fact(), &fact, expected_fact,);
    }

    #[test]
    fn fact_comment() {
        let parser = RuleParser::new();
        let predicate = "p";
        let value = "foo";
        let datatype = "bar";
        let p = Identifier(predicate.to_string());
        let v = value.to_string();
        let t = datatype.to_string();
        let fact = format!(
            r#"{predicate}(% comment 1
                 "{value}"^^<{datatype}> % comment 2
                 ) % comment 3
               . % comment 4
               %"#
        );

        let expected_fact = Fact(Atom::new(
            p,
            vec![Term::RdfLiteral(RdfLiteral::DatatypeValue {
                value: v,
                datatype: t,
            })],
        ));

        assert_parse!(parser.parse_fact(), &fact, expected_fact,);
    }

    #[test]
    fn filter() {
        let parser = RuleParser::new();
        let aa = "A";
        let a = Identifier(aa.to_string());
        let bb = "B";
        let b = Identifier(bb.to_string());
        let pp = "P";
        let p = Identifier(pp.to_string());
        let xx = "X";
        let x = Identifier(xx.to_string());
        let yy = "Y";
        let y = Identifier(yy.to_string());
        let zz = "Z";
        let z = Identifier(zz.to_string());

        let rule = format!(
            "{pp}(?{xx}) :- {aa}(?{xx}, ?{yy}), ?{yy} > ?{xx}, {bb}(?{zz}), ?{xx} = 3, ?{zz} < 7, ?{xx} <= ?{zz}, ?{zz} >= ?{yy} ."
        );

        let expected_rule = Rule::new(
            vec![Atom::new(
                p,
                vec![Term::Variable(Variable::Universal(x.clone()))],
            )],
            vec![
                Literal::Positive(Atom::new(
                    a,
                    vec![
                        Term::Variable(Variable::Universal(x.clone())),
                        Term::Variable(Variable::Universal(y.clone())),
                    ],
                )),
                Literal::Positive(Atom::new(
                    b,
                    vec![Term::Variable(Variable::Universal(z.clone()))],
                )),
            ],
            vec![
                Filter::new(
                    FilterOperation::GreaterThan,
                    Variable::Universal(y.clone()),
                    Term::Variable(Variable::Universal(x.clone())),
                ),
                Filter::new(
                    FilterOperation::Equals,
                    Variable::Universal(x.clone()),
                    Term::NumericLiteral(NumericLiteral::Integer(3)),
                ),
                Filter::new(
                    FilterOperation::LessThan,
                    Variable::Universal(z.clone()),
                    Term::NumericLiteral(NumericLiteral::Integer(7)),
                ),
                Filter::new(
                    FilterOperation::LessThanEq,
                    Variable::Universal(x),
                    Term::Variable(Variable::Universal(z.clone())),
                ),
                Filter::new(
                    FilterOperation::GreaterThanEq,
                    Variable::Universal(z),
                    Term::Variable(Variable::Universal(y)),
                ),
            ],
        );

        assert_parse!(parser.parse_rule(), &rule, expected_rule,);
    }

    #[test]
    #[allow(clippy::redundant_clone)]
    fn parse_output() {
        let parser = RuleParser::new();

        let j2 = Identifier("J2".to_string());

        assert_parse!(
            parser.parse_output(),
            "@output J2 .",
            QualifiedPredicateName::new(j2.clone())
        );
        assert_parse!(
            parser.parse_output(),
            "@output J2[3] .",
            QualifiedPredicateName::with_arity_or_types(j2.clone(), ArityOrTypes::Arity(3))
        );
    }

    #[test]
    fn parse_errors() {
        let parser = RuleParser::new();

        assert_expected_token!(parser.parse_dot(), "", ".");
        assert_expected_token!(parser.parse_dot(), ":-", ".");
        assert_expected_token!(parser.parse_comma(), "", ",");
        assert_expected_token!(parser.parse_comma(), ":-", ",");
        assert_expected_token!(parser.parse_not(), "", "~");
        assert_expected_token!(parser.parse_not(), ":-", "~");
        assert_expected_token!(parser.parse_arrow(), "", ":-");
        assert_expected_token!(parser.parse_arrow(), "-:", ":-");
        assert_expected_token!(parser.parse_open_parenthesis(), "", "(");
        assert_expected_token!(parser.parse_open_parenthesis(), "-:", "(");
        assert_expected_token!(parser.parse_close_parenthesis(), "", ")");
        assert_expected_token!(parser.parse_close_parenthesis(), "-:", ")");

        assert_parse_error!(
            parser.parse_base(),
            "@base <example.org .",
            ParseError::ExpectedBaseDeclaration
        );

        assert_parse_error!(
            parser.parse_type_name(),
            "https://example.org/non-existant-type-name",
            ParseError::ExpectedLogicalTypeName
        );

        assert_parse_error!(parser.parse_variable(), "!23", ParseError::ExpectedVariable);
        assert_parse_error!(parser.parse_variable(), "?23", ParseError::ExpectedVariable);
        assert_parse_error!(
            parser.parse_predicate_declaration(),
            "@declare p(InvalidTypeName) .",
            ParseError::ExpectedPredicateDeclaration
        );
        assert_parse_error!(
            parser.parse_program(),
            "@declare p(InvalidTypeName) .",
            ParseError::ExpectedPredicateDeclaration
        );

        assert_parse_error!(
            parser.parse_rule(),
            r#"a(?X, !V) :- b23__#?\(?X, ?Y) ."#,
            ParseError::ExpectedRule,
        );
    }
}
