//! A parser for rulewerk-style rules.

use std::{cell::RefCell, collections::HashMap, fmt::Debug};

use crate::{
    error::Error,
    io::parser::types::{ArithmeticOperator, BodyExpression},
    model::{rule_model::Constraint, *},
};
use nemo_physical::error::ReadingError;
use nom::{
    branch::alt,
    bytes::complete::{is_not, tag},
    character::complete::{alpha1, digit1, multispace1, none_of, satisfy},
    combinator::{all_consuming, cut, map, map_res, opt, recognize, value},
    multi::{many0, many1, separated_list0, separated_list1},
    sequence::{delimited, pair, preceded, separated_pair, terminated, tuple},
    Err,
};

use macros::traced;

mod types;

use types::{ConstraintOperator, IntermediateResult, Span};
pub(crate) mod iri;
pub(crate) mod rfc5234;
pub(crate) mod sparql;
pub(crate) mod turtle;
pub use types::{span_from_str, LocatedParseError, ParseError, ParseResult};

use super::formats::{
    dsv::DSVFormat,
    rdf::RDFFormat,
    types::{Direction, ExportSpec, FileFormat, FileFormatError, ImportExportSpec, ImportSpec},
};

/// Parse a program in the given `input`-String and return a [`Program`].
///
/// The program will be parsed and checked for unsupported features.
///
/// # Error
/// Returns an appropriate [`Error`] variant on parsing and feature check issues.
pub fn parse_program(input: impl AsRef<str>) -> Result<Program, Error> {
    let program = all_input_consumed(RuleParser::new().parse_program())(input.as_ref())?;
    Ok(program)
}

/// Parse a single fact in the given `input`-String and return a [`Program`].
///
/// The program will be parsed and checked for unsupported features.
///
/// # Error
/// Returns an appropriate [`Error`] variant on parsing and feature check issues.
pub fn parse_fact(mut input: String) -> Result<Fact, Error> {
    input += ".";
    let fact = all_input_consumed(RuleParser::new().parse_fact())(input.as_str())?;
    Ok(fact)
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
pub(crate) fn parse_bare_name(input: Span<'_>) -> IntermediateResult<Span<'_>> {
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

#[traced("parser")]
fn parse_simple_name(input: Span<'_>) -> IntermediateResult<Span<'_>> {
    map_error(
        recognize(pair(
            alpha1,
            opt(preceded(
                many0(tag(" ")),
                separated_list1(
                    many1(tag(" ")),
                    many1(satisfy(|c| {
                        ['0'..='9', 'a'..='z', 'A'..='Z', '_'..='_']
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

fn parse_constant_term<'a>(
    prefixes: &'a RefCell<HashMap<&'a str, &'a str>>,
) -> impl FnMut(Span<'a>) -> IntermediateResult<'a, Constant> {
    traced(
        "parse_constant_term",
        alt((
            map(parse_iri_constant(prefixes), Constant::Abstract),
            map(turtle::numeric_literal, Constant::NumericLiteral),
            map_res(turtle::rdf_literal, move |literal| {
                Constant::try_from(resolve_prefixed_rdf_literal(&prefixes.borrow(), literal))
                    .map_err(ReadingError::from)
            }),
            map(turtle::string, move |literal| {
                Constant::StringLiteral(literal.to_string())
            }),
        )),
    )
}

/// Parse a ground term.
pub fn parse_ground_term<'a>(
    prefixes: &'a RefCell<HashMap<&'a str, &'a str>>,
) -> impl FnMut(Span<'a>) -> IntermediateResult<'a, PrimitiveTerm> {
    traced(
        "parse_ground_term",
        map_error(
            map(parse_constant_term(prefixes), PrimitiveTerm::Constant),
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
    /// Declarations of predicates with their types.
    predicate_declarations: RefCell<HashMap<Identifier, Vec<PrimitiveType>>>,
    /// Number counting up for generating distinct wildcards.
    wildcard_generator: RefCell<usize>,
}

impl<'a> RuleParser<'a> {
    /// Construct a new [`RuleParser`].
    pub fn new() -> Self {
        Default::default()
    }

    fn parse_complex_constant_term(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<'a, Constant> {
        traced(
            "parse_complex_constant_term",
            alt((
                parse_constant_term(&self.prefixes),
                map(self.parse_map_literal(), Constant::MapLiteral),
            )),
        )
    }

    /// Parse the dot that ends declarations, optionally surrounded by spaces.
    fn parse_dot(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced("parse_dot", space_delimited_token("."))
    }

    /// Parse a comma, optionally surrounded by spaces.
    fn parse_comma(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced("parse_comma", space_delimited_token(","))
    }

    /// Parse an equality sign, optionally surrounded by spaces.
    fn parse_equals(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced("parse_equals", space_delimited_token("="))
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

    /// Matches an opening parenthesis,
    /// then gets an object from the parser,
    /// and finally matches an closing parenthesis.
    pub fn parenthesised<'b, O, F>(
        &'a self,
        parser: F,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<O>
    where
        O: Debug + 'a,
        F: FnMut(Span<'a>) -> IntermediateResult<O> + 'a,
    {
        traced(
            "parenthesised",
            map_error(
                delimited(
                    self.parse_open_parenthesis(),
                    parser,
                    self.parse_close_parenthesis(),
                ),
                || ParseError::ExpectedParenthesisedExpression,
            ),
        )
    }

    /// Parse an opening brace, optionally surrounded by spaces.
    fn parse_open_brace(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced("parse_open_brace", space_delimited_token("{"))
    }

    /// Parse a closing brace, optionally surrounded by spaces.
    fn parse_close_brace(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced("parse_close_brace", space_delimited_token("}"))
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
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<(Identifier, Vec<PrimitiveType>)> {
        traced(
            "parse_predicate_declaration",
            map_error(
                move |input| {
                    let (remainder, (predicate, types)) = delimited(
                        terminated(token("@declare"), cut(multispace_or_comment1)),
                        cut(pair(
                            self.parse_iri_like_identifier(),
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

    fn parse_type_name(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<PrimitiveType> {
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

    /// Parse a data source declaration.
    pub fn parse_source(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<ImportSpec> {
        traced(
            "parse_source",
            map_error(
                move |input| {
                    let (remainder, (predicate, tuple_constraint)) = preceded(
                        terminated(token("@source"), cut(multispace_or_comment1)),
                        cut(self.parse_qualified_predicate_name(true)),
                    )(input)?;

                    let (remainder, datasource): (_, Result<_, ParseError>) = cut(delimited(
                        delimited(multispace_or_comment0, token(":"), multispace_or_comment1),
                        alt((
                            map(
                                delimited(
                                    preceded(token("load-csv"), cut(self.parse_open_parenthesis())),
                                    turtle::string,
                                    self.parse_close_parenthesis(),
                                ),
                                |filename| {
                                    Ok(DSVFormat::csv().try_into_import(
                                        filename.to_string(),
                                        predicate.clone(),
                                        tuple_constraint.clone(),
                                    )?)
                                },
                            ),
                            map(
                                delimited(
                                    preceded(token("load-tsv"), cut(self.parse_open_parenthesis())),
                                    turtle::string,
                                    self.parse_close_parenthesis(),
                                ),
                                |filename| {
                                    Ok(DSVFormat::tsv().try_into_import(
                                        filename.to_string(),
                                        predicate.clone(),
                                        tuple_constraint.clone(),
                                    )?)
                                },
                            ),
                            map(
                                delimited(
                                    preceded(token("load-rdf"), cut(self.parse_open_parenthesis())),
                                    turtle::string,
                                    self.parse_close_parenthesis(),
                                ),
                                |filename| {
                                    Ok(RDFFormat::new().try_into_import(
                                        filename.to_string(),
                                        predicate.clone(),
                                        tuple_constraint.clone(),
                                        self.base().map(String::from),
                                    )?)
                                },
                            ),
                            map(
                                delimited(
                                    preceded(token("sparql"), cut(self.parse_open_parenthesis())),
                                    tuple((
                                        self.parse_iri_identifier(),
                                        delimited(
                                            self.parse_comma(),
                                            turtle::string,
                                            self.parse_comma(),
                                        ),
                                        turtle::string,
                                    )),
                                    self.parse_close_parenthesis(),
                                ),
                                |(_endpoint, _projection, _query)| {
                                    Err(ParseError::UnsupportedSparqlSource(predicate.clone().0))
                                },
                            ),
                        )),
                        cut(self.parse_dot()),
                    ))(
                        remainder
                    )?;

                    let spec = datasource.map_err(|e| Err::Failure(e.at(input)))?;

                    Ok((remainder, spec))
                },
                || ParseError::ExpectedDataSourceDeclaration,
            ),
        )
    }

    /// Parse an output directive.
    pub fn parse_output(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<QualifiedPredicateName> {
        traced(
            "parse_output",
            map_error(
                delimited(
                    terminated(token("@output"), cut(multispace_or_comment1)),
                    cut(alt((
                        map_res::<_, _, _, _, Error, _, _>(
                            self.parse_qualified_predicate_name(false),
                            |(identifier, associated_type)| {
                                Ok(QualifiedPredicateName::with_constraint(
                                    identifier,
                                    TypeConstraint::Tuple(associated_type),
                                ))
                            },
                        ),
                        map_res::<_, _, _, _, Error, _, _>(
                            self.parse_iri_like_identifier(),
                            |identifier| Ok(identifier.into()),
                        ),
                    ))),
                    cut(self.parse_dot()),
                ),
                || ParseError::ExpectedOutputDeclaration,
            ),
        )
    }

    /// Parse the [Key] of a [Map], i.e., a predicate name or a string.
    pub fn parse_map_key(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Key> {
        traced(
            "parse_map_key",
            alt((
                map(self.parse_iri_like_identifier(), Key::Identifier),
                map(turtle::string, |s| Key::String(s.to_string())),
            )),
        )
    }

    /// Parse an entry in a [Map], i.e., a [Key]--[Term] pair.
    pub fn parse_map_entry(&'a self, s: Span<'a>) -> IntermediateResult<(Key, Constant)> {
        traced(
            "parse_map_entry",
            separated_pair(
                self.parse_map_key(),
                self.parse_equals(),
                map(self.parse_complex_constant_term(), |term| term),
            ),
        )(s)
    }

    /// Parse an object literal.
    pub fn parse_map_literal(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Map> {
        traced(
            "parse_map_literal",
            delimited(
                self.parse_open_brace(),
                map(
                    separated_list0(self.parse_comma(), |s| self.parse_map_entry(s)),
                    Map::from_iter,
                ),
                self.parse_close_brace(),
            ),
        )
    }

    /// Parse a file format name.
    pub fn parse_file_format(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<FileFormat> {
        traced("parse_file_format", move |input| {
            let (remainder, format) =
                map_res(alpha1, |format: Span<'a>| format.parse::<FileFormat>())(input)?;

            Ok((remainder, format))
        })
    }

    /// Parse an import/export specification for the given
    /// [`Direction`].
    pub fn parse_import_export_spec(
        &'a self,
        direction: Direction,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<ImportExportSpec> {
        traced("parse_import_export_spec", move |input| {
            let (remainder, predicate) = self.parse_qualified_predicate_name(true)(input)?;
            let (remainder, format) = delimited(
                space_delimited_token(":"),
                self.parse_file_format(),
                multispace_or_comment0,
            )(remainder)?;
            let (remainder, (format, attributes, constraint)) =
                map_res(self.parse_map_literal(), |attributes| {
                    let mut meta = format.into_meta();
                    let constraint = meta.validated_and_refined_type_declaration(
                        direction,
                        &attributes,
                        predicate.1.clone(),
                    )?;
                    Ok::<_, FileFormatError>((meta, attributes, constraint))
                })(remainder)?;

            Ok((
                remainder,
                ImportExportSpec {
                    predicate: predicate.0,
                    constraint,
                    format,
                    attributes,
                },
            ))
        })
    }

    /// Parse an import directive.
    pub fn parse_import(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<ImportSpec> {
        traced(
            "parse_import",
            delimited(
                terminated(token("@import"), multispace_or_comment1),
                cut(map(
                    self.parse_import_export_spec(Direction::Reading),
                    ImportSpec::from,
                )),
                cut(self.parse_dot()),
            ),
        )
    }

    /// Parse an export directive.
    pub fn parse_export(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<ExportSpec> {
        traced(
            "parse_export",
            delimited(
                terminated(token("@export"), multispace_or_comment1),
                cut(map(
                    self.parse_import_export_spec(Direction::Writing),
                    ExportSpec::from,
                )),
                cut(self.parse_dot()),
            ),
        )
    }

    /// Parse a statement.
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
                            self.parse_iri_like_identifier(),
                            self.parenthesised(separated_list1(
                                self.parse_comma(),
                                parse_ground_term(&self.prefixes),
                            )),
                        ),
                        self.parse_dot(),
                    )(input)?;

                    let predicate_name = predicate.name();
                    log::trace!(target: "parser", "found fact {predicate_name}({terms:?})");

                    // We do not allow complex term trees in facts for now
                    let terms = terms.into_iter().map(Term::Primitive).collect();

                    Ok((remainder, Fact(Atom::new(predicate, terms))))
                },
                || ParseError::ExpectedFact,
            ),
        )
    }

    /// Parse an IRI identifier, e.g. for predicate names.
    pub fn parse_iri_identifier(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<Identifier> {
        map_error(
            move |input| {
                let (remainder, name) = traced(
                    "parse_iri_identifier",
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
            || ParseError::ExpectedIriIdentifier,
        )
    }

    /// Parse an IRI-like identifier.
    ///
    /// This is being used for:
    /// * predicate names
    /// * built-in functions in term trees
    pub fn parse_iri_like_identifier(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<Identifier> {
        traced(
            "parse_iri_like_identifier",
            map_error(
                alt((
                    self.parse_iri_identifier(),
                    self.parse_bare_iri_like_identifier(),
                )),
                || ParseError::ExpectedIriLikeIdentifier,
            ),
        )
    }

    /// Parse a qualified predicate name – currently, this is a
    /// predicate name together with its arity.
    fn parse_qualified_predicate_name(
        &'a self,
        constraint_is_lower_bound: bool,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<(Identifier, TupleConstraint)> {
        traced(
            "parse_qualified_predicate_name",
            pair(
                self.parse_iri_like_identifier(),
                preceded(
                    multispace_or_comment0,
                    delimited(
                        token("["),
                        cut(alt((
                            map_res(digit1, |number: Span<'a>| {
                                number.parse::<usize>().map(TupleConstraint::from_arity)
                            }),
                            map(
                                separated_list1(self.parse_comma(), self.parse_type_name()),
                                move |type_names| {
                                    if constraint_is_lower_bound {
                                        TupleConstraint::at_least(type_names)
                                    } else {
                                        TupleConstraint::exact(type_names)
                                    }
                                },
                            ),
                        ))),
                        cut(token("]")),
                    ),
                ),
            ),
        )
    }

    /// Parse an IRI-like identifier (e.g. a predicate name) that is not an IRI.
    pub fn parse_bare_iri_like_identifier(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<Identifier> {
        traced("parse_bare_iri_like_identifier", move |input| {
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
                    let constraints = body
                        .into_iter()
                        .filter_map(|expr| match expr {
                            BodyExpression::Constraint(c) => Some(c),
                            _ => None,
                        })
                        .collect();
                    Ok((
                        remainder,
                        Rule::new_validated(head, literals, constraints)
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
                    let (remainder, predicate) = self.parse_iri_like_identifier()(input)?;
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

    /// Parse a [`PrimitiveTerm`].
    pub fn parse_primitive_term(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<PrimitiveTerm> {
        traced(
            "parse_primitive_term",
            map_error(
                alt((parse_ground_term(&self.prefixes), self.parse_variable())),
                || ParseError::ExpectedPrimitiveTerm,
            ),
        )
    }

    /// Parse an aggregate term.
    pub fn parse_aggregate(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Term> {
        traced(
            "parse_aggregate",
            map_error(
                move |input| {
                    let (remainder, _) = nom::character::complete::char('#')(input)?;
                    let (remainder, aggregate_operation_identifier) =
                        self.parse_bare_iri_like_identifier()(remainder)?;
                    let (remainder, variables) = self.parenthesised(separated_list1(
                        self.parse_comma(),
                        self.parse_universal_variable(),
                    ))(remainder)?;

                    if let Some(logical_aggregate_operation) =
                        (&aggregate_operation_identifier).into()
                    {
                        let aggregate = Aggregate {
                            logical_aggregate_operation,
                            terms: variables.into_iter().map(PrimitiveTerm::Variable).collect(),
                        };

                        Ok((remainder, Term::Aggregation(aggregate)))
                    } else {
                        Err(Err::Failure(
                            ParseError::UnknownAggregateOperation(
                                aggregate_operation_identifier.name(),
                            )
                            .at(input),
                        ))
                    }
                },
                || ParseError::ExpectedAggregate,
            ),
        )
    }

    /// Parse a variable.
    pub fn parse_variable(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<PrimitiveTerm> {
        traced(
            "parse_variable",
            map_error(
                map(
                    alt((
                        self.parse_universal_variable(),
                        self.parse_existential_variable(),
                    )),
                    PrimitiveTerm::Variable,
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
                    let (remainder, name) = parse_simple_name(input)?;

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
    pub fn parse_constraint_operator(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<ConstraintOperator> {
        traced(
            "parse_constraint_operator",
            map_error(
                delimited(
                    multispace_or_comment0,
                    alt((
                        value(ConstraintOperator::LessThanEq, token("<=")),
                        value(ConstraintOperator::LessThan, token("<")),
                        value(ConstraintOperator::Equals, token("=")),
                        value(ConstraintOperator::Unequals, token("!=")),
                        value(ConstraintOperator::GreaterThanEq, token(">=")),
                        value(ConstraintOperator::GreaterThan, token(">")),
                    )),
                    multispace_or_comment0,
                ),
                || ParseError::ExpectedFilterOperator,
            ),
        )
    }

    /// Parse a term tree.
    ///
    /// This may consist of:
    /// * A function term
    /// * An arithmetic expression, which handles e.g. precedence of addition over multiplication
    pub fn parse_term(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Term> {
        traced(
            "parse_term",
            map_error(
                move |input| {
                    delimited(
                        multispace_or_comment0,
                        alt((
                            self.parse_arithmetic_expression(),
                            self.parse_parenthesised_term(),
                            self.parse_function_term(),
                            self.parse_aggregate(),
                            self.parse_wildcard(),
                        )),
                        multispace_or_comment0,
                    )(input)
                },
                || ParseError::ExpectedTerm,
            ),
        )
    }

    /// Parse a wildcard variable.
    pub fn parse_wildcard(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Term> {
        traced(
            "parse_wildcard",
            map_res(space_delimited_token("_"), |_| {
                let wildcard = Variable::create_wildcard(*self.wildcard_generator.borrow());
                *self.wildcard_generator.borrow_mut() += 1;
                Ok::<_, ParseError>(Term::Primitive(PrimitiveTerm::Variable(wildcard)))
            }),
        )
    }

    /// Parse a parenthesised term tree.
    pub fn parse_parenthesised_term(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Term> {
        traced(
            "parse_parenthesised_term",
            map_error(self.parenthesised(self.parse_term()), || {
                ParseError::ExpectedParenthesisedTerm
            }),
        )
    }

    /// Parse a function term, possibly with nested term trees.
    pub fn parse_function_term(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Term> {
        traced(
            "parse_function_term",
            map_error(
                move |input| {
                    let (remainder, name) = self.parse_iri_like_identifier()(input)?;

                    if let Ok(op) = UnaryOperation::construct_from_name(&name.0) {
                        let (remainder, subterm) =
                            (self.parenthesised(self.parse_term()))(remainder)?;

                        Ok((remainder, Term::Unary(op, Box::new(subterm))))
                    } else {
                        let (remainder, subterms) = (self.parenthesised(separated_list0(
                            self.parse_comma(),
                            self.parse_term(),
                        )))(remainder)?;

                        Ok((remainder, Term::Function(name, subterms)))
                    }
                },
                || ParseError::ExpectedFunctionTerm,
            ),
        )
    }

    /// Parse an arithmetic expression
    pub fn parse_arithmetic_expression(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<Term> {
        traced(
            "parse_arithmetic_expression",
            map_error(
                move |input| {
                    let (remainder, first) = self.parse_arithmetic_product()(input)?;
                    let (remainder, expressions) = many0(alt((
                        preceded(
                            delimited(multispace_or_comment0, token("+"), multispace_or_comment0),
                            map(self.parse_arithmetic_product(), |term| {
                                (ArithmeticOperator::Addition, term)
                            }),
                        ),
                        preceded(
                            delimited(multispace_or_comment0, token("-"), multispace_or_comment0),
                            map(self.parse_arithmetic_product(), |term| {
                                (ArithmeticOperator::Subtraction, term)
                            }),
                        ),
                    )))(remainder)?;

                    Ok((
                        remainder,
                        Self::fold_arithmetic_expressions(first, expressions),
                    ))
                },
                || ParseError::ExpectedArithmeticExpression,
            ),
        )
    }

    /// Parse an arithmetic product, i.e., an expression involving
    /// only `*` and `/` over subexpressions.
    pub fn parse_arithmetic_product(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Term> {
        traced(
            "parse_arithmetic_product",
            map_error(
                move |input| {
                    let (remainder, first) = self.parse_arithmetic_factor()(input)?;
                    let (remainder, factors) = many0(alt((
                        preceded(
                            delimited(multispace_or_comment0, token("*"), multispace_or_comment0),
                            map(self.parse_arithmetic_factor(), |term| {
                                (ArithmeticOperator::Multiplication, term)
                            }),
                        ),
                        preceded(
                            delimited(multispace_or_comment0, token("/"), multispace_or_comment0),
                            map(self.parse_arithmetic_factor(), |term| {
                                (ArithmeticOperator::Division, term)
                            }),
                        ),
                    )))(remainder)?;

                    Ok((remainder, Self::fold_arithmetic_expressions(first, factors)))
                },
                || ParseError::ExpectedArithmeticProduct,
            ),
        )
    }

    /// Parse an arithmetic factor.
    pub fn parse_arithmetic_factor(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Term> {
        traced(
            "parse_arithmetic_factor",
            map_error(
                alt((
                    self.parse_function_term(),
                    self.parse_aggregate(),
                    map(self.parse_primitive_term(), Term::Primitive),
                    self.parse_parenthesised_term(),
                )),
                || ParseError::ExpectedArithmeticFactor,
            ),
        )
    }

    /// Fold a sequence of ([`ArithmeticOperator`], [`PrimitiveTerm`]) pairs into a single [`Term`].
    fn fold_arithmetic_expressions(
        initial: Term,
        sequence: Vec<(ArithmeticOperator, Term)>,
    ) -> Term {
        sequence.into_iter().fold(initial, |acc, pair| {
            let (operation, expression) = pair;

            use ArithmeticOperator::*;

            let operation = match operation {
                Addition => BinaryOperation::Addition,
                Subtraction => BinaryOperation::Subtraction,
                Multiplication => BinaryOperation::Multiplication,
                Division => BinaryOperation::Division,
            };

            Term::Binary {
                operation,
                lhs: Box::new(acc),
                rhs: Box::new(expression),
            }
        })
    }

    /// Parse expression of the form `<term> <operation> <term>` expressing a constraint.
    pub fn parse_constraint(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Constraint> {
        traced(
            "parse_constraint",
            map_error(
                map(
                    tuple((
                        self.parse_term(),
                        self.parse_constraint_operator(),
                        cut(self.parse_term()),
                    )),
                    |(lhs, operation, rhs)| operation.into_constraint(lhs, rhs),
                ),
                || ParseError::ExpectedConstraint,
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
                    map(self.parse_constraint(), BodyExpression::Constraint),
                    map(self.parse_literal(), BodyExpression::Literal),
                )),
                || ParseError::ExpectedBodyExpression,
            ),
        )
    }

    /// Parse a program in the rules language.
    pub fn parse_program(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Program> {
        fn check_for_invalid_statement<'a, F>(
            parser: &mut F,
            input: Span<'a>,
        ) -> IntermediateResult<'a, ()>
        where
            F: FnMut(Span<'a>) -> IntermediateResult<ParseError>,
        {
            if let Ok((_, e)) = parser(input) {
                return Err(Err::Failure(e.at(input)));
            }

            Ok((input, ()))
        }

        traced("parse_program", move |input| {
            let (remainder, _) = multispace_or_comment0(input)?;
            let (remainder, _) = opt(self.parse_base())(remainder)?;

            check_for_invalid_statement(
                &mut map(self.parse_base(), |_| ParseError::LateBaseDeclaration),
                remainder,
            )?;

            let (remainder, _) = many0(self.parse_prefix())(remainder)?;

            check_for_invalid_statement(
                &mut map(self.parse_base(), |_| ParseError::LateBaseDeclaration),
                remainder,
            )?;
            check_for_invalid_statement(
                &mut map(self.parse_prefix(), |_| ParseError::LatePrefixDeclaration),
                remainder,
            )?;

            let mut statements = Vec::new();
            let mut output_predicates = Vec::new();
            let mut sources = Vec::new();
            let mut imports = Vec::new();
            let mut exports = Vec::new();

            let (remainder, _) = many0(alt((
                map(self.parse_predicate_declaration(), |_| ()),
                map(self.parse_source(), |source| sources.push(source)),
                map(self.parse_import(), |import| imports.push(import)),
                map(self.parse_export(), |export| exports.push(export)),
                map(self.parse_statement(), |statement| {
                    statements.push(statement)
                }),
                map(self.parse_output(), |output_predicate| {
                    output_predicates.push(output_predicate)
                }),
            )))(remainder)?;

            check_for_invalid_statement(
                &mut map(self.parse_base(), |_| ParseError::LateBaseDeclaration),
                remainder,
            )?;
            check_for_invalid_statement(
                &mut map(self.parse_prefix(), |_| ParseError::LatePrefixDeclaration),
                remainder,
            )?;

            let base = self.base().map(String::from);
            let prefixes = self
                .prefixes
                .borrow()
                .iter()
                .map(|(&prefix, &iri)| (prefix.to_string(), iri.to_string()))
                .collect::<Vec<_>>();
            let mut rules = Vec::new();
            let mut facts = Vec::new();

            statements.iter().for_each(|statement| match statement {
                Statement::Fact(value) => facts.push(value.clone()),
                Statement::Rule(value) => rules.push(value.clone()),
            });

            let mut program_builder = Program::builder()
                .prefixes(prefixes)
                .imports(sources)
                .imports(imports)
                .exports(exports)
                .rules(rules)
                .facts(facts)
                .predicate_declarations(self.predicate_declarations.borrow().clone());

            if let Some(base) = base {
                program_builder = program_builder.base(base);
            }

            if !output_predicates.is_empty() {
                program_builder = program_builder.output_predicates(output_predicates);
            }

            Ok((remainder, program_builder.build()))
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

    use crate::{io::formats::dsv::DSVFormat, model::rule_model::Constraint};

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
        ($parser:expr, $left:expr, $right:pat $(,) ?) => {{
            // Store in intermediate variable to prevent from being dropped too early
            let result = all_input_consumed($parser)($left);
            assert_matches!(result, Err($right))
        }};
    }

    macro_rules! assert_parse_error {
        ($parser:expr, $left:expr, $right:pat $(,) ?) => {
            assert_fails!($parser, $left, LocatedParseError { source: $right, .. })
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
    #[cfg_attr(miri, ignore)]
    fn source() {
        let parser = RuleParser::new();
        let file = "drinks.csv";
        let predicate_name = "drink";
        let predicate = Identifier(predicate_name.to_string());
        let default_import = DSVFormat::csv()
            .try_into_import(
                file.to_string(),
                predicate.clone(),
                TupleConstraint::from_arity(1),
            )
            .unwrap();

        let any_and_int_import = DSVFormat::csv()
            .try_into_import(
                file.to_string(),
                predicate.clone(),
                [
                    TypeConstraint::AtLeast(PrimitiveType::Any),
                    TypeConstraint::AtLeast(PrimitiveType::Integer),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();

        let single_string_import = DSVFormat::csv()
            .try_into_import(
                file.to_string(),
                predicate,
                [TypeConstraint::AtLeast(PrimitiveType::String)]
                    .into_iter()
                    .collect(),
            )
            .unwrap();

        // rulewerk accepts all of these variants
        let input = format!(r#"@source {predicate_name}[1]: load-csv("{file}") ."#);
        assert_parse!(parser.parse_source(), &input, default_import);
        let input = format!(r#"@source {predicate_name}[1] : load-csv("{file}") ."#);
        assert_parse!(parser.parse_source(), &input, default_import);
        let input = format!(r#"@source {predicate_name}[1] : load-csv ( "{file}" ) ."#);
        assert_parse!(parser.parse_source(), &input, default_import);
        let input = format!(r#"@source {predicate_name} [1] : load-csv ( "{file}" ) ."#);
        assert_parse!(parser.parse_source(), &input, default_import);
        let input = format!(r#"@source {predicate_name}[string]: load-csv ( "{file}" ) ."#);
        assert_parse!(parser.parse_source(), &input, single_string_import);
        let input = format!(r#"@source {predicate_name} [string] : load-csv ( "{file}" ) ."#);
        assert_parse!(parser.parse_source(), &input, single_string_import);
        let input = format!(r#"@source {predicate_name}[any, integer]: load-csv ( "{file}" ) ."#);
        assert_parse!(parser.parse_source(), &input, any_and_int_import);
        let input =
            format!(r#"@source {predicate_name} [any  ,  integer] : load-csv ( "{file}" ) ."#);
        assert_parse!(parser.parse_source(), &input, any_and_int_import);
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
            vec![Term::Primitive(PrimitiveTerm::Constant(
                Constant::RdfLiteral(RdfLiteral::DatatypeValue {
                    value: v,
                    datatype: t,
                }),
            ))],
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

        let expected_fact = Fact(Atom::new(
            p,
            vec![Term::Primitive(PrimitiveTerm::Constant(
                Constant::Abstract(v),
            ))],
        ));

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

        let expected_fact = Fact(Atom::new(
            p,
            vec![Term::Primitive(PrimitiveTerm::Constant(
                Constant::Abstract(v),
            ))],
        ));

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
                Term::Primitive(PrimitiveTerm::Constant(Constant::NumericLiteral(
                    NumericLiteral::Integer(int),
                ))),
                Term::Primitive(PrimitiveTerm::Constant(Constant::NumericLiteral(
                    NumericLiteral::Double(dbl),
                ))),
                Term::Primitive(PrimitiveTerm::Constant(Constant::NumericLiteral(
                    NumericLiteral::Decimal(13, 37),
                ))),
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
            vec![Term::Primitive(PrimitiveTerm::Constant(
                Constant::StringLiteral(v),
            ))],
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

        let expected_fact = Fact(Atom::new(
            p,
            vec![Term::Primitive(PrimitiveTerm::Constant(
                Constant::StringLiteral(v),
            ))],
        ));

        assert_parse!(parser.parse_fact(), &fact, expected_fact,);
    }

    #[test]
    fn fact_language_string() {
        let parser = RuleParser::new();
        let predicate = "p";
        let v = "Qapla";
        let langtag = "tlh";
        let p = Identifier(predicate.to_string());
        let value = v.to_string();
        let fact = format!(r#"{predicate}("{v}"@{langtag}) ."#);
        let tag = langtag.to_string();

        let expected_fact = Fact(Atom::new(
            p,
            vec![Term::Primitive(PrimitiveTerm::Constant(
                Constant::RdfLiteral(RdfLiteral::LanguageString { value, tag }),
            ))],
        ));

        assert_parse!(parser.parse_fact(), &fact, expected_fact);
    }

    #[test]
    fn fact_abstract() {
        let parser = RuleParser::new();
        let predicate = "p";
        let name = "a";
        let p = Identifier(predicate.to_string());
        let a = Identifier(name.to_string());
        let fact = format!(r#"{predicate}({name}) ."#);

        let expected_fact = Fact(Atom::new(
            p,
            vec![Term::Primitive(PrimitiveTerm::Constant(
                Constant::Abstract(a),
            ))],
        ));

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
            vec![Term::Primitive(PrimitiveTerm::Constant(
                Constant::RdfLiteral(RdfLiteral::DatatypeValue {
                    value: v,
                    datatype: t,
                }),
            ))],
        ));

        assert_parse!(parser.parse_fact(), &fact, expected_fact,);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
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
                vec![Term::Primitive(PrimitiveTerm::Variable(
                    Variable::Universal(x.clone()),
                ))],
            )],
            vec![
                Literal::Positive(Atom::new(
                    a,
                    vec![
                        Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(x.clone()))),
                        Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(y.clone()))),
                    ],
                )),
                Literal::Positive(Atom::new(
                    b,
                    vec![Term::Primitive(PrimitiveTerm::Variable(
                        Variable::Universal(z.clone()),
                    ))],
                )),
            ],
            vec![
                Constraint::GreaterThan(
                    Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(y.clone()))),
                    Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(x.clone()))),
                ),
                Constraint::Equals(
                    Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(x.clone()))),
                    Term::Primitive(PrimitiveTerm::Constant(Constant::NumericLiteral(
                        NumericLiteral::Integer(3),
                    ))),
                ),
                Constraint::LessThan(
                    Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(z.clone()))),
                    Term::Primitive(PrimitiveTerm::Constant(Constant::NumericLiteral(
                        NumericLiteral::Integer(7),
                    ))),
                ),
                Constraint::LessThanEq(
                    Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(x))),
                    Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(z.clone()))),
                ),
                Constraint::GreaterThanEq(
                    Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(z))),
                    Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(y))),
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
            QualifiedPredicateName::with_constraint(
                j2.clone(),
                TypeConstraint::Tuple(TupleConstraint::from_arity(3))
            )
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
            r"a(?X, !V) :- b23__#?\(?X, ?Y) .",
            ParseError::ExpectedRule,
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_arithmetic_expressions() {
        let parser = RuleParser::new();

        let twenty_three = Term::Primitive(PrimitiveTerm::Constant(Constant::NumericLiteral(
            NumericLiteral::Integer(23),
        )));
        let fourty_two = Term::Primitive(PrimitiveTerm::Constant(Constant::NumericLiteral(
            NumericLiteral::Integer(42),
        )));
        let twenty_three_times_fourty_two = Term::Binary {
            operation: BinaryOperation::Multiplication,
            lhs: Box::new(twenty_three.clone()),
            rhs: Box::new(fourty_two),
        };

        assert_parse_error!(
            parser.parse_arithmetic_factor(),
            "",
            ParseError::ExpectedArithmeticFactor
        );
        assert_parse_error!(
            parser.parse_parenthesised_term(),
            "",
            ParseError::ExpectedParenthesisedTerm
        );
        assert_parse_error!(
            parser.parse_arithmetic_product(),
            "",
            ParseError::ExpectedArithmeticProduct
        );
        assert_parse_error!(
            parser.parse_arithmetic_expression(),
            "",
            ParseError::ExpectedArithmeticExpression
        );
        assert_parse!(parser.parse_arithmetic_factor(), "23", twenty_three);
        assert_parse!(parser.parse_arithmetic_expression(), "23", twenty_three);
        assert_parse!(
            parser.parse_arithmetic_product(),
            "23 * 42",
            twenty_three_times_fourty_two
        );
        assert_parse!(
            parser.parse_arithmetic_expression(),
            "23 * 42",
            twenty_three_times_fourty_two
        );

        assert_parse!(
            parser.parse_arithmetic_expression(),
            "23 + 23 * 42 + 42 - (23 * 42)",
            Term::Binary {
                operation: BinaryOperation::Subtraction,
                lhs: Box::new(Term::Binary {
                    operation: BinaryOperation::Addition,
                    lhs: Box::new(Term::Binary {
                        operation: BinaryOperation::Addition,
                        lhs: Box::new(Term::Primitive(PrimitiveTerm::Constant(
                            Constant::NumericLiteral(NumericLiteral::Integer(23),)
                        ))),
                        rhs: Box::new(Term::Binary {
                            operation: BinaryOperation::Multiplication,
                            lhs: Box::new(Term::Primitive(PrimitiveTerm::Constant(
                                Constant::NumericLiteral(NumericLiteral::Integer(23)),
                            ))),
                            rhs: Box::new(Term::Primitive(PrimitiveTerm::Constant(
                                Constant::NumericLiteral(NumericLiteral::Integer(42)),
                            ))),
                        })
                    }),
                    rhs: Box::new(Term::Primitive(PrimitiveTerm::Constant(
                        Constant::NumericLiteral(NumericLiteral::Integer(42),)
                    ))),
                }),
                rhs: Box::new(Term::Binary {
                    operation: BinaryOperation::Multiplication,
                    lhs: Box::new(Term::Primitive(PrimitiveTerm::Constant(
                        Constant::NumericLiteral(NumericLiteral::Integer(23))
                    ))),
                    rhs: Box::new(Term::Primitive(PrimitiveTerm::Constant(
                        Constant::NumericLiteral(NumericLiteral::Integer(42))
                    )))
                })
            }
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn program_statement_order() {
        assert_matches!(
            parse_program(
                r#"@output s .
                   s(?s, ?p, ?o) :- t(?s, ?p, ?o) .
                   @source t[3]: load-rdf("triples.nt") .
                  "#
            ),
            Ok(_)
        );

        let parser = RuleParser::new();
        assert_parse_error!(
            parser.parse_program(),
            "@base <foo> . @base <bar> .",
            ParseError::LateBaseDeclaration
        );

        assert_parse_error!(
            parser.parse_program(),
            "@prefix f: <foo> . @base <bar> .",
            ParseError::LateBaseDeclaration
        );

        assert_parse_error!(
            parser.parse_program(),
            "@output p . @base <bar> .",
            ParseError::LateBaseDeclaration
        );

        assert_parse_error!(
            parser.parse_program(),
            "@output p . @prefix g: <foo> .",
            ParseError::LatePrefixDeclaration
        );
    }
    #[test]
    #[cfg_attr(miri, ignore)]
    fn parse_function_terms() {
        let parser = RuleParser::new();

        let twenty_three = Term::Primitive(PrimitiveTerm::Constant(Constant::NumericLiteral(
            NumericLiteral::Integer(23),
        )));
        let fourty_two = Term::Primitive(PrimitiveTerm::Constant(Constant::NumericLiteral(
            NumericLiteral::Integer(42),
        )));
        let twenty_three_times_fourty_two = Term::Binary {
            operation: BinaryOperation::Multiplication,
            lhs: Box::new(twenty_three.clone()),
            rhs: Box::new(fourty_two.clone()),
        };

        assert_parse_error!(
            parser.parse_function_term(),
            "",
            ParseError::ExpectedFunctionTerm
        );

        let nullary_function = Term::Function(Identifier(String::from("nullary_function")), vec![]);
        assert_parse!(
            parser.parse_function_term(),
            "nullary_function()",
            nullary_function
        );
        assert_parse!(
            parser.parse_function_term(),
            "nullary_function(   )",
            nullary_function
        );
        assert_parse_error!(
            parser.parse_function_term(),
            "nullary_function(  () )",
            ParseError::ExpectedFunctionTerm
        );

        let unary_function = Term::Function(
            Identifier(String::from("unary_function")),
            vec![fourty_two.clone()],
        );
        assert_parse!(
            parser.parse_function_term(),
            "unary_function(42)",
            unary_function
        );
        assert_parse!(
            parser.parse_function_term(),
            "unary_function((42))",
            unary_function
        );
        assert_parse!(
            parser.parse_function_term(),
            "unary_function(( (42 )))",
            unary_function
        );

        let binary_function = Term::Function(
            Identifier(String::from("binary_function")),
            vec![fourty_two.clone(), twenty_three.clone()],
        );
        assert_parse!(
            parser.parse_function_term(),
            "binary_function(42, 23)",
            binary_function
        );

        let function_with_nested_algebraic_expression = Term::Function(
            Identifier(String::from("function")),
            vec![twenty_three_times_fourty_two],
        );
        assert_parse!(
            parser.parse_function_term(),
            "function( 23 *42)",
            function_with_nested_algebraic_expression
        );

        let nested_function = Term::Function(
            Identifier(String::from("nested_function")),
            vec![nullary_function.clone()],
        );

        assert_parse!(
            parser.parse_function_term(),
            "nested_function(nullary_function())",
            nested_function
        );

        let triple_nested_function = Term::Function(
            Identifier(String::from("nested_function")),
            vec![Term::Function(
                Identifier(String::from("nested_function")),
                vec![Term::Function(
                    Identifier(String::from("nested_function")),
                    vec![nullary_function.clone()],
                )],
            )],
        );
        assert_parse!(
            parser.parse_function_term(),
            "nested_function(  nested_function(  (nested_function(nullary_function()) )  ))",
            triple_nested_function
        );
    }

    #[test]
    fn parse_terms() {
        let parser = RuleParser::new();

        assert_parse_error!(parser.parse_term(), "", ParseError::ExpectedTerm);

        assert_parse!(
            parser.parse_term(),
            "constant",
            Term::Primitive(PrimitiveTerm::Constant(Constant::Abstract(Identifier(
                String::from("constant")
            ))))
        );
    }

    #[test]
    fn parse_aggregates() {
        let parser = RuleParser::new();

        assert_parse_error!(parser.parse_aggregate(), "", ParseError::ExpectedAggregate);

        assert_parse!(
            parser.parse_aggregate(),
            "#min(?VARIABLE)",
            Term::Aggregation(Aggregate {
                logical_aggregate_operation: LogicalAggregateOperation::MinNumber,
                terms: vec![PrimitiveTerm::Variable(Variable::Universal(Identifier(
                    String::from("VARIABLE")
                )))]
            })
        );

        assert_parse_error!(
            parser.parse_aggregate(),
            "#test(?VAR1, ?VAR2)",
            ParseError::ExpectedAggregate
        )
    }

    #[test]
    fn parse_unary_function() {
        let parser = RuleParser::new();

        let expression = "Abs(4)";
        let expected_term = Term::Unary(
            UnaryOperation::Abs,
            Box::new(Term::Primitive(PrimitiveTerm::Constant(
                Constant::NumericLiteral(NumericLiteral::Integer(4)),
            ))),
        );

        assert_parse!(parser.parse_arithmetic_factor(), expression, expected_term);
    }

    #[test]
    fn parse_arithmetic_and_functions() {
        let parser = RuleParser::new();

        let expression = "5 * Abs(Sqrt(4) - 3)";

        let expected_term = Term::Binary {
            operation: BinaryOperation::Multiplication,
            lhs: Box::new(Term::Primitive(PrimitiveTerm::Constant(
                Constant::NumericLiteral(NumericLiteral::Integer(5)),
            ))),
            rhs: Box::new(Term::Unary(
                UnaryOperation::Abs,
                Box::new(Term::Binary {
                    operation: BinaryOperation::Subtraction,
                    lhs: Box::new(Term::Unary(
                        UnaryOperation::SquareRoot,
                        Box::new(Term::Primitive(PrimitiveTerm::Constant(
                            Constant::NumericLiteral(NumericLiteral::Integer(4)),
                        ))),
                    )),
                    rhs: Box::new(Term::Primitive(PrimitiveTerm::Constant(
                        Constant::NumericLiteral(NumericLiteral::Integer(3)),
                    ))),
                }),
            )),
        };

        assert_parse!(parser.parse_term(), expression, expected_term);
    }

    #[test]
    fn parse_assignment() {
        let parser = RuleParser::new();

        let expression = "?X = Abs(?Y - 5) * (7 + ?Z)";

        let variable = Term::Primitive(PrimitiveTerm::Variable(Variable::Universal(Identifier(
            "X".to_string(),
        ))));

        let term = Term::Binary {
            operation: BinaryOperation::Multiplication,
            lhs: Box::new(Term::Unary(
                UnaryOperation::Abs,
                Box::new(Term::Binary {
                    operation: BinaryOperation::Subtraction,
                    lhs: Box::new(Term::Primitive(PrimitiveTerm::Variable(
                        Variable::Universal(Identifier::new("Y".to_string())),
                    ))),
                    rhs: Box::new(Term::Primitive(PrimitiveTerm::Constant(
                        Constant::NumericLiteral(NumericLiteral::Integer(5)),
                    ))),
                }),
            )),
            rhs: Box::new(Term::Binary {
                operation: BinaryOperation::Addition,
                lhs: Box::new(Term::Primitive(PrimitiveTerm::Constant(
                    Constant::NumericLiteral(NumericLiteral::Integer(7)),
                ))),
                rhs: Box::new(Term::Primitive(PrimitiveTerm::Variable(
                    Variable::Universal(Identifier::new("Z".to_string())),
                ))),
            }),
        };

        let expected = Constraint::Equals(variable, term);

        assert_parse!(parser.parse_constraint(), expression, expected);
    }

    #[test]
    fn parse_complex_condition() {
        let parser = RuleParser::new();

        let expression = "Abs(?X - ?Y) <= ?Z + Sqrt(?Y)";

        let left_term = Term::Unary(
            UnaryOperation::Abs,
            Box::new(Term::Binary {
                operation: BinaryOperation::Subtraction,
                lhs: Box::new(Term::Primitive(PrimitiveTerm::Variable(
                    Variable::Universal(Identifier(String::from("X"))),
                ))),
                rhs: Box::new(Term::Primitive(PrimitiveTerm::Variable(
                    Variable::Universal(Identifier(String::from("Y"))),
                ))),
            }),
        );

        let right_term = Term::Binary {
            operation: BinaryOperation::Addition,
            lhs: Box::new(Term::Primitive(PrimitiveTerm::Variable(
                Variable::Universal(Identifier(String::from("Z"))),
            ))),
            rhs: Box::new(Term::Unary(
                UnaryOperation::SquareRoot,
                Box::new(Term::Primitive(PrimitiveTerm::Variable(
                    Variable::Universal(Identifier(String::from("Y"))),
                ))),
            )),
        };

        let expected = Constraint::LessThanEq(left_term, right_term);

        assert_parse!(parser.parse_constraint(), expression, expected);
    }

    #[test]
    fn map_literal() {
        let parser = RuleParser::new();
        assert_parse!(
            parser.parse_map_literal(),
            r#"{}"#,
            <Map as Default>::default()
        );

        let ident = "foo";
        let key = Key::from_identifier(Identifier(ident.to_string()));

        assert_parse!(parser.parse_map_key(), ident, key.clone());

        let entry = format!("{ident}=23");
        assert_parse!(
            |s| parser.parse_map_entry(s),
            &entry,
            (
                key.clone(),
                Constant::NumericLiteral(NumericLiteral::Integer(23))
            )
        );

        let pairs = vec![
            (
                Key::from_string("23".to_string()),
                Constant::NumericLiteral(NumericLiteral::Integer(42)),
            ),
            (
                Key::from_identifier(Identifier("foo".to_string())),
                Constant::NumericLiteral(NumericLiteral::Integer(23)),
            ),
        ];

        assert_parse!(
            parser.parse_map_literal(),
            r#"{foo = 23, "23" = 42}"#,
            pairs.clone().into_iter().collect::<Map>()
        );
    }

    #[test]
    fn nested_map_literal() {
        let parser = RuleParser::new();

        let pairs = vec![(
            Key::from_identifier(Identifier("inner".to_string())),
            Constant::MapLiteral(Default::default()),
        )];

        assert_parse!(
            parser.parse_map_literal(),
            r#"{inner = {}}"#,
            pairs.clone().into_iter().collect::<Map>()
        );
    }

    #[test]
    fn qualified_predicate_name() {
        let parser = RuleParser::new();
        let predicate = Identifier("p".to_string());

        assert_parse!(
            parser.parse_qualified_predicate_name(true),
            "p[3]",
            (predicate.clone(), TupleConstraint::from_arity(3))
        );

        assert_parse!(
            parser.parse_qualified_predicate_name(true),
            "p[integer, any]",
            (
                predicate.clone(),
                TupleConstraint::at_least([PrimitiveType::Integer, PrimitiveType::Any])
            )
        );

        assert_parse!(
            parser.parse_qualified_predicate_name(false),
            "p[float64, any]",
            (
                predicate.clone(),
                TupleConstraint::exact([PrimitiveType::Float64, PrimitiveType::Any])
            )
        );

        assert_parse!(
            parser.parse_qualified_predicate_name(false),
            "p[integer, float64]",
            (
                predicate.clone(),
                TupleConstraint::exact([PrimitiveType::Integer, PrimitiveType::Float64])
            )
        );
    }

    #[test]
    fn import_export() {
        let parser = RuleParser::new();

        let direction = Direction::Reading;
        let name = "p".to_string();
        let predicate = Identifier(name.clone());
        let qualified = format!("{name}[integer, float64]");
        let arguments = r#"{delimiter = ";", resource = <http://example.org/test.nt>}"#;
        let spec = format!("{qualified}: dsv{arguments}");
        let directive = format!("@import {spec} .");
        let directive_export = format!("@export {spec} .");
        let attributes = parser.parse_map_literal()(arguments.into()).unwrap().1;

        let types = [PrimitiveType::Integer, PrimitiveType::Float64];
        let constraints = TupleConstraint::at_least(types);

        assert_parse!(
            parser.parse_qualified_predicate_name(false),
            &qualified,
            (predicate.clone(), TupleConstraint::exact(types))
        );

        assert_parse!(
            parser.parse_import_export_spec(direction),
            &spec,
            ImportExportSpec {
                predicate: predicate.clone(),
                constraint: constraints.clone(),
                format: Box::<DSVFormat>::default(),
                attributes: attributes.clone(),
            }
        );

        assert_parse!(
            parser.parse_import(),
            &directive,
            ImportSpec::from(ImportExportSpec {
                predicate: predicate.clone(),
                constraint: constraints.clone(),
                format: Box::<DSVFormat>::default(),
                attributes: attributes.clone()
            })
        );

        assert_parse!(
            parser.parse_export(),
            &directive_export,
            ExportSpec::from(ImportExportSpec {
                predicate: predicate.clone(),
                constraint: constraints.clone(),
                format: Box::<DSVFormat>::default(),
                attributes: attributes.clone()
            })
        );
    }
}
