//! A parser for rulewerk-style rules.

use std::{cell::RefCell, collections::HashMap, fmt::Debug};

use nom::{
    branch::alt,
    bytes::complete::{is_not, tag},
    character::complete::{alpha1, digit1, multispace1, none_of, satisfy},
    combinator::{all_consuming, map, map_res, opt, recognize, value},
    multi::{many0, many1, separated_list1},
    sequence::{delimited, pair, preceded, terminated, tuple},
};

use crate::logical::{model::*, types::LogicalTypeEnum};

mod types;
use types::{IntermediateResult, Span};
pub(crate) mod iri;
pub(crate) mod rfc5234;
pub(crate) mod sparql;
pub(crate) mod turtle;
pub use types::{ParseError, ParseResult};

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
) -> impl FnMut(&'a str) -> Result<T, crate::error::Error> + 'a {
    let mut p = all_consuming(parser);
    move |input| {
        p(Span::new(input))
            .map(|(_, result)| result)
            .map_err(|e| match e {
                nom::Err::Incomplete(e) => ParseError::MissingInput(match e {
                    nom::Needed::Unknown => {
                        "expected an unknown amount of further input".to_string()
                    }
                    nom::Needed::Size(size) => format!("expected at least {size} more bytes"),
                })
                .into(),
                nom::Err::Error(e) | nom::Err::Failure(e) => e,
            })
    }
}

/// A combinator that recognises a comment, starting at a `%`
/// character and ending at the end of the line.
pub fn comment(input: Span) -> IntermediateResult<()> {
    alt((
        value((), pair(tag("%"), is_not("\n\r"))),
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
        traced(
            "parse_dot",
            delimited(multispace_or_comment0, tag("."), multispace_or_comment0),
        )
    }

    /// Parse a comma, optionally surrounded by spaces.
    fn parse_comma(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced(
            "parse_comma",
            delimited(multispace_or_comment0, tag(","), multispace_or_comment0),
        )
    }

    /// Parse a negation sign (`~`), optionally surrounded by spaces.
    fn parse_not(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced(
            "parse_not",
            delimited(multispace_or_comment0, tag("~"), multispace_or_comment0),
        )
    }

    /// Parse an arrow (`:-`), optionally surrounded by spaces.
    fn parse_arrow(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced(
            "parse_arrow",
            delimited(multispace_or_comment0, tag(":-"), multispace_or_comment0),
        )
    }

    /// Parse an opening parenthesis, optionally surrounded by spaces.
    fn parse_open_parenthesis(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced(
            "parse_open_parenthesis",
            delimited(multispace_or_comment0, tag("("), multispace_or_comment0),
        )
    }

    /// Parse a closing parenthesis, optionally surrounded by spaces.
    fn parse_close_parenthesis(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced(
            "parse_close_parenthesis",
            delimited(multispace_or_comment0, tag(")"), multispace_or_comment0),
        )
    }

    /// Parse a base declaration.
    fn parse_base(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Identifier> {
        traced("parse_base", move |input| {
            let (remainder, base) = delimited(
                terminated(tag("@base"), multispace_or_comment1),
                sparql::iriref,
                self.parse_dot(),
            )(input)?;

            log::debug!(target: "parser", r#"parse_base: set new base: "{base}""#);
            *self.base.borrow_mut() = Some(&base);

            Ok((remainder, Identifier(base.to_string())))
        })
    }

    fn parse_prefix(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced("parse_prefix", move |input| {
            let (remainder, (prefix, iri)) = delimited(
                terminated(tag("@prefix"), multispace_or_comment1),
                tuple((
                    terminated(sparql::pname_ns, multispace_or_comment1),
                    sparql::iriref,
                )),
                self.parse_dot(),
            )(input)?;

            log::debug!(target: "parser", r#"parse_prefix: got prefix "{prefix}" for iri "{iri}""#);
            if self.prefixes.borrow_mut().insert(&prefix, &iri).is_some() {
                Err(nom::Err::Error(
                    ParseError::RedeclaredPrefix(prefix.to_string()).into(),
                ))
            } else {
                Ok((remainder, prefix))
            }
        })
    }

    fn parse_predicate_declaration(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<(Identifier, Vec<LogicalTypeEnum>)> {
        traced("parse_predicate_declaration", move |input| {
            let (remainder, (predicate, types)) = delimited(
                terminated(tag("@declare"), multispace1),
                pair(
                    self.parse_predicate_name(),
                    delimited(
                        self.parse_open_parenthesis(),
                        separated_list1(self.parse_comma(), self.parse_type_name()),
                        self.parse_close_parenthesis(),
                    ),
                ),
                self.parse_dot(),
            )(input)?;

            self.predicate_declarations
                .borrow_mut()
                .entry(predicate.clone())
                .or_insert(types.clone());
            Ok((remainder, (predicate, types)))
        })
    }

    fn parse_type_name(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<LogicalTypeEnum> {
        traced("parse_type_name", move |input| {
            let (remainder, type_name) = (map_res(
                recognize(pair(alpha1, many0(none_of(",)")))),
                |name: Span<'a>| name.parse(),
                // NOTE: type names may not contain commata but any
                // other character (they should start with [a-zA-Z]
                // though)
            ))(input)?;

            Ok((remainder, type_name))
        })
    }

    /// Parses a data source declaration.
    pub fn parse_source(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<DataSourceDeclaration> {
        traced("parse_source", move |input| {
            let (remainder, (predicate, arity)) = preceded(
                terminated(tag("@source"), multispace_or_comment1),
                pair(
                    self.parse_predicate_name(),
                    preceded(
                        multispace_or_comment0,
                        delimited(
                            tag("["),
                            map_res(digit1, |number: Span<'a>| number.parse::<usize>()),
                            tag("]"),
                        ),
                    ),
                ),
            )(input)?;

            let (remainder, datasource) = delimited(
                delimited(multispace_or_comment0, tag(":"), multispace_or_comment1),
                alt((
                    map(
                        delimited(
                            preceded(tag("load-csv"), self.parse_open_parenthesis()),
                            turtle::string,
                            self.parse_close_parenthesis(),
                        ),
                        |filename| DataSource::csv_file(&filename),
                    ),
                    map(
                        delimited(
                            preceded(tag("load-tsv"), self.parse_open_parenthesis()),
                            turtle::string,
                            self.parse_close_parenthesis(),
                        ),
                        |filename| DataSource::tsv_file(&filename),
                    ),
                    map(
                        delimited(
                            preceded(tag("load-rdf"), self.parse_open_parenthesis()),
                            turtle::string,
                            self.parse_close_parenthesis(),
                        ),
                        |filename| DataSource::rdf_file(&filename),
                    ),
                    map(
                        delimited(
                            preceded(tag("sparql"), self.parse_open_parenthesis()),
                            tuple((
                                self.parse_iri_pred(),
                                delimited(self.parse_comma(), turtle::string, self.parse_comma()),
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
                self.parse_dot(),
            )(remainder)?;

            let source = DataSourceDeclaration::new_validated(predicate, arity, datasource?)?;

            log::trace!("Found external data source {source:?}");
            self.sources.borrow_mut().push(source.clone());

            Ok((remainder, source))
        })
    }

    /// Parses a statement.
    pub fn parse_statement(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Statement> {
        traced(
            "parse_statement",
            alt((
                map(self.parse_fact(), Statement::Fact),
                map(self.parse_rule(), Statement::Rule),
            )),
        )
    }

    /// Parse a fact.
    pub fn parse_fact(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Fact> {
        traced("parse_fact", move |input| {
            let (remainder, (predicate, terms)) = terminated(
                pair(
                    self.parse_predicate_name(),
                    delimited(
                        self.parse_open_parenthesis(),
                        separated_list1(self.parse_comma(), self.parse_ground_term()),
                        self.parse_close_parenthesis(),
                    ),
                ),
                self.parse_dot(),
            )(input)?;

            let predicate_name = predicate.name();
            log::trace!(target: "parser", "found fact {predicate_name}({terms:?})");

            Ok((remainder, Fact(Atom::new(predicate, terms))))
        })
    }

    /// Parse an IRI which can be a predicate name.
    pub fn parse_iri_pred(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Identifier> {
        move |input| {
            let (remainder, name) = traced(
                "parse_iri_pred",
                alt((
                    map(sparql::iriref, |name| sparql::Name::IriReference(&name)),
                    sparql::prefixed_name,
                    sparql::blank_node_label,
                )),
            )(input)?;

            Ok((remainder, Identifier(self.resolve_prefixed_name(name)?)))
        }
    }

    /// Parse an IRI representing a constant.
    pub fn parse_iri_constant(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Identifier> {
        move |input| {
            let (remainder, name) = traced(
                "parse_iri_constant",
                alt((
                    map(sparql::iriref, |name| sparql::Name::IriReference(&name)),
                    sparql::prefixed_name,
                    sparql::blank_node_label,
                    map(self.parse_bare_name(), |name| {
                        sparql::Name::IriReference(&name)
                    }),
                )),
            )(input)?;

            Ok((remainder, Identifier(self.resolve_prefixed_name(name)?)))
        }
    }

    /// Parse a predicate name.
    pub fn parse_predicate_name(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<Identifier> {
        traced(
            "parse_predicate_name",
            alt((self.parse_iri_pred(), self.parse_pred_name())),
        )
    }

    fn parse_bare_name(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Span<'a>> {
        traced(
            "parse_bare_name",
            recognize(pair(
                alpha1,
                many0(satisfy(|c| {
                    ['0'..='9', 'a'..='z', 'A'..='Z', '-'..='-', '_'..='_']
                        .iter()
                        .any(|range| range.contains(&c))
                })),
            )),
        )
    }

    /// Parse a PREDNAME, i.e., a predicate name that is not an IRI.
    pub fn parse_pred_name(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Identifier> {
        traced("parse_pred_name", move |input| {
            let (remainder, name) = self.parse_bare_name()(input)?;

            Ok((remainder, Identifier(name.to_string())))
        })
    }

    /// Parse a ground term.
    pub fn parse_ground_term(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Term> {
        traced(
            "parse_ground_term",
            alt((
                map(self.parse_iri_constant(), Term::Constant),
                map(turtle::numeric_literal, Term::NumericLiteral),
                map(turtle::rdf_literal, move |literal| {
                    Term::RdfLiteral(self.intern_rdf_literal(literal))
                }),
            )),
        )
    }

    /// Parse a rule.
    pub fn parse_rule(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Rule> {
        traced("parse_rule", move |input| {
            let (remainder, (head, body)) = pair(
                terminated(
                    separated_list1(self.parse_comma(), self.parse_atom()),
                    self.parse_arrow(),
                ),
                terminated(
                    separated_list1(self.parse_comma(), self.parse_body_expression()),
                    self.parse_dot(),
                ),
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
            Ok((remainder, Rule::new_validated(head, literals, filters)?))
        })
    }

    /// Parse an atom.
    pub fn parse_atom(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Atom> {
        traced("parse_atom", move |input| {
            let (remainder, predicate) = self.parse_predicate_name()(input)?;
            let (remainder, terms) = delimited(
                self.parse_open_parenthesis(),
                separated_list1(self.parse_comma(), self.parse_term()),
                self.parse_close_parenthesis(),
            )(remainder)?;

            let predicate_name = predicate.name();
            log::trace!(target: "parser", "found atom {predicate_name}({terms:?})");

            Ok((remainder, Atom::new(predicate, terms)))
        })
    }

    /// Parse a term.
    pub fn parse_term(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Term> {
        traced(
            "parse_term",
            alt((self.parse_ground_term(), self.parse_variable())),
        )
    }

    /// Parse a variable.
    pub fn parse_variable(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Term> {
        traced(
            "parse_variable",
            map(
                alt((
                    self.parse_universal_variable(),
                    self.parse_existential_variable(),
                )),
                Term::Variable,
            ),
        )
    }

    /// Parse a universally quantified variable.
    pub fn parse_universal_variable(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<Variable> {
        traced(
            "parse_universal_variable",
            map(
                preceded(tag("?"), self.parse_variable_name()),
                Variable::Universal,
            ),
        )
    }

    /// Parse an existentially quantified variable.
    pub fn parse_existential_variable(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<Variable> {
        traced(
            "parse_existential_variable",
            map(
                preceded(tag("!"), self.parse_variable_name()),
                Variable::Existential,
            ),
        )
    }

    /// Parse a variable name.
    pub fn parse_variable_name(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Identifier> {
        traced("parse_variable", move |input| {
            let (remainder, name) = self.parse_bare_name()(input)?;

            Ok((remainder, Identifier(name.to_string())))
        })
    }

    /// Parse a literal (i.e., a possibly negated atom).
    pub fn parse_literal(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Literal> {
        traced(
            "parse_literal",
            alt((self.parse_positive_literal(), self.parse_negative_literal())),
        )
    }

    /// Parse a non-negated literal.
    pub fn parse_positive_literal(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Literal> {
        traced(
            "parse_positive_literal",
            map(self.parse_atom(), Literal::Positive),
        )
    }

    /// Parse a negated literal.
    pub fn parse_negative_literal(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Literal> {
        traced(
            "parse_negative_literal",
            map(
                preceded(self.parse_not(), self.parse_atom()),
                Literal::Negative,
            ),
        )
    }

    /// Parse operation that is filters a variable
    pub fn parse_filter_operator(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<FilterOperation> {
        traced(
            "parse_filter_operator",
            delimited(
                multispace_or_comment0,
                alt((
                    value(FilterOperation::LessThanEq, tag("<=")),
                    value(FilterOperation::LessThan, tag("<")),
                    value(FilterOperation::Equals, tag("=")),
                    value(FilterOperation::GreaterThanEq, tag(">=")),
                    value(FilterOperation::GreaterThan, tag(">")),
                )),
                multispace_or_comment0,
            ),
        )
    }

    /// Parse expression of the form `<variable> <operation> <term>`
    /// or `<term> <operation> <variable>`.
    pub fn parse_filter_expression(&'a self) -> impl FnMut(Span<'a>) -> IntermediateResult<Filter> {
        traced(
            "parse_filter_expression",
            alt((
                map(
                    tuple((
                        self.parse_universal_variable(),
                        self.parse_filter_operator(),
                        self.parse_term(),
                    )),
                    |(lhs, operation, rhs)| Filter::new(operation, lhs, rhs),
                ),
                map(
                    tuple((
                        self.parse_term(),
                        self.parse_filter_operator(),
                        self.parse_universal_variable(),
                    )),
                    |(lhs, operation, rhs)| Filter::flipped(operation, lhs, rhs),
                ),
            )),
        )
    }

    /// Parse body expression
    pub fn parse_body_expression(
        &'a self,
    ) -> impl FnMut(Span<'a>) -> IntermediateResult<BodyExpression> {
        traced(
            "parse_body_expression",
            alt((
                map(self.parse_literal(), BodyExpression::Literal),
                map(self.parse_filter_expression(), BodyExpression::Filter),
            )),
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
                ),
            ))
        })
    }

    /// Return the declared base, if set, or None.
    #[must_use]
    pub fn base(&self) -> Option<&'a str> {
        *self.base.borrow()
    }

    /// Expand a prefix.
    pub fn resolve_prefix(&self, prefix: &str) -> Result<&'a str, ParseError> {
        self.prefixes
            .borrow()
            .get(prefix)
            .copied()
            .ok_or_else(|| ParseError::UndeclaredPrefix(prefix.to_string()))
    }

    /// Expand a prefixed name.
    pub fn resolve_prefixed_name(&self, name: sparql::Name) -> Result<String, ParseError> {
        match name {
            sparql::Name::IriReference(iri) => Ok(format!("<{iri}>")),
            sparql::Name::PrefixedName { prefix, local } => self
                .resolve_prefix(prefix)
                .map(|iri| format!("<{iri}{local}>")),
            sparql::Name::BlankNode(label) => Ok(format!("_:{label}")),
        }
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

    /// Intern a [`turtle::RdfLiteral`].
    #[must_use]
    fn intern_rdf_literal(&self, literal: turtle::RdfLiteral) -> RdfLiteral {
        match literal {
            turtle::RdfLiteral::LanguageString { value, tag } => RdfLiteral::LanguageString {
                value: value.to_string(),
                tag: tag.to_string(),
            },
            turtle::RdfLiteral::DatatypeValue { value, datatype } => RdfLiteral::DatatypeValue {
                value: value.to_string(),
                datatype: self
                    .resolve_prefixed_name(datatype)
                    .expect("prefix should have been registered during parsing"),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use test_log::test;

    use crate::physical::datatypes::Double;

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
        assert!(parser.resolve_prefix(&prefix).is_err());
        assert_parse!(parser.parse_prefix(), input.as_str(), prefix);
        assert_eq!(parser.resolve_prefix(&prefix), Ok(iri));
    }

    #[test]
    fn source() {
        let parser = RuleParser::new();
        let file = "drinks.csv";
        let predicate_name = "drink";
        let predicate = Identifier(predicate_name.to_string());
        let source = DataSourceDeclaration::new(predicate, 1, DataSource::csv_file(file).unwrap());
        // rulewerk accepts all of these variants
        let input = format!(r#"@source {predicate_name}[1]: load-csv("{file}") ."#);
        assert_parse!(parser.parse_source(), &input, source);
        let input = format!(r#"@source {predicate_name}[1] : load-csv("{file}") ."#);
        assert_parse!(parser.parse_source(), &input, source);
        let input = format!(r#"@source {predicate_name}[1] : load-csv ( "{file}" ) ."#);
        assert_parse!(parser.parse_source(), &input, source);
        let input = format!(r#"@source {predicate_name} [1] : load-csv ( "{file}" ) ."#);
        assert_parse!(parser.parse_source(), &input, source);
    }

    #[test]
    fn fact() {
        let parser = RuleParser::new();
        let predicate = "p";
        let value = "foo";
        let datatype = "bar";
        let p = Identifier(predicate.to_string());
        let v = value.to_string();
        let t = format!("<{datatype}>");
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
        let v = Identifier(format!("<{iri}{name}>"));
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
    fn fact_abstract() {
        let parser = RuleParser::new();
        let predicate = "p";
        let name = "a";
        let p = Identifier(predicate.to_string());
        let a = Identifier(format!("<{name}>"));
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
        let t = format!("<{datatype}>");
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
}
