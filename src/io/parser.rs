//! A parser for rulewerk-style rules.

use std::{cell::RefCell, collections::HashMap, fmt::Debug};

use nom::{
    branch::alt,
    bytes::complete::{is_not, tag},
    character::complete::{alpha1, alphanumeric0, digit1, multispace1, satisfy},
    combinator::{all_consuming, map, map_res, opt, recognize, value},
    multi::{many0, many1, separated_list1},
    sequence::{delimited, pair, preceded, terminated, tuple},
};

use crate::{
    logical::model::*, logical::types::LogicalTypeCollection, physical::dictionary::Dictionary,
};

mod types;
use types::IntermediateResult;
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
) -> impl FnMut(&'a str) -> IntermediateResult<T>
where
    T: Debug,
    P: FnMut(&'a str) -> IntermediateResult<T>,
{
    move |input| {
        log::trace!(target: "parser", "{fun}({input:?})");
        let result = parser(input);
        log::trace!(target: "parser", "{fun}({input:?}) -> {result:?}");
        result
    }
}

/// A combinator that makes sure all input has been consumed.
pub fn all_input_consumed<'a, T>(
    parser: impl FnMut(&'a str) -> IntermediateResult<'a, T>,
) -> impl FnMut(&'a str) -> Result<T, crate::error::Error> {
    let mut p = all_consuming(parser);
    move |input| {
        p(input).map(|(_, result)| result).map_err(|e| match e {
            nom::Err::Incomplete(e) => ParseError::MissingInput(match e {
                nom::Needed::Unknown => "expected an unknown amount of further input".to_owned(),
                nom::Needed::Size(size) => format!("expected at least {size} more bytes"),
            })
            .into(),
            nom::Err::Error(e) | nom::Err::Failure(e) => e,
        })
    }
}

/// A combinator that recognises a comment, starting at a `%`
/// character and ending at the end of the line.
pub fn comment(input: &str) -> IntermediateResult<()> {
    value((), pair(tag("%"), is_not("\n\r")))(input)
}

/// A combinator that recognises an arbitrary amount of whitespace and
/// comments.
pub fn multispace_or_comment0(input: &str) -> IntermediateResult<()> {
    value((), many0(alt((value((), multispace1), comment))))(input)
}

/// A combinator that recognises any non-empty amount of whitespace
/// and comments.
pub fn multispace_or_comment1(input: &str) -> IntermediateResult<()> {
    value((), many1(alt((value((), multispace1), comment))))(input)
}

/// The main parser. Holds a dictionary for terms and a hash map for
/// prefixes, as well as the base IRI.
#[derive(Debug)]
pub struct RuleParser<'a, Dict: Dictionary, LogicalTypes: LogicalTypeCollection> {
    /// The [`Dictionary`] mapping names to their internal handles.
    names: RefCell<Dict>,
    /// The base IRI, if set.
    base: RefCell<Option<&'a str>>,
    /// A map from Prefixes to IRIs.
    prefixes: RefCell<HashMap<&'a str, &'a str>>,
    /// The external data sources.
    sources: RefCell<Vec<DataSourceDeclaration>>,
    /// Declarations of predicates with their types.
    pub predicate_declarations: RefCell<HashMap<Identifier, Vec<LogicalTypes>>>,
}

// NOTE: deriving this does not work
// since rust then requires a Default trait bound
// on the generic argument LogicalTypes as well,
// which we do not need or want
impl<'a, Dict: Dictionary, LogicalTypes: LogicalTypeCollection> Default
    for RuleParser<'a, Dict, LogicalTypes>
{
    fn default() -> Self {
        Self {
            names: Default::default(),
            base: Default::default(),
            prefixes: Default::default(),
            sources: Default::default(),
            predicate_declarations: Default::default(),
        }
    }
}

/// Body may contain literals or filter expressions
#[derive(Debug, Clone)]
pub enum BodyExpression {
    /// Literal
    Literal(Literal),
    /// Filter
    Filter(Filter),
}

impl<'a, Dict: Dictionary, LogicalTypes: LogicalTypeCollection> RuleParser<'a, Dict, LogicalTypes> {
    /// Construct a new [`RuleParser`].
    pub fn new() -> Self {
        Default::default()
    }

    /// Return a clone of the internal names dictionary
    pub fn clone_names(&self) -> Dict {
        self.names.replace_with(|dict| dict.clone())
    }

    /// Parse the dot that ends declarations, optionally surrounded by spaces.
    fn parse_dot(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced(
            "parse_dot",
            delimited(multispace_or_comment0, tag("."), multispace_or_comment0),
        )
    }

    /// Parse a comma, optionally surrounded by spaces.
    fn parse_comma(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced(
            "parse_comma",
            delimited(multispace_or_comment0, tag(","), multispace_or_comment0),
        )
    }

    /// Parse a negation sign (`~`), optionally surrounded by spaces.
    fn parse_not(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced(
            "parse_not",
            delimited(multispace_or_comment0, tag("~"), multispace_or_comment0),
        )
    }

    /// Parse an arrow (`:-`), optionally surrounded by spaces.
    fn parse_arrow(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced(
            "parse_arrow",
            delimited(multispace_or_comment0, tag(":-"), multispace_or_comment0),
        )
    }

    /// Parse an opening parenthesis, optionally surrounded by spaces.
    fn parse_open_parenthesis(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced(
            "parse_open_parenthesis",
            delimited(multispace_or_comment0, tag("("), multispace_or_comment0),
        )
    }

    /// Parse a closing parenthesis, optionally surrounded by spaces.
    fn parse_close_parenthesis(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced(
            "parse_close_parenthesis",
            delimited(multispace_or_comment0, tag(")"), multispace_or_comment0),
        )
    }

    /// Parse a base declaration.
    fn parse_base(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Identifier> {
        traced("parse_base", move |input| {
            let (remainder, base) = delimited(
                terminated(tag("@base"), multispace_or_comment1),
                sparql::iriref,
                self.parse_dot(),
            )(input)?;

            log::debug!(target: "parser", r#"parse_base: set new base: "{base}""#);
            *self.base.borrow_mut() = Some(base);

            Ok((remainder, self.intern(base.to_owned())))
        })
    }

    fn parse_prefix(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'a str> {
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
            if self.prefixes.borrow_mut().insert(prefix, iri).is_some() {
                Err(nom::Err::Error(
                    ParseError::RedeclaredPrefix(prefix.to_owned()).into(),
                ))
            } else {
                Ok((remainder, prefix))
            }
        })
    }

    fn parse_predicate_declaration(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<(Identifier, Vec<LogicalTypes>)> {
        traced("parse_predicate_declaration", move |input| {
            let (remainder, (predicate, types)) = delimited(
                terminated(tag("@decl"), multispace1),
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
                .entry(predicate)
                .or_insert(types.clone());
            Ok((remainder, (predicate, types)))
        })
    }

    fn parse_type_name(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<LogicalTypes> {
        traced("parse_type_name", move |input| {
            let (remainder, type_name) =
                (map_res(recognize(pair(alpha1, alphanumeric0)), |name: &str| {
                    name.parse::<LogicalTypes>()
                }))(input)?;

            Ok((remainder, type_name))
        })
    }

    /// Parses a data source declaration.
    pub fn parse_source(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<DataSourceDeclaration> {
        traced("parse_source", move |input| {
            let (remainder, (predicate, arity)) = preceded(
                terminated(tag("@source"), multispace_or_comment1),
                pair(
                    self.parse_predicate_name(),
                    preceded(
                        multispace_or_comment0,
                        delimited(
                            tag("["),
                            map_res(digit1, |number: &str| number.parse::<usize>()),
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
                        DataSource::csv_file,
                    ),
                    map(
                        delimited(
                            preceded(tag("load-rdf"), self.parse_open_parenthesis()),
                            turtle::string,
                            self.parse_close_parenthesis(),
                        ),
                        DataSource::rdf_file,
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
                                self.resolve(&endpoint)
                                    .expect("should have been interned during parsing"),
                                projection.to_owned(),
                                query.to_owned(),
                            ))
                        },
                    ),
                )),
                self.parse_dot(),
            )(remainder)?;

            let source = DataSourceDeclaration::new_validated(predicate, arity, datasource?, self)?;

            log::trace!("Found external data source {source:?}");
            self.sources.borrow_mut().push(source.clone());

            Ok((remainder, source))
        })
    }

    /// Parses a statement.
    pub fn parse_statement(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<Statement<LogicalTypes>> {
        traced(
            "parse_statement",
            alt((
                map(self.parse_fact(), Statement::Fact),
                map(self.parse_rule(), Statement::Rule),
            )),
        )
    }

    /// Parse a fact.
    pub fn parse_fact(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Fact> {
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

            let predicate_name = self
                .resolve(&predicate)
                .expect("should have been interned during parsing");
            log::trace!(target: "parser", "found fact {predicate_name}({terms:?})");

            Ok((remainder, Fact(Atom::new(predicate, terms))))
        })
    }

    /// Parse an IRI which can be a predicate name.
    pub fn parse_iri_pred(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Identifier> {
        move |input| {
            let (remainder, name) = traced(
                "parse_iri_pred",
                alt((
                    map(sparql::iriref, sparql::Name::IriReference),
                    sparql::prefixed_name,
                    sparql::blank_node_label,
                )),
            )(input)?;

            Ok((remainder, self.intern(self.resolve_prefixed_name(name)?)))
        }
    }

    /// Parse an IRI representing a constant.
    pub fn parse_iri_constant(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Identifier> {
        move |input| {
            let (remainder, name) = traced(
                "parse_iri_constant",
                alt((
                    map(sparql::iriref, sparql::Name::IriReference),
                    sparql::prefixed_name,
                    sparql::blank_node_label,
                    map(self.parse_bare_name(), sparql::Name::IriReference),
                )),
            )(input)?;

            Ok((remainder, self.intern(self.resolve_prefixed_name(name)?)))
        }
    }

    /// Parse a predicate name.
    pub fn parse_predicate_name(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Identifier> {
        traced(
            "parse_predicate_name",
            alt((self.parse_iri_pred(), self.parse_pred_name())),
        )
    }

    fn parse_bare_name(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'a str> {
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
    pub fn parse_pred_name(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Identifier> {
        traced("parse_pred_name", move |input| {
            let (remainder, name) = self.parse_bare_name()(input)?;

            Ok((remainder, self.intern(name.to_owned())))
        })
    }

    /// Parse a ground term.
    pub fn parse_ground_term(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Term> {
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
    pub fn parse_rule(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Rule<LogicalTypes>> {
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
                .iter()
                .filter_map(|expr| match expr {
                    BodyExpression::Filter(f) => Some(*f),
                    _ => None,
                })
                .collect();
            Ok((
                remainder,
                Rule::new_validated(head, literals, filters, self)?,
            ))
        })
    }

    /// Parse an atom.
    pub fn parse_atom(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Atom> {
        traced("parse_atom", move |input| {
            let (remainder, predicate) = self.parse_predicate_name()(input)?;
            let (remainder, terms) = delimited(
                self.parse_open_parenthesis(),
                separated_list1(self.parse_comma(), self.parse_term()),
                self.parse_close_parenthesis(),
            )(remainder)?;

            let predicate_name = self
                .resolve(&predicate)
                .expect("term should have been interned during parsing");
            log::trace!(target: "parser", "found atom {predicate_name}({terms:?})");

            Ok((remainder, Atom::new(predicate, terms)))
        })
    }

    /// Parse a term.
    pub fn parse_term(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Term> {
        traced(
            "parse_term",
            alt((self.parse_ground_term(), self.parse_variable())),
        )
    }

    /// Parse a variable.
    pub fn parse_variable(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Term> {
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
    ) -> impl FnMut(&'a str) -> IntermediateResult<Variable> {
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
    ) -> impl FnMut(&'a str) -> IntermediateResult<Variable> {
        traced(
            "parse_existential_variable",
            map(
                preceded(tag("!"), self.parse_variable_name()),
                Variable::Existential,
            ),
        )
    }

    /// Parse a variable name.
    pub fn parse_variable_name(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Identifier> {
        traced("parse_variable", move |input| {
            let (remainder, name) = recognize(pair(alpha1, alphanumeric0))(input)?;

            Ok((remainder, self.intern(name.to_owned())))
        })
    }

    /// Parse a literal (i.e., a possibly negated atom).
    pub fn parse_literal(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Literal> {
        traced(
            "parse_literal",
            alt((self.parse_positive_literal(), self.parse_negative_literal())),
        )
    }

    /// Parse a non-negated literal.
    pub fn parse_positive_literal(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Literal> {
        traced(
            "parse_positive_literal",
            map(self.parse_atom(), Literal::Positive),
        )
    }

    /// Parse a negated literal.
    pub fn parse_negative_literal(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Literal> {
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
    ) -> impl FnMut(&'a str) -> IntermediateResult<FilterOperation> {
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

    /// Parse expression of the for <variable> <operation> <term>
    pub fn parse_filter_expression(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Filter> {
        traced(
            "parse_filter_expression",
            map(
                tuple((
                    self.parse_universal_variable(),
                    self.parse_filter_operator(),
                    self.parse_term(),
                )),
                |(left, operation, right)| Filter::new(operation, left, right),
            ),
        )
    }

    /// Parse body expression
    pub fn parse_body_expression(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<BodyExpression> {
        traced(
            "parse_body_expression",
            alt((
                map(self.parse_literal(), BodyExpression::Literal),
                map(self.parse_filter_expression(), BodyExpression::Filter),
            )),
        )
    }

    /// Parses a program in the rules language.
    pub fn parse_program(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<Program<Dict, LogicalTypes>> {
        traced("parse_program", move |input| {
            let (remainder, _) = multispace_or_comment0(input)?;
            let (remainder, _) = opt(self.parse_base())(remainder)?;
            let (remainder, _) = many0(self.parse_prefix())(remainder)?;

            let (remainder, _) = many0(self.parse_predicate_declaration())(remainder)?;

            let (remainder, _) = many0(self.parse_source())(remainder)?;
            let (remainder, statements) = many0(self.parse_statement())(remainder)?;

            let base = self.base().map(|base| self.intern(base.to_owned()).0);
            let prefixes = self
                .prefixes
                .borrow()
                .iter()
                .map(|(&prefix, &iri)| (prefix.to_owned(), self.intern(iri.to_owned()).0))
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
                    self.clone_names(),
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
            .ok_or_else(|| ParseError::UndeclaredPrefix(prefix.to_owned()))
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
    pub fn absolutize_iri(&self, iri: &str) -> String {
        if iri::is_absolute(iri) {
            iri.to_owned()
        } else {
            format!("{}{iri}", self.base().unwrap_or_default())
        }
    }

    /// Try to abbreviate an IRI given declared prefixes and base.
    #[must_use]
    pub fn unresolve_absolute_iri(iri: &str) -> String {
        if iri::is_relative(iri) {
            iri.to_owned()
        } else {
            todo!()
        }
    }

    /// Intern a term.
    #[must_use]
    pub fn intern(&self, term: String) -> Identifier {
        log::trace!(target: "parser", r#"interning term "{term}""#);
        let result = self.names.borrow_mut().add(term);
        log::trace!(target: "parser", "interned as {result}");
        Identifier(result)
    }

    /// Resolve an interned [Identifier].
    #[must_use]
    pub fn resolve(&self, identifier: &Identifier) -> Option<String> {
        self.names.borrow().entry(identifier.0)
    }

    /// Intern a [`turtle::RdfLiteral`].
    #[must_use]
    fn intern_rdf_literal(&self, literal: turtle::RdfLiteral) -> RdfLiteral {
        match literal {
            turtle::RdfLiteral::LanguageString { value, tag } => RdfLiteral::LanguageString {
                value: self.intern(value.to_owned()).0,
                tag: self.intern(tag.to_owned()).0,
            },
            turtle::RdfLiteral::DatatypeValue { value, datatype } => RdfLiteral::DatatypeValue {
                value: self.intern(value.to_owned()).0,
                datatype: self
                    .intern(
                        self.resolve_prefixed_name(datatype)
                            .expect("prefix should have been registered during parsing"),
                    )
                    .0,
            },
        }
    }
}

#[cfg(test)]
mod test {
    use test_log::test;

    use crate::physical::datatypes::Double;

    use super::*;

    type Dict = crate::physical::dictionary::PrefixedStringDictionary;
    type LogicalTypes = crate::logical::types::DefaultLogicalTypeCollection;

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
        let parser = RuleParser::<Dict, LogicalTypes>::new();
        let b = parser.intern(base.to_owned());
        assert!(parser.base().is_none());
        assert_parse!(parser.parse_base(), input.as_str(), b);
        assert_eq!(parser.base(), Some(base));
    }

    #[test]
    fn prefix_directive() {
        let prefix = "foo";
        let iri = "http://example.org/foo";
        let input = format!("@prefix {prefix}: <{iri}> .");
        let parser = RuleParser::<Dict, LogicalTypes>::new();
        assert!(parser.resolve_prefix(prefix).is_err());
        assert_parse!(parser.parse_prefix(), input.as_str(), prefix);
        assert_eq!(parser.resolve_prefix(prefix), Ok(iri));
    }

    #[test]
    fn source() {
        let parser = RuleParser::<Dict, LogicalTypes>::new();
        let file = "drinks.csv";
        let predicate_name = "drink";
        let predicate = parser.intern(predicate_name.to_owned());
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
        let parser = RuleParser::<Dict, LogicalTypes>::new();
        let predicate = "p";
        let value = "foo";
        let datatype = "bar";
        let p = parser.intern(predicate.to_owned());
        let v = parser.intern(value.to_owned()).0;
        let t = parser.intern(format!("<{datatype}>")).0;
        let fact = format!(r#"{predicate}("{value}"^^<{datatype}>) ."#);

        assert_parse!(
            parser.parse_fact(),
            &fact,
            Fact(Atom::new(
                p,
                vec![Term::RdfLiteral(RdfLiteral::DatatypeValue {
                    value: v,
                    datatype: t
                })]
            ))
        );
    }

    #[test]
    fn fact_namespaced() {
        let parser = RuleParser::<Dict, LogicalTypes>::new();
        let predicate = "p";
        let name = "foo";
        let prefix = "eg";
        let iri = "http://example.org/foo";
        let prefix_declaration = format!("@prefix {prefix}: <{iri}> .");
        let p = parser.intern(predicate.to_owned());
        let pn = format!("{prefix}:{name}");
        let v = parser.intern(format!("<{iri}{name}>"));
        let fact = format!(r#"{predicate}({pn}) ."#);

        assert_parse!(parser.parse_prefix(), &prefix_declaration, prefix);

        assert_parse!(
            parser.parse_fact(),
            &fact,
            Fact(Atom::new(p, vec![Term::Constant(v)]))
        );
    }

    #[test]
    fn fact_bnode() {
        let parser = RuleParser::<Dict, LogicalTypes>::new();
        let predicate = "p";
        let name = "foo";
        let p = parser.intern(predicate.to_owned());
        let pn = format!("_:{name}");
        let fact = format!(r#"{predicate}({pn}) ."#);
        let v = parser.intern(pn);

        assert_parse!(
            parser.parse_fact(),
            &fact,
            Fact(Atom::new(p, vec![Term::Constant(v)]))
        );
    }

    #[test]
    fn fact_numbers() {
        let parser = RuleParser::<Dict, LogicalTypes>::new();
        let predicate = "p";
        let p = parser.intern(predicate.to_owned());
        let int = 23_i64;
        let dbl = Double::new(42.0).expect("is not NaN");
        let dec = 13.37;
        let fact = format!(r#"{predicate}({int}, {dbl:.1}E0, {dec:.2}) ."#);

        assert_parse!(
            parser.parse_fact(),
            &fact,
            Fact(Atom::new(
                p,
                vec![
                    Term::NumericLiteral(NumericLiteral::Integer(int)),
                    Term::NumericLiteral(NumericLiteral::Double(dbl)),
                    Term::NumericLiteral(NumericLiteral::Decimal(13, 37)),
                ]
            ))
        );
    }

    #[test]
    fn fact_abstract() {
        let parser = RuleParser::<Dict, LogicalTypes>::new();
        let predicate = "p";
        let name = "a";
        let p = parser.intern(predicate.to_owned());
        let a = parser.intern(format!("<{name}>"));
        let fact = format!(r#"{predicate}({name}) ."#);

        assert_parse!(
            parser.parse_fact(),
            &fact,
            Fact(Atom::new(p, vec![Term::Constant(a)]))
        );
    }

    #[test]
    fn fact_comment() {
        let parser = RuleParser::<Dict, LogicalTypes>::new();
        let predicate = "p";
        let value = "foo";
        let datatype = "bar";
        let p = parser.intern(predicate.to_owned());
        let v = parser.intern(value.to_owned()).0;
        let t = parser.intern(format!("<{datatype}>")).0;
        let fact = format!(
            r#"{predicate}(% comment 1
                 "{value}"^^<{datatype}> % comment 2
                 ) % comment 3
               . % comment 4"#
        );

        assert_parse!(
            parser.parse_fact(),
            &fact,
            Fact(Atom::new(
                p,
                vec![Term::RdfLiteral(RdfLiteral::DatatypeValue {
                    value: v,
                    datatype: t
                })]
            ))
        );
    }

    #[test]
    fn filter() {
        let parser = RuleParser::<Dict, LogicalTypes>::new();
        let aa = "A";
        let a = parser.intern(aa.to_owned());
        let bb = "B";
        let b = parser.intern(bb.to_owned());
        let pp = "P";
        let p = parser.intern(pp.to_owned());
        let xx = "X";
        let x = parser.intern(xx.to_owned());
        let yy = "Y";
        let y = parser.intern(yy.to_owned());
        let zz = "Z";
        let z = parser.intern(zz.to_owned());

        let rule = format!(
            "{pp}(?{xx}) :- {aa}(?{xx}, ?{yy}), ?{yy} > ?{xx}, {bb}(?{zz}), ?{xx} = 3, ?{zz} < 7, ?{xx} <= ?{zz}, ?{zz} >= ?{yy} ."
        );

        let mut var_types = HashMap::new();

        var_types.insert(Variable::Universal(x), LogicalTypes::GenericEverything);
        var_types.insert(Variable::Universal(y), LogicalTypes::GenericEverything);
        var_types.insert(Variable::Universal(z), LogicalTypes::GenericEverything);

        assert_parse!(
            parser.parse_rule(),
            &rule,
            Rule::<LogicalTypes>::new_with_types(
                vec![Atom::new(p, vec![Term::Variable(Variable::Universal(x))])],
                vec![
                    Literal::Positive(Atom::new(
                        a,
                        vec![
                            Term::Variable(Variable::Universal(x)),
                            Term::Variable(Variable::Universal(y))
                        ]
                    )),
                    Literal::Positive(Atom::new(b, vec![Term::Variable(Variable::Universal(z))]))
                ],
                vec![
                    Filter::new(
                        FilterOperation::GreaterThan,
                        Variable::Universal(y),
                        Term::Variable(Variable::Universal(x)),
                    ),
                    Filter::new(
                        FilterOperation::Equals,
                        Variable::Universal(x),
                        Term::NumericLiteral(NumericLiteral::Integer(3))
                    ),
                    Filter::new(
                        FilterOperation::LessThan,
                        Variable::Universal(z),
                        Term::NumericLiteral(NumericLiteral::Integer(7))
                    ),
                    Filter::new(
                        FilterOperation::LessThanEq,
                        Variable::Universal(x),
                        Term::Variable(Variable::Universal(z)),
                    ),
                    Filter::new(
                        FilterOperation::GreaterThanEq,
                        Variable::Universal(z),
                        Term::Variable(Variable::Universal(y)),
                    ),
                ],
                var_types.clone(), // TODO: not sure why I needed to clone here...
            )
        );
    }
}
