//! A parser for rulewerk-style rules.

use std::{cell::RefCell, collections::HashMap, fmt::Debug};

use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric0, digit1, multispace0, multispace1},
    combinator::{map, map_res, opt, recognize},
    multi::{many0, separated_list1},
    sequence::{delimited, pair, preceded, terminated, tuple},
};

use crate::{
    logical::model::*,
    physical::dictionary::{Dictionary, PrefixedStringDictionary},
};

mod types;
use types::IntermediateResult;
mod iri;
mod rfc5234;
mod sparql;
mod turtle;
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

/// The main parser. Holds a dictionary for terms and a hash map for
/// prefixes, as well as the base IRI.
#[derive(Debug, Default)]
pub struct RuleParser<'a> {
    /// The [`PrefixedStringDictionary`] mapping term names to their internal handles.
    names: RefCell<PrefixedStringDictionary>,
    /// The base IRI, if set.
    base: RefCell<Option<&'a str>>,
    /// A map from Prefixes to IRIs.
    prefixes: RefCell<HashMap<&'a str, &'a str>>,
    /// The external data sources.
    sources: RefCell<Vec<DataSourceDeclaration>>,
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
    fn parse_dot(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced("parse_dot", delimited(multispace0, tag("."), multispace0))
    }

    /// Parse a comma, optionally surrounded by spaces.
    fn parse_comma(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced("parse_comma", delimited(multispace0, tag(","), multispace0))
    }

    /// Parse a negation sign (`~`), optionally surrounded by spaces.
    fn parse_not(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced("parse_not", delimited(multispace0, tag("~"), multispace0))
    }

    /// Parse an arrow (`:-`), optionally surrounded by spaces.
    fn parse_arrow(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced(
            "parse_arrow",
            delimited(multispace0, tag(":-"), multispace0),
        )
    }

    /// Parse an opening parenthesis, optionally surrounded by spaces.
    fn parse_open_parenthesis(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced(
            "parse_open_parenthesis",
            delimited(multispace0, tag("("), multispace0),
        )
    }

    /// Parse a closing parenthesis, optionally surrounded by spaces.
    fn parse_close_parenthesis(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced(
            "parse_close_parenthesis",
            delimited(multispace0, tag(")"), multispace0),
        )
    }

    /// Parse a base declaration.
    fn parse_base(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'a str> {
        traced("parse_base", move |input| {
            let (remainder, base) = delimited(
                terminated(tag("@base"), multispace1),
                sparql::iriref,
                self.parse_dot(),
            )(input)?;

            log::debug!(target: "parser", r#"parse_base: set new base: "{base}""#);
            *self.base.borrow_mut() = Some(base);

            Ok((remainder, base))
        })
    }

    fn parse_prefix(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'a str> {
        traced("parse_prefix", move |input| {
            let (remainder, (prefix, iri)) = delimited(
                terminated(tag("@prefix"), multispace1),
                tuple((terminated(sparql::pname_ns, multispace1), sparql::iriref)),
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

    /// Parses a data source declaration.
    pub fn parse_source(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<DataSourceDeclaration> {
        traced("parse_source", move |input| {
            let (remainder, (predicate_name, arity)) = preceded(
                terminated(tag("@source"), multispace1),
                pair(
                    self.parse_predicate_name(),
                    delimited(
                        tag("["),
                        map_res(digit1, |number: &str| number.parse::<usize>()),
                        tag("]"),
                    ),
                ),
            )(input)?;

            let predicate = Identifier(self.intern_term(predicate_name.to_owned()));

            let (remainder, datasource) = preceded(
                terminated(tag(":"), multispace1),
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
                                self.parse_iri(),
                                delimited(self.parse_comma(), turtle::string, self.parse_comma()),
                                turtle::string,
                            )),
                            self.parse_close_parenthesis(),
                        ),
                        |(endpoint, projection, query)| {
                            DataSource::sparql_query(SparqlQuery::new(
                                endpoint.to_owned(),
                                projection.to_owned(),
                                query.to_owned(),
                            ))
                        },
                    ),
                )),
            )(remainder)?;

            let source = DataSourceDeclaration::new_validated(predicate, arity, datasource?, self)?;

            log::trace!("Found external data source {source:?}");
            self.sources.borrow_mut().push(source.clone());

            Ok((remainder, source))
        })
    }

    /// Parses a statement.
    pub fn parse_statement(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Statement> {
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
            let (remainder, (predicate_name, terms)) = terminated(
                pair(
                    self.parse_predicate_name(),
                    delimited(
                        tag("("),
                        separated_list1(self.parse_comma(), self.parse_ground_term()),
                        tag(")"),
                    ),
                ),
                self.parse_dot(),
            )(input)?;

            log::trace!(target: "parser", "found fact {predicate_name}({terms:?})");
            let predicate = Identifier(self.intern_term(predicate_name.to_owned()));

            Ok((remainder, Fact(Atom::new(predicate, terms))))
        })
    }

    /// Parse an IRI.
    pub fn parse_iri(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'a str> {
        traced("parse_iri", alt((sparql::iriref, sparql::prefixed_name)))
    }

    /// Parse a predicate name.
    pub fn parse_predicate_name(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'a str> {
        traced(
            "parse_predicate_name",
            alt((self.parse_iri(), self.parse_pred_name())),
        )
    }

    /// Parse a PREDNAME, i.e., a predicate name that is not an IRI.
    pub fn parse_pred_name(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'a str> {
        traced("parse_pred_name", recognize(pair(alpha1, alphanumeric0)))
    }

    /// Parse a ground term.
    pub fn parse_ground_term(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Term> {
        traced(
            "parse_ground_term",
            alt((
                map(self.parse_iri(), |iri| {
                    Term::Constant(Identifier(self.intern_term(iri.to_owned())))
                }),
                map(turtle::numeric_literal, Term::NumericLiteral),
                map(turtle::rdf_literal, move |literal| {
                    Term::RdfLiteral(self.intern_rdf_literal(literal))
                }),
            )),
        )
    }

    /// Parse a rule.
    pub fn parse_rule(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Rule> {
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

            let body_literals = body
                .iter()
                .filter(|e| -> bool { matches!(e, BodyExpression::Literal(_)) })
                .map(|b| {
                    if let BodyExpression::Literal(l) = (*b).clone() {
                        l
                    } else {
                        unreachable!()
                    }
                })
                .collect();
            let body_filters = body
                .iter()
                .filter(|e| -> bool { matches!(e, BodyExpression::Filter(_)) })
                .map(|b| {
                    if let BodyExpression::Filter(f) = b.clone() {
                        f
                    } else {
                        unreachable!()
                    }
                })
                .collect();

            Ok((
                remainder,
                Rule::new_validated(head, body_literals, body_filters, self)?,
            ))
        })
    }

    /// Parse an atom.
    pub fn parse_atom(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Atom> {
        traced("parse_atom", move |input| {
            let (remainder, predicate_name) = self.parse_predicate_name()(input)?;
            let (remainder, terms) = delimited(
                self.parse_open_parenthesis(),
                separated_list1(self.parse_comma(), self.parse_term()),
                self.parse_close_parenthesis(),
            )(remainder)?;

            log::trace!(target: "parser", "found atom {predicate_name}({terms:?})");
            let predicate = Identifier(self.intern_term(predicate_name.to_owned()));

            Ok((remainder, Atom::new(predicate, terms)))
        })
    }

    /// Parse a term.
    pub fn parse_term(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Term> {
        traced(
            "parse_term",
            alt((
                self.parse_ground_term(),
                map(self.parse_variable(), |v| Term::Variable(v)),
            )),
        )
    }

    /// Parse a variable.
    pub fn parse_variable(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Variable> {
        traced(
            "parse_variable",
            alt((
                self.parse_universal_variable(),
                self.parse_existential_variable(),
            )),
        )
    }

    /// Parse a universally quantified variable.
    pub fn parse_universal_variable(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<Variable> {
        traced(
            "parse_universal_variable",
            map(preceded(tag("?"), self.parse_variable_name()), |var| {
                Variable::Universal(var)
            }),
        )
    }

    /// Parse an existentially quantified variable.
    pub fn parse_existential_variable(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<Variable> {
        traced(
            "parse_existential_variable",
            map(preceded(tag("!"), self.parse_variable_name()), |var| {
                Variable::Existential(var)
            }),
        )
    }

    /// Parse a variable name.
    pub fn parse_variable_name(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Identifier> {
        traced("parse_variable", move |input| {
            let (remainder, name) = recognize(pair(alpha1, alphanumeric0))(input)?;

            Ok((remainder, Identifier(self.intern_term(name.to_owned()))))
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

    /// Parse an equal sign (=), optionally surrounded by spaces.
    pub fn parse_equals_sign(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced(
            "parse_equals_sign",
            delimited(multispace0, tag("="), multispace0),
        )
    }

    /// Parse a less than sign (<), optionally surrounded by spaces.
    pub fn parse_lessthan_sign(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced(
            "parse_lessthan_sign",
            delimited(multispace0, tag("<"), multispace0),
        )
    }

    /// Parse a greater than sign (>), optionally surrounded by spaces.
    pub fn parse_greaterthan_sign(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced(
            "parse_greaterthan_sign",
            delimited(multispace0, tag(">"), multispace0),
        )
    }

    /// Parse a less than or equals (<=) sign, optionally surrounded by spaces.
    pub fn parse_lessthaneq_sign(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced(
            "parse_lessthaneq_sign",
            delimited(multispace0, tag("<="), multispace0),
        )
    }

    /// Parse a greater than or equals (>=) sign, optionally surrounded by spaces.
    pub fn parse_greaterthaneq_sign(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<&'_ str> {
        traced(
            "parse_greaterthaneq_sign",
            delimited(multispace0, tag(">="), multispace0),
        )
    }

    /// Parse filter operation equals
    pub fn parse_filter_equals(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<FilterOperation> {
        traced(
            "parse_filter_equals",
            map(self.parse_equals_sign(), |_| FilterOperation::Equals),
        )
    }

    /// Parse filter operation less than
    pub fn parse_filter_lessthan(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<FilterOperation> {
        traced(
            "parse_filter_lessthan",
            map(self.parse_lessthan_sign(), |_| FilterOperation::LessThan),
        )
    }

    /// Parse filter operation greater than
    pub fn parse_filter_greaterthan(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<FilterOperation> {
        traced(
            "parse_filter_greaterthan",
            map(self.parse_greaterthan_sign(), |_| {
                FilterOperation::GreaterThan
            }),
        )
    }

    /// Parse filter operation less than or equals
    pub fn parse_filter_lessthaneq(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<FilterOperation> {
        traced(
            "parse_filter_lessthaneq",
            map(self.parse_lessthaneq_sign(), |_| {
                FilterOperation::LessThanEq
            }),
        )
    }

    /// Parse filter operation greater than or equals
    pub fn parse_filter_greaterthaneq(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<FilterOperation> {
        traced(
            "parse_filter_greaterthaneq",
            map(self.parse_greaterthaneq_sign(), |_| {
                FilterOperation::GreaterThanEq
            }),
        )
    }

    /// Parse operation that is filters a variable
    pub fn parse_filter_operation(
        &'a self,
    ) -> impl FnMut(&'a str) -> IntermediateResult<FilterOperation> {
        traced(
            "parse_filter_operation",
            alt((
                self.parse_filter_lessthan(),
                self.parse_filter_lessthaneq(),
                self.parse_filter_greaterthan(),
                self.parse_filter_greaterthaneq(),
                self.parse_filter_equals(),
            )),
        )
    }

    /// Parse expression of the for <variable> <operation> <term>
    pub fn parse_filter_expression(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Filter> {
        traced(
            "parse_filter_expression",
            map(
                tuple((
                    self.parse_universal_variable(),
                    self.parse_filter_operation(),
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
                map(self.parse_literal(), |l| BodyExpression::Literal(l)),
                map(self.parse_filter_expression(), |f| {
                    BodyExpression::Filter(f)
                }),
            )),
        )
    }

    /// Parses a program in the rules language.
    pub fn parse_program(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Program> {
        traced("parse_program", move |input| {
            let (remainder, _) = opt(self.parse_base())(input)?;
            let (remainder, _) = many0(self.parse_prefix())(remainder)?;
            let (remainder, _) = many0(self.parse_source())(remainder)?;
            let (remainder, statements) = many0(self.parse_statement())(remainder)?;

            let base = self.base().map(|base| self.intern_term(base.to_owned()));
            let prefixes = self
                .prefixes
                .borrow()
                .iter()
                .map(|(&prefix, &iri)| (prefix.to_owned(), self.intern_term(iri.to_owned())))
                .collect();
            let mut rules = Vec::new();
            let mut facts = Vec::new();

            statements.iter().for_each(|statement| match statement {
                Statement::Fact(value) => facts.push(value.clone()),
                Statement::Rule(value) => rules.push(value.clone()),
            });

            Ok((
                remainder,
                Program::new(base, prefixes, self.sources.borrow().clone(), rules, facts),
            ))
        })
    }

    /// Return the declared base, if set, or None.
    #[must_use]
    pub fn base(&self) -> Option<&'a str> {
        *self.base.borrow()
    }

    /// Expand a prefix.
    #[must_use]
    pub fn resolve_prefix(&self, prefix: &str) -> Option<&'a str> {
        self.prefixes.borrow().get(prefix).copied()
    }

    /// Expand a prefixed name.
    #[must_use]
    pub fn resolve_prefixed_name(&self, name: &str) -> Option<String> {
        let (prefix, suffix) = name.split_once(':')?;
        self.resolve_prefix(prefix)
            .map(|iri| format!("{iri}{suffix}"))
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
    pub fn intern_term(&self, term: String) -> usize {
        log::trace!(target: "parser", r#"interning term "{term}""#);
        let result = self.names.borrow_mut().add(term);
        log::trace!(target: "parser", "interned as {result}");
        result
    }

    /// Resolve an interned term.
    #[must_use]
    pub fn resolve_term(&self, term: usize) -> Option<String> {
        self.names.borrow().entry(term)
    }

    /// Intern a [`turtle::RdfLiteral`].
    #[must_use]
    fn intern_rdf_literal(&self, literal: turtle::RdfLiteral) -> RdfLiteral {
        match literal {
            turtle::RdfLiteral::LanguageString { value, tag } => RdfLiteral::LanguageString {
                value: self.intern_term(value.to_owned()),
                tag: self.intern_term(tag.to_owned()),
            },
            turtle::RdfLiteral::DatatypeValue { value, datatype } => RdfLiteral::DatatypeValue {
                value: self.intern_term(value.to_owned()),
                datatype: self.intern_term(datatype.to_owned()),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use nom::combinator::all_consuming;
    use test_log::test;

    use super::*;

    fn all<'a, T>(
        parser: impl FnMut(&'a str) -> IntermediateResult<T>,
    ) -> impl FnMut(&'a str) -> Option<T> {
        let mut p = all_consuming(parser);
        move |input| p(input).map(|(_, result)| result).ok()
    }

    macro_rules! assert_parse {
        ($parser:expr, $left:expr, $right:expr $(,) ?) => {
            assert_eq!(
                all($parser)($left).expect("should not be a parse error"),
                $right
            );
        };
    }

    #[test]
    fn base_directive() {
        let base = "http://example.org/foo";
        let input = format!("@base <{base}> .");
        let parser = RuleParser::new();
        assert!(parser.base().is_none());
        assert_parse!(parser.parse_base(), input.as_str(), base);
        assert_eq!(parser.base(), Some(base));
    }

    #[test]
    fn prefix_directive() {
        let prefix = "foo";
        let iri = "http://example.org/foo";
        let input = format!("@prefix {prefix}: <{iri}> .");
        let parser = RuleParser::new();
        assert!(parser.resolve_prefix(prefix).is_none());
        assert_parse!(parser.parse_prefix(), input.as_str(), prefix);
        assert_eq!(parser.resolve_prefix(prefix), Some(iri));
    }

    #[test]
    fn fact() {
        let parser = RuleParser::new();
        let predicate = "p";
        let value = "foo";
        let datatype = "bar";
        let p = Identifier(parser.intern_term(predicate.to_owned()));
        let v = parser.intern_term(value.to_owned());
        let t = parser.intern_term(datatype.to_owned());
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

    // TODO: <= and >= dont work for some reason
    #[test]
    fn filter() {
        let parser = RuleParser::new();
        let rule_string = "P(?X) :- A(?X, ?Y), ?Y > ?X, B(?Z), ?X = 3, ?Z < 7 .";

        assert_parse!(
            parser.parse_rule(),
            rule_string,
            Rule::new(
                vec![Atom::new(
                    Identifier(1),
                    vec![Term::Variable(Variable::Universal(Identifier(0)))]
                )],
                vec![
                    Literal::Positive(Atom::new(
                        Identifier(3),
                        vec![
                            Term::Variable(Variable::Universal(Identifier(0))),
                            Term::Variable(Variable::Universal(Identifier(2)))
                        ]
                    )),
                    Literal::Positive(Atom::new(
                        Identifier(5),
                        vec![Term::Variable(Variable::Universal(Identifier(4)))]
                    ))
                ],
                vec![
                    Filter::new(
                        FilterOperation::GreaterThan,
                        Variable::Universal(Identifier(2)),
                        Term::Variable(Variable::Universal(Identifier(0))),
                    ),
                    Filter::new(
                        FilterOperation::Equals,
                        Variable::Universal(Identifier(0)),
                        Term::NumericLiteral(NumericLiteral::Integer(3))
                    ),
                    Filter::new(
                        FilterOperation::LessThan,
                        Variable::Universal(Identifier(4)),
                        Term::NumericLiteral(NumericLiteral::Integer(7))
                    )
                ]
            )
        );
    }
}
