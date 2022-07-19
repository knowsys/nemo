//! A parser for rulewerk-style rules.

use std::{cell::RefCell, collections::HashMap, fmt::Debug};

use nom::{
    bytes::complete::tag,
    character::complete::{multispace0, multispace1},
    sequence::{delimited, terminated, tuple},
};

use crate::{logical::model::*, physical::dictionary::PrefixedStringDictionary};

mod types;
use types::IntermediateResult;
mod iri;
mod rfc5234;
mod sparql;
pub use types::ParseResult;

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
    terms: RefCell<PrefixedStringDictionary>,
    /// The base IRI, if set.
    base: RefCell<Option<&'a str>>,
    /// A map from Prefixes to IRIs.
    prefixes: RefCell<HashMap<&'a str, &'a str>>,
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
            self.prefixes
                .borrow_mut()
                .entry(prefix)
                .and_modify(|entry| {
                    // TODO: should this throw a parse error instead?
                    log::warn!(target: "parser", r#"redefining prefix "{prefix}" from "{entry}" to "{iri}""#);
                    *entry = iri;
                })
                .or_insert(iri);

            Ok((remainder, prefix))
        })
    }

    /// Parses a program in the rules language.
    pub fn parse_program(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Program> {
        |_| todo!()
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
    pub fn unresolve_absolute_iri(iri: &str) -> &str {
        todo!()
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
}
