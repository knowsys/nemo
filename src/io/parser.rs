//! A parser for rulewerk-style rules.

use std::{cell::RefCell, collections::HashMap, fmt::Debug};

use log::trace;

use crate::{logical::model::*, physical::dictionary::PrefixedStringDictionary};

mod types;
use types::IntermediateResult;
mod iri;
mod rfc5234;
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
        log::trace!(target: "parser", "{}({:?})", fun, input);
        let result = parser(input);
        log::trace!(target: "parser", "{}({:?}) -> {:?}", fun, input, result);
        result
    }
}

/// The main parser. Holds a dictionary for [terms] and a hash map for [prefixes].
#[derive(Debug)]
pub struct RuleParser<'a> {
    terms: RefCell<PrefixedStringDictionary>,
    base: RefCell<Option<&'a str>>,
    prefixes: RefCell<HashMap<&'a str, &'a str>>,
}

impl<'a> RuleParser<'a> {
    pub fn parse_program(&'a self) -> impl FnMut(&'a str) -> IntermediateResult<Program> {
        |_| todo!()
    }
}
