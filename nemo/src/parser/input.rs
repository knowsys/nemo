//! This module defines [ParseInput].

use std::str::{CharIndices, Chars};

use nom::{
    error::ErrorKind, AsBytes, IResult, InputIter, InputLength, InputTake, InputTakeAtPosition,
};
use nom_locate::LocatedSpan;

use super::{span::Span, ParserState};

/// Input to a nom parser function
#[derive(Debug, Clone)]
pub struct ParserInput<'a> {
    pub(crate) span: Span<'a>,
    pub(crate) state: ParserState,
}

impl<'a> ParserInput<'a> {
    /// Create a new [ParserInput] from a string slice.
    pub fn new(input: &'a str, state: ParserState) -> Self {
        Self {
            span: Span(LocatedSpan::new(input)),
            state,
        }
    }
}

impl<'a> AsBytes for ParserInput<'a> {
    fn as_bytes(&self) -> &[u8] {
        self.span.0.fragment().as_bytes()
    }
}

impl<'a> nom::Compare<ParserInput<'a>> for ParserInput<'a> {
    fn compare(&self, t: ParserInput) -> nom::CompareResult {
        self.span.0.compare(t.as_bytes())
    }

    fn compare_no_case(&self, t: ParserInput) -> nom::CompareResult {
        self.span.0.compare_no_case(t.as_bytes())
    }
}

impl<'a> nom::Compare<&str> for ParserInput<'a> {
    fn compare(&self, t: &str) -> nom::CompareResult {
        self.span.0.compare(t)
    }

    fn compare_no_case(&self, t: &str) -> nom::CompareResult {
        self.span.0.compare_no_case(t)
    }
}

impl<'a> nom::ExtendInto for ParserInput<'a> {
    type Item = char;

    type Extender = String;

    fn new_builder(&self) -> Self::Extender {
        self.span.0.new_builder()
    }

    fn extend_into(&self, acc: &mut Self::Extender) {
        self.span.0.extend_into(acc)
    }
}

impl<'a> nom::FindSubstring<&str> for ParserInput<'a> {
    fn find_substring(&self, substr: &str) -> Option<usize> {
        self.span.0.find_substring(substr)
    }
}

impl<'a> InputLength for ParserInput<'a> {
    fn input_len(&self) -> usize {
        self.span.0.input_len()
    }
}

impl<'a> InputIter for ParserInput<'a> {
    type Item = char;
    type Iter = CharIndices<'a>;
    type IterElem = Chars<'a>;

    fn iter_indices(&self) -> Self::Iter {
        self.span.0.iter_indices()
    }

    fn iter_elements(&self) -> Self::IterElem {
        self.span.0.iter_elements()
    }

    fn position<P>(&self, predicate: P) -> Option<usize>
    where
        P: Fn(Self::Item) -> bool,
    {
        self.span.0.position(predicate)
    }

    fn slice_index(&self, count: usize) -> Result<usize, nom::Needed> {
        self.span.0.slice_index(count)
    }
}

impl InputTake for ParserInput<'_> {
    fn take(&self, count: usize) -> Self {
        Self {
            span: Span(self.span.0.take(count)),
            state: self.state.clone(),
        }
    }

    fn take_split(&self, count: usize) -> (Self, Self) {
        let (first, second) = self.span.0.take_split(count);
        (
            Self {
                span: Span(first),
                state: self.state.clone(),
            },
            Self {
                span: Span(second),
                state: self.state.clone(),
            },
        )
    }
}

impl InputTakeAtPosition for ParserInput<'_> {
    type Item = char;

    fn split_at_position<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
    ) -> IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        match self.span.0.position(predicate) {
            Some(n) => Ok(self.take_split(n)),
            None => Err(nom::Err::Incomplete(nom::Needed::new(1))),
        }
    }

    fn split_at_position1<P, E: nom::error::ParseError<Self>>(
        &self,
        _predicate: P,
        _e: ErrorKind,
    ) -> IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        // self.input.0.split_at_position1(predicate, e)
        todo!()
    }

    fn split_at_position_complete<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
    ) -> IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        match self.split_at_position(predicate) {
            Err(nom::Err::Incomplete(_)) => Ok(self.take_split(self.input_len())),
            res => res,
        }
    }

    fn split_at_position1_complete<P, E: nom::error::ParseError<Self>>(
        &self,
        predicate: P,
        e: ErrorKind,
    ) -> IResult<Self, Self, E>
    where
        P: Fn(Self::Item) -> bool,
    {
        match self.span.0.fragment().position(predicate) {
            Some(0) => Err(nom::Err::Error(E::from_error_kind(self.clone(), e))),
            Some(n) => Ok(self.take_split(n)),
            None => {
                if self.span.0.fragment().input_len() == 0 {
                    Err(nom::Err::Error(E::from_error_kind(self.clone(), e)))
                } else {
                    Ok(self.take_split(self.input_len()))
                }
            }
        }
    }
}

impl nom::Offset for ParserInput<'_> {
    fn offset(&self, second: &Self) -> usize {
        self.span.0.offset(&second.span.0)
    }
}

impl<R> nom::ParseTo<R> for ParserInput<'_> {
    fn parse_to(&self) -> Option<R> {
        todo!()
    }
}

impl<'a, R> nom::Slice<R> for ParserInput<'a>
where
    &'a str: nom::Slice<R>,
{
    fn slice(&self, range: R) -> Self {
        ParserInput {
            span: Span(self.span.0.slice(range)),
            state: self.state.clone(),
        }
    }
}

impl nom_greedyerror::Position for ParserInput<'_> {
    fn position(&self) -> usize {
        nom_greedyerror::Position::position(&self.span.0)
    }
}

impl std::fmt::Display for ParserInput<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "line {}, column {}",
            self.span.0.location_line(),
            self.span.0.get_utf8_column()
        )
    }
}