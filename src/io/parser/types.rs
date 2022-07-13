use nom::IResult;

/// An intermediate parsing result
pub(super) type IntermediateResult<'a, T> = IResult<&'a str, T>;

/// The result of a parse
pub type ParseResult<'a, T> = Result<T, nom::error::Error<&'a str>>;
