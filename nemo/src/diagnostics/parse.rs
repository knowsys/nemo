//! The kinds of problems the parser reports.

use nemo_physical::diagnostic_kinds;

diagnostic_kinds! {
    /// A problem in a program's text: the lexer or the parser could not
    /// read what was written.
    pub enum ParseError {
        Expected { expected: String, found: String }
            => EXPECTED = 202, error, "expected {expected}, found {found}",
               explain "The parser found a token that cannot be used at its position, or a \
                        statement of another kind than the one asked for. The message names \
                        what was expected there and what was found, and the label points at \
                        the token found.";
        UnexpectedCharacter { character: char }
            => UNEXPECTED_CHARACTER = 210, error, "unexpected character `{character}`",
               explain "The text contains a character that does not start any token of \
                        the language.";
        LoneCaret
            => LONE_CARET = 211, error, "unexpected `^`",
               explain "A single `^` appears. The datatype of a literal is introduced by \
                        two carets, as in \
                        `\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>`, and a power is \
                        written with the builtin `POW`, as in `POW(?x, 2)`.";
        MarkerWithoutName
            => MARKER_WITHOUT_NAME = 212, error, "expected a name after this character",
               explain "A marker character such as `?`, `!`, `$`, `#`, or `@` must be \
                        directly followed by the name it marks.";
        UnterminatedString
            => UNTERMINATED_STRING = 213, error, "unterminated string literal",
               explain "A string literal is not closed before the end of the text.";
        UnterminatedFormatString
            => UNTERMINATED_FORMAT_STRING = 214, error, "unterminated format string",
               explain "A format string (`f\"…\"`) is not closed before the end of the \
                        text.";
        StrayBrace
            => STRAY_BRACE = 215, error, "unmatched `}}` in format string",
               explain "A closing brace in a format string does not close an embedded \
                        expression. A literal brace is written twice.",
               hint "write `}}` for a literal `}`";
        TooDeep { limit: usize }
            => TOO_DEEP = 216, error, "terms are nested more than {limit} levels deep",
               explain "Terms are nested deeper than the parser allows.";
        MalformedNumber
            => MALFORMED_NUMBER = 217, error, "number literal is out of range",
               explain "A number literal in a radix form (`0b`, `0o`, or `0x`) exceeds the \
                        128-bit range, or a decimal literal is longer than the length limit \
                        of numbers.";
        BuiltinLabel { name: String }
            => BUILTIN_LABEL = 218, error,
               "`{name}` is a builtin and cannot be used as a map label",
               explain "A map is labeled with the name of a builtin function. A builtin \
                        name always calls the builtin, so no value can carry it as a \
                        label.";
        BuiltinNamedArguments { name: String }
            => BUILTIN_NAMED_ARGUMENTS = 219, error,
               "builtin `{name}` does not take named arguments",
               explain "A builtin function is called with named arguments. Only atoms of \
                        predicates whose columns are named by a `@declare` or an `@input` \
                        take named arguments.",
               hint "pass the arguments by position";
        MixedArguments
            => MIXED_ARGUMENTS = 220, error, "cannot mix named and positional arguments",
               explain "An atom has both named and positional arguments. Either all \
                        arguments are named, or none is.";
        UnknownAggregate { name: String, known: String }
            => UNKNOWN_AGGREGATE = 221, error,
               "unknown aggregate `#{name}`, expected one of {known}",
               explain "The name after `#` is not an aggregate function.";
        EmptyAggregate { kind: String }
            => EMPTY_AGGREGATE = 222, error, "`{kind}` needs a value to aggregate",
               explain "An aggregate other than `#count` has no aggregated term. Only \
                        `#count()` may be empty.";
        ScopeWithoutValue { kind: String }
            => SCOPE_WITHOUT_VALUE = 223, error, "`{kind}` needs a value before `per`",
               explain "An aggregate has a `per` scope but no aggregated term. Only \
                        `#count` can be scoped without one.",
               hint "write the aggregated term before `per`";
        MisplacedAttribute
            => MISPLACED_ATTRIBUTE = 225, error, "attributes are only allowed before rules",
               explain "An attribute (`#[…]`) is placed before a statement that is not a \
                        rule.";
        FactWithSeveralHeads
            => FACT_WITH_SEVERAL_HEADS = 226, error, "fact has more than one atom",
               explain "A statement without a body lists several atoms. A fact states \
                        exactly one atom.",
               hint "write one fact per atom";
        ExpectedAtom
            => EXPECTED_ATOM = 227, error, "expected an atom",
               explain "Something other than an atom appears where a head or body atom is \
                        expected. An atom is a predicate applied to terms, as in \
                        `edge(?x, ?y)`.";
        ExpectedAlias
            => EXPECTED_ALIAS = 228, error, "expected `as` before the module alias",
               explain "A `@use` directive lacks the `as` keyword before its alias.";
        UnknownDirective { name: String }
            => UNKNOWN_DIRECTIVE = 229, error, "unknown directive `@{name}`",
               explain "The name after `@` is not a directive.",
               hint "use one of `@base`, `@prefix`, `@import`, `@export`, `@input`, \
                     `@output`, `@declare`, `@type`, `@parameter`, `@use`, `@define`, or \
                     `@extern`";
        ExpectedConnective { connective: String }
            => EXPECTED_CONNECTIVE = 230, error, "expected `{connective}` before the format",
               explain "An import or export directive lacks the keyword (`from` or `to`) \
                        before its format.";
        TextTooLarge
            => TEXT_TOO_LARGE = 231, error, "source is too large: the limit is 4 GiB",
               explain "The text is longer than 4 GiB, more than source positions can \
                        address.";
        SuperInsideModuleName
            => SUPER_INSIDE_MODULE_NAME = 232, error, "`super` can only begin a module name",
               explain "A module name begins with any number of `super` steps, each going \
                        one directory up, followed by directories and the module's file \
                        name, as in `super::util::pairs`. A `super` after a name would go \
                        back up a directory the name just went into.",
               hint "remove `super` together with the name before it";
        NegationInWhere
            => NEGATION_IN_WHERE = 233, error, "a `where` clause cannot negate an atom",
               explain "The `where` clause of an import holds binding atoms and conditions \
                        over the import's columns. A negated atom would ask for the rows \
                        that something does not bind, which no import can fetch.";
    }
}
