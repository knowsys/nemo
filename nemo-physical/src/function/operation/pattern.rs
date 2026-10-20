//! Regular-expression matching and replacement:
//! SPARQL's [`REGEX`](https://www.w3.org/TR/sparql12-query/#func-regex) and
//! [`REPLACE`](https://www.w3.org/TR/sparql12-query/#func-replace),
//! which are XPath's [`fn:matches`](https://www.w3.org/TR/xpath-functions-31/#func-matches) and
//! [`fn:replace`](https://www.w3.org/TR/xpath-functions-31/#func-replace).
//!
//! A pattern is written in the `fancy-regex` dialect, backreferences included,
//! rather than XPath's regular expression syntax.
//! XPath's [flags](https://www.w3.org/TR/xpath-functions-31/#flags) apply:
//! `i` for case-insensitive matching, `s` for a dot matching a line break,
//! `m` for anchors matching at line breaks,
//! `x` for whitespace in the pattern ignored (the engine's verbose mode,
//! which also reads `#` comments),
//! and `q` for a pattern, and a replacement, taken literally.
//! A replacement reads `$n` as a capture group and `\$` and `\\` as `$` and `\`.
//! An unknown flag or an invalid pattern or replacement is malformed;
//! a pattern too large to compile, or a subject the engine gives up on, overflows.
//!
//! Compiled patterns are cached per thread by their final source,
//! so a constant pattern compiles once per evaluation thread rather than once per row.
//! The cache is bounded and starts over when full.

use std::{cell::RefCell, collections::HashMap, rc::Rc};

use fancy_regex::{CompileError, Error, ParseError, Regex};

use crate::{
    function::{StackValue, operation::string::text},
    value::{DataValue, EvaluationError, ValueRef},
};

thread_local! {
    /// The compiled patterns of this thread, by final source.
    static COMPILED: RefCell<HashMap<String, Rc<Regex>>> = RefCell::new(HashMap::new());
}

/// The most patterns the cache holds before it starts over.
const COMPILED_BOUND: usize = 256;

/// The parsed flags of a pattern operation.
struct Flags {
    /// Case-insensitive matching.
    insensitive: bool,
    /// A dot matches a line break too.
    dot_all: bool,
    /// The anchors match at line breaks too.
    multi_line: bool,
    /// Whitespace in the pattern is ignored.
    extended: bool,
    /// The pattern, and a replacement, taken literally.
    literal: bool,
}

impl Flags {
    /// Parses a flags string; `None` for an unknown flag.
    fn parse(flags: &str) -> Option<Self> {
        let mut parsed = Self {
            insensitive: false,
            dot_all: false,
            multi_line: false,
            extended: false,
            literal: false,
        };
        for flag in flags.chars() {
            match flag {
                'i' => parsed.insensitive = true,
                's' => parsed.dot_all = true,
                'm' => parsed.multi_line = true,
                'x' => parsed.extended = true,
                'q' => parsed.literal = true,
                _ => return None,
            }
        }
        Some(parsed)
    }

    /// Returns the engine's inline form of the matching flags,
    /// empty if none is set.
    /// A literal pattern has no whitespace to ignore.
    fn inline(&self) -> String {
        let letters = [
            (self.insensitive, 'i'),
            (self.dot_all, 's'),
            (self.multi_line, 'm'),
            (self.extended && !self.literal, 'x'),
        ];
        let set: String = letters
            .iter()
            .filter(|(on, _)| *on)
            .map(|(_, letter)| *letter)
            .collect();

        if set.is_empty() {
            set
        } else {
            format!("(?{set})")
        }
    }
}

/// The operands of a pattern operation, read from their values.
struct Operands<'a> {
    /// The text the pattern is applied to.
    subject: &'a str,
    /// The compiled pattern.
    regex: Rc<Regex>,
    /// The flags.
    flags: Flags,
}

impl<'a> Operands<'a> {
    /// Reads the operands: a string subject, a plain-string pattern,
    /// and plain-string flags if given.
    ///
    /// An operand of another kind is a [TypeMismatch](EvaluationError::TypeMismatch),
    /// an unknown flag is [Malformed](EvaluationError::Malformed),
    /// and a pattern that does not compile fails as [compile] says.
    fn read(
        subject: ValueRef<'a>,
        pattern: ValueRef<'_>,
        flags: Option<ValueRef<'_>>,
    ) -> Result<Self, EvaluationError> {
        let subject = text(subject).ok_or(EvaluationError::TypeMismatch)?;

        let ValueRef::String(pattern) = pattern else {
            return Err(EvaluationError::TypeMismatch);
        };
        let flags = match flags {
            Some(ValueRef::String(flags)) => flags,
            Some(_) => return Err(EvaluationError::TypeMismatch),
            None => "",
        };
        let flags = Flags::parse(flags).ok_or(EvaluationError::Malformed)?;
        let regex = compile(pattern, &flags)?;

        Ok(Self {
            subject,
            regex,
            flags,
        })
    }
}

/// Returns the regex of a pattern under flags, from the cache if it is there.
///
/// A pattern beyond the engine's size or nesting limits
/// is an [Overflow](EvaluationError::Overflow);
/// any other one that does not compile is [Malformed](EvaluationError::Malformed).
fn compile(pattern: &str, flags: &Flags) -> Result<Rc<Regex>, EvaluationError> {
    let mut source = flags.inline();
    if flags.literal {
        source.push_str(&fancy_regex::escape(pattern));
    } else {
        source.push_str(pattern);
    }

    COMPILED.with(|compiled| {
        let mut compiled = compiled.borrow_mut();
        if let Some(regex) = compiled.get(&source) {
            return Ok(regex.clone());
        }
        let regex = Rc::new(Regex::new(&source).map_err(|error| match error {
            Error::ParseError(_, ParseError::RecursionExceeded) => EvaluationError::Overflow,
            Error::CompileError(CompileError::InnerError(inner))
                if inner.size_limit().is_some() =>
            {
                EvaluationError::Overflow
            }
            _ => EvaluationError::Malformed,
        })?);
        if compiled.len() >= COMPILED_BOUND {
            compiled.clear();
        }
        compiled.insert(source, regex.clone());
        Ok(regex)
    })
}

/// Returns whether a pattern matches somewhere in a string.
///
/// A subject the engine gives up on is an [Overflow](EvaluationError::Overflow).
pub(crate) fn matches(
    subject: ValueRef<'_>,
    pattern: ValueRef<'_>,
    flags: Option<ValueRef<'_>>,
) -> StackValue<'static> {
    let operands = match Operands::read(subject, pattern, flags) {
        Ok(operands) => operands,
        Err(error) => return error.into(),
    };
    match operands.regex.is_match(operands.subject) {
        Ok(holds) => holds.into(),
        Err(_) => EvaluationError::Overflow.into(),
    }
}

/// Replaces every match of a pattern in a string,
/// the replacement read as XPath's `fn:replace` reads it:
/// `$n` for a capture group, `\$` and `\\` for a literal `$` and `\`.
///
/// The subject's language tag is kept.
pub(crate) fn replace(
    subject: ValueRef<'_>,
    pattern: ValueRef<'_>,
    replacement: ValueRef<'_>,
    flags: Option<ValueRef<'_>>,
) -> StackValue<'static> {
    let ValueRef::String(replacement) = replacement else {
        return EvaluationError::TypeMismatch.into();
    };
    let operands = match Operands::read(subject, pattern, flags) {
        Ok(operands) => operands,
        Err(error) => return error.into(),
    };

    let replaced = if operands.flags.literal {
        // A literal replacement: `$` has no meaning.
        operands
            .regex
            .try_replacen(operands.subject, 0, fancy_regex::NoExpand(replacement))
    } else {
        let Some(expansion) = expansion(replacement, operands.regex.captures_len() - 1) else {
            return EvaluationError::Malformed.into();
        };
        operands
            .regex
            .try_replacen(operands.subject, 0, expansion.as_str())
    };
    // The engine may give up on a subject, as it does for matching.
    let Ok(replaced) = replaced else {
        return EvaluationError::Overflow.into();
    };
    let replaced = replaced.into_owned();
    match subject {
        ValueRef::LanguageString(value) => DataValue::language_string(replaced, value.language())
            .expect("the tag of a language string is valid")
            .into(),
        _ => DataValue::string(replaced).into(),
    }
}

/// Translates an XPath replacement string, for a pattern with `groups` capture groups,
/// into the expansion syntax of `fancy-regex`,
/// or returns `None` if it is malformed:
/// a `$` not followed by a digit, or a `\` not followed by `$` or `\`.
///
/// A `$` reads its digits as one number;
/// one that names no group drops its last digit, kept as a literal, while it is above 9,
/// and one of at most 9 that names no group stands for nothing.
fn expansion(replacement: &str, groups: usize) -> Option<String> {
    let mut expanded = String::with_capacity(replacement.len());
    let mut characters = replacement.chars().peekable();
    while let Some(character) = characters.next() {
        match character {
            '\\' => match characters.next()? {
                '$' => expanded.push_str("$$"),
                '\\' => expanded.push('\\'),
                _ => return None,
            },
            '$' => {
                let mut digits = String::new();
                while let Some(digit) = characters.next_if(char::is_ascii_digit) {
                    digits.push(digit);
                }
                if digits.is_empty() {
                    return None;
                }
                let mut length = digits.len();
                let group = loop {
                    let number = digits[..length].parse::<usize>().unwrap_or(usize::MAX);
                    if number <= groups {
                        break Some(number);
                    }
                    if number <= 9 {
                        break None;
                    }
                    length -= 1;
                };
                if let Some(group) = group {
                    expanded.push_str(&format!("${{{group}}}"));
                }
                expanded.push_str(&digits[length..]);
            }
            other => expanded.push(other),
        }
    }
    Some(expanded)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Whether a pattern under flags matches a subject.
    fn holds(subject: &str, pattern: &str, flags: &str) -> Result<bool, EvaluationError> {
        let (subject, pattern, flags) = (
            DataValue::string(subject),
            DataValue::string(pattern),
            DataValue::string(flags),
        );
        matches(subject.as_ref(), pattern.as_ref(), Some(flags.as_ref()))
            .as_result()
            .map(|value| value == ValueRef::Boolean(true))
    }

    #[test]
    fn the_flags_are_xpaths() {
        assert_eq!(holds("a\nb", "a.b", ""), Ok(false));
        assert_eq!(holds("a\nb", "a.b", "s"), Ok(true));
        assert_eq!(holds("a\nb", "^b$", ""), Ok(false));
        assert_eq!(holds("a\nb", "^b$", "m"), Ok(true));
        assert_eq!(holds("ab", "a b", ""), Ok(false));
        assert_eq!(holds("ab", "a b", "x"), Ok(true));
        assert_eq!(holds("a b", "a b", "xq"), Ok(true));
        assert_eq!(holds("A.C", "a.c", "qi"), Ok(true));
        assert_eq!(holds("abc", "a.c", "q"), Ok(false));
        assert_eq!(holds("abc", "a.c", "g"), Err(EvaluationError::Malformed));
    }

    #[test]
    fn an_invalid_pattern_is_malformed() {
        assert_eq!(holds("a", "(", ""), Err(EvaluationError::Malformed));
        assert_eq!(holds("a", "a{2,1}", ""), Err(EvaluationError::Malformed));

        // A pattern with a language tag is not a plain string.
        let tagged = DataValue::language_string("a", "en").expect("the tag is valid");
        assert_eq!(
            matches(DataValue::string("a").as_ref(), tagged.as_ref(), None).as_result(),
            Err(EvaluationError::TypeMismatch)
        );
    }

    /// The result of replacing a pattern in a subject.
    fn replaced(
        subject: &str,
        pattern: &str,
        replacement: &str,
    ) -> Result<String, EvaluationError> {
        let (subject, pattern, replacement) = (
            DataValue::string(subject),
            DataValue::string(pattern),
            DataValue::string(replacement),
        );
        replace(
            subject.as_ref(),
            pattern.as_ref(),
            replacement.as_ref(),
            None,
        )
        .as_result()
        .map(|value| match value {
            ValueRef::String(text) => text.to_string(),
            other => panic!("a replacement is a string, not {other}"),
        })
    }

    #[test]
    fn replacements_read_xpaths_syntax() {
        assert_eq!(
            replaced("2024-03", r"(\d+)-(\d+)", "$2_$1"),
            Ok("03_2024".into())
        );
        assert_eq!(replaced("abc", "(b)", "$1x"), Ok("abxc".into()));
        assert_eq!(replaced("abc", "b", "[$0]"), Ok("a[b]c".into()));
        // Digits beyond the groups are literal; a single digit naming no group is nothing.
        assert_eq!(replaced("abc", "(b)", "$10"), Ok("ab0c".into()));
        assert_eq!(replaced("abc", "(b)", "$2"), Ok("ac".into()));
        assert_eq!(replaced("abc", "(b)", "$05"), Ok("ac".into()));
        assert_eq!(replaced("cost", "cost", r"\$5"), Ok("$5".into()));
        assert_eq!(replaced("a/b", "/", r"\\"), Ok(r"a\b".into()));
        assert_eq!(replaced("ab", "b", "$"), Err(EvaluationError::Malformed));
        assert_eq!(replaced("ab", "b", r"\n"), Err(EvaluationError::Malformed));
    }

    #[test]
    fn a_subject_the_engine_gives_up_on_overflows() {
        // Backtracking explodes on the backreference, which the engine reports
        // instead of matching.
        let subject = format!("{}!", "a".repeat(42));
        assert_eq!(
            replaced(&subject, r"^(a+)+\1$", "x"),
            Err(EvaluationError::Overflow)
        );
        assert_eq!(
            holds(&subject, r"^(a+)+\1$", ""),
            Err(EvaluationError::Overflow)
        );
    }
}
