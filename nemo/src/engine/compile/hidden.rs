//! Hidden names: the names of the predicates and variables that the engine adds to a program.
//!
//! A hidden name begins with the character `U+0000`, which no name written in a program can
//! contain, so a hidden name is never one of the program's own.
//! Whatever names a program's predicates and variables to the user leaves the hidden ones out.

/// The character that every hidden name begins with.
const MARK: char = '\u{0}';

/// What the engine adds to a program under a hidden name.
///
/// No two of them have the same name.
#[derive(Debug, Clone, Copy)]
pub(in crate::engine) enum Hidden<'a> {
    /// The predicate with one row, which a rule body without positive atoms reads.
    Unit,
    /// The variable for an expression argument of a rule's body atoms,
    /// numbered among such arguments of the rule.
    Expression(usize),
    /// The predicate and the variable that stand for the named parameter,
    /// where a check has no value for it.
    Parameter(&'a str),
}

impl Hidden<'_> {
    /// Returns the hidden name.
    pub(in crate::engine) fn name(self) -> String {
        match self {
            Hidden::Unit => format!("{MARK}unit"),
            Hidden::Expression(index) => format!("{MARK}expression{index}"),
            // No other hidden name has a `$`.
            Hidden::Parameter(name) => format!("{MARK}${name}"),
        }
    }
}

/// Returns whether a name is hidden.
pub(in crate::engine) fn is_hidden(name: &str) -> bool {
    name.starts_with(MARK)
}

#[cfg(test)]
mod test {
    use super::*;

    /// A parameter may be named like the engine's other hidden names without sharing theirs.
    #[test]
    fn the_hidden_names_are_distinct() {
        let names = [
            Hidden::Unit.name(),
            Hidden::Expression(0).name(),
            Hidden::Parameter("unit").name(),
            Hidden::Parameter("expression0").name(),
        ];

        for (index, name) in names.iter().enumerate() {
            assert!(is_hidden(name));
            assert!(!names[..index].contains(name), "{names:?}");
        }
    }

    #[test]
    fn a_written_name_is_not_hidden() {
        assert!(!is_hidden("unit"));
        assert!(!is_hidden(""));
    }
}
