//! The constructs only linking resolves, which a loaded program cannot
//! hold.

use std::fmt::{self, Display, Formatter};

use nemo_physical::diagnostics::Diagnostic;

use crate::model::{Directive, Identifier, IdentifierRole, Span, Statement};

/// A construct of the surface language that linking resolves
/// ([link](crate::engine::Engine::link)), so that a loaded program never
/// holds it.
///
/// A commit panics when it finishes if its stage wrote one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum UnlinkedConstruct {
    /// The `@use` directive.
    Use,
    /// A module-qualified name, `alias::name`.
    QualifiedName,
}

impl UnlinkedConstruct {
    /// Every construct linking resolves.
    pub(crate) const ALL: [UnlinkedConstruct; 2] =
        [UnlinkedConstruct::Use, UnlinkedConstruct::QualifiedName];

    /// Returns whether the construct occurs in a statement.
    pub(crate) fn occurs_in(self, statement: &Statement) -> bool {
        match self {
            UnlinkedConstruct::Use => matches!(statement, Statement::Directive(Directive::Use(_))),
            UnlinkedConstruct::QualifiedName => {
                let mut found = false;
                let mut look = |identifier: &Identifier,
                                _: IdentifierRole,
                                _: Option<Span>|
                 -> Result<Option<Identifier>, Diagnostic> {
                    found |= matches!(identifier, Identifier::Qualified { .. });
                    Ok(None)
                };
                statement
                    .clone()
                    .rename_identifiers(&mut look)
                    .expect("looking at names fails nowhere");
                found
            }
        }
    }
}

impl Display for UnlinkedConstruct {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            UnlinkedConstruct::Use => f.write_str("a `@use` directive"),
            UnlinkedConstruct::QualifiedName => f.write_str("a module-qualified name"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testing::parse;

    /// The statements of a program text.
    fn statements(source: &str) -> Vec<Statement> {
        let program = parse(source);
        program.statements
    }

    #[test]
    fn constructs_are_found_wherever_they_occur() {
        let found = statements("@use lib as lib .\np(?x) :- lib::q(?x) .\np(?x) :- q(?x) .");
        assert!(UnlinkedConstruct::Use.occurs_in(&found[0]));
        assert!(!UnlinkedConstruct::Use.occurs_in(&found[1]));
        assert!(UnlinkedConstruct::QualifiedName.occurs_in(&found[1]));
        assert!(!UnlinkedConstruct::QualifiedName.occurs_in(&found[2]));
    }
}
