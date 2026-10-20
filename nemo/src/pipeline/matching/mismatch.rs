//! Why a rule does not replace an input rule as a stage declared.

use std::fmt;

use crate::{
    model::{Atom, Literal},
    pipeline::LiteralPath,
};

/// Why a new rule does not replace an input rule as a stage declared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CorrespondenceMismatch {
    /// The rules have different numbers of head atoms.
    HeadCount {
        /// The input rule's number.
        input: usize,
        /// The new rule's number.
        new: usize,
    },
    /// A head atom of the new rule does not match the input rule's.
    HeadAtom {
        /// The input rule's head atom.
        pattern: Atom,
        /// The new rule's head atom.
        atom: Atom,
    },
    /// The stage gave another number of literal paths than the input rule
    /// has body literals.
    PathCount {
        /// The input rule's number of body literals.
        input: usize,
        /// The number of paths.
        paths: usize,
    },
    /// A literal of the input rule does not match the literal at its
    /// path.
    PathLiteral {
        /// The input rule's literal.
        pattern: Literal,
        /// The literal at its path.
        literal: Literal,
        /// The path.
        path: LiteralPath,
    },
    /// A path is empty.
    EmptyPath,
    /// A path names a position the new rule's body does not have.
    NoBodyLiteral {
        /// The position.
        position: usize,
    },
    /// A path unfolds an operation, which has no definition.
    NotUnfoldable {
        /// The operation's literal.
        literal: Literal,
    },
    /// A path unfolds an atom over a predicate the commit did not
    /// define.
    NotDefined {
        /// The atom.
        atom: Atom,
    },
    /// A path unfolds a negated atom whose definition is not one atom.
    NegatedDefinition {
        /// The negated literal.
        literal: Literal,
    },
    /// A path names a position the unfolded definition does not have.
    NoDefinitionLiteral {
        /// The atom over the defined predicate.
        atom: Atom,
        /// The position.
        position: usize,
    },
    /// An atom over a defined predicate has another number of terms than
    /// the definition's head.
    DefinitionArity {
        /// The atom.
        atom: Atom,
        /// The number of terms of the definition's head.
        head: usize,
    },
}

impl fmt::Display for CorrespondenceMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CorrespondenceMismatch::HeadCount { input, new } => {
                write!(f, "it has {new} head atoms and the input rule has {input}")
            }
            CorrespondenceMismatch::HeadAtom { pattern, atom } => {
                write!(f, "its head `{atom}` does not match `{pattern}`")
            }
            CorrespondenceMismatch::PathCount { input, paths } => write!(
                f,
                "the stage gave {paths} literal paths for {input} input literals"
            ),
            CorrespondenceMismatch::PathLiteral {
                pattern,
                literal,
                path,
            } => write!(
                f,
                "`{pattern}` does not match `{literal}`, the literal at its path {path:?}"
            ),
            CorrespondenceMismatch::EmptyPath => {
                f.write_str("the stage gave an empty literal path")
            }
            CorrespondenceMismatch::NoBodyLiteral { position } => {
                write!(f, "its body has no literal {position}")
            }
            CorrespondenceMismatch::NotUnfoldable { literal } => {
                write!(f, "a path unfolds `{literal}`, which is an operation")
            }
            CorrespondenceMismatch::NotDefined { atom } => {
                write!(
                    f,
                    "a path unfolds `{atom}`, whose predicate the stage did not define"
                )
            }
            CorrespondenceMismatch::NegatedDefinition { literal } => write!(
                f,
                "`{literal}` is negated, and its definition is not one atom"
            ),
            CorrespondenceMismatch::NoDefinitionLiteral { atom, position } => write!(
                f,
                "the definition of the predicate of `{atom}` has no literal {position}"
            ),
            CorrespondenceMismatch::DefinitionArity { atom, head } => write!(
                f,
                "`{atom}` has {} terms and its definition's head has {head}",
                atom.terms.len()
            ),
        }
    }
}
