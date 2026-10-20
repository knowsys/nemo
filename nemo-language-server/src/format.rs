//! Formatting: statements rewritten as the model prints them.
//!
//! Every `Display` in the model produces valid Nemo syntax,
//! and the parser and the printer are round-trip tested against each other,
//! so formatting a statement is printing it.
//!
//! What the model does *not* carry is trivia: the comments and blank lines a program is
//! written with.
//! So only statements are rewritten, each inside its own span,
//! and everything between them, which holds the comments, is left exactly as it was.
//! A statement whose own text holds a `%` or a `/*` is left alone as well,
//! since a comment inside it would not survive the round trip,
//! and a comment marker inside a string is indistinguishable here from one that opens a
//! comment.
//! Leaving such a statement as written is the safe direction of that doubt.

use nemo::model::{Span, Statement};

/// One replacement: the text to put, and its place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Edit {
    /// The span to replace.
    pub span: Span,
    /// What to put in its place.
    pub text: String,
}

/// Returns the edits that normalize a document's statements, in source order.
pub(crate) fn edits(statements: &[Statement], text: &str) -> Vec<Edit> {
    let mut edits = Vec::new();
    for statement in statements {
        let Some(span) = statement.span() else {
            continue;
        };
        let Some(source) = slice(text, span) else {
            continue;
        };
        if source.contains('%') || source.contains("/*") {
            continue;
        }
        let printed = printed(statement);
        if printed != source {
            edits.push(Edit {
                span,
                text: printed,
            });
        }
    }
    edits
}

/// Returns a statement as the model prints it.
///
/// A rule's span begins after its attributes while its printing includes them,
/// so an attributed rule is printed without them,
/// since what the span covers is what the edit may replace.
fn printed(statement: &Statement) -> String {
    match statement {
        Statement::Rule(rule) if !rule.attributes.is_empty() => {
            let mut bare = rule.clone();
            bare.attributes.clear();
            bare.to_string()
        }
        other => other.to_string(),
    }
}

/// Returns the text a span covers.
fn slice(text: &str, span: Span) -> Option<&str> {
    let start = usize::try_from(span.start).ok()?;
    let end = usize::try_from(span.end).ok()?;
    text.get(start..end)
}

#[cfg(test)]
mod tests {
    use super::*;

    use nemo::model::{FileId, Program};

    /// A document's text with its formatting edits applied.
    fn formatted(source: &str) -> String {
        let (program, diagnostics) = Program::parse(source, FileId::new(0));
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        let mut text = source.to_string();
        // Back to front, so an earlier edit's span stays valid.
        for edit in edits(&program.statements, source).into_iter().rev() {
            text.replace_range(edit.span.start as usize..edit.span.end as usize, &edit.text);
        }
        text
    }

    #[test]
    fn spacing_is_normalized_within_a_statement() {
        assert_eq!(formatted("p(1,2)   .\n"), "p(1, 2) .\n");
        assert_eq!(
            formatted("q(?x,?y):-p(?x,?y),r(?y) .\n"),
            "q(?x, ?y) :- p(?x, ?y), r(?y) .\n"
        );
    }

    #[test]
    fn what_lies_between_statements_is_left_alone() {
        // The comment and the blank lines are not the model's to keep,
        // so nothing that would rewrite them is emitted.
        let source = "% what this file is about\np(1,2) .\n\n\n% and this rule\nq(?x):-p(?x,1) .\n";
        assert_eq!(
            formatted(source),
            "% what this file is about\np(1, 2) .\n\n\n% and this rule\nq(?x) :- p(?x, 1) .\n"
        );
    }

    #[test]
    fn a_statement_holding_a_comment_is_left_as_written() {
        // The comment sits inside the rule's own span; printing the rule
        // would drop it, so the rule is not printed.
        let source = "q(?x) :-  % why\n    p(?x) .\n";
        assert_eq!(formatted(source), source);
    }

    #[test]
    fn a_statement_holding_a_block_comment_is_left_as_written() {
        // A block comment is trivia wherever whitespace is, so one can
        // be inside a statement, where printing would drop it.
        let source = "q(?x) :- /* why */ p(?x) .\n";
        assert_eq!(formatted(source), source);
        let source = "p(1, /* unit: cm */ 2) .\n";
        assert_eq!(formatted(source), source);
    }

    #[test]
    fn a_string_that_looks_like_a_comment_is_left_alone_too() {
        // `%` in a string cannot be told from one that opens a comment
        // without lexing, and leaving it is the safe way to be wrong.
        let source = "p(\"100%\",1) .\n";
        assert_eq!(formatted(source), source);
    }

    #[test]
    fn an_attributed_rule_keeps_its_attribute_once() {
        // The rule's span starts after the attribute; printing the rule
        // would print the attribute again.
        let source = "#[name(\"reach\")]\nq(?x):-p(?x) .\n";
        assert_eq!(formatted(source), "#[name(\"reach\")]\nq(?x) :- p(?x) .\n");
    }

    #[test]
    fn a_formatted_document_is_left_alone() {
        let source = "p(1, 2) .\nq(?x) :- p(?x, 2) .\n@output q .\n";
        let (program, diagnostics) = Program::parse(source, FileId::new(0));
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        assert!(
            edits(&program.statements, source).is_empty(),
            "nothing to change"
        );
    }

    #[test]
    fn directives_are_normalized_too() {
        assert_eq!(formatted("@output   p ,q .\n"), "@output p, q .\n");
    }
}
