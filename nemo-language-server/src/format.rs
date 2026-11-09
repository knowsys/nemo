//! Formatting a document by printing its statements as the model prints them.
//!
//! Every `Display` of the model writes valid Nemo syntax that parses back to the same
//! statement, so formatting a statement means printing it.
//! The model does not keep comments and blank lines,
//! so only the statements are replaced, each within its own span,
//! and the text between them, including the comments, is left as it is.

use nemo::model::{Span, Statement};

/// A replacement of the text of a span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Edit {
    /// The span to replace.
    pub span: Span,
    /// The new text.
    pub text: String,
}

/// Returns the edits that normalize a document's statements, in source order.
///
/// A statement whose text contains `%` or `/*` is left as written,
/// since printing it would drop a comment inside it.
/// This check cannot distinguish a comment marker inside a string from one that starts a
/// comment, so such a statement is left as written too, which is the safe choice.
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

/// Returns a statement as the model prints it, but without a rule's attributes.
///
/// A rule's span begins after its attributes, but its printed form includes them.
/// An edit may replace only what the span covers, so the attributes are left out.
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

/// Returns the text of a span, or `None` if the span is not inside the text.
fn slice(text: &str, span: Span) -> Option<&str> {
    let start = usize::try_from(span.start).ok()?;
    let end = usize::try_from(span.end).ok()?;
    text.get(start..end)
}

#[cfg(test)]
mod tests {
    use super::*;

    use nemo::model::{FileId, Program};

    /// Returns the text of a source that parses, with its formatting edits applied.
    fn formatted(source: &str) -> String {
        let (program, diagnostics) = Program::parse(source, FileId::new(0));
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        let mut text = source.to_string();
        // The edits are applied from the back, so that the spans of earlier edits stay valid.
        for edit in edits(&program.statements, source).into_iter().rev() {
            text.replace_range(edit.span.start as usize..edit.span.end as usize, &edit.text);
        }
        text
    }

    /// The spacing inside a statement is normalized.
    #[test]
    fn spacing_is_normalized_within_a_statement() {
        assert_eq!(formatted("p(1,2)   .\n"), "p(1, 2) .\n");
        assert_eq!(
            formatted("q(?x,?y):-p(?x,?y),r(?y) .\n"),
            "q(?x, ?y) :- p(?x, ?y), r(?y) .\n"
        );
    }

    /// The comments and blank lines between statements are kept.
    #[test]
    fn what_lies_between_statements_is_left_alone() {
        // The model does not keep the comments and blank lines, so no edit touches them.
        let source = "% what this file is about\np(1,2) .\n\n\n% and this rule\nq(?x):-p(?x,1) .\n";
        assert_eq!(
            formatted(source),
            "% what this file is about\np(1, 2) .\n\n\n% and this rule\nq(?x) :- p(?x, 1) .\n"
        );
    }

    /// A statement that contains a `%` comment is left as written.
    #[test]
    fn a_statement_holding_a_comment_is_left_as_written() {
        // The comment is inside the rule's span, and printing the rule would drop it,
        // so the rule is not printed.
        let source = "q(?x) :-  % why\n    p(?x) .\n";
        assert_eq!(formatted(source), source);
    }

    /// A statement that contains a block comment is left as written.
    #[test]
    fn a_statement_holding_a_block_comment_is_left_as_written() {
        // A block comment may appear wherever whitespace may, so it can be inside a
        // statement, where printing would drop it.
        let source = "q(?x) :- /* why */ p(?x) .\n";
        assert_eq!(formatted(source), source);
        let source = "p(1, /* unit: cm */ 2) .\n";
        assert_eq!(formatted(source), source);
    }

    /// A statement with a `%` inside a string is left as written as well.
    #[test]
    fn a_string_that_looks_like_a_comment_is_left_alone_too() {
        // Without lexing, a `%` in a string cannot be distinguished from one that starts a
        // comment, and leaving the statement as written is the safe mistake.
        let source = "p(\"100%\",1) .\n";
        assert_eq!(formatted(source), source);
    }

    /// Formatting a rule with an attribute does not repeat the attribute.
    #[test]
    fn an_attributed_rule_keeps_its_attribute_once() {
        // The rule's span starts after the attribute,
        // so printing the rule with it would write the attribute twice.
        let source = "#[name(\"reach\")]\nq(?x):-p(?x) .\n";
        assert_eq!(formatted(source), "#[name(\"reach\")]\nq(?x) :- p(?x) .\n");
    }

    /// A document that is already formatted gets no edits.
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

    /// The spacing inside a directive is normalized too.
    #[test]
    fn directives_are_normalized_too() {
        assert_eq!(formatted("@output   p ,q .\n"), "@output p, q .\n");
    }
}
