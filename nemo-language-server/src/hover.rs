//! The hover text of the name under the cursor.
//!
//! It shows a predicate's signature and where its rows come from, a variable's type,
//! what a builtin or an aggregate computes, and a value label's declaration.

use nemo::{
    model::{BuiltinDocumentation, Span, VariableKind},
    values::TypeSyntax,
};

use crate::{
    document::Document,
    symbols::{Mention, Occurrence, Origin},
};

/// Returns the hover text, in Markdown, of what is at a byte offset, with the span it
/// describes, or `None` if there is nothing to describe there.
pub(crate) fn describe(document: &Document, offset: u32) -> Option<(String, Span)> {
    let symbols = document.symbols();
    if let Some(mentioned) = symbols.mention_at(offset) {
        return mention(document, &mentioned.mention).map(|text| (text, mentioned.span));
    }
    let occurrence = symbols.at(offset)?;
    Some((predicate(document, occurrence), occurrence.span))
}

/// Returns the hover text of a predicate: its signature, where its rows come from,
/// and its documentation.
///
/// The signature of a declared predicate is its declaration.
/// Any other predicate's signature lists the column types that the check inferred,
/// if they are known.
fn predicate(document: &Document, occurrence: &Occurrence) -> String {
    let name = &occurrence.name;
    let documentation = document.documentation(name);
    let signature = match documentation {
        Some(documentation) => documentation.signature.clone(),
        None => match document.types().predicate(&occurrence.identifier) {
            Some(types) => {
                let columns: Vec<String> = types
                    .iter()
                    .map(|datatype| TypeSyntax::of(datatype).to_string())
                    .collect();
                format!("{name}({})", columns.join(", "))
            }
            None => name.clone(),
        },
    };
    let mut text = format!("{}\n\n{}", code(&signature), role(document, name));
    // The comment above the declaration documents the predicate.
    // Without one, the comments above the statements that derive it do.
    let comments = match documentation.and_then(|documentation| documentation.comment.as_ref()) {
        Some(comment) => std::slice::from_ref(comment),
        None => document.comments(name),
    };
    for comment in comments {
        text.push_str("\n\n");
        text.push_str(comment);
    }
    text
}

/// Returns where a predicate's rows come from, and whether it is an output of the program,
/// as `facts · imported from csv · derived · output`.
fn role(document: &Document, name: &str) -> String {
    let symbols = document.symbols();
    let origins: Vec<&Origin> = symbols
        .definitions(name)
        .filter_map(|occurrence| occurrence.origin.as_ref())
        .collect();
    let mut formats: Vec<&str> = Vec::new();
    for origin in &origins {
        if let Origin::Import(format) = origin
            && !formats.contains(&format.as_str())
        {
            formats.push(format);
        }
    }
    let mut parts: Vec<String> = Vec::new();
    if origins.contains(&&Origin::Fact) {
        parts.push(String::from("facts"));
    }
    if !formats.is_empty() {
        parts.push(format!("imported from {}", formats.join(", ")));
    }
    if origins.contains(&&Origin::Input) {
        parts.push(String::from("input"));
    }
    if origins.contains(&&Origin::Rule) {
        parts.push(String::from("derived"));
    }
    if parts.is_empty() {
        parts.push(String::from("nothing derives it"));
    }
    if symbols.is_output(name) {
        parts.push(String::from("output"));
    }
    parts.join(" · ")
}

/// Returns the hover text of a variable, a builtin, an aggregate or a value label,
/// or `None` if there is nothing to say about it.
///
/// There is nothing to say, for example, about a variable whose type is not known,
/// about a parameter, or about a value label that no `@type` declares.
fn mention(document: &Document, mention: &Mention) -> Option<String> {
    match mention {
        Mention::Variable(variable, rule) => {
            let datatype = match variable.kind {
                // An existential variable stands for a null that the rule creates.
                VariableKind::Existential => String::from("null"),
                VariableKind::Universal => {
                    let rule = document.slice(*rule);
                    let datatype = document.types().variable(rule, &variable.name)?;
                    TypeSyntax::of(datatype).to_string()
                }
                VariableKind::Global | VariableKind::Anonymous => return None,
            };
            Some(code(&format!("{variable}: {datatype}")))
        }
        Mention::Builtin(operator) => Some(builtin(&operator.documentation())),
        Mention::Aggregate(kind) => Some(builtin(&kind.documentation())),
        Mention::Label(label) => {
            let documentation = document.label(label)?;
            let mut text = code(&documentation.signature);
            if let Some(comment) = &documentation.comment {
                text.push_str("\n\n");
                text.push_str(comment);
            }
            Some(text)
        }
    }
}

/// Returns the hover text of a builtin or an aggregate: its signatures, and what it computes.
fn builtin(documentation: &BuiltinDocumentation) -> String {
    format!(
        "{}\n\n{}",
        code(&documentation.signatures.join("\n")),
        documentation.summary
    )
}

/// Returns source text as a Markdown code block of Nemo code.
fn code(source: &str) -> String {
    let wall = fence(source);
    format!("{wall}nemo\n{source}\n{wall}")
}

/// Returns a code fence of at least three backticks that is longer than the longest run of
/// backticks in `text`, so that the text cannot close it.
fn fence(text: &str) -> String {
    let mut longest = 0;
    let mut run = 0;
    for character in text.chars() {
        run = if character == '`' { run + 1 } else { 0 };
        longest = longest.max(run);
    }
    "`".repeat(longest.max(2) + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fence has three backticks, or one more than the longest run of backticks in the text.
    #[test]
    fn a_fence_outlasts_the_backticks_it_encloses() {
        assert_eq!(fence("p(1)"), "```");
        assert_eq!(fence("p(\"a ```` b\")"), "`````");
    }
}
