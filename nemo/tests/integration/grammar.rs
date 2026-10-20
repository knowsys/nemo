//! The editor grammar and the engine agree about what a builtin is.
//!
//! `nemo-vscode/syntaxes/nemo.tmLanguage.json` is generated from
//! [Operator::NAMES] by `nemo-vscode/tools/grammar.py`. Generated files
//! drift, so this is the thing that notices: a builtin added to the
//! catalogue and not to the grammar would simply stop being highlighted,
//! which nobody would report as a bug.

use std::{fs, path::PathBuf};

use nemo::model::{AggregateKind, Operator};

/// The generated grammar, or `None` where only the crates were checked
/// out.
fn grammar() -> Option<String> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()?
        .join("nemo-vscode")
        .join("syntaxes")
        .join("nemo.tmLanguage.json");
    fs::read_to_string(path).ok()
}

/// The alternation of names inside one of the grammar's rules.
fn listed(grammar: &str, rule: &str) -> Vec<String> {
    let rule = grammar
        .split(&format!("\"{rule}\":"))
        .nth(1)
        .expect("the grammar has the rule");
    let pattern = rule
        .split("\"match\": \"")
        .nth(1)
        .expect("the rule has a pattern")
        .split('"')
        .next()
        .expect("the pattern is terminated");
    // The names sit in the last parenthesised group before the
    // trailing lookahead, whether or not it is case-insensitive, and
    // whether or not the lookahead also stops at a hyphen.
    let alternation = pattern
        .rsplit_once("(?![A-Za-z0-9_%]")
        .map_or(pattern, |(head, _)| head)
        .rsplit_once('(')
        .expect("the names are grouped")
        .1
        .trim_end_matches(')')
        .trim_start_matches("?i:")
        .trim_start_matches("?:");
    alternation.split('|').map(str::to_owned).collect()
}

#[test]
fn the_editor_grammar_knows_every_builtin() {
    let Some(grammar) = grammar() else {
        println!("no editor grammar checked out; nothing to compare");
        return;
    };
    let mut listed = listed(&grammar, "builtin");
    listed.sort_unstable();

    let mut known: Vec<String> = Operator::NAMES
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    known.sort_unstable();

    assert_eq!(
        listed, known,
        "the grammar and the catalogue disagree; re-run nemo-vscode/tools/grammar.py"
    );
}

#[test]
fn the_editor_grammar_knows_every_aggregate() {
    let Some(grammar) = grammar() else {
        println!("no editor grammar checked out; nothing to compare");
        return;
    };
    let mut listed = listed(&grammar, "aggregate");
    listed.sort_unstable();

    let mut known: Vec<String> = AggregateKind::ALL
        .iter()
        .map(|kind| kind.name().to_owned())
        .collect();
    known.sort_unstable();

    assert_eq!(
        listed, known,
        "the grammar and the aggregates disagree; re-run nemo-vscode/tools/grammar.py"
    );
}
