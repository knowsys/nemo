//! Tests of declared rewrites: the statements that each shorthand makes,
//! and the panics of rewrites that break the rules of a commit.

use crate::{
    model::{Identifier, ImportDirective, Statement, Substitution, Term, Variable},
    pipeline::{Commit, Pipeline, RevisionId, StatementId},
    testing::{parse, rule},
};

/// Returns a pipeline loaded with a program text, the loaded revision, and its statements.
fn loaded(source: &str) -> (Pipeline, RevisionId, Vec<StatementId>) {
    let program = parse(source);
    let mut pipeline = Pipeline::new();
    let revision = pipeline.load(program);
    let ids = pipeline.revision_statements(revision).to_vec();
    (pipeline, revision, ids)
}

/// Runs a rewrite over a program, and returns the text of the revision that it makes.
fn rewritten(source: &str, rewrite: impl FnOnce(&mut Commit<'_>, &[StatementId])) -> String {
    let (mut pipeline, revision, ids) = loaded(source);
    let mut commit = pipeline.commit(revision, "test");
    rewrite(&mut commit, &ids);
    let after = commit.finish();
    pipeline.program(after).to_string()
}

/// Returns a SPARQL import of a predicate over the given columns.
fn import(predicate: &str, variables: &[&str]) -> ImportDirective {
    let mut import = ImportDirective::new(
        Identifier::bare(predicate),
        Identifier::bare("sparql"),
        Vec::new(),
    );
    import.variables = variables.iter().map(|name| (*name).to_owned()).collect();
    import
}

/// Folding moves literals into a new predicate, defined before the rule that uses it.
#[test]
fn folding_moves_literals_into_a_defined_predicate() {
    let text = rewritten("p(?x) :- q(?x), r(?x, ?y), s(?y) .", |commit, ids| {
        let name = commit.fresh("part");
        commit.fold(ids[0], &[1, 2], name);
    });
    assert_eq!(
        text,
        "part1(?x) :- r(?x, ?y), s(?y) .\np(?x) :- q(?x), part1(?x) ."
    );
}

/// Extracting literals adds an import, which the rule reads in their place.
#[test]
fn extracting_literals_reads_the_import_in_their_place() {
    let text = rewritten("p(?x) :- q(?x), t(?x, ?y), t(?y, ?z) .", |commit, ids| {
        commit.extract(ids[0], &[1, 2], import("t_query1", &["x"]), &[]);
    });
    assert_eq!(
        text,
        "@import t_query1(?x) from sparql { } .\np(?x) :- q(?x), t_query1(?x) ."
    );
}

/// Extracting a negated literal adds an import of its atom, which the rule reads negated.
#[test]
fn extracting_a_negated_literal_reads_the_import_negated() {
    let text = rewritten("p(?x) :- q(?x), ~t(?x, ?y) .", |commit, ids| {
        commit.extract(ids[0], &[1], import("t_query1", &["x"]), &[]);
    });
    assert!(text.ends_with("p(?x) :- q(?x), ~t_query1(?x) ."), "{text}");
}

/// Extracting a rule replaces it by an import, in place.
#[test]
fn extracting_a_rule_puts_the_import_in_its_place() {
    let text = rewritten("e(?x) :- t(?x, a) .\nf(?x) :- e(?x) .", |commit, ids| {
        commit.extract_rule(ids[0], import("e", &["x"]), &[]);
    });
    assert_eq!(text, "@import e(?x) from sparql { } .\nf(?x) :- e(?x) .");
}

/// An import that names no columns uses the variables of its view's head as its columns.
#[test]
fn an_import_naming_no_columns_takes_the_views() {
    let text = rewritten("e(?x, ?y) :- t(?x, ?y) .", |commit, ids| {
        let import =
            ImportDirective::new(Identifier::bare("e"), Identifier::bare("csv"), Vec::new());
        commit.extract_rule(ids[0], import, &[]);
    });
    assert_eq!(text, "@import e from csv { } .");
}

/// Extracting a rule whose head repeats a variable panics, since the head is not the import's
/// atom.
#[test]
#[should_panic(expected = "whose head is not the import's atom")]
fn a_view_repeating_a_variable_panics() {
    rewritten("e(?x, ?x) :- t(?x) .", |commit, ids| {
        let import =
            ImportDirective::new(Identifier::bare("e"), Identifier::bare("csv"), Vec::new());
        commit.extract_rule(ids[0], import, &[]);
    });
}

/// A filter defined from a rule copies its literals, and the rule keeps them.
#[test]
fn defining_a_filter_from_a_rule_copies_its_literals() {
    let text = rewritten("p(?x) :- q(?x), r(?x) .", |commit, ids| {
        let name = commit.fresh("b");
        let atom = commit.define_filter_from(ids[0], &[0], &["x".to_owned()], name);
        assert_eq!(atom.to_string(), "b1(?x)");
    });
    assert_eq!(text, "b1(?x) :- q(?x) .\np(?x) :- q(?x), r(?x) .");
}

/// Substituting replaces every occurrence of the variable in the rule.
#[test]
fn substituting_specializes_the_rule() {
    let text = rewritten("p(?x, ?y) :- q(?x, ?y) .", |commit, ids| {
        let substitution: Substitution = [(Variable::universal("y"), Term::integer(5))]
            .into_iter()
            .collect();
        commit.substitute(ids[0], &substitution);
    });
    assert_eq!(text, "p(?x, 5) :- q(?x, 5) .");
}

/// A rule that replaces an input rule may add a literal over a filter, which no path leads to.
#[test]
fn a_replacing_rule_may_add_filter_literals() {
    let text = rewritten("p(?x) :- q(?x) .", |commit, ids| {
        let name = commit.fresh("magic");
        let atom = commit.define_filter_from(ids[0], &[0], &["x".to_owned()], name);
        commit.replace_rule(
            ids[0],
            rule(&format!("p(?x) :- q(?x), {atom} .")),
            vec![vec![0]],
        );
    });
    assert_eq!(text, "magic1(?x) :- q(?x) .\np(?x) :- q(?x), magic1(?x) .");
}

/// A rewrite whose definition drops a literal of the input rule panics.
#[test]
#[should_panic(expected = "replaces `p(?x) :- q(?x), r(?x, ?y), s(?y) .`, but")]
fn a_rewrite_dropping_a_literal_panics() {
    rewritten("p(?x) :- q(?x), r(?x, ?y), s(?y) .", |commit, ids| {
        commit.define(rule("part(?x) :- r(?x, ?y) ."), &[ids[0]]);
        commit.replace_rule(
            ids[0],
            rule("p(?x) :- q(?x), part(?x) ."),
            vec![vec![0], vec![1, 0], vec![1, 1]],
        );
    });
}

/// Defining a predicate twice panics.
#[test]
#[should_panic(expected = "defined `part` twice")]
fn defining_a_predicate_twice_panics() {
    rewritten("p(?x) :- q(?x), r(?x) .", |commit, ids| {
        commit.define(rule("part(?x) :- q(?x) ."), &[ids[0]]);
        commit.define(rule("part(?x) :- r(?x) ."), &[ids[0]]);
    });
}

/// Defining a predicate that the input uses panics.
#[test]
#[should_panic(expected = "introduced `q`, which the input uses")]
fn defining_a_predicate_the_input_uses_panics() {
    rewritten("p(?x) :- q(?x) .", |commit, ids| {
        commit.define(rule("q(?x) :- p(?x) ."), &[ids[0]]);
    });
}

/// Adding a rule without a declared rewrite panics.
#[test]
#[should_panic(expected = "without declaring it")]
fn adding_a_rule_freely_panics() {
    rewritten("p(?x) :- q(?x) .", |commit, _| {
        commit.add(Statement::Rule(rule("r(?x) :- q(?x) .")), &[]);
    });
}

/// A statement that produces no facts is added freely, before its first source.
#[test]
fn a_statement_that_produces_no_facts_changes_freely() {
    let text = rewritten("p(?x) :- q(?x) .", |commit, ids| {
        let output = parse("@output p .")
            .statements
            .into_iter()
            .next()
            .expect("one statement");
        commit.add(output, &[ids[0]]);
    });
    assert_eq!(text, "@output p .\np(?x) :- q(?x) .");
}

/// An import whose predicate no statement names can be removed.
#[test]
fn an_import_nothing_reads_can_be_removed() {
    let text = rewritten(
        "@import t from csv { } .\np(?x) :- q(?x) .",
        |commit, ids| {
            commit.remove_import(ids[0]);
        },
    );
    assert_eq!(text, "p(?x) :- q(?x) .");
}

/// Removing an import whose predicate a rule still reads panics.
#[test]
#[should_panic(expected = "but the program still names `t`")]
fn removing_an_import_still_read_panics() {
    rewritten(
        "@import t from csv { } .\np(?x) :- t(?x) .",
        |commit, ids| {
            commit.remove_import(ids[0]);
        },
    );
}
