//! The pipeline: loading, commits and their provenance, stage lists,
//! frozen statements, fresh names, and the report sink.

use super::*;

use crate::model::{
    Atom, Fact, FileId, Identifier, Literal, ModuleName, Rule, Substitution, Term, UseDirective,
    Variable,
};

/// A fact statement `p(value) .`.
fn fact(predicate: &str, value: i64) -> Statement {
    Statement::Fact(Fact::new(
        Identifier::bare(predicate),
        vec![Term::integer(value)],
    ))
}

/// A rule statement `head(?x) :- body(?x) .`.
fn rule(head: &str, body: &str) -> Statement {
    let variable = || Term::variable(Variable::universal("x"));
    Statement::Rule(Rule::new(
        vec![Atom::new(Identifier::bare(head), vec![variable()])],
        vec![Literal::Positive(Atom::new(
            Identifier::bare(body),
            vec![variable()],
        ))],
    ))
}

/// A rule reading a module-qualified predicate:
/// `reach(?x) :- lib::edge(?x) .`.
fn qualified_rule() -> Statement {
    let variable = || Term::variable(Variable::universal("x"));
    let qualified = Identifier::Qualified {
        module: "lib".to_owned(),
        name: "edge".to_owned(),
    };
    Statement::Rule(Rule::new(
        vec![Atom::new(Identifier::bare("reach"), vec![variable()])],
        vec![Literal::Positive(Atom::new(qualified, vec![variable()]))],
    ))
}

/// A pipeline loaded with three statements.
fn loaded() -> (Pipeline, RevisionId) {
    let mut pipeline = Pipeline::new();
    let revision = pipeline.load(Program::new(vec![
        fact("edge", 1),
        rule("reach", "edge"),
        fact("edge", 2),
    ]));
    (pipeline, revision)
}

#[test]
fn loading_round_trips_the_program() {
    let (pipeline, revision) = loaded();
    assert_eq!(
        pipeline.program(revision).to_string(),
        "edge(1) .\nreach(?x) :- edge(?x) .\nedge(2) ."
    );
}

#[test]
fn keeping_is_the_default() {
    let (mut pipeline, revision) = loaded();
    let before = pipeline.revision_statements(revision).to_vec();
    let commit = pipeline.commit(revision, "noop");
    let after = commit.finish();
    assert_eq!(pipeline.revision_statements(after), before);
    assert!(pipeline.ran(after, "noop"));
}

#[test]
fn replacing_preserves_position_and_records_provenance() {
    let (mut pipeline, revision) = loaded();
    let ids = pipeline.revision_statements(revision).to_vec();

    let mut commit = pipeline.commit(revision, "rewrite");
    let new = commit.split(ids[1], vec![rule("reach", "link")])[0];
    let after = commit.finish();

    let live = pipeline.revision_statements(after).to_vec();
    assert_eq!(live.len(), 3);
    assert_eq!(live[1], new);
    assert_eq!(live[0], ids[0]);
    assert_eq!(pipeline.origins(new), vec![ids[1]]);
    // The old version stays addressable.
    assert_eq!(
        pipeline.statement(ids[1]).to_string(),
        "reach(?x) :- edge(?x) ."
    );
}

#[test]
fn a_new_statement_takes_the_span_of_its_source() {
    let mut pipeline = Pipeline::new();
    let file = pipeline.add_source("program.rls", "");
    let span = Span {
        file,
        start: 10,
        end: 30,
    };
    let mut spanned = rule("reach", "edge");
    spanned.set_span(Some(span));
    let revision = pipeline.load(Program::new(vec![spanned]));
    let ids = pipeline.revision_statements(revision).to_vec();

    let mut commit = pipeline.commit(revision, "normalize");
    let new = commit.split(ids[0], vec![rule("reach", "link")])[0];
    commit.finish();

    assert_eq!(pipeline.statement(new).span(), Some(span));
    assert_eq!(pipeline.source_span(new), Some(span));
}

#[test]
fn splitting_shares_one_edge() {
    let (mut pipeline, revision) = loaded();
    let ids = pipeline.revision_statements(revision).to_vec();

    let mut commit = pipeline.commit(revision, "normalize");
    let parts = commit.split(ids[1], vec![rule("reach", "step"), rule("step", "edge")]);
    let after = commit.finish();

    let live = pipeline.revision_statements(after).to_vec();
    assert_eq!(live, vec![ids[0], parts[0], parts[1], ids[2]]);
    for part in parts {
        assert_eq!(pipeline.origins(part), vec![ids[1]]);
    }
}

#[test]
fn deriving_merges_without_losing_roots() {
    let (mut pipeline, revision) = loaded();
    let ids = pipeline.revision_statements(revision).to_vec();

    let mut commit = pipeline.commit(revision, "merge");
    let merged = commit.derive(&[ids[0], ids[2]], fact("edge", 3));
    let after = commit.finish();

    assert_eq!(
        pipeline.revision_statements(after).to_vec(),
        vec![ids[0], ids[1], ids[2], merged]
    );
    assert_eq!(pipeline.origins(merged), vec![ids[0], ids[2]]);
}

#[test]
#[should_panic(expected = "without declaring it")]
fn removing_a_fact_panics() {
    let (mut pipeline, revision) = loaded();
    let ids = pipeline.revision_statements(revision).to_vec();
    let mut commit = pipeline.commit(revision, "drop");
    commit.remove(ids[0]);
}

#[test]
fn origins_walk_transformation_chains() {
    let (mut pipeline, revision) = loaded();
    let ids = pipeline.revision_statements(revision).to_vec();

    let mut commit = pipeline.commit(revision, "first");
    let middle = commit.split(ids[1], vec![rule("reach", "step")])[0];
    let second = commit.finish();
    let mut commit = pipeline.commit(second, "second");
    let last = commit.split(middle, vec![rule("reach", "hop")])[0];
    commit.finish();

    assert_eq!(pipeline.origins(last), vec![ids[1]]);
    // A loaded statement is its own origin.
    assert_eq!(pipeline.origins(ids[0]), vec![ids[0]]);
}

/// A transformation replacing every rule's body predicate.
struct RenameBodies(&'static str);

impl Transformation for RenameBodies {
    fn name(&self) -> &'static str {
        self.0
    }

    fn apply(&self, commit: &mut Commit<'_>) -> Result<(), Diagnostic> {
        for id in commit.ids() {
            if matches!(commit.statement(id), Statement::Rule(_)) {
                commit.split(id, vec![rule("reach", self.0)]);
            }
        }
        Ok(())
    }
}

#[test]
fn applying_skips_stages_already_run() {
    let (mut pipeline, revision) = loaded();
    let mut stages = Stages::new();
    stages.push(RenameBodies("alpha"));
    stages.push(RenameBodies("beta"));

    let first = pipeline.apply(&stages, revision).expect("stages run");
    assert!(pipeline.ran(first, "alpha") && pipeline.ran(first, "beta"));

    // Re-applying the same list changes nothing.
    let second = pipeline.apply(&stages, first).expect("stages run");
    assert_eq!(second, first);

    // A grown list applies only the new stage.
    stages.push(RenameBodies("gamma"));
    let third = pipeline.apply(&stages, first).expect("stages run");
    assert_ne!(third, first);
    assert_eq!(
        pipeline.program(third).to_string(),
        "edge(1) .\nreach(?x) :- gamma(?x) .\nedge(2) ."
    );
}

#[test]
fn stage_lists_support_insertion() {
    let mut stages = Stages::new();
    stages.push(RenameBodies("normalize"));
    stages.push(RenameBodies("validate"));
    stages.insert_before("validate", RenameBodies("custom"));
    stages.insert_after("normalize", RenameBodies("early"));
    assert_eq!(
        stages.names(),
        vec!["normalize", "early", "custom", "validate"]
    );
}

#[test]
#[should_panic(expected = "there is no stage named `missing`")]
fn inserting_beside_an_unknown_stage_panics() {
    let mut stages = Stages::new();
    stages.push(RenameBodies("normalize"));
    stages.insert_before("missing", RenameBodies("x"));
}

/// A stage that requires other stages, rewriting by a function.
struct RequiringStage {
    /// The stage's name.
    name: &'static str,
    /// The stages it requires.
    requires: &'static [&'static str],
    /// The rewrite.
    rewrite: fn(&mut Commit<'_>),
}

impl Transformation for RequiringStage {
    fn name(&self) -> &'static str {
        self.name
    }

    fn requires(&self) -> &'static [&'static str] {
        self.requires
    }

    fn apply(&self, commit: &mut Commit<'_>) -> Result<(), Diagnostic> {
        (self.rewrite)(commit);
        Ok(())
    }
}

/// A stage that rewrites nothing and requires the given stages.
fn idle(name: &'static str, requires: &'static [&'static str]) -> RequiringStage {
    RequiringStage {
        name,
        requires,
        rewrite: |_| {},
    }
}

#[test]
#[should_panic(expected = "the `late` stage requires the `early` stage before it")]
fn a_stage_before_what_it_requires_panics() {
    let (mut pipeline, revision) = loaded();
    let mut stages = Stages::new();
    stages.push(idle("late", &["early"]));
    stages.push(idle("early", &[]));
    let _ = pipeline.apply(&stages, revision);
}

#[test]
fn a_stage_after_what_it_requires_runs() {
    let (mut pipeline, revision) = loaded();
    let mut stages = Stages::new();
    stages.push(idle("early", &[]));
    stages.push(idle("late", &["early"]));
    let after = pipeline.apply(&stages, revision).expect("the stages run");
    assert!(pipeline.ran(after, "late"));
}

#[test]
#[should_panic(expected = "which a loaded program cannot hold")]
fn writing_a_qualified_name_panics() {
    let (mut pipeline, revision) = loaded();
    let mut stages = Stages::new();
    stages.push(RequiringStage {
        name: "writer",
        requires: &[],
        rewrite: |commit| {
            commit.derive(&[], qualified_rule());
        },
    });
    let _ = pipeline.apply(&stages, revision);
}

#[test]
fn an_instance_knows_the_uses_that_made_it() {
    let mut pipeline = Pipeline::new();
    let main = pipeline.add_source("main.rls", "@use a as one .");
    let module = pipeline.add_source("a.rls", "@use b as two .");
    let inner = pipeline.add_source("b.rls", "p(1) .");
    let using = |alias: &str, file: FileId| UseDirective {
        module: ModuleName {
            parents: 0,
            segments: vec![alias.to_owned()],
        },
        bindings: Vec::new(),
        values: Vec::new(),
        alias: alias.to_owned(),
        span: Some(Span {
            file,
            start: 0,
            end: 1,
        }),
    };
    let one = pipeline.add_instance(module, using("one", main));
    let two = pipeline.add_instance(inner, using("two", one));
    let aliases: Vec<&str> = pipeline
        .instances(two)
        .iter()
        .map(|directive| directive.alias.as_str())
        .collect();
    assert_eq!(aliases, ["one", "two"]);
    assert_eq!(pipeline.file_name(two), Some("b.rls"));
    assert_eq!(pipeline.text(two), Some("p(1) ."));
    // The file itself is no instance.
    assert!(pipeline.instances(inner).is_empty());
}

#[test]
fn stages_over_an_extension_keep_the_loaded_statements() {
    let (mut pipeline, revision) = loaded();
    let addition = pipeline.load(Program::new(vec![rule("next", "edge")]));
    let merged = pipeline.merge(revision, addition);
    let mut stages = Stages::new();
    stages.push(RequiringStage {
        name: "rewrite-rules",
        requires: &[],
        rewrite: |commit| {
            assert_eq!(commit.all_ids().len(), 4);
            for id in commit.ids() {
                if matches!(commit.statement(id), Statement::Rule(_)) {
                    commit.split(id, vec![rule("next", "link")]);
                }
            }
        },
    });
    let after = pipeline.extend(&stages, merged, 3).expect("the stage runs");
    assert_eq!(
        pipeline.program(after).to_string(),
        "edge(1) .
reach(?x) :- edge(?x) .
edge(2) .
next(?x) :- link(?x) ."
    );
}

#[test]
#[should_panic(expected = "rewrote a frozen statement")]
fn rewriting_a_frozen_statement_panics() {
    let (mut pipeline, revision) = loaded();
    let ids = pipeline.revision_statements(revision).to_vec();
    let mut commit = Commit::new(&mut pipeline, revision, "late", 3);
    commit.remove(ids[1]);
}

#[test]
fn fresh_names_avoid_every_name_in_use() {
    let mut pipeline = Pipeline::new();
    let revision = pipeline.load(Program::new(vec![fact("part1", 1), rule("reach", "part2")]));
    let mut commit = pipeline.commit(revision, "fold");
    assert_eq!(commit.fresh("part"), Identifier::bare("part3"));
    assert_eq!(commit.fresh("part"), Identifier::bare("part4"));
    assert_eq!(commit.fresh("other"), Identifier::bare("other1"));
}

#[test]
fn reports_accumulate_in_one_sink() {
    let (mut pipeline, _) = loaded();
    pipeline.report(Diagnostic::warning(
        nemo_physical::diagnostics::Code::new(999),
        "something noteworthy",
    ));
    assert_eq!(pipeline.reports().len(), 1);
}

#[test]
fn substitutions_add_definition_labels_to_diagnostics() {
    let mut pipeline = Pipeline::new();
    let file = pipeline.add_source("program.rls", "");
    let occurrence = Span {
        file,
        start: 20,
        end: 22,
    };
    let definition = Span {
        file,
        start: 0,
        end: 10,
    };
    let mut written = rule("reach", "edge");
    if let Statement::Rule(written) = &mut written
        && let Literal::Positive(atom) = &mut written.body[0]
    {
        atom.terms[0].span = Some(occurrence);
    }
    let revision = pipeline.load(Program::new(vec![written]));
    let ids = pipeline.revision_statements(revision).to_vec();

    let mut commit = pipeline.commit(revision, "substitute");
    let mut value = Term::integer(5);
    value.span = Some(definition);
    let substitution: Substitution = [(Variable::universal("x"), value)].into_iter().collect();
    let new = commit.substitute(ids[0], &substitution);
    commit.finish();

    // A diagnostic labeled inside the occurrence gains the
    // definition label; one elsewhere does not.
    let inside = Diagnostic::error(nemo_physical::diagnostics::Code::new(999), "bad value")
        .with_label(
            Span {
                file,
                start: 20,
                end: 21,
            },
            "here",
        );
    let refined = pipeline.refine(inside, new);
    assert_eq!(refined.labels().len(), 2);
    assert_eq!(refined.labels()[1].span, definition);

    let outside = Diagnostic::error(nemo_physical::diagnostics::Code::new(999), "bad value")
        .with_label(
            Span {
                file,
                start: 40,
                end: 41,
            },
            "here",
        );
    let refined = pipeline.refine(outside, new);
    assert_eq!(refined.labels().len(), 1);
}
