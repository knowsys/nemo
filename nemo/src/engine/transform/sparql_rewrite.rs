//! The SPARQL rewrite stage:
//! rules over plain `sparql` imports become rules over bound imports,
//! so that the endpoint is queried only for what the rules need.
//!
//! A *plain* import is a `sparql` import without a `where` clause.
//! A *bound* import has a `where` clause, whose binding atoms restrict what it fetches.
//! A plain import `@import t from sparql { query = "SELECT ?s ?p ?o …" }` read by a rule
//! fetches the whole query result, however little of it the rule reads.
//! This stage rewrites each rule reading such imports:
//!
//! - The rule's atoms over one endpoint that share variables are merged into one query:
//!   their patterns are instantiated at the atoms, constants substituted,
//!   private variables renamed apart,
//!   and negated atoms whose variables the rest binds become `FILTER NOT EXISTS`.
//! - The query projects what the rest of the rule reads.
//! - Every group of local atoms sharing variables with the merged atoms becomes a
//!   *binding predicate*, defined by a fresh rule over those atoms,
//!   and the new import is bound by them.
//!   The group computes what its variables determine:
//!   an assignment like `?label = STRLANG(?org, "en")`,
//!   and an argument of a merged atom like `STRLANG(?org, "en")`, which the query sees as a
//!   fresh variable.
//! - The rule reads the new import in place of the merged atoms.
//!
//! A query calling a `SERVICE` keeps its variable names and is never merged,
//! since a service may read or bind variables by name,
//! as Wikidata's label service binds `?xLabel` for `?x`.
//!
//! A rule that only copies a bound import into its head is folded into the import itself.
//! A plain import no rule reads anymore is removed.
//! Everything the stage writes is program syntax,
//! and the revision it produces prints and parses back to itself.
//!
//! Queries whose result cannot be restricted (a `LIMIT`, a nondeterministic construct)
//! are left alone, as are atoms whose rewrite would not reduce what they fetch:
//! one atom of distinct variables sharing nothing with the local atoms.
//!
//! The stage surveys the program into a [RewriteContext],
//! rewrites each [rule] over the [components] of its body,
//! and emits the [statements] replacing it.

mod components;
mod context;
mod ground;
mod rule;
mod statements;
mod variables;

use std::collections::HashSet;

use nemo_physical::diagnostics::Diagnostic;

use crate::{
    model::{Atom, Directive, Identifier, ImportDirective, Rule, Statement},
    pipeline::{Commit, StatementId, Transformation, grouped_paths},
};

use self::{
    context::RewriteContext,
    rule::{ComponentReplacement, RuleRewrite},
    variables::variable_terms,
};

/// The SPARQL rewrite stage (see the module documentation).
///
/// If the program is extended, the statements loaded earlier are frozen,
/// since they were rewritten when they were loaded, and their rules must keep their indices.
/// The extension's rules are rewritten with the whole program in scope.
#[derive(Debug, Default)]
pub(crate) struct SparqlRewrite;

impl SparqlRewrite {
    /// Removes each candidate import no rule reads anymore,
    /// since the rewritten rules query its endpoint directly
    /// (see [Commit::remove_import]).
    ///
    /// A frozen import is kept, even if nothing reads it.
    fn remove_unread(&self, commit: &mut Commit<'_>, context: &RewriteContext) {
        let mut read: HashSet<String> = HashSet::new();
        for id in commit.all_ids() {
            let Statement::Rule(rule) = commit.statement(id) else {
                continue;
            };
            for literal in &rule.body {
                let Some(atom) = literal.atom() else {
                    continue;
                };
                if let Ok(name) = context.scope.resolve(&atom.predicate, atom.span) {
                    read.insert(name);
                }
            }
        }
        for (name, candidate) in &context.candidates {
            if !read.contains(name) && !commit.is_frozen(candidate.id) {
                commit.remove_import(candidate.id);
            }
        }
    }
}

impl Transformation for SparqlRewrite {
    fn name(&self) -> &'static str {
        "sparql"
    }

    fn apply(&self, commit: &mut Commit<'_>) -> Result<(), Diagnostic> {
        let ids = commit.all_ids();
        let statements: Vec<Statement> =
            ids.iter().map(|&id| commit.statement(id).clone()).collect();
        if !used(&statements) {
            return Ok(());
        }
        let mut context = RewriteContext::new(&statements, &ids);
        if context.candidates.is_empty() {
            return Ok(());
        }
        for (&id, statement) in ids.iter().zip(&statements) {
            let Statement::Rule(rule) = statement else {
                continue;
            };
            if commit.is_frozen(id) {
                continue;
            }
            if let Some(rewrite) = context.rewrite(rule, commit) {
                declare(commit, id, rule, rewrite);
            }
        }
        self.remove_unread(commit, &context);
        Ok(())
    }
}

/// Writes a rule's rewrite into the commit as declared rewrites.
///
/// Each binding rule is added as a filter ([Commit::define_filter]):
/// a rule whose predicate other statements use only to restrict their matches.
/// Each bound import defines its predicate by the atoms it replaces,
/// and the rewritten rule replaces the original.
/// If the rule was folded into its one import, that import replaces the rule instead.
fn declare(commit: &mut Commit<'_>, id: StatementId, rule: &Rule, rewrite: RuleRewrite) {
    let mut sources = vec![id];
    sources.extend(rewrite.imports.iter().copied());
    let folded = rewrite.rule.is_none();
    // The rule reads each replacement's import at the position of its first atom.
    let groups: Vec<&[usize]> = rewrite
        .replacements
        .iter()
        .map(|replacement| replacement.positions.as_slice())
        .collect();
    let literals = grouped_paths(rule.body.len(), &groups);
    for replacement in rewrite.replacements {
        for binding in &replacement.bindings {
            commit.define_filter(binding.clone(), &sources);
        }
        if let Some(declaration) = &replacement.declaration {
            commit.add(
                Statement::Directive(Directive::Declare(declaration.clone())),
                &sources,
            );
        }
        if folded {
            commit.extract_rule(id, replacement.import, &rewrite.imports);
        } else {
            let view = view(&replacement);
            commit.define_import(replacement.import, view, &sources);
        }
    }
    if let Some(rewritten) = rewrite.rule {
        commit.replace_rule(id, rewritten, literals);
    }
}

/// Returns the view of a replacement's import: its atom, defined by the atoms it replaces.
fn view(replacement: &ComponentReplacement) -> Rule {
    let atom = Atom::new(
        replacement.import.predicate.clone(),
        variable_terms(&replacement.import.variables),
    );
    Rule::new(vec![atom], replacement.definition.clone())
}

/// Returns whether a program has a plain `sparql` import and a rule,
/// which decides whether the stage looks any further.
fn used(statements: &[Statement]) -> bool {
    let mut imports = false;
    let mut rules = false;
    for statement in statements {
        match statement {
            Statement::Directive(Directive::Import(import)) => {
                imports |= is_plain(import);
            }
            Statement::Rule(_) => rules = true,
            _ => {}
        }
    }
    imports && rules
}

/// Returns whether an import directive names the `sparql` format.
fn is_sparql(import: &ImportDirective) -> bool {
    matches!(&import.format, Identifier::Bare(name) if name == "sparql")
}

/// Returns whether a statement is a plain `sparql` import,
/// one the stage may rewrite rules over and removes once no rule reads it.
pub(crate) fn is_plain_sparql_import(statement: &Statement) -> bool {
    matches!(statement, Statement::Directive(Directive::Import(import)) if is_plain(import))
}

/// Returns whether an import directive is a plain `sparql` import: one without a `where` clause.
///
/// The stage gives a rewritten import other columns,
/// so it leaves alone an import whose conditions read its columns.
fn is_plain(import: &ImportDirective) -> bool {
    is_sparql(import) && import.bindings.is_empty() && import.conditions.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        pipeline::{Pipeline, Stages},
        testing::parse,
    };

    /// Runs the stage over a program, returning the revision's text.
    fn rewrite(source: &str) -> String {
        let program = parse(source);
        assert!(used(&program.statements));
        let mut pipeline = Pipeline::new();
        let mut stages = Stages::new();
        stages.push(SparqlRewrite);
        let base = pipeline.load(program);
        let revision = pipeline
            .apply(&stages, base)
            .unwrap_or_else(|problem| panic!("{problem:?}"));
        let program = pipeline.program(revision);
        let text = program.to_string();
        // The revision parses back to itself.
        assert_eq!(parse(&text), program, "{text}");
        text
    }

    /// A plain import of every triple of an endpoint.
    const TRIPLES: &str = r#"@prefix wdt: <http://www.wikidata.org/prop/direct/> .
@prefix wd: <http://www.wikidata.org/entity/> .
@import t from sparql { endpoint = <https://query.wikidata.org/sparql>, query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }" } .
"#;

    #[test]
    fn a_constant_atom_specialises_the_query_and_folds_into_its_head() {
        let text = rewrite(&format!(
            "{TRIPLES}@parameter $kind = wd:Q1 .\nevent(?e) :- t(?e, wdt:P31, $kind) .\n@output event .\n"
        ));
        assert!(
            text.contains("@import event(?e) from sparql { endpoint = <https://query.wikidata.org/sparql>, query = \"SELECT ?e WHERE { ?e <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q1> . }\" } ."),
            "{text}"
        );
        assert!(
            !text.contains("@import t "),
            "the plain import is gone: {text}"
        );
        assert!(!text.contains(":- "), "the copying rule folded: {text}");
    }

    #[test]
    fn chained_atoms_merge_and_bind_on_the_local_atom() {
        let text = rewrite(&format!(
            "{TRIPLES}sportsEvent(a) .\nnext(?e, ?n) :- sportsEvent(?e), t(?e, wdt:P1, ?stmt), t(?stmt, wdt:P2, ?n) .\n@output next .\n"
        ));
        assert!(
            text.contains("t_bindings1(?e) :- sportsEvent(?e) ."),
            "{text}"
        );
        assert!(
            text.contains("@import t_query1(?e, ?n) from sparql { endpoint = <https://query.wikidata.org/sparql>, query = \"SELECT ?e ?n WHERE { ?e <http://www.wikidata.org/prop/direct/P1> ?stmt . ?stmt <http://www.wikidata.org/prop/direct/P2> ?n . }\" } where t_bindings1(?e) ."),
            "{text}"
        );
        assert!(
            text.contains("next(?e, ?n) :- sportsEvent(?e), t_query1(?e, ?n) ."),
            "{text}"
        );
    }

    #[test]
    fn a_computed_argument_binds_the_import() {
        let text = rewrite(&format!(
            "{TRIPLES}org(\"Shell\") .\nbusiness(?id) :- org(?org), t(?id, wdt:P1, STRLANG(?org, \"en\")) .\n@output business .\n"
        ));
        assert!(
            text.contains(
                "t_bindings1(?computed1) :- org(?org), ?computed1 = STRLANG(?org, \"en\") ."
            ),
            "{text}"
        );
        assert!(text.contains("where t_bindings1(?computed1) ."), "{text}");
        // The rule compares the fetched column with the argument as written.
        assert!(
            text.contains("business(?id) :- org(?org), t_query1(?id, STRLANG(?org, \"en\")) ."),
            "{text}"
        );
    }

    #[test]
    fn an_assignment_binds_the_import() {
        let text = rewrite(&format!(
            "{TRIPLES}org(\"Shell\") .\nbusiness(?id) :- org(?org), ?label = STRLANG(?org, \"en\"), t(?id, wdt:P1, ?label) .\n@output business .\n"
        ));
        assert!(
            text.contains("t_bindings1(?label) :- org(?org), ?label = STRLANG(?org, \"en\") ."),
            "{text}"
        );
        assert!(text.contains("where t_bindings1(?label) ."), "{text}");
    }

    #[test]
    fn a_query_calling_a_service_keeps_its_variable_names() {
        // Wikidata's label service binds `?qidLabel` because the query names `?qid`.
        let labels = "@import wdLabel from sparql { endpoint = <https://query.wikidata.org/sparql>, query = \"SELECT ?qid ?qidLabel WHERE { SERVICE <http://wikiba.se/ontology#label> { <http://www.bigdata.com/rdf#serviceParam> <http://wikiba.se/ontology#language> 'en' } }\" } .\n";
        let text = rewrite(&format!(
            "{labels}item(<urn:a>) .\nnamed(?item, ?name) :- item(?item), wdLabel(?item, ?name) .\none(?name) :- wdLabel(<urn:b>, ?name) .\n@output named .\n@output one .\n"
        ));
        assert!(
            text.contains("@import wdLabel_query1(?item, ?name) from sparql"),
            "{text}"
        );
        assert!(text.contains("SELECT ?qid ?qidLabel WHERE"), "{text}");
        assert!(
            text.contains("named(?item, ?name) :- item(?item), wdLabel_query1(?item, ?name) ."),
            "{text}"
        );
        // A constant binds the variable the service reads, though no pattern mentions it.
        assert!(text.contains("VALUES ( ?qid )"), "{text}");
        assert!(text.contains("SELECT ?qidLabel WHERE"), "{text}");
    }

    #[test]
    fn a_negated_remote_atom_becomes_a_filter_not_exists() {
        let text = rewrite(&format!(
            "{TRIPLES}inverse(?p, ?i) :- t(?p, wdt:P1, ?i) .\nviolation(?s, ?o) :- inverse(?p, ?i), t(?s, ?p, ?o), ~t(?o, ?i, ?s) .\n@output violation .\n"
        ));
        assert!(text.contains("FILTER(NOT EXISTS { ?o ?i ?s . })"), "{text}");
        assert!(
            text.contains("where t_bindings1(?p, ?i)"),
            "the negation's variables come from the binding: {text}"
        );
    }

    #[test]
    fn a_negation_bridging_two_components_joins_them_into_one_query() {
        // The negated atom refers to ?i, which only an atom sharing no
        // variable with the other four binds.
        let text = rewrite(&format!(
            "{TRIPLES}inverse(?p, ?i) :- t(?p, wdt:P1, ?i) .\n\
             violation(?stmt, ?prop, ?inverse) :- inverse(?prop, ?inverse), \
             t(?prop, wdt:claim, ?p), t(?prop, wdt:statementProperty, ?ps), \
             t(?inverse, wdt:directClaim, ?i), t(?subject, ?p, ?stmt), \
             t(?stmt, ?ps, ?object), ~t(?object, ?i, ?subject) .\n@output violation .\n"
        ));
        assert!(
            text.contains("FILTER(NOT EXISTS { ?object ?i ?subject . })"),
            "{text}"
        );
        // One import for the whole body, bound by the local atom.
        assert!(
            text.contains("@import t_query2(?prop, ?stmt, ?inverse) from sparql"),
            "{text}"
        );
        assert!(
            text.contains("where t_bindings1(?prop, ?inverse) ."),
            "{text}"
        );
        assert!(
            text.contains("violation(?stmt, ?prop, ?inverse) :- inverse(?prop, ?inverse), t_query2(?prop, ?stmt, ?inverse) ."),
            "{text}"
        );
    }

    #[test]
    fn a_recursive_binding_reaches_the_import() {
        let text = rewrite(&format!(
            "{TRIPLES}anc(wd:Q7) .\nanc(?p) :- anc(?c), t(?c, wdt:P22, ?p) .\n@output anc .\n"
        ));
        assert!(text.contains("t_bindings1(?c) :- anc(?c) ."), "{text}");
        assert!(text.contains("where t_bindings1(?c) ."), "{text}");
        assert!(
            text.contains("anc(?p) :- anc(?c), t_query1(?c, ?p) ."),
            "{text}"
        );
    }

    #[test]
    fn plain_atoms_and_opaque_queries_stay_as_written() {
        let text = rewrite(&format!("{TRIPLES}copy(?s, ?p, ?o) :- t(?s, ?p, ?o) .\n"));
        assert!(
            text.contains("copy(?s, ?p, ?o) :- t(?s, ?p, ?o) ."),
            "{text}"
        );
        assert!(text.contains("@import t from sparql"), "{text}");

        let source = "@import t from sparql { endpoint = <https://e/sparql>, query = \"SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 5\" } .\nq(?s) :- t(?s, <urn:p>, <urn:o>) .\n";
        let text = rewrite(source);
        assert!(
            text.contains("q(?s) :- t(?s, <urn:p>, <urn:o>) ."),
            "{text}"
        );
    }

    #[test]
    fn a_program_declaring_no_outputs_rewrites_its_imports() {
        // Its outputs are the derived predicates, so the triples are a
        // candidate as in a program declaring `@output anc`.
        let text = rewrite(&format!(
            "{TRIPLES}anc(wd:Q7) .\nanc(?p) :- anc(?c), t(?c, wdt:P22, ?p) .\n"
        ));
        assert!(!text.contains("@import t from sparql"), "{text}");
        assert!(text.contains("where t_bindings1(?c) ."), "{text}");
    }

    #[test]
    fn two_local_groups_bind_separately() {
        let text = rewrite(&format!(
            "{TRIPLES}p(a) . q(b) .\nh(?x, ?y) :- p(?x), q(?y), t(?x, wdt:P1, ?y) .\n@output h .\n"
        ));
        assert!(text.contains("t_bindings1(?x) :- p(?x) ."), "{text}");
        assert!(text.contains("t_bindings2(?y) :- q(?y) ."), "{text}");
        assert!(
            text.contains("where t_bindings1(?x), t_bindings2(?y) ."),
            "{text}"
        );
    }
}
