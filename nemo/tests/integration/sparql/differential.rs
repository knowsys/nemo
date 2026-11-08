//! The rewrite stage keeps what a program means: each program runs twice against one endpoint
//! that evaluates its queries over a fixed dataset, once as written and once as the rewrite
//! stage makes it, and both runs must derive the same facts.
//!
//! The endpoint keeps terms as written and labels each blank node the same in every response,
//! as the rewrite assumes of a store (see the `sparql` stage).
//! Its data holds values in Nemo's canonical terms, except under `ex:odd`,
//! whose other spellings a binding or a constant misses by design,
//! and under `ex:quantity`, which holds integers as decimals, as Wikidata does.

use std::sync::Arc;

use nemo::{engine::Engine, pipeline::Stages};
use nemo_io::sparql::spargebra::SparqlParser;
use oxrdf::{Dataset, GraphName};
use oxttl::TurtleParser;
use sparesults::{QueryResultsFormat, QueryResultsSerializer};
use spareval::{QueryEvaluator, QueryResults};

use crate::common::{Responder, parse_into, rows};

/// The endpoint's data.
const DATA: &str = r#"
@prefix ex: <http://ex.org/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:alice a ex:Person ; ex:name "Alice"@en ; ex:age 70 ; ex:email "a@x" ;
    ex:parent ex:bob ; ex:knows ex:bob .
ex:bob a ex:Person ; ex:name "Bob"@en, "Robert"@de ; ex:age 45 ;
    ex:parent ex:carol ; ex:knows ex:carol ; ex:child ex:alice .
ex:carol a ex:Person ; ex:name "Carol" ; ex:age 20 ; ex:email "c@x" ;
    ex:parent ex:dave ; ex:knows ex:alice ; ex:child ex:bob .
ex:dave a ex:Person ; ex:age "unknown" ; ex:knows ex:erin .
ex:erin a ex:Robot ; ex:age 33 ; ex:parent ex:frank .
ex:frank a ex:Person ; ex:knows ex:frank ; ex:child ex:erin .

ex:parent ex:inverse ex:child .

ex:shell ex:name "Shell"@en ; ex:founded 1907 .
ex:bp ex:name "BP"@en .

ex:m1 ex:value 1.75, "2.5E0"^^xsd:double, "1990-01-01"^^xsd:date, true, "colour"@en-gb .
ex:m2 ex:value 3, "-5.0E0"^^xsd:double, "2024-02-29"^^xsd:date, false, "color"@en-us .
ex:m3 ex:value "2024-01-01T12:00:00Z"^^xsd:dateTime, "P1D"^^xsd:dayTimeDuration,
    "NaN"^^xsd:double, 4 .
ex:m4 ex:value "2024-02-29Z"^^xsd:date, "4.0E0"^^xsd:double .
ex:m5 ex:other "x" .

# Values in other spellings than Nemo's canonical terms: Nemo reads each as one of the values
# above, but a store keeping terms as written finds none through them.
ex:u1 ex:odd "03"^^xsd:int, "3"^^xsd:int, "+4"^^xsd:integer,
    "4"^^xsd:long, "-0.5e1"^^xsd:double, "1"^^xsd:boolean,
    "2024-01-01T12:00:00.000Z"^^xsd:dateTime, "2024-01-01T12:00:00+00:00"^^xsd:dateTime,
    "PT24H"^^xsd:duration, "P1D"^^xsd:duration .
# Integers as decimals, as Wikidata holds its quantities: Nemo reads them as integers.
ex:q1 ex:quantity "3700577.0"^^xsd:decimal .
ex:q2 ex:quantity "12.0"^^xsd:decimal, 2.5 .

ex:t1 ex:text "say \"hi\"\tnow\nthen", "back\\slash", "Grüße, 東京" .
ex:t2 ex:text "plain" .
<http://ex.org/space%20d/ü> ex:text "an IRI with escapes" .

# Blank nodes: an OWL-style restriction and an RDF list.
ex:Parent ex:subClassOf [ a ex:Restriction ; ex:onProperty ex:hasChild ] .
ex:Parent ex:unionOf ( ex:Mother ex:Father ) .

ex:e1 a ex:Event ; ex:p1 ex:s1 . ex:s1 ex:p2 ex:n1 .
ex:e2 a ex:Event ; ex:p1 ex:s2 . ex:s2 ex:p2 ex:n2 .
ex:e3 a ex:Event ; ex:p1 ex:s3 .
"#;

/// The prefixes and the plain import of every triple that the programs share.
const PRELUDE: &str = r#"
@prefix ex: <http://ex.org/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@import t from sparql {
    endpoint = <http://endpoint.test/sparql>,
    query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
} .
"#;

/// Returns the dataset the endpoint holds.
fn dataset() -> Arc<Dataset> {
    let mut dataset = Dataset::new();
    for triple in TurtleParser::new().for_slice(DATA) {
        let triple = triple.expect("the data is valid Turtle");
        dataset.insert(&triple.in_graph(GraphName::DefaultGraph));
    }
    Arc::new(dataset)
}

/// Evaluates a query over the dataset, returning its solutions as SPARQL TSV.
fn evaluate(dataset: &Dataset, text: &str) -> String {
    let query = SparqlParser::new()
        .parse_query(text)
        .unwrap_or_else(|problem| panic!("the engine sends valid SPARQL: {problem}\n{text}"));
    let results = QueryEvaluator::new()
        .prepare(&query)
        .execute(dataset)
        .unwrap_or_else(|problem| panic!("the query evaluates: {problem}\n{text}"));
    let QueryResults::Solutions(solutions) = results else {
        panic!("the engine sends SELECT queries: {text}");
    };
    let mut serializer = QueryResultsSerializer::from_format(QueryResultsFormat::Tsv)
        .serialize_solutions_to_writer(Vec::new(), solutions.variables().to_vec())
        .expect("writes to memory");
    for solution in solutions {
        let solution = solution.unwrap_or_else(|problem| panic!("{problem}\n{text}"));
        serializer
            .serialize(solution.iter())
            .expect("writes to memory");
    }
    String::from_utf8(serializer.finish().expect("writes to memory")).expect("TSV is UTF-8")
}

/// What a run derived, what it asked the endpoint, and what it reported.
struct Run {
    /// Per output predicate, its rows with nulls masked, sorted.
    outputs: Vec<(String, Vec<String>)>,
    /// The query texts sent, in order.
    queries: Vec<String>,
    /// How many of them reasoning sent, the fetches of bound imports.
    fetched: usize,
    /// The codes of the reports of the run.
    reports: Vec<nemo_physical::diagnostics::Code>,
}

/// Runs a program against the endpoint, with or without the rewrite stage.
fn run(source: &str, outputs: &[&str], rewrite: bool, dataset: &Arc<Dataset>) -> Run {
    let data = Arc::clone(dataset);
    let endpoint = Responder::endpoint(move |text| evaluate(&data, text));
    let mut engine = Engine::new();
    if !rewrite {
        *engine.stages_mut() = Stages::new();
    }
    let program = parse_into(&mut engine, source);
    let stage = if rewrite { "rewritten" } else { "as written" };
    endpoint
        .serve(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{stage}: the program loads: {problem:?}"));
    let loaded = endpoint.queries().len();
    endpoint
        .serve(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{stage}: the run reaches its fixpoint: {problem:?}"));
    let outputs = outputs
        .iter()
        .map(|&predicate| {
            let mut rows: Vec<String> = rows(&mut engine, predicate)
                .iter()
                .map(|row| mask_nulls(row))
                .collect();
            rows.sort();
            (predicate.to_owned(), rows)
        })
        .collect();
    let queries = endpoint.queries();
    Run {
        outputs,
        fetched: queries.len() - loaded,
        queries,
        reports: engine
            .reports()
            .iter()
            .map(|report| report.code())
            .collect(),
    }
}

/// Replaces the label of every null (`_:12`) by `_:`, since two runs number their nulls apart.
fn mask_nulls(row: &str) -> String {
    let mut masked = String::with_capacity(row.len());
    let mut rest = row;
    while let Some(start) = rest.find("_:") {
        masked.push_str(&rest[..start + 2]);
        rest = rest[start + 2..].trim_start_matches(|c: char| c.is_ascii_digit());
    }
    masked.push_str(rest);
    masked
}

/// How the rewrite stage is expected to fetch a program's data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fetch {
    /// Through a bound import, which reasoning fetches.
    Bound,
    /// Without bindings: every query goes out at load.
    Unbound,
}

/// Runs a program as written and rewritten, and checks that both derive the same facts,
/// that the program derives something, and that the rewrite fetches as expected.
fn check(program: &str, outputs: &[&str], fetch: Fetch) {
    let source = format!("{PRELUDE}{program}");
    let dataset = dataset();
    let written = run(&source, outputs, false, &dataset);
    let rewritten = run(&source, outputs, true, &dataset);
    assert!(
        written.outputs.iter().any(|(_, rows)| !rows.is_empty()),
        "the program derives something: {:?}",
        written.outputs
    );
    assert_eq!(
        written.outputs, rewritten.outputs,
        "the rewrite changed the results\nqueries as written: {:#?}\nqueries rewritten: {:#?}",
        written.queries, rewritten.queries
    );
    assert_eq!(
        rewritten.fetched > 0,
        fetch == Fetch::Bound,
        "expected {fetch:?}: {:#?}",
        rewritten.queries
    );
}

/// Checks that a bound import derives what the same query does as a plain import whose rows
/// a rule joins with the bindings.
///
/// The query projects `columns`; `facts` hold the binding rows, `bindings` are the import's
/// `where` atoms, and `head` is the head the rows go into, as `out(…)`.
fn check_bound_import(query: &str, columns: &str, facts: &str, bindings: &str, head: &str) {
    let import = |name: &str, signature: &str| {
        format!(
            "@import {name}{signature} from sparql {{\n    endpoint = <http://endpoint.test/sparql>,\n    query = \"{query}\"\n}}"
        )
    };
    let bound = format!(
        "{} where {bindings} .\n{facts}\n{head} :- fetched({columns}) .\n@output out .\n",
        import("fetched", &format!("({columns})"))
    );
    let joined = format!(
        "{} .\n{facts}\n{head} :- {bindings}, fetched({columns}) .\n@output out .\n",
        import("fetched", "")
    );
    let dataset = dataset();
    let bound = run(&format!("{PRELUDE}{bound}"), &["out"], true, &dataset);
    // The join stays as written: the rewrite stage would bind the plain import too.
    let joined = run(&format!("{PRELUDE}{joined}"), &["out"], false, &dataset);
    assert!(
        !joined.outputs[0].1.is_empty(),
        "the program derives something"
    );
    assert!(
        bound.fetched > 0,
        "the import is bound: {:#?}",
        bound.queries
    );
    assert_eq!(
        bound.outputs, joined.outputs,
        "the bound import changed the results\nbound queries: {:#?}",
        bound.queries
    );
}

#[test]
fn typed_literals_bind_as_the_same_terms() {
    check(
        r#"
wanted(1.75) . wanted("1990-01-01"^^<http://www.w3.org/2001/XMLSchema#date>) .
wanted(true) . wanted("colour"@en-GB) . wanted("color"@en-us) . wanted(1) .
holder(?x, ?v) :- wanted(?v), t(?x, ex:value, ?v) .
@output holder .
"#,
        &["holder"],
        Fetch::Bound,
    );
}

/// Numbers go out as their canonical terms, `"3"^^xsd:integer` and `"2.5E0"^^xsd:double`,
/// which find the values the endpoint holds in those terms.
#[test]
fn numbers_bind_as_their_canonical_terms() {
    check(
        r#"
wanted(3.0) . wanted(2.5E0) . wanted(-5.0E0) . wanted(4) .
holder(?x, ?v) :- wanted(?v), t(?x, ex:value, ?v) .
three(?x) :- t(?x, ex:value, 3) .
@output holder, three .
"#,
        &["holder", "three"],
        Fetch::Bound,
    );
}

#[test]
fn bound_numbers_match_their_canonical_terms() {
    check_bound_import(
        "SELECT ?x ?v WHERE { ?x <http://ex.org/value> ?v }",
        "?x, ?v",
        "wanted(3.0) . wanted(2.5E0) . wanted(-5.0E0) . wanted(4) .",
        "wanted(?v)",
        "out(?x, ?v)",
    );
}

/// The other values Nemo writes in canonical terms: booleans, dateTimes, dates,
/// a duration given in another spelling (`PT24H` is `P1D`), and NaN,
/// which a term matches although SPARQL's `=` never holds for it.
const OTHER_VALUES: &str = r#"wanted(true) . wanted("2024-01-01T12:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>) .
wanted("2024-02-29"^^<http://www.w3.org/2001/XMLSchema#date>) .
wanted("PT24H"^^<http://www.w3.org/2001/XMLSchema#dayTimeDuration>) .
wanted("NaN"^^<http://www.w3.org/2001/XMLSchema#double>) ."#;

#[test]
fn other_values_bind_as_their_canonical_terms() {
    check(
        &format!(
            r#"{OTHER_VALUES}
holder(?x, ?v) :- wanted(?v), t(?x, ex:value, ?v) .
truth(?x) :- t(?x, ex:value, true) .
oneDay(?x) :- t(?x, ex:value, "P1D"^^<http://www.w3.org/2001/XMLSchema#dayTimeDuration>) .
@output holder, truth, oneDay .
"#
        ),
        &["holder", "truth", "oneDay"],
        Fetch::Bound,
    );
}

#[test]
fn bound_other_values_match_their_canonical_terms() {
    check_bound_import(
        "SELECT ?x ?v WHERE { ?x <http://ex.org/value> ?v }",
        "?x, ?v",
        OTHER_VALUES,
        "wanted(?v)",
        "out(?x, ?v)",
    );
}

#[test]
fn a_negated_constant_excludes_its_term() {
    check(
        r#"
item(ex:m1) . item(ex:m2) . item(ex:m3) .
notThree(?x) :- item(?x), ~t(?x, ex:value, 3) .
@output notThree .
"#,
        &["notThree"],
        Fetch::Bound,
    );
}

/// The documented limit of the rewrite: a store keeping terms as written finds a value through a
/// binding or a constant only in the terms Nemo sends. It misses `"3"^^xsd:int`,
/// `"+4"`, `"4"^^xsd:long`, `"-0.5e1"`, `+00:00`, `.000Z` and the `xsd:duration`s,
/// which Nemo reads, as written, as the values it asks for,
/// and which a store canonicalising them on load would find.
#[test]
fn values_held_in_other_spellings_are_missed_by_bindings() {
    let source = format!(
        r#"{PRELUDE}
wanted(3) . wanted(4) . wanted(-5.0E0) .
wanted("2024-01-01T12:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>) .
wanted("P1D"^^<http://www.w3.org/2001/XMLSchema#dayTimeDuration>) .
odd(?v) :- wanted(?v), t(ex:u1, ex:odd, ?v) .
@output odd .
"#
    );
    let dataset = dataset();
    let written = run(&source, &["odd"], false, &dataset);
    let rewritten = run(&source, &["odd"], true, &dataset);
    assert_eq!(written.outputs[0].1.len(), 5, "{:?}", written.outputs);
    assert!(rewritten.fetched > 0, "{:#?}", rewritten.queries);
    assert_eq!(rewritten.outputs[0].1.len(), 0, "{:?}", rewritten.outputs);
}

/// Checks that a condition pushed into a plain import of the values under `ex:value` keeps what
/// the rule keeps (see [check_condition_on]).
fn check_condition(condition: &str) -> Vec<String> {
    check_condition_on("value", condition)
}

/// Checks that a condition pushed into a plain import of the values under a property keeps what
/// the rule keeps (see [compare_condition]), and that it is pushed.
fn check_condition_on(property: &str, condition: &str) -> Vec<String> {
    let queries = compare_condition(property, condition);
    assert!(
        queries
            .iter()
            .any(|text| text.contains("FILTER") || text.contains("VALUES")),
        "the condition is pushed: {queries:#?}"
    );
    queries
}

/// Checks that whatever part of a condition goes to the endpoint keeps what the rule keeps:
/// the program with the rule as the import's only reader, whose condition may be pushed,
/// derives what it derives with a second, unfiltered reader, which keeps the condition local.
///
/// The import may leave a variable unbound, so the rewrite stage leaves it alone.
/// Returns the queries the condition went out with.
fn compare_condition(property: &str, condition: &str) -> Vec<String> {
    let program = format!(
        r#"
@import values from sparql {{
    endpoint = <http://endpoint.test/sparql>,
    query = "SELECT ?x ?v ?o WHERE {{ ?x <http://ex.org/{property}> ?v OPTIONAL {{ ?x <http://ex.org/other> ?o }} }}"
}} .
out(?x, ?v) :- values(?x, ?v, ?o), {condition} .
@output out .
"#
    );
    let dataset = dataset();
    let pushed = run(&format!("{PRELUDE}{program}"), &["out"], true, &dataset);
    let local = run(
        &format!("{PRELUDE}{program}all(?x, ?v, ?o) :- values(?x, ?v, ?o) .\n@output all .\n"),
        &["out"],
        true,
        &dataset,
    );
    assert!(
        !pushed.outputs[0].1.is_empty(),
        "the program derives something"
    );
    assert_eq!(
        pushed.outputs, local.outputs,
        "the pushed condition changed the results\npushed queries: {:#?}",
        pushed.queries
    );
    pushed.queries
}

#[test]
fn a_pushed_identity_with_a_number_compares_terms() {
    check_condition("?v = 3");
}

#[test]
fn a_pushed_identity_with_a_boolean_compares_terms() {
    check_condition("?v = true");
}

#[test]
fn a_pushed_lexical_form_compares_canonical_forms() {
    check_condition(r#"STR(?v) = "3""#);
}

#[test]
fn a_pushed_lexical_form_keeps_the_doubles_stores_write_differently() {
    // Nemo writes the double 2.5 as XPath does, "2.5"; an endpoint's `STR` gives the stored
    // "2.5E0" or its own writing of the number, as stores differ, so the endpoint keeps every
    // number for the local filter to decide.
    let queries = check_condition(r#"STR(?v) = "2.5""#);
    assert!(queries[0].contains("isNUMERIC(?v)"), "{queries:#?}");
}

#[test]
fn a_pushed_datatype_compares_canonical_terms() {
    check_condition("DATATYPE(?v) = <http://www.w3.org/2001/XMLSchema#integer>");
}

#[test]
fn a_pushed_language_reads_tags_in_lower_case() {
    // The evaluator writes tags in lower case too, so only the query shows it.
    let queries = check_condition(r#"LANG(?v) = "en-gb""#);
    assert!(queries[0].contains("LCASE(LANG(?v))"), "{queries:#?}");
}

#[test]
fn an_import_condition_testing_for_nulls_keeps_unbound_values() {
    // A condition of the import itself goes to the endpoint and is applied locally,
    // as the rule's filter is.
    let query = "SELECT ?x ?v ?o WHERE { ?x <http://ex.org/value> ?v OPTIONAL { ?x <http://ex.org/other> ?o } }";
    let conditioned = format!(
        "@import values(?x, ?v, ?o) from sparql {{ endpoint = <http://endpoint.test/sparql>, query = \"{query}\" }} where ISNULL(?o) .
out(?x, ?v) :- values(?x, ?v, ?o) .
@output out .
"
    );
    let filtered = format!(
        "@import values from sparql {{ endpoint = <http://endpoint.test/sparql>, query = \"{query}\" }} .
out(?x, ?v) :- values(?x, ?v, ?o), ISNULL(?o) .
all(?x, ?v, ?o) :- values(?x, ?v, ?o) .
@output out, all .
"
    );
    let dataset = dataset();
    let conditioned = run(&format!("{PRELUDE}{conditioned}"), &["out"], true, &dataset);
    let filtered = run(&format!("{PRELUDE}{filtered}"), &["out"], true, &dataset);
    assert!(
        conditioned
            .queries
            .iter()
            .any(|text| text.contains("BOUND")),
        "{:#?}",
        conditioned.queries
    );
    assert!(
        !filtered.outputs[0].1.is_empty(),
        "the program derives something"
    );
    assert_eq!(
        conditioned.outputs, filtered.outputs,
        "{:#?}",
        conditioned.queries
    );
}

#[test]
fn strings_with_escapes_bind_as_the_same_terms() {
    check(
        r#"
wanted("say \"hi\"\tnow\nthen") . wanted("back\\slash") . wanted("Grüße, 東京") .
wanted("an IRI with escapes") . wanted("missing") .
holder(?x, ?v) :- wanted(?v), t(?x, ex:text, ?v) .
everything(?x, ?v) :- t(?x, ex:text, ?v) .
@output holder, everything .
"#,
        &["holder", "everything"],
        Fetch::Bound,
    );
}

#[test]
fn bound_typed_literals_and_escapes_keep_their_meaning() {
    check_bound_import(
        "SELECT ?x ?v WHERE { ?x <http://ex.org/value>|<http://ex.org/text> ?v }",
        "?x, ?v",
        r#"wanted(1.75) . wanted("2024-02-29"^^<http://www.w3.org/2001/XMLSchema#date>) .
wanted(false) . wanted("color"@en-US) . wanted("say \"hi\"\tnow\nthen") . wanted("Grüße, 東京") ."#,
        "wanted(?v)",
        "out(?x, ?v)",
    );
}

#[test]
fn a_bound_filter_requiring_an_optional_variable_keeps_its_meaning() {
    check_bound_import(
        "SELECT ?h ?e WHERE { ?h a <http://ex.org/Person> OPTIONAL { ?h <http://ex.org/email> ?e } FILTER(BOUND(?e)) }",
        "?h, ?e",
        r#"mail("a@x") . mail("zzz") ."#,
        "mail(?e)",
        "out(?h)",
    );
}

#[test]
fn a_bound_optional_variable_matches_only_where_bound() {
    check_bound_import(
        "SELECT ?h ?e WHERE { ?h a <http://ex.org/Person> OPTIONAL { ?h <http://ex.org/email> ?e } }",
        "?h, ?e",
        r#"mail("a@x") . mail("zzz") ."#,
        "mail(?e)",
        "out(?h)",
    );
}

#[test]
fn a_bound_mandatory_variable_keeps_the_optional_one_unbound() {
    check_bound_import(
        "SELECT ?h ?e WHERE { ?h a <http://ex.org/Person> OPTIONAL { ?h <http://ex.org/email> ?e } }",
        "?h, ?e",
        "known(ex:alice) . known(ex:dave) .",
        "known(?h)",
        "out(?h, ?e)",
    );
}

#[test]
fn a_bound_computed_value_that_may_fail_matches_only_where_computed() {
    check_bound_import(
        "SELECT ?x ?n WHERE { ?x <http://ex.org/age> ?a BIND(?a + 1 AS ?n) }",
        "?x, ?n",
        "want(71) . want(46) .",
        "want(?n)",
        "out(?x, ?n)",
    );
}

#[test]
fn a_bound_union_keeps_its_meaning() {
    check_bound_import(
        "SELECT ?x ?c WHERE { { ?x <http://ex.org/email> ?c } UNION { ?x <http://ex.org/knows> ?y } }",
        "?x, ?c",
        r#"mail("c@x") . mail("zzz") ."#,
        "mail(?c)",
        "out(?x)",
    );
}

#[test]
fn a_bound_aggregate_keeps_its_meaning() {
    check_bound_import(
        "SELECT ?x (COUNT(?y) AS ?n) WHERE { ?x <http://ex.org/knows> ?y } GROUP BY ?x",
        "?x, ?n",
        "known(ex:alice) . known(ex:frank) . known(ex:nobody) .",
        "known(?x)",
        "out(?x, ?n)",
    );
}

#[test]
fn a_bound_subquery_keeps_its_meaning() {
    check_bound_import(
        "SELECT ?x ?y WHERE { ?x <http://ex.org/knows> ?y { SELECT ?y WHERE { ?y a <http://ex.org/Person> } } }",
        "?x, ?y",
        "known(ex:alice) . known(ex:dave) .",
        "known(?x)",
        "out(?x, ?y)",
    );
}

#[test]
fn two_binding_atoms_keep_their_meaning() {
    check_bound_import(
        "SELECT ?x ?y WHERE { ?x <http://ex.org/knows> ?y }",
        "?x, ?y",
        "p(ex:alice) . p(ex:dave) . q(ex:bob) . q(ex:erin) . q(ex:carol) .",
        "p(?x), q(?y)",
        "out(?x, ?y)",
    );
}

#[test]
fn a_constant_specialises_the_query() {
    check(
        "person(?x) :- t(?x, rdf:type, ex:Person) .\n@output person .\n",
        &["person"],
        Fetch::Unbound,
    );
}

#[test]
fn literal_constants_specialise_the_query() {
    check(
        r#"
english(?x) :- t(?x, ex:name, "Bob"@en) .
plain(?x) :- t(?x, ex:name, "Carol") .
aged(?x) :- t(?x, ex:age, 45) .
@output english, plain, aged .
"#,
        &["english", "plain", "aged"],
        Fetch::Unbound,
    );
}

#[test]
fn a_repeated_variable_specialises_the_query() {
    check(
        "self(?x) :- t(?x, ex:knows, ?x) .\n@output self .\n",
        &["self"],
        Fetch::Unbound,
    );
}

#[test]
fn chained_atoms_bind_on_a_local_atom() {
    check(
        r#"
seed(ex:e1) . seed(ex:e3) .
next(?e, ?n) :- seed(?e), t(?e, ex:p1, ?m), t(?m, ex:p2, ?n) .
@output next .
"#,
        &["next"],
        Fetch::Bound,
    );
}

#[test]
fn a_computed_argument_binds_the_import() {
    check(
        r#"
org("Shell") . org("Nope") .
business(?id) :- org(?o), t(?id, ex:name, STRLANG(?o, "en")) .
@output business .
"#,
        &["business"],
        Fetch::Bound,
    );
}

#[test]
fn an_assignment_binds_the_import() {
    check(
        r#"
org("BP") .
business(?id) :- org(?o), ?l = STRLANG(?o, "en"), t(?id, ex:name, ?l) .
@output business .
"#,
        &["business"],
        Fetch::Bound,
    );
}

#[test]
fn a_negated_atom_becomes_a_filter() {
    check(
        r#"
inverse(?p, ?i) :- t(?p, ex:inverse, ?i) .
violation(?s, ?o) :- inverse(?p, ?i), t(?s, ?p, ?o), ~t(?o, ?i, ?s) .
@output violation .
"#,
        &["violation"],
        Fetch::Bound,
    );
}

#[test]
fn a_recursion_binds_each_step() {
    check(
        r#"
start(ex:alice) .
reach(?x) :- start(?x) .
reach(?y) :- reach(?x), t(?x, ex:parent, ?y) .
@output reach .
"#,
        &["reach"],
        Fetch::Bound,
    );
}

#[test]
fn two_local_groups_bind_together() {
    check(
        r#"
p(ex:alice) . p(ex:dave) . q(ex:bob) . q(ex:erin) . q(ex:carol) .
h(?x, ?y) :- p(?x), q(?y), t(?x, ex:knows, ?y) .
@output h .
"#,
        &["h"],
        Fetch::Bound,
    );
}

#[test]
fn a_variable_predicate_binds_the_import() {
    check(
        r#"
wanted(ex:knows) . wanted(ex:inverse) .
edge(?s, ?p, ?o) :- wanted(?p), t(?s, ?p, ?o) .
@output edge .
"#,
        &["edge"],
        Fetch::Bound,
    );
}

#[test]
fn a_comparison_after_the_import_keeps_its_rows() {
    check(
        r#"
known(ex:alice) . known(ex:carol) . known(ex:dave) .
old(?x) :- known(?x), t(?x, ex:age, ?a), ?a > 40 .
@output old .
"#,
        &["old"],
        Fetch::Bound,
    );
}

#[test]
fn two_imports_in_one_rule() {
    check(
        r#"
@import names from sparql {
    endpoint = <http://endpoint.test/sparql>,
    query = "SELECT ?s ?n WHERE { ?s <http://ex.org/name> ?n }"
} .
start(ex:alice) . start(ex:bob) .
parentName(?x, ?n) :- start(?x), t(?x, ex:parent, ?y), names(?y, ?n) .
@output parentName .
"#,
        &["parentName"],
        Fetch::Bound,
    );
}

/// A query with an `OPTIONAL`, over which a filter requires its variable bound.
const WITH_EMAIL: &str = r#"
@import withEmail from sparql {
    endpoint = <http://endpoint.test/sparql>,
    query = "SELECT ?h ?e WHERE { ?h a <http://ex.org/Person> OPTIONAL { ?h <http://ex.org/email> ?e } FILTER(BOUND(?e)) }"
} .
"#;

#[test]
fn a_filter_requiring_an_optional_variable_binds_correctly() {
    check(
        &format!(
            r#"{WITH_EMAIL}
mail("a@x") . mail("zzz") .
who(?h) :- mail(?e), withEmail(?h, ?e) .
@output who .
"#
        ),
        &["who"],
        Fetch::Bound,
    );
}

#[test]
fn an_optional_variable_keeps_the_import_unbound() {
    check(
        r#"
@import emails from sparql {
    endpoint = <http://endpoint.test/sparql>,
    query = "SELECT ?h ?e WHERE { ?h a <http://ex.org/Person> OPTIONAL { ?h <http://ex.org/email> ?e } }"
} .
known(ex:alice) . known(ex:dave) .
mailOf(?h, ?e) :- known(?h), emails(?h, ?e) .
everyone(?h, ?e) :- emails(?h, ?e) .
paired(?h) :- mailOf(?h, ?e), everyone(?h, ?e) .
@output mailOf, paired .
"#,
        &["mailOf", "paired"],
        Fetch::Unbound,
    );
}

#[test]
fn a_union_binding_its_variables_in_both_branches_binds() {
    check(
        r#"
@import contacts from sparql {
    endpoint = <http://endpoint.test/sparql>,
    query = "SELECT ?x ?c WHERE { { ?x <http://ex.org/email> ?c } UNION { ?x <http://ex.org/name> ?c } }"
} .
known(ex:alice) . known(ex:carol) .
contact(?x, ?c) :- known(?x), contacts(?x, ?c) .
@output contact .
"#,
        &["contact"],
        Fetch::Bound,
    );
}

#[test]
fn a_union_leaving_a_variable_unbound_stays_unbound() {
    check(
        r#"
@import contacts from sparql {
    endpoint = <http://endpoint.test/sparql>,
    query = "SELECT ?x ?c WHERE { { ?x <http://ex.org/email> ?c } UNION { ?x <http://ex.org/knows> ?y } }"
} .
known(ex:alice) . known(ex:dave) .
contact(?x, ?c) :- known(?x), contacts(?x, ?c) .
@output contact .
"#,
        &["contact"],
        Fetch::Unbound,
    );
}

#[test]
fn a_computed_value_that_may_fail_stays_unbound() {
    check(
        r#"
@import next from sparql {
    endpoint = <http://endpoint.test/sparql>,
    query = "SELECT ?x ?n WHERE { ?x <http://ex.org/age> ?a BIND(?a + 1 AS ?n) }"
} .
known(ex:alice) . known(ex:dave) .
nextAge(?x, ?n) :- known(?x), next(?x, ?n) .
@output nextAge .
"#,
        &["nextAge"],
        Fetch::Unbound,
    );
}

#[test]
fn an_aggregate_stays_unbound() {
    check(
        r#"
@import degrees from sparql {
    endpoint = <http://endpoint.test/sparql>,
    query = "SELECT ?x (COUNT(?y) AS ?n) WHERE { ?x <http://ex.org/knows> ?y } GROUP BY ?x"
} .
known(ex:alice) . known(ex:frank) .
degree(?x, ?n) :- known(?x), degrees(?x, ?n) .
@output degree .
"#,
        &["degree"],
        Fetch::Unbound,
    );
}

#[test]
fn a_subquery_stays_unbound() {
    check(
        r#"
@import knowsPeople from sparql {
    endpoint = <http://endpoint.test/sparql>,
    query = "SELECT ?x ?y WHERE { ?x <http://ex.org/knows> ?y { SELECT ?y WHERE { ?y a <http://ex.org/Person> } } }"
} .
known(ex:alice) . known(ex:dave) .
friend(?x, ?y) :- known(?x), knowsPeople(?x, ?y) .
@output friend .
"#,
        &["friend"],
        Fetch::Unbound,
    );
}

#[test]
fn solution_modifiers_keep_the_binding() {
    check(
        r#"
@import parents from sparql {
    endpoint = <http://endpoint.test/sparql>,
    query = "SELECT DISTINCT ?x ?y WHERE { ?x <http://ex.org/parent> ?y } ORDER BY ?x"
} .
start(ex:bob) . start(ex:erin) .
parentOf(?x, ?y) :- start(?x), parents(?x, ?y) .
@output parentOf .
"#,
        &["parentOf"],
        Fetch::Bound,
    );
}

#[test]
fn a_filter_of_the_query_keeps_the_binding() {
    check(
        r#"
@import ages from sparql {
    endpoint = <http://endpoint.test/sparql>,
    query = "SELECT ?x ?a WHERE { ?x <http://ex.org/age> ?a FILTER(isNumeric(?a)) }"
} .
known(ex:alice) . known(ex:dave) . known(ex:erin) .
age(?x, ?a) :- known(?x), ages(?x, ?a) .
@output age .
"#,
        &["age"],
        Fetch::Bound,
    );
}

#[test]
fn a_property_path_binds() {
    check(
        r#"
@import ancestors from sparql {
    endpoint = <http://endpoint.test/sparql>,
    query = "SELECT ?x ?y WHERE { ?x <http://ex.org/parent>+ ?y }"
} .
start(ex:alice) .
ancestor(?y) :- start(?x), ancestors(?x, ?y) .
@output ancestor .
"#,
        &["ancestor"],
        Fetch::Bound,
    );
}

#[test]
fn a_query_with_a_base_and_prefixes_binds() {
    check(
        r#"
@import parents from sparql {
    endpoint = <http://endpoint.test/sparql>,
    query = "BASE <http://ex.org/> PREFIX e: <http://ex.org/> SELECT ?x ?y WHERE { ?x <parent> ?y . ?y a e:Person }"
} .
start(ex:alice) . start(ex:erin) .
parentOf(?x, ?y) :- start(?x), parents(?x, ?y) .
@output parentOf .
"#,
        &["parentOf"],
        Fetch::Bound,
    );
}

/// The rules read one blank node through two queries the rewrite specialises apart:
/// a label names one null in every response of the endpoint, so they still meet.
#[test]
fn a_blank_node_joins_across_the_queries_of_a_split_import() {
    check(
        r#"
restriction(?r) :- t(?r, rdf:type, ex:Restriction) .
onProperty(?r, ?p) :- t(?r, ex:onProperty, ?p) .
restricted(?p) :- restriction(?r), onProperty(?r, ?p) .
@output restricted .
"#,
        &["restricted"],
        Fetch::Unbound,
    );
}

/// The documented limit for blank nodes: no query can name one, so a recursion following
/// blank nodes, as along an RDF list, stops at the first one once rewritten,
/// and the run says how many binding rows fetched nothing.
#[test]
fn a_recursion_through_blank_nodes_stops_and_warns() {
    let source = format!(
        r#"{PRELUDE}
cell(?c, ?l) :- t(?c, ex:unionOf, ?l) .
cell(?c, ?n) :- cell(?c, ?l), t(?l, rdf:rest, ?n) .
member(?c, ?m) :- cell(?c, ?l), t(?l, rdf:first, ?m) .
@output member .
"#
    );
    let dataset = dataset();
    let written = run(&source, &["member"], false, &dataset);
    let rewritten = run(&source, &["member"], true, &dataset);
    assert_eq!(
        written.outputs[0].1,
        [
            "<http://ex.org/Parent>,<http://ex.org/Father>",
            "<http://ex.org/Parent>,<http://ex.org/Mother>"
        ]
    );
    assert!(rewritten.outputs[0].1.is_empty(), "{:?}", rewritten.outputs);
    assert!(
        rewritten
            .reports
            .contains(&nemo::codes::BLANK_NODE_BINDINGS),
        "{:?}",
        rewritten.reports
    );
    assert!(
        !written.reports.contains(&nemo::codes::BLANK_NODE_BINDINGS),
        "{:?}",
        written.reports
    );
}

/// Integers go out as integers and as decimals, so values held as decimals, as Wikidata holds
/// its quantities, are found through bindings, constants and negated constants.
#[test]
fn integers_find_the_decimals_an_endpoint_holds_them_as() {
    check(
        r#"
wanted(3700577) . wanted(5) . item(ex:q1) . item(ex:q2) .
holder(?x, ?v) :- wanted(?v), t(?x, ex:quantity, ?v) .
twelve(?x) :- t(?x, ex:quantity, 12) .
notTwelve(?x) :- item(?x), ~t(?x, ex:quantity, 12) .
@output holder, twelve, notTwelve .
"#,
        &["holder", "twelve", "notTwelve"],
        Fetch::Bound,
    );
}

#[test]
fn bound_integers_match_the_decimals_an_endpoint_holds_them_as() {
    check_bound_import(
        "SELECT ?x ?v WHERE { ?x <http://ex.org/quantity> ?v }",
        "?x, ?v",
        "wanted(3700577) . wanted(12) .",
        "wanted(?v)",
        "out(?x, ?v)",
    );
}

/// Nemo reads `"12.0"^^xsd:decimal` as the integer 12: a pushed identity also asks for the
/// decimal, and a pushed `STR` or `DATATYPE` keeps the decimals for the local filter.
#[test]
fn pushed_conditions_keep_the_integers_an_endpoint_holds_as_decimals() {
    let queries = check_condition_on("quantity", "?v = 12");
    assert!(queries[0].contains("\"12.0\"^^"), "{queries:#?}");
    check_condition_on("quantity", r#"STR(?v) = "12""#);
    check_condition_on(
        "quantity",
        "DATATYPE(?v) = <http://www.w3.org/2001/XMLSchema#integer>",
    );
}

/// Checks that an import's own condition, which the endpoint applies as far as it has a SPARQL
/// form, keeps what the same condition keeps as a rule's filter over the plain import,
/// and returns the queries the conditioned import sent.
fn compare_import_condition(condition: &str) -> Vec<String> {
    let query = "SELECT ?x ?v ?o WHERE { ?x <http://ex.org/value> ?v OPTIONAL { ?x <http://ex.org/other> ?o } }";
    let conditioned = format!(
        "@import values(?x, ?v, ?o) from sparql {{ endpoint = <http://endpoint.test/sparql>, query = \"{query}\" }} where {condition} .
out(?x, ?v) :- values(?x, ?v, ?o) .
@output out .
"
    );
    let filtered = format!(
        "@import values from sparql {{ endpoint = <http://endpoint.test/sparql>, query = \"{query}\" }} .
out(?x, ?v) :- values(?x, ?v, ?o), {condition} .
all(?x, ?v, ?o) :- values(?x, ?v, ?o) .
@output out, all .
"
    );
    let dataset = dataset();
    let conditioned = run(&format!("{PRELUDE}{conditioned}"), &["out"], true, &dataset);
    let filtered = run(&format!("{PRELUDE}{filtered}"), &["out"], true, &dataset);
    assert!(
        !filtered.outputs[0].1.is_empty(),
        "{condition}: the program derives something"
    );
    assert_eq!(
        conditioned.outputs, filtered.outputs,
        "{condition}: the import's condition changed the results\nqueries: {:#?}",
        conditioned.queries
    );
    conditioned.queries
}

/// An identity under a negation goes to the endpoint only with an IRI: SPARQL's `=` holds for
/// the double `4.0E0` and the integer 4, which Nemo tells apart, so `!(?v = 4)` would drop the
/// double that Nemo's `NOT(?v = 4)` keeps.
#[test]
fn a_negated_identity_with_a_literal_stays_local() {
    let queries = compare_import_condition("NOT(?v = 4)");
    assert!(!queries[0].contains("FILTER"), "{queries:#?}");
    let queries = compare_condition("value", "?v != 4");
    assert!(
        queries
            .iter()
            .all(|text| !text.contains("FILTER") && !text.contains("VALUES")),
        "{queries:#?}"
    );
}

#[test]
fn a_negated_identity_with_an_iri_is_pushed() {
    let queries = compare_import_condition("NOT(?x = ex:m1)");
    assert!(
        queries[0].contains("!(?x = <http://ex.org/m1>)"),
        "{queries:#?}"
    );
    let queries = check_condition_on("value", "?x != ex:m1");
    assert!(
        queries[0].contains("!(?x = <http://ex.org/m1>)"),
        "{queries:#?}"
    );
}

/// Nested in a disjunction, an identity with a literal goes out as SPARQL's `=`,
/// unless the literal is a NaN, which `=` never holds for.
#[test]
fn an_identity_in_a_disjunction_compares_values() {
    let queries = compare_import_condition("OR(?v = 4, ?v = true)");
    assert!(
        queries[0].contains("(?v = \"4\"^^<http://www.w3.org/2001/XMLSchema#integer>)"),
        "{queries:#?}"
    );
    let queries = compare_import_condition(
        r#"OR(?v = "NaN"^^<http://www.w3.org/2001/XMLSchema#double>, ?v = 4)"#,
    );
    assert!(!queries[0].contains("FILTER"), "{queries:#?}");
}

/// QLever implements no `sameTerm`, so nothing pushed uses it.
#[test]
fn type_tests_compare_datatypes_without_same_term() {
    let queries = compare_import_condition("ISDOUBLE(?v)");
    assert!(
        queries[0].contains("DATATYPE(?v) = <http://www.w3.org/2001/XMLSchema#double>"),
        "{queries:#?}"
    );
    assert!(!queries[0].contains("sameTerm"), "{queries:#?}");
}
