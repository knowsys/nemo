//! What reaches the endpoint: the projected variables, computed arguments as bindings,
//! conditions and type tests as filters, and each binding asked once.

use nemo::{engine::Engine, model::Fact, trace::TraceNode};
use nemo_physical::value::DataValue;

use crate::common::{Responder, parse_into};

use super::{values_blocks, written_only};

/// The variables a query text projects, in order.
fn projection(text: &str) -> Vec<&str> {
    text.split_once("SELECT ")
        .and_then(|(_, rest)| rest.split_once(" WHERE"))
        .map_or_else(Vec::new, |(projection, _)| {
            projection.split_whitespace().collect()
        })
}

#[test]
fn a_query_calling_a_service_keeps_its_variable_names() {
    // A make-believe label service: as Wikidata's, it labels `?x` as
    // `?xLabel`, so a renamed label variable would stay unbound.
    let fetcher = Responder::endpoint(|text| {
        let variables = projection(text);
        let mut body = variables.join("\t");
        body.push('\n');
        for iri in values_blocks(text).into_iter().flatten() {
            let cells: Vec<String> = variables
                .iter()
                .map(|variable| match variable.strip_suffix("Label") {
                    Some(labeled) if variables.contains(&labeled) => {
                        format!("\"{iri} label\"@en")
                    }
                    Some(_) => String::new(),
                    None => format!("<{iri}>"),
                })
                .collect();
            body.push_str(&cells.join("\t"));
            body.push('\n');
        }
        body
    });
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        r#"
@import wdLabel from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?qid ?qidLabel WHERE { SERVICE <http://wikiba.se/ontology#label> { <http://www.bigdata.com/rdf#serviceParam> <http://wikiba.se/ontology#language> 'en' } }"
} .
item(<urn:a>) . item(<urn:b>) .
named(?item, ?name) :- item(?item), wdLabel(?item, ?name) .
@output named .
"#,
    );
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));

    let mut named: Vec<String> = engine
        .rows("named")
        .expect("derived")
        .iter()
        .map(|row| format!("{} {}", row[0], row[1]))
        .collect();
    named.sort();
    assert_eq!(
        named,
        ["<urn:a> \"urn:a label\"@en", "<urn:b> \"urn:b label\"@en"]
    );
}

#[test]
fn a_computed_argument_reaches_the_endpoint_as_a_binding() {
    let fetcher = Responder::endpoint(|text| {
        // The import is bound by the computed value, not fetched whole.
        assert!(text.contains("\"Shell\"@en"), "{text}");
        let mut body = projection(text).join("\t");
        // The endpoint answers more than it was asked: the rule compares
        // the column with the argument as written.
        body.push_str("\n<urn:shell>\t\"Shell\"@en\n<urn:other>\t\"Other\"@en\n");
        body
    });
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        r#"
@import triple from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
} .
org("Shell") .
business(?id, ?org) :- org(?org), triple(?id, <urn:name>, STRLANG(?org, "en")) .
@output business .
"#,
    );
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));

    assert_eq!(
        engine.rows("business").expect("derived"),
        vec![vec![
            DataValue::iri("urn:shell"),
            DataValue::string("Shell")
        ]]
    );
    assert_eq!(fetcher.queries().len(), 1);
    assert!(engine.rows("triple").is_err());

    // The proof shows the rule and the remote premise as written.
    let fact = Fact::parse("business(<urn:shell>, \"Shell\") .").expect("parses");
    let response = engine.trace(&[fact]).expect("tracing answers");
    written_only(&response);
    let TraceNode::Derived { rule, premises, .. } =
        response.facts[0].tree.as_ref().expect("a proof tree")
    else {
        panic!("a derived fact gets a rule node");
    };
    assert_eq!(
        rule.text,
        "business(?id, ?org) :- org(?org), triple(?id, <urn:name>, STRLANG(?org, \"en\")) ."
    );
    assert_eq!(
        premises[1].fact().to_string(),
        "triple(<urn:shell>, <urn:name>, \"Shell\"@en)"
    );
}

#[test]
fn pushed_conditions_reach_the_endpoint_as_filters() {
    let fetcher = Responder::endpoint(|text| {
        // Nemo's `=` is term identity: `sameTerm` in SPARQL.
        assert!(
            text.contains("FILTER(sameTerm(LANG(?l), \"en\"))"),
            "{text}"
        );
        let mut body = String::from("?c\t?l\n");
        for iri in values_blocks(text).into_iter().flatten() {
            body.push_str(&format!("<{iri}>\t\"label of {iri}\"@en\n"));
            // A row the endpoint should not have sent: the rule's own
            // filter still drops it.
            body.push_str(&format!("<{iri}>\t\"Beschriftung\"@de\n"));
        }
        body
    });
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        r#"
@import triple from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
} .
thing(<urn:a>) . thing(<urn:b>) .
caption(?c, ?l) :- thing(?c), triple(?c, <urn:label>, ?l), LANG(?l) = "en" .
@output caption .
"#,
    );
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let labels = engine.rows("caption").expect("derived");
    assert_eq!(labels.len(), 2, "{labels:?}");
    assert_eq!(fetcher.queries().len(), 1);
}

/// The type tests of an import's conditions reach the endpoint as tests of a literal's
/// datatype, and the local filter applies them regardless.
#[test]
fn type_tests_reach_the_endpoint_as_datatype_tests() {
    let fetcher = Responder::endpoint(|text| {
        let lowered = text.to_lowercase();
        assert!(
            lowered.contains("isliteral(?o)")
                && text.contains("<http://www.w3.org/2001/XMLSchema#string>"),
            "{text}"
        );
        assert!(lowered.contains("isblank(?s)"), "{text}");
        // Rows the endpoint should not have sent: the local filter drops them.
        String::from("?s\t?o\n<urn:a>\t\"x\"\n<urn:b>\t5\n_:b\t\"y\"\n")
    });
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        r#"
@import t(?s, ?o) from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?s ?o WHERE { ?s <urn:p> ?o }"
} where ISSTRING(?o), NOT(ISNULL(?s)) .
@output t .
"#,
    );
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(
        engine.rows("t").expect("loaded"),
        vec![vec![DataValue::iri("urn:a"), DataValue::string("x")]]
    );
}

/// A pushed condition names the columns the loader delivers,
/// after the projection pass dropped the ones no rule reads,
/// so a condition on a later column lands on that column's query variable.
#[test]
fn pushed_conditions_name_delivered_columns_past_pruned_ones() {
    let fetcher = Responder::endpoint(|text| {
        // The endpoint applies the FILTER it is sent: only a filter on
        // the object's language finds the label.
        let mut body = String::from("?s\t?p\t?o\n");
        if text.contains("LANG(?o)") {
            body.push_str("<urn:a>\t<urn:label>\t\"label a\"@en\n");
        }
        body
    });
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        r#"
        @import triple from sparql {
            endpoint = <http://example.org/sparql>,
            query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
        } .
        caption(?l) :- triple(?a, ?b, ?l), LANG(?l) = "en" .
        @output caption .
        "#,
    );
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let seen = fetcher.queries();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(
        seen[0].contains("LANG(?o)") && !seen[0].contains("LANG(?s)"),
        "{}",
        seen[0]
    );
    assert_eq!(engine.count("caption").expect("derived"), 1);
}

/// A plain import a bound import's binding atom reads stays whatever
/// the rules read of it: the fetch reads it as stored.
#[test]
fn imports_read_by_binding_atoms_are_kept() {
    let fetcher = Responder::endpoint(|text| {
        if text.contains("VALUES") {
            let mut body = String::from("?s\n");
            for iri in values_blocks(text).into_iter().flatten() {
                body.push_str(&format!("<{iri}>\n"));
            }
            body
        } else {
            String::from("?s\t?p\t?o\n<urn:x>\t<urn:p>\t<urn:o>\n")
        }
    });
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        r#"
        @import t from sparql {
            endpoint = <http://example.org/sparql>,
            query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
        } .
        @import q(?s) from sparql {
            endpoint = <http://example.org/sparql>,
            query = "SELECT ?s WHERE { ?s <urn:a> <urn:b> }"
        } where t(?s, _, _) .
        h(?s) :- t(?s, <urn:p>, <urn:o>) .
        @output q, h .
        "#,
    );
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(engine.count("h").expect("derived"), 1);
    assert_eq!(engine.count("q").expect("derived"), 1);
}

/// A variable repeated within a binding atom binds its column once:
/// the request names it once, and the repetition holds locally.
#[test]
fn repeated_binding_variables_are_asked_once() {
    let fetcher = Responder::endpoint(|text| {
        let mut body = String::from("?x\n");
        for iri in values_blocks(text).into_iter().flatten() {
            body.push_str(&format!("<{iri}>\n"));
        }
        body
    });
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        r#"
        @import q(?x) from sparql {
            endpoint = <http://example.org/sparql>,
            query = "SELECT ?x WHERE { ?x <urn:p> ?y }"
        } where b(?x, ?x) .
        b(<urn:a>, <urn:a>) . b(<urn:a>, <urn:b>) .
        @output q .
        "#,
    );
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    let seen = fetcher.queries();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(!seen[0].contains("?x ?x"), "{}", seen[0]);
    assert_eq!(
        engine.rows("q").expect("derived"),
        vec![vec![DataValue::iri("urn:a")]]
    );
}

/// A binding key that stood before the delta is not asked again when a
/// new row of the binding predicate repeats it: the endpoint's
/// blank nodes in its results would otherwise create new nulls.
#[test]
fn known_binding_keys_are_not_asked_again() {
    let fetcher = Responder::endpoint(|text| {
        let mut body = String::from("?x\t?n\n");
        for iri in values_blocks(text).into_iter().flatten() {
            body.push_str(&format!("<{iri}>\t_:n\n"));
        }
        body
    });
    let mut engine = Engine::new();
    let program = parse_into(
        &mut engine,
        r#"
        @import q(?x, ?n) from sparql {
            endpoint = <http://example.org/sparql>,
            query = "SELECT ?x ?n WHERE { ?x <urn:p> ?n }"
        } where b(?x, _) .
        b(<urn:a>, 1) .
        @output q .
        "#,
    );
    fetcher
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(engine.count("q").expect("derived"), 1);
    engine
        .import_rows("b", [vec![DataValue::iri("urn:a"), DataValue::integer(2)]])
        .expect("the row lands");
    fetcher
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    assert_eq!(fetcher.queries().len(), 1, "the key was asked once");
    assert_eq!(engine.count("q").expect("derived"), 1);
}
