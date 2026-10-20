//! Bound SPARQL imports: the endpoint is queried in every iteration with the
//! bindings of the program, through a responder that responds by query text.
//!
//! One module per subject, sharing the programs and responders below.

mod bindings;
mod programs;
mod queries;
mod requests;
mod tracing;

use nemo::engine::Engine;

use crate::common::{Responder, parse_into};

/// The IRIs of each `VALUES` block of a query text, block by block.
fn values_blocks(text: &str) -> Vec<Vec<String>> {
    text.split("VALUES")
        .skip(1)
        .map(|block| {
            let body = block.split_once('{').map_or("", |(_, rest)| rest);
            let body = body.split_once('}').map_or(body, |(inner, _)| inner);
            body.split('<')
                .skip(1)
                .filter_map(|piece| piece.split_once('>').map(|(iri, _)| iri.to_owned()))
                .collect()
        })
        .collect()
}

/// A chain of parents in a make-believe graph: a → b → c → d.
fn parent_of(iri: &str) -> Option<&'static str> {
    match iri {
        "urn:a" => Some("urn:b"),
        "urn:b" => Some("urn:c"),
        "urn:c" => Some("urn:d"),
        _ => None,
    }
}

/// The ancestor program over a parent import bound by the ancestors
/// found so far.
const ANCESTORS: &str = r#"
@import parent(?c, ?p) from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?c ?p WHERE { ?c <urn:parent> ?p }"
} where anc(?c) .
anc(<urn:a>) .
anc(?p) :- parent(?c, ?p) .
@output anc .
"#;

/// The responder to the ancestor program's requests.
fn parents() -> Responder {
    Responder::endpoint(|text| {
        let mut body = String::from("?c\t?p\n");
        for iri in values_blocks(text).into_iter().flatten() {
            if let Some(parent) = parent_of(&iri) {
                body.push_str(&format!("<{iri}>\t<{parent}>\n"));
            }
        }
        body
    })
}

/// The ancestor program as the paper writes it: over the generic
/// triple import, with the rewrite stage doing the binding.
const ANCESTORS_GENERIC: &str = r#"
@import triple from sparql {
    endpoint = <http://example.org/sparql>,
    query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
} .
anc(<urn:a>) .
anc(?p) :- anc(?c), triple(?c, <urn:parent>, ?p) .
@output anc .
"#;

/// The fetcher of the generic ancestor program.
fn parents_generic() -> Responder {
    Responder::endpoint(|text| {
        let mut body = String::from("?c\t?p\n");
        for iri in values_blocks(text).into_iter().flatten() {
            if let Some(parent) = parent_of(&iri) {
                body.push_str(&format!("<{iri}>\t<{parent}>\n"));
            }
        }
        body
    })
}

/// The generic ancestor program, loaded and reasoned.
fn generic_ancestors() -> Engine {
    let mut engine = Engine::new();
    let endpoint = parents_generic();
    let program = parse_into(&mut engine, ANCESTORS_GENERIC);
    endpoint
        .drive(&mut engine, |engine| engine.load(program))
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    endpoint
        .drive(&mut engine, Engine::reason)
        .unwrap_or_else(|problem| panic!("{problem:?}"));
    engine
}

/// A response's JSON, which must name nothing a stage introduced.
fn written_only(response: &nemo::trace::TraceResponse) -> String {
    let json = serde_json::to_string(response).expect("the response serializes");
    assert!(
        !json.contains("_query") && !json.contains("_bindings"),
        "a stage's predicate shows: {json}"
    );
    json
}
