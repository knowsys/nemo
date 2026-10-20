//! SPARQL queries: validation, shapes, bindings and filters joined in,
//! ground terms, and reading TSV responses.

use super::*;

#[test]
fn queries_validate_and_count_their_projection() {
    let query = SparqlQuery::parse("SELECT ?a ?b WHERE { ?a ?b ?c }", None, []).expect("valid");
    assert_eq!(query.arity(), 2);
    assert_eq!(query.variables(), ["a", "b"]);
    // Prefixes seed the parser, so directives need no PREFIX line.
    let query = SparqlQuery::parse(
        "SELECT ?x WHERE { ?x a ex:Thing }",
        None,
        [("ex", "http://example.org/")],
    )
    .expect("the prefix is in scope");
    assert_eq!(query.arity(), 1);
    assert!(matches!(
        SparqlQuery::parse("SELECT ?x WHERE { broken", None, []),
        Err(QueryError::Syntax { .. })
    ));
    assert!(matches!(
        SparqlQuery::parse("SELECT ?x WHERE { ?x ?p ?o }", Some("not an IRI"), []),
        Err(QueryError::Base { .. })
    ));
    assert_eq!(
        SparqlQuery::parse("ASK { ?s ?p ?o }", None, []).err(),
        Some(QueryError::NotSelect)
    );
}

#[test]
fn short_queries_travel_as_get_long_ones_as_post() {
    let query = SparqlQuery::parse(DEFAULT_QUERY, None, []).expect("valid");
    let request = query.request("http://example.org/sparql");
    assert!(request.form.is_none());
    assert_eq!(request.parameters.len(), 1);

    let long = format!(
        "SELECT ?x WHERE {{ ?x <http://example.org/p> \"{}\" }}",
        "y".repeat(3_000)
    );
    let query = SparqlQuery::parse(&long, None, []).expect("valid");
    let request = query.request("http://example.org/sparql");
    assert!(request.form.is_some());
    assert!(request.parameters.is_empty());
}

#[test]
fn shapes_tell_patterns_from_complex_and_opaque_queries() {
    let shape = |text: &str| SparqlQuery::parse(text, None, []).expect("valid").shape();
    assert_eq!(
        shape("SELECT DISTINCT ?a WHERE { ?a ?b ?c . FILTER(?b != <urn:p>) } ORDER BY ?a"),
        QueryShape::Pattern
    );
    assert_eq!(
        shape("SELECT ?a WHERE { ?a ?b ?c . BIND(STR(?a) AS ?s) }"),
        QueryShape::Pattern
    );
    // A part binding only some solutions makes the query a subquery
    // in a merge: a term substituted into it would change it.
    assert_eq!(
        shape("SELECT ?a WHERE { ?a ?b ?c . OPTIONAL { ?c ?d ?e } }"),
        QueryShape::Complex
    );
    assert_eq!(
        shape("SELECT ?a (COUNT(?c) AS ?n) WHERE { ?a ?b ?c } GROUP BY ?a"),
        QueryShape::Complex
    );
    assert_eq!(
        shape("SELECT ?a WHERE { { SELECT ?a WHERE { ?a ?b ?c } } ?a ?d ?e }"),
        QueryShape::Complex
    );
    // LIMIT bounds the whole result: no per-request selection
    // reproduces it, at the top or inside a subquery.
    assert!(matches!(
        shape("SELECT ?a WHERE { ?a ?b ?c } LIMIT 100"),
        QueryShape::Opaque(_)
    ));
    assert!(matches!(
        shape("SELECT ?a WHERE { { SELECT ?a WHERE { ?a ?b ?c } LIMIT 10 } ?a ?d ?e }"),
        QueryShape::Opaque(_)
    ));
    assert!(matches!(
        shape("SELECT ?a WHERE { ?a ?b ?c } OFFSET 5"),
        QueryShape::Opaque(_)
    ));
    // A nondeterministic query has no one result to restrict.
    assert!(matches!(
        shape("SELECT ?a WHERE { ?a ?b ?c . FILTER(RAND() < 0.5) }"),
        QueryShape::Opaque(_)
    ));
    assert!(matches!(
        shape("SELECT ?a (SAMPLE(?c) AS ?s) WHERE { ?a ?b ?c } GROUP BY ?a"),
        QueryShape::Opaque(_)
    ));
    assert!(matches!(
        shape("SELECT ?a WHERE { ?a ?b ?c . FILTER NOT EXISTS { ?c ?d ?e . FILTER(NOW() > ?e) } }"),
        QueryShape::Opaque(_)
    ));
}

#[test]
fn bound_variables_are_required_below_the_values_join() {
    // An OPTIONAL that did not match leaves ?p unbound; the block
    // must not join with such solutions.
    let query = SparqlQuery::parse(
        "SELECT ?c ?p WHERE { ?c a <urn:C> OPTIONAL { ?c <urn:parent> ?p } }",
        None,
        [],
    )
    .expect("parses");
    let text = query.with_values(&[ValuesBlock {
        positions: vec![1],
        rows: vec![vec![GroundTerm::NamedNode(
            spargebra::term::NamedNode::new("urn:p").expect("an IRI"),
        )]],
    }]);
    let guard = text.find("FILTER(BOUND(?p))").expect("the guard is there");
    let values = text.find("VALUES").expect("the block is there");
    assert!(guard < values, "the guard sits below the join: {text}");
    // The guard closes its group before the block joins it.
    let between = &text[guard..values];
    assert!(between.contains('}'), "{text}");
}

#[test]
fn values_join_in_under_the_projection() {
    let term = |iri: &str| GroundTerm::NamedNode(spargebra::term::NamedNode::new_unchecked(iri));
    let query = SparqlQuery::parse(
        "SELECT DISTINCT ?a ?c WHERE { ?a <urn:p> ?c } ORDER BY ?c",
        None,
        [],
    )
    .expect("valid");
    let bound = query.with_values(&[ValuesBlock {
        positions: vec![0],
        rows: vec![vec![term("urn:x")], vec![term("urn:y")]],
    }]);
    assert!(bound.contains("VALUES ( ?a )"), "{bound}");
    assert!(
        bound.contains("<urn:x>") && bound.contains("<urn:y>"),
        "{bound}"
    );
    // The modifiers stay above the injection.
    assert!(bound.starts_with("SELECT DISTINCT ?a ?c"), "{bound}");
    assert!(bound.trim_end().ends_with("ORDER BY ASC(?c)"), "{bound}");
    // Re-parsing keeps the projection: the block did not widen it.
    let reparsed = SparqlQuery::parse(&bound, None, []).expect("the bound query parses");
    assert_eq!(reparsed.variables(), ["a", "c"]);
    assert_eq!(reparsed.shape(), QueryShape::Pattern);
}

#[test]
fn values_land_inside_the_filters_so_absences_see_them() {
    let term = |iri: &str| GroundTerm::NamedNode(spargebra::term::NamedNode::new_unchecked(iri));
    let query = SparqlQuery::parse(
        "SELECT ?s ?i WHERE { ?s <urn:p> ?o . FILTER NOT EXISTS { ?o ?i ?s } }",
        None,
        [],
    )
    .expect("valid");
    let bound = query.with_values(&[ValuesBlock {
        positions: vec![1],
        rows: vec![vec![term("urn:inverse")]],
    }]);
    // The block binds ?i before the absence is decided: it is
    // in the same group, before the FILTER.
    let values = bound.find("VALUES").expect("bound");
    let filter = bound.find("FILTER").expect("filtered");
    assert!(values < filter, "{bound}");
    let reparsed = SparqlQuery::parse(&bound, None, []).expect("parses");
    assert_eq!(reparsed.variables(), ["s", "i"]);
}

#[test]
fn values_on_an_aggregate_result_land_above_the_group() {
    let query = SparqlQuery::parse(
        "SELECT ?a (COUNT(?c) AS ?n) WHERE { ?a ?b ?c } GROUP BY ?a",
        None,
        [],
    )
    .expect("valid");
    let literal = GroundTerm::Literal(spargebra::term::Literal::new_typed_literal(
        "2",
        spargebra::term::NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
    ));
    let bound = query.with_values(&[ValuesBlock {
        positions: vec![1],
        rows: vec![vec![literal]],
    }]);
    // The block sits where ?n exists: outside the grouping, which
    // the rendering keeps whole as a subquery.
    assert!(bound.contains("VALUES ( ?n )"), "{bound}");
    let reparsed = SparqlQuery::parse(&bound, None, []).expect("parses");
    assert_eq!(reparsed.variables(), ["a", "n"]);
    assert_eq!(reparsed.shape(), QueryShape::Complex);
}

#[test]
#[should_panic(expected = "opaque query")]
fn values_cannot_join_an_opaque_query() {
    SparqlQuery::parse("SELECT ?a WHERE { ?a ?b ?c } LIMIT 3", None, [])
        .expect("valid")
        .with_values(&[]);
}

#[test]
fn filters_push_under_the_projection() {
    let query =
        SparqlQuery::parse("SELECT ?a ?l WHERE { ?a <urn:label> ?l }", None, []).expect("valid");
    let condition = Expression::Equal(
        Box::new(Expression::FunctionCall(
            spargebra::algebra::Function::Lang,
            vec![Expression::Variable(Variable::new_unchecked("l"))],
        )),
        Box::new(Expression::Literal(
            spargebra::term::Literal::new_simple_literal("en"),
        )),
    );
    let filtered = query.with_filter(condition);
    assert!(filtered.text().contains("FILTER"), "{}", filtered.text());
    assert!(filtered.text().contains("LANG(?l)"), "{}", filtered.text());
    assert_eq!(filtered.variables(), ["a", "l"]);
}

#[test]
fn ground_terms_render_values_in_their_lexical_forms() {
    let render = |value: DataValue| SparqlQuery::ground_term(&value).map(|term| term.to_string());
    assert_eq!(
        render(DataValue::iri("http://example.org/a")).as_deref(),
        Some("<http://example.org/a>")
    );
    assert_eq!(
        render(DataValue::string("x\"y")).as_deref(),
        Some("\"x\\\"y\"")
    );
    assert_eq!(
        render(DataValue::language_string("café", "fr").expect("valid")).as_deref(),
        Some("\"café\"@fr")
    );
    assert_eq!(
        render(DataValue::integer(42)).as_deref(),
        Some("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>")
    );
    assert_eq!(
        render(DataValue::boolean(true)).as_deref(),
        Some("\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>")
    );
    // A query cannot ask an endpoint about a null.
    assert_eq!(render(DataValue::null(7)), None);
}

#[test]
fn tsv_cells_parse_as_turtle_terms() {
    let body = "?s\t?p\t?o\n\
        <http://example.org/a>\t\"plain\"\t42\n\
        _:b0\t\"caf\\u00e9\"@fr\t-1.5\n\
        _:b0\t\"7\"^^<http://www.w3.org/2001/XMLSchema#integer>\t1e3\n\
        true\t\"tab\\there\"\t\n";
    let mut dictionary = Dictionary::new();
    let loaded = read_tsv(
        body.as_bytes(),
        3,
        &RowSelection::default(),
        &mut dictionary,
        EvaluationContext::now(),
    )
    .expect("the response reads");
    assert_eq!((loaded.rows.len(), loaded.malformed), (4, 0));
    let row = |index: usize| -> Vec<DataValue> {
        loaded
            .rows
            .row(index)
            .iter()
            .map(|&value| dictionary.decode(value).expect("decodes").to_owned())
            .collect()
    };
    assert_eq!(
        row(0),
        vec![
            DataValue::iri("http://example.org/a"),
            DataValue::string("plain"),
            DataValue::integer(42),
        ]
    );
    assert_eq!(
        row(1)[1],
        DataValue::language_string("café", "fr").expect("valid tag")
    );
    assert_eq!(
        row(1)[2],
        DataValue::decimal(-15, -1).expect("representable")
    );
    // The blank label names one null across rows.
    assert_eq!(row(1)[0], row(2)[0]);
    assert_eq!(row(2)[1], DataValue::integer(7));
    assert_eq!(row(2)[2], DataValue::double(1000.0));
    assert_eq!(row(3)[0], DataValue::boolean(true));
    assert_eq!(row(3)[1], DataValue::string("tab\there"));
    // The unbound cell minted a null distinct from the labeled one.
    assert!(matches!(
        row(3)[2].as_ref(),
        nemo_physical::value::ValueRef::Null(id) if row(1)[0].as_ref() != nemo_physical::value::ValueRef::Null(id)
    ));
}

#[test]
fn a_cut_off_response_is_an_error() {
    let whole = "?x\n<http://example.org/a>\n";
    assert!(check_complete("http://e/sparql", whole.as_bytes()).is_ok());
    let cut = "?x\n<http://example.org/a>\njava.util.concurrent.ExecutionException: \
               java.util.concurrent.TimeoutException\n\tat java.base/...\n";
    let problem = check_complete("http://e/sparql", cut.as_bytes()).expect_err("cut");
    assert_eq!(problem.code(), crate::codes::SPARQL_RESPONSE_CUT);
    assert!(
        problem
            .field("reason")
            .is_some_and(|reason| reason.contains("TimeoutException")),
        "{problem:?}"
    );
    // A trace longer than any fixed tail is still found.
    let long = format!("{cut}{}", "\tat java.base/...\n".repeat(2000));
    assert!(check_complete("http://e/sparql", long.as_bytes()).is_err());
}

#[test]
fn solution_rows_may_contain_the_markers() {
    // The words inside a term are data, not a trace.
    let body = "?x\t?l\n<http://example.org/x>\t\"see QueryTimeoutException in the docs\"\n\
                <http://example.org/java/util/concurrent/TimeoutException>\t\"\"\n";
    assert!(check_complete("http://e/sparql", body.as_bytes()).is_ok());
    // Unbound cells are rows too.
    let sparse = "?x\t?l\n<http://example.org/x>\t\n";
    assert!(check_complete("http://e/sparql", sparse.as_bytes()).is_ok());
}

#[test]
fn a_cell_that_is_no_term_makes_the_row_malformed() {
    let body = "?x\nnot a term\n<http://example.org/ok>\n";
    let mut dictionary = Dictionary::new();
    let loaded = read_tsv(
        body.as_bytes(),
        1,
        &RowSelection::default(),
        &mut dictionary,
        EvaluationContext::now(),
    )
    .expect("reads");
    assert_eq!((loaded.rows.len(), loaded.malformed), (1, 1));
    assert_eq!(loaded.first_malformed, Some(2));
}
