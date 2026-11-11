//! Tests of queries: validation, shapes, values and filters joined in, and ground terms.

use super::*;

/// A `SELECT` query parses with its projection and the given prefixes,
/// and a broken text, a base that is not an IRI, or another form of query is rejected.
#[test]
fn queries_validate_and_count_their_projection() {
    let query = SparqlQuery::parse("SELECT ?a ?b WHERE { ?a ?b ?c }", None, []).expect("valid");
    assert_eq!(query.width(), 2);
    assert_eq!(query.variables(), ["a", "b"]);

    // The given prefixes are in scope for the parser, so the query needs no PREFIX line.
    let query = SparqlQuery::parse(
        "SELECT ?x WHERE { ?x a ex:Thing }",
        None,
        [("ex", "http://example.org/")],
    )
    .expect("the prefix is in scope");
    assert_eq!(query.width(), 1);
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

/// A short query is sent as a GET parameter, and a long one as a POST form.
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

/// A query's shape distinguishes plain patterns from complex and opaque queries.
#[test]
fn shapes_distinguish_patterns_from_complex_and_opaque_queries() {
    let shape = |text: &str| SparqlQuery::parse(text, None, []).expect("valid").shape();
    assert_eq!(
        shape("SELECT DISTINCT ?a WHERE { ?a ?b ?c . FILTER(?b != <urn:p>) } ORDER BY ?a"),
        QueryShape::Pattern
    );
    assert_eq!(
        shape("SELECT ?a WHERE { ?a ?b ?c . BIND(STR(?a) AS ?s) }"),
        QueryShape::Pattern
    );

    // A part that binds only some solutions makes the query a subquery in a merge,
    // since substituting a term into the part would change the query.
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

    // LIMIT bounds the whole result, at the top or inside a subquery,
    // so no selection in each request reproduces it.
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

    // A nondeterministic query has no single result to restrict.
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

/// A bound variable that the pattern may leave unbound is required to be bound below the
/// `VALUES` join.
#[test]
fn bound_variables_are_required_below_the_values_join() {
    // An OPTIONAL that did not match leaves ?p unbound, and the block
    // must not join with such solutions.
    let query = SparqlQuery::parse(
        "SELECT ?c ?p WHERE { ?c a <urn:C> OPTIONAL { ?c <urn:parent> ?p } }",
        None,
        [],
    )
    .expect("parses");
    let text = query
        .with_values(&[ValuesBlock {
            positions: vec![1],
            rows: vec![vec![DataValue::iri("urn:p")]],
        }])
        .text()
        .to_owned();
    let guard = text.find("FILTER(BOUND(?p))").expect("the guard is there");
    let values = text.find("VALUES").expect("the block is there");
    assert!(guard < values, "the guard sits below the join: {text}");

    // The guard's group closes before the block is joined with it.
    let between = &text[guard..values];
    assert!(between.contains('}'), "{text}");
}

/// A query says whether some solution may leave a projected variable unbound.
#[test]
fn queries_report_whether_a_solution_may_leave_a_variable_unbound() {
    let leaves = |text: &str| {
        SparqlQuery::parse(text, None, [])
            .expect("parses")
            .may_leave_unbound()
    };
    assert!(!leaves("SELECT ?s ?o WHERE { ?s <urn:p> ?o }"));
    assert!(!leaves(
        "SELECT ?s WHERE { ?s <urn:p> ?o OPTIONAL { ?s <urn:q> ?r } }"
    ));
    assert!(leaves(
        "SELECT ?s ?r WHERE { ?s <urn:p> ?o OPTIONAL { ?s <urn:q> ?r } }"
    ));
    assert!(leaves(
        "SELECT ?s WHERE { { ?s <urn:p> ?o } UNION { ?t <urn:q> ?o } }"
    ));

    // A filter that requires the variable to be bound keeps only the solutions that bind it.
    assert!(!leaves(
        "SELECT ?s ?r WHERE { ?s <urn:p> ?o OPTIONAL { ?s <urn:q> ?r } FILTER(BOUND(?r)) }"
    ));

    // A computed value counts as possibly unbound.
    assert!(leaves(
        "SELECT ?s (COUNT(?o) AS ?n) WHERE { ?s <urn:p> ?o } GROUP BY ?s"
    ));

    // A variable no pattern binds is unbound in every solution,
    // unless the query calls a service, which may bind it.
    assert!(leaves("SELECT ?s ?label WHERE { ?s <urn:p> <urn:o> }"));
    assert!(!leaves(
        "SELECT ?s ?sLabel WHERE { ?s <urn:p> <urn:o> SERVICE <urn:label> { <urn:a> <urn:b> 'en' } }"
    ));
}

/// A query lists the projected variables that no pattern binds, in projection order.
#[test]
fn queries_list_the_projected_variables_no_pattern_binds() {
    let never = |text: &str| {
        SparqlQuery::parse(text, None, [])
            .expect("parses")
            .never_bound_variables()
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>()
    };
    assert!(never("SELECT ?s ?o WHERE { ?s <urn:p> ?o }").is_empty());
    assert!(never("SELECT ?s ?r WHERE { ?s <urn:p> ?o OPTIONAL { ?s <urn:q> ?r } }").is_empty());
    assert_eq!(
        never("SELECT ?s ?x ?y WHERE { ?s <urn:p> <urn:o> }"),
        ["x", "y"]
    );
}

/// A query with a grouping or a subquery is computed in full, and one without is not.
#[test]
fn groupings_and_subqueries_are_computed_in_full() {
    let in_full = |text: &str| {
        SparqlQuery::parse(text, None, [])
            .expect("parses")
            .computes_in_full()
    };
    assert!(!in_full(
        "SELECT DISTINCT ?s WHERE { ?s <urn:p> ?o } ORDER BY ?s"
    ));
    assert!(!in_full(
        "SELECT ?s ?r WHERE { ?s <urn:p> ?o OPTIONAL { ?s <urn:q> ?r } }"
    ));
    assert!(in_full(
        "SELECT ?s (COUNT(?o) AS ?n) WHERE { ?s <urn:p> ?o } GROUP BY ?s"
    ));
    assert!(in_full("SELECT (COUNT(*) AS ?n) WHERE { ?s <urn:p> ?o }"));
    assert!(in_full(
        "SELECT ?s WHERE { ?s <urn:p> ?o { SELECT ?o WHERE { ?o <urn:q> ?r } LIMIT 3 } }"
    ));
}

/// The query's own filter above the `VALUES` block does not replace the guard below it.
#[test]
fn a_filter_above_the_values_does_not_guard_them() {
    // The query's own filter sees ?p as the block binds it, so a solution without a parent
    // would join the block and pass. The guard must be below the block.
    let query = SparqlQuery::parse(
        "SELECT ?c ?p WHERE { ?c a <urn:C> OPTIONAL { ?c <urn:parent> ?p } FILTER(BOUND(?p)) }",
        None,
        [],
    )
    .expect("parses");
    assert!(!query.may_leave_unbound());
    let text = query
        .with_values(&[ValuesBlock {
            positions: vec![1],
            rows: vec![vec![DataValue::iri("urn:p")]],
        }])
        .text()
        .to_owned();
    assert_eq!(text.matches("BOUND(?p)").count(), 2, "{text}");
    let guard = text.find("BOUND(?p)").expect("a guard");
    assert!(guard < text.find("VALUES").expect("a block"), "{text}");
}

/// A `VALUES` block is joined in under the projection, with the modifiers kept above it.
#[test]
fn values_join_in_under_the_projection() {
    let term = DataValue::iri;
    let query = SparqlQuery::parse(
        "SELECT DISTINCT ?a ?c WHERE { ?a <urn:p> ?c } ORDER BY ?c",
        None,
        [],
    )
    .expect("valid");
    let bound = query
        .with_values(&[ValuesBlock {
            positions: vec![0],
            rows: vec![vec![term("urn:x")], vec![term("urn:y")]],
        }])
        .text()
        .to_owned();
    assert!(bound.contains("VALUES ( ?a )"), "{bound}");
    assert!(
        bound.contains("<urn:x>") && bound.contains("<urn:y>"),
        "{bound}"
    );

    // The modifiers stay above the block.
    assert!(bound.starts_with("SELECT DISTINCT ?a ?c"), "{bound}");
    assert!(bound.trim_end().ends_with("ORDER BY ASC(?c)"), "{bound}");

    // Parsing the text again gives the same projection, so the block did not widen it.
    let reparsed = SparqlQuery::parse(&bound, None, []).expect("the bound query parses");
    assert_eq!(reparsed.variables(), ["a", "c"]);
    assert_eq!(reparsed.shape(), QueryShape::Pattern);
}

/// A `VALUES` block is placed inside the filters at the top,
/// so a `NOT EXISTS` sees what it binds.
#[test]
fn values_land_inside_the_filters_so_absences_see_them() {
    let term = DataValue::iri;
    let query = SparqlQuery::parse(
        "SELECT ?s ?i WHERE { ?s <urn:p> ?o . FILTER NOT EXISTS { ?o ?i ?s } }",
        None,
        [],
    )
    .expect("valid");
    let bound = query
        .with_values(&[ValuesBlock {
            positions: vec![1],
            rows: vec![vec![term("urn:inverse")]],
        }])
        .text()
        .to_owned();

    // The block binds ?i before the absence is decided: it is
    // in the same group, before the FILTER.
    let values = bound.find("VALUES").expect("bound");
    let filter = bound.find("FILTER").expect("filtered");
    assert!(values < filter, "{bound}");
    let reparsed = SparqlQuery::parse(&bound, None, []).expect("parses");
    assert_eq!(reparsed.variables(), ["s", "i"]);
}

/// A `VALUES` block on an aggregate's variable is placed above the grouping.
#[test]
fn values_on_an_aggregate_result_land_above_the_group() {
    let query = SparqlQuery::parse(
        "SELECT ?a (COUNT(?c) AS ?n) WHERE { ?a ?b ?c } GROUP BY ?a",
        None,
        [],
    )
    .expect("valid");
    let literal = DataValue::integer(2);
    let bound = query
        .with_values(&[ValuesBlock {
            positions: vec![1],
            rows: vec![vec![literal]],
        }])
        .text()
        .to_owned();

    // The block is placed where ?n exists, outside the grouping, which
    // the rendered text keeps whole as a subquery.
    assert!(bound.contains("VALUES ( ?n )"), "{bound}");
    assert!(
        bound.contains("\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        "{bound}"
    );
    let reparsed = SparqlQuery::parse(&bound, None, []).expect("parses");
    assert_eq!(reparsed.variables(), ["a", "n"]);
    assert_eq!(reparsed.shape(), QueryShape::Complex);
}

/// Joining `VALUES` blocks into an opaque query panics.
#[test]
#[should_panic(expected = "opaque query")]
fn values_cannot_join_an_opaque_query() {
    SparqlQuery::parse("SELECT ?a WHERE { ?a ?b ?c } LIMIT 3", None, [])
        .expect("valid")
        .with_values(&[]);
}

/// A filter is added under the projection, which stays as it was.
#[test]
fn filters_push_under_the_projection() {
    let query =
        SparqlQuery::parse("SELECT ?a ?l WHERE { ?a <urn:label> ?l }", None, []).expect("valid");
    let condition = Expression::Equal(
        Box::new(Expression::FunctionCall(
            spargebra::algebra::Function::Lang,
            vec![Expression::Variable(Variable::new_unchecked("l"))],
        )),
        Box::new(Expression::Literal(Literal::new_simple_literal("en"))),
    );
    let filtered = query.with_filter(condition);
    assert!(filtered.text().contains("FILTER"), "{}", filtered.text());
    assert!(filtered.text().contains("LANG(?l)"), "{}", filtered.text());
    assert_eq!(filtered.variables(), ["a", "l"]);
}

/// A value's ground term is in its canonical lexical form, and a null has none.
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

    // An integer is an `xsd:integer`, whatever datatype it was read from.
    let int = DataValue::typed_literal("http://www.w3.org/2001/XMLSchema#int", "03");
    assert_eq!(
        render(int.expect("an int")).as_deref(),
        Some("\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>")
    );

    // Doubles and floats are in XSD's canonical form,
    // and the special values are written as Nemo writes them.
    let double = |value: f64| render(DataValue::double(value));
    let xsd_double = |lexical: &str| {
        Some(format!(
            "\"{lexical}\"^^<http://www.w3.org/2001/XMLSchema#double>"
        ))
    };
    assert_eq!(double(2.5), xsd_double("2.5E0"));
    assert_eq!(double(1000.0), xsd_double("1.0E3"));
    assert_eq!(double(-0.1), xsd_double("-1.0E-1"));
    assert_eq!(double(0.0), xsd_double("0.0E0"));
    assert_eq!(double(f64::NAN), xsd_double("NaN"));
    assert_eq!(double(f64::NEG_INFINITY), xsd_double("-INF"));
    assert_eq!(
        render(DataValue::float(0.5)).as_deref(),
        Some("\"5.0E-1\"^^<http://www.w3.org/2001/XMLSchema#float>")
    );

    // A query cannot ask an endpoint about a null.
    assert_eq!(render(DataValue::null(7)), None);
}

/// An integer is sent as an integer and as a decimal, and every other value as its one term.
#[test]
fn an_integer_is_sent_as_an_integer_and_as_a_decimal() {
    let terms = |value: DataValue| -> Vec<String> {
        SparqlQuery::ground_terms(&value)
            .iter()
            .map(ToString::to_string)
            .collect()
    };
    assert_eq!(
        terms(DataValue::integer(3)),
        [
            "\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            "\"3.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"
        ]
    );
    assert_eq!(
        terms(DataValue::integer(-12)),
        [
            "\"-12\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            "\"-12.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"
        ]
    );

    // Every other value is sent as its one term.
    assert_eq!(
        terms(DataValue::decimal(15, -1).expect("representable")),
        ["\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"]
    );
    assert_eq!(
        terms(DataValue::double(3.0)),
        ["\"3.0E0\"^^<http://www.w3.org/2001/XMLSchema#double>"]
    );
    assert_eq!(terms(DataValue::string("3")), ["\"3\""]);
    assert_eq!(terms(DataValue::iri("urn:a")), ["<urn:a>"]);
    assert!(terms(DataValue::null(1)).is_empty());
}
