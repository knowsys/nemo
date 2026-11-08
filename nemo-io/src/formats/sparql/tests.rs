//! SPARQL queries: validation, shapes, bindings and filters joined in,
//! ground terms, and reading TSV responses.

use nemo_physical::value::ValueRef;

use super::*;

/// Reads a response as a read of its own, without bindings, delivering every column.
fn read_response(body: &str, arity: usize, dictionary: &mut Dictionary) -> Loaded {
    let blanks = BlankNodes::open(None, "response", dictionary);
    let loaded = read_tsv(
        body.as_bytes(),
        arity,
        &RowSelection::default(),
        &[],
        &blanks,
        dictionary,
        EvaluationContext::now(),
    );
    blanks.close(dictionary);
    loaded.expect("the response reads")
}

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
    // The guard closes its group before the block joins it.
    let between = &text[guard..values];
    assert!(between.contains('}'), "{text}");
}

#[test]
fn queries_tell_whether_a_solution_may_leave_a_variable_unbound() {
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
    // A filter requiring the variable bound keeps only the solutions binding it.
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

#[test]
fn a_filter_above_the_values_does_not_guard_them() {
    // The query's own filter sees ?p as the block binds it, so a solution without a parent
    // would join the block and pass; the guard must sit below the block.
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
    // The block sits where ?n exists: outside the grouping, which
    // the rendering keeps whole as a subquery.
    assert!(bound.contains("VALUES ( ?n )"), "{bound}");
    assert!(
        bound.contains("\"2\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        "{bound}"
    );
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
        Box::new(Expression::Literal(Literal::new_simple_literal("en"))),
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
    // An integer is an `xsd:integer`, whatever datatype it was read from.
    let int = DataValue::typed_literal("http://www.w3.org/2001/XMLSchema#int", "03");
    assert_eq!(
        render(int.expect("an int")).as_deref(),
        Some("\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>")
    );
    // Doubles and floats in XSD's canonical form, the special values as Nemo writes them.
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

#[test]
fn an_integer_goes_out_as_an_integer_and_as_a_decimal() {
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
    // Every other value goes out as its one term.
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

#[test]
fn tsv_cells_parse_as_turtle_terms() {
    let body = "?s\t?p\t?o\n\
        <http://example.org/a>\t\"plain\"\t42\n\
        _:b0\t\"caf\\u00e9\"@fr\t-1.5\n\
        _:b0\t\"7\"^^<http://www.w3.org/2001/XMLSchema#integer>\t1e3\n\
        true\t\"tab\\there\"\t\n";
    let mut dictionary = Dictionary::new();
    let loaded = read_response(body, 3, &mut dictionary);
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
        ValueRef::Null(id) if row(1)[0].as_ref() != ValueRef::Null(id)
    ));
}

/// In a response of one column, a solution whose variable is unbound is an empty line,
/// and a row of its own; in a wider response, an empty line is no solution.
#[test]
fn an_unbound_solution_of_one_column_is_an_empty_line() {
    let read = |body: &str, arity: usize| read_response(body, arity, &mut Dictionary::new());
    let loaded = read("?c\n\n\"c2\"\r\n\n", 1);
    assert_eq!((loaded.rows.len(), loaded.malformed), (3, 0));
    let loaded = read("?c\n", 1);
    assert_eq!(loaded.rows.len(), 0, "a header alone holds no solution");
    let loaded = read("?a\t?b\n<http://e/a>\t1\n\n<http://e/b>\t2\n", 2);
    assert_eq!((loaded.rows.len(), loaded.malformed), (2, 0));
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
fn a_response_ending_partway_through_a_row_is_cut_off() {
    // A connection closing early leaves an unterminated, broken last line.
    for cut in [
        "?x\t?y\n<http://e/a>\t<http://e/b>\n<http://e/c>\t<http://e/d",
        "?x\t?y\n<http://e/a>\t\"half a lit",
        "?x\t?y\n<http://e/a>\t\"a\"^^<http://www.w3.org/2001/XMLSchema#str",
        "?x\t?y\n<http://e/a>\t<http://e/b>\n<http://e/c>",
        "?x\t?y\n<http://e/a>\t<http://e/b>\n<http://e/c>\t_:",
    ] {
        let problem = check_complete("http://e/sparql", cut.as_bytes()).expect_err(cut);
        assert_eq!(problem.code(), crate::codes::SPARQL_RESPONSE_CUT, "{cut}");
        assert!(SparqlReader::is_cut_off(cut.as_bytes()), "{cut}");
    }
    // A whole last row needs no line end, nor does a header alone.
    for whole in [
        "?x\t?y\n<http://e/a>\t<http://e/b>",
        "?x\t?y\n<http://e/a>\t\"a\"@en",
        "?x\t?y\n<http://e/a>\t12",
        "?x\t?y\n<http://e/a>\t\r\n",
        "?x\t?y\n",
        "?x\t?y",
    ] {
        assert!(
            check_complete("http://e/sparql", whole.as_bytes()).is_ok(),
            "{whole}"
        );
    }
}

/// A last row without a line end is whole if its cells are whole terms by their syntax,
/// even if a datatype rejects a lexical form, which then reads as one malformed row.
#[test]
fn a_whole_last_row_is_told_by_its_syntax() {
    let whole = [
        "?a\t?b\n<urn:a>\t\"2020-13-45\"^^<http://www.w3.org/2001/XMLSchema#date>",
        "?a\t?b\n<urn:a>\t\"say \\\"hi\\\"\"@en-gb",
        "?a\t?b\n_:b0\t42",
        "?a\t?b\n<urn:a>\t",
    ];
    for body in whole {
        assert!(
            check_complete("http://e/sparql", body.as_bytes()).is_ok(),
            "{body}"
        );
    }
    let cut = [
        "?a\t?b\n<urn:a>\t<urn:b",
        "?a\t?b\n<urn:a>\t\"unfinish",
        "?a\t?b\n<urn:a>\t\"typed\"^^<http://www.w3.org/2001/XML",
        "?a\t?b\n<urn:a>",
    ];
    for body in cut {
        assert!(
            check_complete("http://e/sparql", body.as_bytes()).is_err(),
            "{body}"
        );
    }
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
    let loaded = read_response(body, 1, &mut Dictionary::new());
    assert_eq!((loaded.rows.len(), loaded.malformed), (1, 1));
    assert_eq!(loaded.first_malformed, Some(2));
}

#[test]
fn a_row_goes_out_as_the_combinations_of_its_terms() {
    let query = SparqlQuery::parse(
        "SELECT ?s ?v ?b WHERE { ?s <urn:p> ?v ; <urn:b> ?b }",
        None,
        [],
    )
    .expect("valid");
    let text = query
        .with_values(&[ValuesBlock {
            positions: vec![0, 1, 2],
            rows: vec![
                vec![
                    DataValue::iri("urn:a"),
                    DataValue::integer(3),
                    DataValue::boolean(true),
                ],
                // A null has no term, so its row is left out.
                vec![
                    DataValue::null(1),
                    DataValue::integer(4),
                    DataValue::boolean(false),
                ],
            ],
        }])
        .text()
        .to_owned();
    // The integer goes out as an integer and as a decimal, so its row as two rows.
    assert!(text.contains("VALUES ( ?s ?v ?b )"), "{text}");
    assert_eq!(text.matches("( <urn:a> ").count(), 2, "{text}");
    assert!(
        text.contains("\"3\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
        "{text}"
    );
    assert!(
        text.contains("\"3.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
        "{text}"
    );
    assert!(!text.contains("\"4\""), "{text}");
    assert!(!text.contains("FILTER"), "{text}");
    SparqlQuery::parse(&text, None, []).expect("the query parses");
}

#[test]
fn responses_sharing_a_scope_agree_on_blank_nodes() {
    let body = "?s\t?o\n_:b0\t<urn:x>\n";
    let read = |reader: &SparqlReader, dictionary: &mut Dictionary| {
        let loaded = reader
            .read(
                "http://e/sparql",
                body.as_bytes(),
                dictionary,
                EvaluationContext::now(),
            )
            .expect("the response reads");
        dictionary
            .decode(loaded.rows.row(0)[0])
            .expect("decodes")
            .to_owned()
    };
    let mut dictionary = Dictionary::new();
    // Per response, as SPARQL promises: two responses, two nulls.
    let own = SparqlReader::new(2, RowSelection::default());
    assert_ne!(read(&own, &mut dictionary), read(&own, &mut dictionary));
    // In the endpoint's scope, one label names one null in every response,
    // which reports the endpoint as its source.
    let shared = SparqlReader::new(2, RowSelection::default()).with_endpoint_blank_nodes();
    let first = read(&shared, &mut dictionary);
    assert_eq!(read(&shared, &mut dictionary), first);
    let ValueRef::Null(id) = first.as_ref() else {
        panic!("a blank node is a null: {first}");
    };
    assert!(SparqlReader::is_endpoint_blank_node(&dictionary, id));
    let fresh = dictionary.fresh_null();
    assert!(!SparqlReader::is_endpoint_blank_node(&dictionary, fresh));
}

#[test]
fn a_reader_told_the_bindings_keeps_the_solutions_that_match_them() {
    let block = ValuesBlock {
        positions: vec![1],
        rows: vec![vec![DataValue::integer(3)], vec![DataValue::double(2.5)]],
    };
    // The second column is not delivered, but the bindings still decide.
    let selection = RowSelection {
        columns: Some(vec![true, false]),
        filter: None,
    };
    let reader = SparqlReader::new(2, selection).with_bindings(std::slice::from_ref(&block));
    let body = [
        "?s\t?v",
        "<urn:a>\t\"3.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
        "<urn:b>\t\"03\"^^<http://www.w3.org/2001/XMLSchema#int>",
        "<urn:c>\t2.5e0",
        "<urn:d>\t\"3.0e0\"^^<http://www.w3.org/2001/XMLSchema#double>",
        "<urn:e>\t",
        "<urn:f>\t_:x",
        "",
    ]
    .join("\n");
    let mut dictionary = Dictionary::new();
    let loaded = reader
        .read(
            "http://e/sparql",
            body.as_bytes(),
            &mut dictionary,
            EvaluationContext::now(),
        )
        .expect("the response reads");
    // Integer 3 for the first two, double 2.5 for the third, however the endpoint matched
    // them; the double 3 is a different value to Nemo, and the last two match nothing.
    assert_eq!((loaded.rows.len(), loaded.malformed), (3, 0));
}

/// A bound cell that is not a term makes its row malformed, counted as such,
/// while an unbound value or a blank node in a bound column simply matches no binding.
#[test]
fn a_malformed_bound_cell_is_counted() {
    let block = ValuesBlock {
        positions: vec![1],
        rows: vec![vec![DataValue::integer(3)]],
    };
    let selection = RowSelection {
        columns: Some(vec![true, false]),
        filter: None,
    };
    let reader = SparqlReader::new(2, selection).with_bindings(std::slice::from_ref(&block));
    let body = "?s\t?v\n<urn:a>\t3\n<urn:b>\tnot a term\n<urn:c>\t\n<urn:d>\t_:x\n";
    let loaded = reader
        .read(
            "http://e/sparql",
            body.as_bytes(),
            &mut Dictionary::new(),
            EvaluationContext::now(),
        )
        .expect("the response reads");
    assert_eq!((loaded.rows.len(), loaded.malformed), (1, 1));
    assert_eq!(loaded.first_malformed, Some(3));
}
