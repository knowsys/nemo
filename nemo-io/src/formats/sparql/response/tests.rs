//! Tests of reading SPARQL TSV responses.

use nemo_physical::value::ValueRef;

use super::*;

/// Reads a response in a blank node scope of its own, without bindings, delivering every column.
fn read_response(body: &str, width: usize, dictionary: &mut Dictionary) -> Loaded {
    let blanks = BlankNodes::open(None, "response", dictionary);
    let loaded = read_tsv(
        body.as_bytes(),
        width,
        &RowSelection::default(),
        &[],
        &blanks,
        dictionary,
        EvaluationContext::now(),
    );
    blanks.close(dictionary);
    loaded.expect("the response reads")
}

/// TSV cells are parsed as Turtle terms.
///
/// A blank node label names one null across rows, and an unbound cell becomes a fresh null.
#[test]
fn tsv_cells_parse_as_turtle_terms() {
    let body = "?s\t?p\t?o\n\
        <http://example.org/a>\t\"plain\"\t42\n\
        _:b0\t\"caf\\u00e9\"@fr\t-1.5\n\
        _:b0\t\"7\"^^<http://www.w3.org/2001/XMLSchema#integer>\t1e3\n\
        true\t\"tab\\there\"\t\n";
    let mut dictionary = Dictionary::new();
    let loaded = read_response(body, 3, &mut dictionary);
    assert_eq!((loaded.rows.len(), loaded.counts.malformed), (4, 0));
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
/// and a row of its own.
///
/// In a wider response, an empty line is not a solution.
#[test]
fn an_unbound_solution_of_one_column_is_an_empty_line() {
    let read = |body: &str, width: usize| read_response(body, width, &mut Dictionary::new());
    let loaded = read("?c\n\n\"c2\"\r\n\n", 1);
    assert_eq!((loaded.rows.len(), loaded.counts.malformed), (3, 0));
    let loaded = read("?c\n", 1);
    assert_eq!(loaded.rows.len(), 0, "a header alone holds no solution");
    let loaded = read("?a\t?b\n<http://e/a>\t1\n\n<http://e/b>\t2\n", 2);
    assert_eq!((loaded.rows.len(), loaded.counts.malformed), (2, 0));
}

/// A cell that is not a term makes its row malformed, reported at its line.
#[test]
fn a_cell_that_is_no_term_makes_the_row_malformed() {
    let body = "?x\nnot a term\n<http://example.org/ok>\n";
    let loaded = read_response(body, 1, &mut Dictionary::new());
    assert_eq!((loaded.rows.len(), loaded.counts.malformed), (1, 1));
    assert_eq!(loaded.counts.first_malformed, Some(2));
}

/// Responses read in the endpoint's scope agree on blank nodes,
/// while responses read in scopes of their own do not.
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

    // In a scope for each response, as SPARQL specifies, two responses give two nulls.
    let own = SparqlReader::new(2, RowSelection::default());
    assert_ne!(read(&own, &mut dictionary), read(&own, &mut dictionary));

    // In the endpoint's scope, one label names one null in every response,
    // and the null's scope shows that it came from an endpoint.
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

/// A reader given the values of a query keeps only the solutions that match them,
/// as Nemo compares values.
#[test]
fn a_reader_given_the_values_keeps_the_solutions_that_match_them() {
    let block = ValuesBlock {
        positions: vec![1],
        rows: vec![vec![DataValue::integer(3)], vec![DataValue::double(2.5)]],
    };

    // The second column is not delivered, but the values still decide.
    let selection = RowSelection {
        columns: Some(vec![true, false]),
        condition: None,
    };
    let reader = SparqlReader::new(2, selection).with_values(std::slice::from_ref(&block));
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

    // The first two are the integer 3 and the third is the double 2.5, however the endpoint
    // matched them. The double 3 is a different value for Nemo, and the last two match nothing.
    assert_eq!((loaded.rows.len(), loaded.counts.malformed), (3, 0));
}

/// A bound cell that is not a term makes its row malformed,
/// while an unbound value or a blank node in a bound column only matches no binding.
#[test]
fn a_malformed_bound_cell_is_counted() {
    let block = ValuesBlock {
        positions: vec![1],
        rows: vec![vec![DataValue::integer(3)]],
    };
    let selection = RowSelection {
        columns: Some(vec![true, false]),
        condition: None,
    };
    let reader = SparqlReader::new(2, selection).with_values(std::slice::from_ref(&block));
    let body = "?s\t?v\n<urn:a>\t3\n<urn:b>\tnot a term\n<urn:c>\t\n<urn:d>\t_:x\n";
    let loaded = reader
        .read(
            "http://e/sparql",
            body.as_bytes(),
            &mut Dictionary::new(),
            EvaluationContext::now(),
        )
        .expect("the response reads");
    assert_eq!((loaded.rows.len(), loaded.counts.malformed), (1, 1));
    assert_eq!(loaded.counts.first_malformed, Some(3));
}

/// A cell that is no term makes its solution malformed in a column that is not delivered too,
/// while an unbound value or a blank node in such a column keeps the solution.
#[test]
fn a_column_that_is_not_delivered_still_decides_whether_a_solution_is_well_formed() {
    let selection = RowSelection {
        columns: Some(vec![true, false]),
        condition: None,
    };
    let reader = SparqlReader::new(2, selection);
    let body = [
        "?s\t?v",
        "<urn:a>\t3",
        "<urn:b>\t\"abc\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        "<urn:c>\t",
        "<urn:d>\t_:x",
        "",
    ]
    .join("\n");
    let loaded = reader
        .read(
            "http://e/sparql",
            body.as_bytes(),
            &mut Dictionary::new(),
            EvaluationContext::now(),
        )
        .expect("the response reads");
    assert_eq!((loaded.rows.len(), loaded.counts.malformed), (3, 1));
    assert_eq!(loaded.counts.first_malformed, Some(3));
    assert_eq!(loaded.rows.arity(), 1);
}
