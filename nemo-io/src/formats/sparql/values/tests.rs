//! Tests of `VALUES` blocks as patterns of a query.

use super::*;

/// A `VALUES` row is sent once per combination of its values' terms,
/// and a row with a null is not sent at all.
#[test]
fn a_row_is_sent_as_the_combinations_of_its_terms() {
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
    // The integer is sent as an integer and as a decimal, so its row is sent as two rows.
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
