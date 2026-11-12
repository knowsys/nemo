//! Tests of recognizing responses that were cut off.

use super::*;

use crate::sparql::SparqlReader;

/// A response with a timeout trace after its rows is cut off, however long the trace.
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

/// A response whose last line is an unfinished row is cut off,
/// and one whose last row is whole is not, with a line end or without.
#[test]
fn a_response_ending_partway_through_a_row_is_cut_off() {
    // A connection closing early leaves a broken last line without a line end.
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
/// even if a datatype rejects a lexical form, which is then read as one malformed row.
#[test]
fn a_whole_last_row_is_recognized_by_its_syntax() {
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

    // A literal is whole as the reader of cells reads it, which knows no long form `"""…"""`.
    let cut = [
        "?a\t?b\n<urn:a>\t<urn:b",
        "?a\t?b\n<urn:a>\t\"unfinish",
        "?a\t?b\n<urn:a>\t\"typed\"^^<http://www.w3.org/2001/XML",
        "?a\t?b\n<urn:a>\t\"\"\"long\"\"\"",
        "?a\t?b\n<urn:a>",
    ];
    for body in cut {
        assert!(
            check_complete("http://e/sparql", body.as_bytes()).is_err(),
            "{body}"
        );
    }
}

/// A marker inside a term of a solution row does not make the response cut off.
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
