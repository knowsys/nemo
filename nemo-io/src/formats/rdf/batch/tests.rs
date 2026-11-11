//! Tests that batches of RDF resources agree with sequential reads:
//! windows, line numbers, blank nodes, conditions, and limits.

use super::*;

use nemo_physical::{
    function::{OpCode, StackProgram},
    value::{DataValue, ValueRef},
};

use crate::{
    file_format::RdfVariant, load::Reader, rdf::RdfReaderOptions, selection::RowCondition,
};

/// The datatype of integers.
const INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

/// Returns N-Triples with one statement per line, of these kinds in turn:
/// blank nodes whose labels recur throughout the text, integers, language-tagged strings,
/// integers that their datatype rejects, plain strings, IRIs, and lines with a syntax error.
fn triples(count: usize) -> String {
    (1..=count)
        .map(|index| match index % 7 {
            1 => format!(
                "_:b{} <http://x/knows> _:b{} .\n",
                index % 50,
                (index + 3) % 50
            ),
            2 => format!("<http://x/s{index}> <http://x/age> \"{index}\"^^<{INTEGER}> .\n"),
            3 => format!("<http://x/s{index}> <http://x/name> \"name {index}\"@en .\n"),
            4 => format!("<http://x/s{index}> <http://x/age> \"old\"^^<{INTEGER}> .\n"),
            5 => format!("<http://x/s{index}> <http://x/label> \"label {index}\" .\n"),
            6 => format!(
                "<http://x/s{index}> <http://x/link> <http://x/s{}> .\n",
                index / 2
            ),
            _ => format!("<http://x/s{index}> <http://x/label> \"unterminated .\n"),
        })
        .collect()
}

/// Loads a text as the one resource of a batch whose windows aim for the given size,
/// and returns the load, its dictionary, and the number of windows.
fn load_windowed(
    text: &str,
    options: RdfReaderOptions,
    window_bytes: u64,
) -> (Loaded, Dictionary, usize) {
    let resource = RdfResource::bytes("statements", text.as_bytes(), RdfReader::new(options));
    let mut batch = RdfBatch::new(vec![resource]);
    batch.window_bytes = window_bytes;

    let mut dictionary = Dictionary::new();
    let mut loaded = batch
        .load(&mut dictionary, EvaluationContext::now())
        .expect("the batch loads");

    (
        loaded.pop().expect("one load per resource"),
        dictionary,
        batch.windows(0),
    )
}

/// N-Triples cut into many windows load as a sequential read loads them:
/// the same rows in the same order, the same null for each blank node label across the
/// windows, and the same counts.
#[test]
fn windows_load_what_a_read_gives() {
    let text = triples(3000);
    let options = RdfReaderOptions::new(RdfVariant::NTriples);
    let (loaded, dictionary, windows) = load_windowed(&text, options.clone(), 4096);
    assert!(windows > 8, "the text is cut into {windows} windows");

    let mut sequential_dictionary = Dictionary::new();
    let sequential = RdfReader::new(options)
        .read(
            text.as_bytes(),
            &mut sequential_dictionary,
            EvaluationContext::now(),
        )
        .expect("the text reads");

    // Of the 3000 lines, 428 have a syntax error and 429 an integer that is none.
    assert_eq!(sequential.rows.len(), 2143);
    assert_eq!(
        (
            sequential.counts.malformed,
            sequential.counts.first_malformed
        ),
        (857, Some(4))
    );

    assert_eq!(
        loaded.decoded(&dictionary),
        sequential.decoded(&sequential_dictionary)
    );
    assert_eq!(loaded.counts, sequential.counts);
}

/// A malformed line in a later window is reported at its line in the resource.
#[test]
fn a_malformed_line_of_a_later_window_keeps_its_line() {
    let mut text: String = (1..=2000)
        .map(|index| format!("<http://x/s{index}> <http://x/p> <http://x/o> .\n"))
        .collect();
    text.push_str("<http://x/s> <http://x/p> .\n");
    text.push_str("<http://x/s> <http://x/p> <http://x/o> .\n");

    let options = RdfReaderOptions::new(RdfVariant::NTriples);
    let (loaded, _, windows) = load_windowed(&text, options, 4096);
    assert!(windows > 8, "the text is cut into {windows} windows");
    assert_eq!(loaded.rows.len(), 2001);
    assert_eq!(
        (loaded.counts.malformed, loaded.counts.first_malformed),
        (1, Some(2001))
    );
}

/// A condition sees a blank node as the null that is stored for it.
///
/// The condition here compares the term syntax of the subject, which has the null's number.
#[test]
fn a_condition_sees_a_blank_node_as_its_stored_null() {
    let text = "_:a <http://x/p> \"1\" .\n\
                _:b <http://x/p> \"2\" .\n\
                _:a <http://x/p> \"3\" .\n\
                <http://x/c> <http://x/p> \"4\" .\n";
    let options = RdfReaderOptions::new(RdfVariant::NTriples);
    let (all, dictionary, _) = load_windowed(text, options.clone(), WINDOW_BYTES);
    let second = all.decoded(&dictionary)[1][0].clone();
    assert!(matches!(second.as_ref(), ValueRef::Null(_)));

    let mut filtered = options;
    filtered.selection.condition = Some(RowCondition::new(
        vec![0],
        StackProgram::new(
            vec![
                OpCode::Input(0),
                OpCode::TermSyntax,
                OpCode::Constant(0),
                OpCode::Equal,
            ],
            vec![DataValue::string(second.to_string())],
            1,
        ),
    ));
    let (loaded, dictionary, _) = load_windowed(text, filtered, WINDOW_BYTES);
    assert_eq!(
        loaded.decoded(&dictionary),
        [[second, DataValue::iri("http://x/p"), DataValue::string("2")]]
    );
    assert_eq!(loaded.counts.excluded, 3);
}

/// A resource is cut into windows only if every line is read on its own:
/// Turtle is not, since its prefixes hold for the lines after them,
/// and neither are N-Triples under a limit, which counts the statements across the lines.
#[test]
fn only_lines_read_on_their_own_are_cut() {
    let statements: String = (1..=2000)
        .map(|index| format!("ex:s{index} ex:p ex:o{index} .\n"))
        .collect();
    let turtle = format!("@prefix ex: <http://x/> .\n{statements}");
    let (loaded, _, windows) =
        load_windowed(&turtle, RdfReaderOptions::new(RdfVariant::Turtle), 4096);
    assert_eq!(windows, 1);
    assert_eq!((loaded.rows.len(), loaded.counts.malformed), (2000, 0));

    let text: String = (1..=2000)
        .map(|index| format!("<http://x/s{index}> <http://x/p> <http://x/o> .\n"))
        .collect();
    let mut limited = RdfReaderOptions::new(RdfVariant::NTriples);
    limited.limit = Some(10);
    let (loaded, dictionary, windows) = load_windowed(&text, limited, 4096);
    assert_eq!(windows, 1);
    let subjects: Vec<DataValue> = loaded
        .decoded(&dictionary)
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    let first: Vec<DataValue> = (1..=10)
        .map(|index| DataValue::iri(format!("http://x/s{index}")))
        .collect();
    assert_eq!(subjects, first);
}
