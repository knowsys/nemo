//! The two readers of term syntax agree: the parser of programs,
//! and the reader of `nemo-physical`, which reads the cells of files.

use nemo_physical::value::{MapValue, TupleValue, ValueRef};

use super::*;

/// Returns the value of a constant as a program without a base and without prefixes reads it,
/// or `None` if the program is rejected.
fn program_value(text: &str) -> Option<DataValue> {
    let (program, diagnostics) = Program::parse(&format!("p({text}) ."));
    if !diagnostics.is_empty() {
        return None;
    }

    let mut engine = Engine::new();
    engine.load(program).ok()?;
    engine.reason().ok()?;
    let mut rows = engine.rows("p").ok()?;

    (rows.len() == 1 && rows[0].len() == 1).then(|| rows.remove(0).remove(0))
}

/// Returns the value of a constant as the reader of files reads it, with every null as the
/// null 0.
fn file_value(text: &str) -> Option<DataValue> {
    DataValue::from_term_syntax(text, &mut |_| DataValue::null(0))
}

/// Both readers read the text that `Display` writes for a value back as that value.
#[test]
fn both_readers_read_back_what_display_writes() {
    let typed = |datatype: &str, lexical: &str| {
        let datatype = format!("http://www.w3.org/2001/XMLSchema#{datatype}");
        DataValue::typed_literal(&datatype, lexical).expect("the literal is valid")
    };
    let tuple = |elements: Vec<DataValue>| DataValue::tuple(TupleValue::new(elements));
    let values = [
        DataValue::iri("alice"),
        DataValue::iri("item-1"),
        DataValue::iri("http://example.org/a"),
        DataValue::iri("a b>c"),
        DataValue::iri("true"),
        DataValue::iri("_"),
        DataValue::iri("42"),
        DataValue::string(""),
        DataValue::string("say \"hi\"\\\n\r\tend"),
        DataValue::language_string("colour", "en-gb").expect("the tag is valid"),
        DataValue::boolean(true),
        DataValue::integer(-42),
        DataValue::decimal_from_lexical("-1.50").expect("valid"),
        DataValue::double(2.5),
        DataValue::double(f64::NEG_INFINITY),
        DataValue::float(2.5),
        typed("dateTime", "2024-02-29T12:00:00Z"),
        typed("duration", "P1Y2M3DT4H"),
        DataValue::other("http://example.org/datatype", "lexical").expect("not native"),
        tuple(vec![]),
        tuple(vec![DataValue::integer(1)]),
        DataValue::tuple(TupleValue::labeled(
            "point",
            vec![DataValue::integer(1), tuple(vec![DataValue::iri("x")])],
        )),
        DataValue::tuple(TupleValue::labeled("http://example.org/point", vec![])),
        DataValue::map(
            MapValue::new([
                (DataValue::iri("a"), DataValue::integer(1)),
                (DataValue::string("b"), DataValue::boolean(false)),
            ])
            .expect("the keys differ"),
        ),
        DataValue::map(MapValue::labeled("box", []).expect("the map is empty")),
    ];

    for value in values {
        let written = value.to_string();
        assert_eq!(program_value(&written).as_ref(), Some(&value), "{written}");
        assert_eq!(file_value(&written), Some(value), "{written}");
    }
}

/// Both readers read the same texts as constants, and each as the same value.
#[test]
fn both_readers_read_the_same_constants() {
    let texts = [
        // Names and IRIs.
        "alice",
        "INF",
        "<alice>",
        r"<http://e.org/café>",
        "ex:alice",
        ":alice",
        "lib::alice",
        "_",
        "http://example.org/a",
        "Alice Smith",
        "<a b>",
        // Numbers and booleans.
        "+5",
        "- 5",
        "007",
        "-0x1F",
        "1.50",
        "1E3",
        "2.5f",
        "2D",
        "1e",
        "5.",
        ".5",
        "true",
        "false",
        "true(1)",
        // Strings.
        r#""aé\d""#,
        "\"\"\"two \"quoted\"\nlines\"\"\"",
        r#""Hallo"@DE"#,
        r#""5"^^<http://www.w3.org/2001/XMLSchema#integer>"#,
        r#""5" ^^ <http://www.w3.org/2001/XMLSchema#integer>"#,
        r#""5"^^meters"#,
        r#""5"^^ex:meters"#,
        r#""five"^^<http://www.w3.org/2001/XMLSchema#integer>"#,
        "\"open",
        r#""a"@"#,
        r#""a" @en"#,
        r#""a"@en^^<b>"#,
        r#""a"^<b>"#,
        // Tuples and maps.
        "()",
        "(1,)",
        "(1)",
        "( 1 , (2, 3) )",
        "(1, 2,)",
        "(,)",
        "(1 2)",
        "point(1, 2)",
        "point (1)",
        "point(1,)",
        "<http://e.org/p>(1)",
        "{}",
        "{a = 1, \"b\" = (2,)}",
        "box{a = 1}",
        "<http://e.org/p>{a = 1}",
        "box {a = 1}",
        "{a = 1,}",
        "{a = 1, a = 2}",
        "{a}",
        // Terms that are no constants.
        "?x",
        "!x",
    ];

    for text in texts {
        assert_eq!(program_value(text), file_value(text), "{text}");
    }
}

/// Both readers read a blank node as a null.
#[test]
fn both_readers_read_a_blank_node_as_a_null() {
    for value in [program_value("_:b.1"), file_value("_:b.1")] {
        let value = value.expect("the blank node is read");
        assert!(matches!(value.as_ref(), ValueRef::Null(_)), "{value}");
    }
}

/// A term that is computed is a term of a program, and not a constant of a file.
#[test]
fn a_file_holds_no_computed_terms() {
    for (text, value) in [
        ("1 + 2", DataValue::integer(3)),
        ("--5", DataValue::integer(5)),
        ("-(5)", DataValue::integer(-5)),
        ("f\"a{1}\"", DataValue::string("a1")),
    ] {
        assert_eq!(program_value(text), Some(value), "{text}");
        assert_eq!(file_value(text), None, "{text}");
    }
}

/// In a file, a name before parentheses labels a tuple, whatever the name is.
/// A program calls the builtin of that name instead,
/// so it does not read back what `Display` writes for a tuple with such a label.
#[test]
fn a_file_reads_the_name_of_a_builtin_as_a_label() {
    let text = "STRLEN(\"abc\")";
    let labeled = DataValue::tuple(TupleValue::labeled(
        "STRLEN",
        vec![DataValue::string("abc")],
    ));

    assert_eq!(labeled.to_string(), text);
    assert_eq!(file_value(text), Some(labeled));
    assert_eq!(program_value(text), Some(DataValue::integer(3)));
}
