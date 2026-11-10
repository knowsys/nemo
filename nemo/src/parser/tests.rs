//! Tests of the parser: the syntax it accepts, how operators bind,
//! how it recovers from errors, and what its diagnostics say.

use super::{cursor::MAX_NESTING, *};

use nemo_physical::value::{Number, ValueDomain};

use crate::model::{Directive, Identifier, Literal, Operator, TermKind};

/// Statements written in their canonical form,
/// so that each one is displayed exactly as written after it is parsed.
const ROUND_TRIPS: &[&str] = &[
    "edge(1, 2) .",
    "reach(?x, ?y) :- edge(?x, ?y) .",
    "reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .",
    "adult(?p) :- person(?p, ?age), ~minor(?p), ?age >= 18 .",
    "big(?x) :- value(?x, ?v), ?v > 2 * ?y + 1 .",
    "p(?x) :- q(?x), CONTAINS(?x, \"a\") .",
    "total(?g, #sum(?v per ?g)) :- data(?g, ?v) .",
    "share(?g, #sum(?v) / 100) :- data(?g, ?v) .",
    "kinds(#count(), #count(?v)) :- data(?g, ?v) .",
    "pairs(?g, #count(per ?x, ?y)) :- data(?g, ?x, ?y) .",
    "roster(?t, #tuple(?p)) :- member(?t, ?p) .",
    "config(?s, #map(?k = ?v)) :- setting(?s, ?k, ?v) .",
    "listing(?t, STRJOIN(#tuple(?p), \", \")) :- member(?t, ?p) .",
    "nothing(()) .",
    "labels(\"hallo\"@de, \"P1Y\"^^xsd:duration) .",
    "structured((1, 2), {a = 1}) .",
    "constants(alice, foaf:Person, <https://example.org/a>, geo::area) .",
    "numbers(5, -5, 1.5, -1.5, true) .",
    "exists(?x) :- q(?x, !v) .",
    "#[name(\"adults\")]\nadult(?p) :- person(?p) .",
    "@base <https://example.org/> .",
    "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
    "@import edge from csv { resource = \"edge.csv\" } .",
    "@import q(?x, ?y) from sparql { endpoint = <https://e/sparql>, query = \"SELECT ?x ?y WHERE { ?x ?p ?y }\" } where b(?x), c(?y, 5) .",
    "@import q(?x) from sparql { endpoint = <https://e/sparql> }, ?g = 1 where b(?x, _) .",
    "@import age(?who, ?years) from csv { resource = \"a.csv\" } where isInteger(?years), ?years >= 0 .",
    "@import q(?x, ?y) from sparql { endpoint = <https://e/sparql> } where b(?x), ?y > 3 .",
    "@export reach to csv { resource = \"reach.csv\" } .",
    "@output reach .",
    "person(name: \"alice\", age: 34) .",
    "adult(?p) :- person(name: ?p, age: ?a), ?a >= 18 .",
    "in(wd:42, ex:, ex: * 2) .",
    "in(ex:v1.2, wd:4.2, _:b.1, :42, :a.b) .",
    "straße(café, ex:größe, _:ü, \"€\") .",
    "groß(?größe) :- maß(?größe, $höhe) .",
    "root(?x) :- p(?x), ?x = ex:, ex: != ?x + ex: .",
    "@declare city(name: string, location: point(double, double)) .",
    "@declare config(settings: { host: string, port: int }) .",
    "@parameter $threshold = 5 .",
    "@parameter $limit: int = 10 .",
    "@input edge .",
    "@input person(name: string, age: int) .",
    "@use tc { edge = link } as t .",
    "@use super::super::lib::tc { edge = link, $limit = 3 } as u .",
    "@define is_adult(?p) = ?p >= 18 .",
    "@extern geodistance(any, any) -> double .",
    "q(?x) :- p(?t), ?x = ?t[1][0] .",
    "q(?x) :- p(?t), ?x = ?t[host], LENGTH(?t) = 2 .",
    "q(?x) :- p((1, ?x)) .",
    "q(?h) :- p({host = ?h}) .",
    "@declare p(_(int, int), kinds: tuple, opts: map) .",
    "@declare q(point{ x: int }, _{ y: int }) .",
    "@type point(double, double) .",
    "@type config{ host: string, port: int } .",
    "@declare city(location: point, name: string) .",
    "@declare owner(iri | null, number, int | (int, int) | point) .",
    "@type tree(int, tree | leaf) .",
    "m(config{port = 80}) .",
    "q(?v) :- p(config{port = ?v}) .",
];

/// Block comments are skipped, even an unterminated one at the end,
/// and blank nodes are terms that are displayed as written.
#[test]
fn block_comments_are_trivia_and_blank_nodes_are_terms() {
    let (program, diagnostics) = Program::parse(
        "p(_:a, /* inline */ 1) . /* between\nstatements */ q(?x) :- p(_:a, ?x) . /* unterminated",
    );
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    assert_eq!(program.statements.len(), 2);
    let rendered = program.statements[0].to_string();
    assert_eq!(rendered, "p(_:a, 1) .");
    let (again, diagnostics) = Program::parse(&rendered);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    assert_eq!(again.statements[0], program.statements[0]);
    // A lone `_` is an anonymous variable, and `_x` is a name.
    let (program, diagnostics) = Program::parse("r(?y) :- s(_, ?y, _x) .");
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    assert!(program.statements[0].to_string().contains("s(_, ?y, _x)"));
}

/// Every IRI written in term syntax is parsed back as the same IRI.
#[test]
fn iris_in_term_syntax_read_back_as_themselves() {
    // Term syntax writes an IRI that is a bare name without brackets.
    for iri in [
        "alice",
        "Q42",
        "_x",
        "item-1",
        "a%20b",
        "_",
        "true",
        "false",
        "1a",
        "a-",
        "urn:a",
        "http://e.org/a#b",
        "café",
        "straße-1",
        "a b",
        "x>y",
        r"C:\data\u1",
        "tab	here",
    ] {
        let source = format!("p({}) .", nemo_physical::value::DataValue::iri(iri));
        let (program, diagnostics) = Program::parse(&source);
        assert!(diagnostics.is_empty(), "{source}: {diagnostics:?}");
        let Statement::Fact(fact) = &program.statements[0] else {
            panic!("{source} is a fact");
        };
        let read = match &fact.terms[0].kind {
            TermKind::Constant(Identifier::Bare(name) | Identifier::Iri(name)) => name.as_str(),
            other => panic!("{source} reads as {other:?}"),
        };
        assert_eq!(read, iri, "{source}");
    }
}

/// Each statement of [ROUND_TRIPS] parses without diagnostics
/// and is displayed exactly as written.
#[test]
fn statements_round_trip_through_display() {
    for source in ROUND_TRIPS {
        let (program, diagnostics) = Program::parse(source);
        assert!(
            diagnostics.is_empty(),
            "{source:?}: {:?}",
            diagnostics.first().map(|d| d.message().to_owned())
        );
        assert_eq!(program.statements.len(), 1, "{source:?}");
        assert_eq!(program.to_string(), *source);
    }
}

/// A string in triple quotes, such as a SPARQL query, keeps its inner quotes and line breaks,
/// and may be empty.
#[test]
fn triple_quoted_strings_span_lines() {
    // `\r\n` is read as `\n`.
    let term = Term::parse("\"\"\"SELECT ?s\r\nWHERE { \"x\" }\"\"\"").expect("parses");
    assert_eq!(
        term.kind,
        TermKind::String(String::from("SELECT ?s\nWHERE { \"x\" }"))
    );
    let empty = Term::parse("\"\"\"\"\"\"").expect("parses");
    assert_eq!(empty.kind, TermKind::String(String::new()));
}

/// A `\r\n` in every form of string literal and format string is read as `\n`.
#[test]
fn line_breaks_in_literals_normalize_in_every_form() {
    // A line break in a literal is part of the text, whatever line endings the file uses.
    for source in ["\"a\r\nb\"", "\"\"\"a\r\nb\"\"\""] {
        let term = Term::parse(source).expect("parses");
        assert_eq!(
            term.kind,
            TermKind::String(String::from("a\nb")),
            "{source:?}"
        );
    }
    for source in ["f\"a\r\nb\"", "f\"\"\"a\r\nb\"\"\""] {
        let TermKind::Operation(concat) = Term::parse(source).expect("parses").kind else {
            panic!("{source:?}: a format string is a concatenation");
        };
        assert_eq!(
            concat.arguments[0].kind,
            TermKind::String(String::from("a\nb")),
            "{source:?}"
        );
    }
}

/// An import requires `from` and an export requires `to` before the format.
#[test]
fn imports_and_exports_take_only_their_own_connective() {
    // The rule arrow connects neither direction.
    for (source, connective) in [
        ("@import edge :- csv { resource = \"e.csv\" } .", "from"),
        ("@export reach :- csv { resource = \"r.csv\" } .", "to"),
    ] {
        let (program, diagnostics) = Program::parse(source);
        assert!(program.statements.is_empty(), "{source:?}");
        assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
        assert_eq!(
            diagnostics[0].code(),
            crate::codes::EXPECTED,
            "{:?}",
            diagnostics[0]
        );
        assert_eq!(
            diagnostics[0].field("expected"),
            Some(format!("`{connective}` before the format").as_str())
        );
        assert_eq!(diagnostics[0].field("found"), Some("`:-`"));
    }

    // An import with `to` is an error too.
    let (_, diagnostics) = Program::parse("@import edge to csv { resource = \"e.csv\" } .");
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    assert_eq!(
        diagnostics[0].code(),
        crate::codes::EXPECTED_CONNECTIVE,
        "{:?}",
        diagnostics[0]
    );
    assert_eq!(diagnostics[0].field("connective"), Some("from"));
}

/// A hyphen inside a bare, prefixed or qualified name is part of the name.
#[test]
fn a_hyphen_joins_the_parts_of_a_name_without_a_marker() {
    let name = |source: &str| match Term::parse(source).expect(source).kind {
        TermKind::Constant(identifier) => Some(identifier),
        _ => None,
    };
    assert_eq!(name("my-lib"), Some(Identifier::bare("my-lib")));
    assert_eq!(name("item-1"), Some(Identifier::bare("item-1")));
    assert_eq!(
        name("ex:spare-part"),
        Some(Identifier::Prefixed {
            prefix: String::from("ex"),
            name: String::from("spare-part"),
        })
    );
    assert_eq!(
        name("my-lib::reach"),
        Some(Identifier::Qualified {
            module: String::from("my-lib"),
            name: String::from("reach"),
        })
    );

    // A `-` is a minus after a variable or a number, next to a space,
    // or before a character that cannot continue a name.
    for source in ["?x-1", "?x-y", "$limit-1", "a - b", "a-?x", "a -b", "3-a"] {
        assert_eq!(name(source), None, "{source:?}");
    }

    let fact = Fact::parse("stock-item(_:b-1, a-1) .").expect("parses");
    assert_eq!(fact.predicate, Identifier::bare("stock-item"));
    assert_eq!(fact.to_string(), "stock-item(_:b-1, a-1) .");
}

/// A module name is any number of `super`, followed by one or more names.
#[test]
fn a_module_name_is_super_steps_then_names() {
    let name = ModuleName::parse("super::super::util::pairs").expect("it is a module name");
    assert_eq!(name.parents, 2);
    assert_eq!(name.segments, ["util", "pairs"]);
    assert_eq!(name.to_string(), "super::super::util::pairs");

    // `super` cannot follow a name, since it would leave a directory that the name entered.
    let problem = ModuleName::parse("util::super::pairs").expect_err("`super` follows a name");
    assert_eq!(
        problem.code(),
        crate::codes::SUPER_INSIDE_MODULE_NAME,
        "{problem:?}"
    );
    // Only `super`, a trailing `::` or nothing at all is not a module name.
    for source in ["super", "super::", "lib::", ""] {
        let problem = ModuleName::parse(source).expect_err(source);
        assert_eq!(
            problem.code(),
            crate::codes::EXPECTED,
            "{source:?}: {problem:?}"
        );
    }
}

/// A `@use` with a path in place of a module name is an error
/// that shows how to write the module name.
#[test]
fn a_use_names_its_module_rather_than_a_path() {
    let (_, diagnostics) = Program::parse("@use \"lib/tc.rls\" as t .");
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    assert_eq!(diagnostics[0].code(), crate::codes::EXPECTED);
    assert_eq!(
        diagnostics[0].field("expected"),
        Some("the module's name, as `lib::tc` for `lib/tc.rls`")
    );
}

/// A byte order mark at the start of a source text is skipped.
#[test]
fn a_leading_byte_order_mark_is_not_content() {
    let (program, diagnostics) = Program::parse("\u{feff}p(1) .\n");
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    assert_eq!(program.statements.len(), 1);

    // Anywhere else, the mark is a zero-width character, which is reported.
    let (_, diagnostics) = Program::parse("p(1\u{feff}) .\n");
    assert!(!diagnostics.is_empty(), "a stray mark is still reported");
}

/// Multiplication binds more tightly than addition, and addition more than a comparison.
#[test]
fn precedence_binds_arithmetic_over_comparisons() {
    let term = Term::parse("?x + 2 * ?y = ?z").expect("the term parses");
    let TermKind::Operation(equality) = &term.kind else {
        panic!("expected an operation");
    };
    assert_eq!(equality.operator, Operator::Equal);
    let TermKind::Operation(sum) = &equality.arguments[0].kind else {
        panic!("expected the sum on the left");
    };
    assert_eq!(sum.operator, Operator::Add);
    let TermKind::Operation(product) = &sum.arguments[1].kind else {
        panic!("expected the product to bind tighter");
    };
    assert_eq!(product.operator, Operator::Multiply);

    // Parentheses group terms, and the display writes the parentheses that grouping needs.
    let grouped = Term::parse("(?x + 1) * ?y").expect("the term parses");
    assert_eq!(grouped.to_string(), "(?x + 1) * ?y");
}

/// Component access binds more tightly than every infix operator,
/// and `GET(?t, 0)` is displayed as `?t[0]`.
#[test]
fn component_access_binds_tightest_and_writes_the_sugar() {
    // `?t[0] + 1` indexes before it adds.
    let term = Term::parse("?t[0] + 1").expect("the term parses");
    let TermKind::Operation(sum) = &term.kind else {
        panic!("expected the sum on top");
    };
    assert_eq!(sum.operator, Operator::Add);
    let TermKind::Operation(access) = &sum.arguments[0].kind else {
        panic!("expected the access below");
    };
    assert_eq!(access.operator, Operator::Get);

    // `GET` is the same operation, and is displayed in the short form.
    // An operation with an infix operator before the brackets is put in parentheses.
    let named = Term::parse("GET(?t, 0)").expect("the term parses");
    assert_eq!(named.to_string(), "?t[0]");
    let based = Term::parse("(?x + ?y)[0]").expect("the term parses");
    assert_eq!(based.to_string(), "(?x + ?y)[0]");
}

/// The form of a number literal decides its domain.
#[test]
fn numbers_classify_by_form() {
    /// Returns the domain of the number a source parses to.
    fn domain(source: &str) -> ValueDomain {
        match Term::parse(source).expect("parses").kind {
            TermKind::Number(number) => number.domain(),
            kind => panic!("`{source}` parses to {kind:?}, not a number"),
        }
    }

    assert_eq!(Term::parse("5").expect("parses"), Term::integer(5));
    // A decimal with an integral value is the same term as the integer,
    // as it is in the value layer.
    assert_eq!(Term::parse("2500.0").expect("parses"), Term::integer(2500));
    assert_eq!(domain("1.5"), ValueDomain::Decimal);
    assert_eq!(domain("2.5e3"), ValueDomain::Double);
    assert_eq!(domain("99999999999999999999"), ValueDomain::Integer);
    assert_eq!(domain("-99999999999999999999.5"), ValueDomain::Decimal);

    // A suffix makes the number a float or a double, with or without an exponent,
    // and the display of a float is parsed back as the same term.
    for (source, double) in [
        ("2.5d", 2.5),
        ("9D", 9.0),
        ("1e3d", 1000.0),
        ("-4.25d", -4.25),
    ] {
        let term = Term::parse(source).expect("parses");
        let TermKind::Number(Number::Double(value)) = term.kind else {
            panic!("`{source}` must be a double");
        };
        assert_eq!(value.get(), double);
    }
    for (source, float) in [("2.5f", 2.5f32), ("9F", 9.0), ("1e3f", 1000.0)] {
        let term = Term::parse(source).expect("parses");
        let TermKind::Number(Number::Float(value)) = term.kind else {
            panic!("`{source}` must be a float");
        };
        assert_eq!(value.get(), float);
        assert_eq!(Term::parse(&term.to_string()).expect("re-parses"), term);
    }
}

/// A prefix `+` is allowed only before a number literal.
#[test]
fn a_plus_signs_only_numbers() {
    assert_eq!(Term::parse("+7").expect("parses"), Term::integer(7));
    assert_eq!(Term::parse("+ 0x1F").expect("parses"), Term::integer(31));
    assert!(matches!(
        Term::parse("+2.5f").expect("parses").kind,
        TermKind::Number(Number::Float(_))
    ));
    // The sign binds to the literal, before any infix operator.
    let Term {
        kind: TermKind::Operation(sum),
        ..
    } = Term::parse("+1 + 2").expect("parses")
    else {
        panic!("expected the sum");
    };
    assert_eq!(sum.operator, Operator::Add);
    assert_eq!(sum.arguments[0], Term::integer(1));

    assert!(Term::parse("+?x").is_err());
    assert!(Term::parse("+(1)").is_err());
}

/// Terms nested more deeply than [MAX_NESTING] are an error.
#[test]
fn nesting_deeper_than_the_limit_returns_an_error() {
    // Deep enough nesting would overflow the stack of the recursive descent,
    // so it is reported as an error.
    let deep = format!(
        "p({}1{}) .",
        "(".repeat(MAX_NESTING + 8),
        ")".repeat(MAX_NESTING + 8)
    );
    let (_, diagnostics) = Program::parse(&deep);
    let problem = diagnostics.first().expect("parsing returns an error");
    assert_eq!(problem.code(), crate::codes::TOO_DEEP, "{problem:?}");
}

/// Format strings nested in format strings count toward the nesting limit.
#[test]
fn nesting_through_format_strings_counts_too() {
    // An embedded expression is parsed as a term one level deeper,
    // so nested format strings meet the same limit.
    let deep = format!(
        "p({}1{}) .",
        "f\"{".repeat(MAX_NESTING + 8),
        "}\"".repeat(MAX_NESTING + 8)
    );
    let (_, diagnostics) = Program::parse(&deep);
    let problem = diagnostics.first().expect("parsing returns an error");
    assert_eq!(problem.code(), crate::codes::TOO_DEEP, "{problem:?}");
}

/// Terms nested less deeply than [MAX_NESTING] parse.
#[test]
fn nesting_within_the_limit_is_accepted() {
    let deep = format!(
        "p({}1{}) .",
        "(".repeat(MAX_NESTING - 8),
        ")".repeat(MAX_NESTING - 8)
    );
    let (_, diagnostics) = Program::parse(&deep);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
}

/// `?x<?y` without spaces is a comparison, since no `>` follows the `<`,
/// while `<…>` is an IRI.
#[test]
fn unspaced_less_than_is_not_an_iri() {
    let rule = Rule::parse("p(?x) :- q(?x, ?y), ?x<?y .").expect("the rule parses");
    let Literal::Operation(operation) = &rule.body[1] else {
        panic!("expected the comparison literal");
    };
    assert_eq!(operation.operator, Operator::Less);

    let iri = Term::parse("<https://example.org/a>").expect("the IRI parses");
    assert!(matches!(iri.kind, TermKind::Constant(_)));
}

/// The literals of a body are atoms, negated atoms or operations.
#[test]
fn body_literals_classify_without_reparsing() {
    let rule =
        Rule::parse("p(?x) :- q(?x), ~r(?x), STRLEN(?s) > 2, s(?s) .").expect("the rule parses");
    assert!(matches!(rule.body[0], Literal::Positive(_)));
    assert!(matches!(rule.body[1], Literal::Negative(_)));
    assert!(matches!(rule.body[2], Literal::Operation(_)));
    assert!(matches!(rule.body[3], Literal::Positive(_)));
}

/// A statement that does not parse becomes one diagnostic,
/// and the statements around it still parse.
#[test]
fn recovery_keeps_the_healthy_statements() {
    let source = "good(1) .\nbroken( .\ngood(2) .";
    let (program, diagnostics) = Program::parse(source);
    assert_eq!(program.statements.len(), 2);
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(program.to_string(), "good(1) .\ngood(2) .");
}

/// The `parse` function of a component fails for input
/// that is not exactly one component of its kind.
#[test]
fn component_parsers_demand_complete_input() {
    assert!(Term::parse("?x + 1").is_ok());
    assert!(Term::parse("?x + 1 extra").is_err());
    assert!(Atom::parse("p(?x, 1)").is_ok());
    assert!(Atom::parse("?x + 1").is_err());
    assert!(Rule::parse("p(?x) :- q(?x) .").is_ok());
    assert!(Rule::parse("p(1) .").is_err());
    assert!(Fact::parse("p(1) .").is_ok());
}

/// The span of a parsed statement has the given file id and covers the statement's text.
#[test]
fn spans_point_into_the_source() {
    let source = "p(?x) :- q(?x) .";
    let (program, diagnostics) = Program::parse_with_file(source, FileId::new(7));
    assert!(diagnostics.is_empty());
    let span = program.statements[0]
        .span()
        .expect("parsed statements have spans");
    assert_eq!(span.file, FileId::new(7));
    assert_eq!(&source[span.start as usize..span.end as usize], source);
}

/// An attribute before a fact or a directive is an error.
#[test]
fn attributes_belong_to_rules_only() {
    let (_, diagnostics) = Program::parse("#[name(\"x\")]\nfact(1) .");
    assert!(!diagnostics.is_empty());
    let (_, diagnostics) = Program::parse("#[name(\"x\")]\n@output p .");
    assert!(!diagnostics.is_empty());
}

/// An unknown directive is an error that names it and has a hint.
#[test]
fn unknown_directives_are_diagnosed_with_hints() {
    let (program, diagnostics) = Program::parse("@baseerror <x> .");
    assert!(program.statements.is_empty());
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(
        diagnostics[0].code(),
        crate::codes::UNKNOWN_DIRECTIVE,
        "{:?}",
        diagnostics[0]
    );
    assert_eq!(diagnostics[0].field("name"), Some("baseerror"));
    assert!(!diagnostics[0].hints().is_empty());
}

/// A string literal decodes its escapes,
/// and a language tag with a subtag is converted to lowercase.
#[test]
fn strings_process_escapes_and_language_subtags() {
    let term = Term::parse("\"say \\\"hi\\\"\\n\"").expect("the string parses");
    let TermKind::String(value) = &term.kind else {
        panic!("expected a string");
    };
    assert_eq!(value, "say \"hi\"\n");

    // Character escapes are decoded as in Turtle.
    // Any other backslash is kept for a later reader of the text, such as a regular expression.
    for (written, value) in [
        (r#""\u0041\U0001F600""#, "A\u{1F600}"),
        (r#""\d+ \user""#, r"\d+ \user"),
        (r#""\u00G1""#, r"\u00G1"),
    ] {
        let term = Term::parse(written).expect("the string parses");
        let TermKind::String(parsed) = &term.kind else {
            panic!("expected a string");
        };
        assert_eq!(parsed, value, "{written}");
    }

    let tagged = Term::parse("\"hello\"@en-US").expect("the tagged string parses");
    let TermKind::LanguageString { language, .. } = &tagged.kind else {
        panic!("expected a language-tagged string");
    };
    assert_eq!(language, "en-us");
}

/// The `where` clause of an import contains binding atoms and conditions,
/// and a negated atom in it is an error.
#[test]
fn a_where_clause_holds_atoms_and_conditions_but_no_negation() {
    let (program, diagnostics) = Program::parse(
        "@import q(?x, ?y) from sparql { endpoint = <https://e/sparql> } where b(?x), ?y > 3 .",
    );
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let Some(Statement::Directive(Directive::Import(import))) = program.statements.first() else {
        panic!("an import: {:?}", program.statements);
    };
    assert_eq!(import.bindings.len(), 1);
    assert_eq!(import.conditions.len(), 1);

    let (_, diagnostics) = Program::parse(
        "@import q(?x) from sparql { endpoint = <https://e/sparql> } where ~b(?x) .",
    );
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    assert_eq!(diagnostics[0].code(), crate::codes::NEGATION_IN_WHERE);
}

/// A fact with several atoms is an error.
#[test]
fn facts_reject_multiple_heads() {
    let (program, diagnostics) = Program::parse("p(1), q(2) .");
    assert!(program.statements.is_empty());
    assert!(!diagnostics.is_empty());
}

/// An error message about a misplaced token stays one short line.
#[test]
fn a_misplaced_token_is_quoted_only_as_far_as_a_message_shows() {
    // A string over several lines in place of a prefix is described by its kind.
    let (_, diagnostics) = Program::parse("@prefix \"\"\"a\nb\"\"\" .");
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    let message = diagnostics[0].message();
    assert!(message.contains("a string literal"), "{message}");
    assert!(!message.contains('\n'), "{message}");

    // Any other token is cut to an excerpt.
    let long = format!("@prefix <{}> .", "x".repeat(1000));
    let (_, diagnostics) = Program::parse(&long);
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    let message = diagnostics[0].message();
    assert!(message.len() < 100, "{message}");
    assert!(message.ends_with("…`"), "{message}");
}

/// A lexical problem is reported once, and not again by the parser.
#[test]
fn a_lexical_problem_is_reported_once() {
    let (program, diagnostics) = Program::parse("p(a) & q(b) .\ngood(1) .");
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    assert_eq!(diagnostics[0].code(), crate::codes::UNEXPECTED_CHARACTER);
    assert_eq!(program.to_string(), "good(1) .");
}

/// A stray `}` in a format string is reported at its position.
#[test]
fn a_stray_brace_is_reported_where_it_stands() {
    // The format string still extends to its closing quote, so the rest of the text parses.
    let source = "p(f\"a}b\") .\ngood(1) .";
    let (program, diagnostics) = Program::parse(source);
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    assert_eq!(diagnostics[0].code(), crate::codes::STRAY_BRACE);
    let span = diagnostics[0].labels()[0].span;
    assert_eq!(&source[span.start as usize..span.end as usize], "}");
    assert_eq!(program.to_string(), "good(1) .");
}

/// An error at the end of an embedded expression names the end of the expression,
/// not the end of the input.
#[test]
fn the_end_of_an_embedded_expression_is_not_the_end_of_the_input() {
    let (_, diagnostics) = Program::parse("p(f\"a{1 +}b\") .");
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    assert_eq!(
        diagnostics[0].field("found"),
        Some("the end of the embedded expression")
    );
}

/// The error for a builtin's name in place of an atom says that it is a call,
/// and the error for an expression in place of an atom has no hint to rename it.
#[test]
fn only_a_builtin_name_in_place_of_an_atom_is_called_a_call() {
    let (_, diagnostics) = Program::parse("strlen(?x) :- p(?x) .");
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    assert_eq!(diagnostics[0].code(), crate::codes::EXPECTED_ATOM);
    let label = &diagnostics[0].labels()[0].message;
    assert!(label.contains("`strlen` is a builtin"), "{label}");

    // An expression names no predicate to rename.
    for source in ["q(?x), ?x > 3 :- p(?x) .", "q(?x) :- p(?x), ~ ?x > 3 ."] {
        let (_, diagnostics) = Program::parse(source);
        assert_eq!(diagnostics.len(), 1, "{source:?}: {diagnostics:?}");
        assert_eq!(diagnostics[0].code(), crate::codes::EXPECTED_ATOM);
        assert!(diagnostics[0].hints().is_empty(), "{:?}", diagnostics[0]);
    }
}

/// Parsing a rule as a fact is an error that quotes the rule as written.
#[test]
fn a_statement_of_another_kind_is_quoted_as_written() {
    let problem = Fact::parse_loose("q(a) :- p(a)").expect_err("a rule is not a fact");
    assert_eq!(problem.code(), crate::codes::EXPECTED, "{problem:?}");
    assert_eq!(problem.field("found"), Some("`q(a) :- p(a)`"));
}

/// A hexadecimal literal that needs more than 128 bits is out of range.
#[test]
fn a_radix_literal_beyond_128_bits_is_out_of_range() {
    let source = format!("0x1{}", "0".repeat(32));
    let problem = Term::parse(&source).expect_err("the value needs 129 bits");
    assert_eq!(
        problem.code(),
        crate::codes::MALFORMED_NUMBER,
        "{problem:?}"
    );
    assert_eq!(problem.message(), "number literal is out of range");
}

/// A lone `^` gets a hint about datatypes after a string, and about powers elsewhere.
#[test]
fn a_lone_caret_is_explained_by_what_precedes_it() {
    let hint = |source: &str| {
        let (_, diagnostics) = Program::parse(source);
        assert_eq!(diagnostics.len(), 1, "{source:?}: {diagnostics:?}");
        assert_eq!(diagnostics[0].code(), crate::codes::LONE_CARET);
        diagnostics[0].hints().join("\n")
    };
    assert!(hint("p(\"1\"^xsd:int) .").contains("`^^`"));
    assert!(hint("p(?y) :- q(?x), ?y = ?x ^ 2 .").contains("`POW(base, exponent)`"));
}

/// Spacing around the `:` distinguishes a prefixed name from a named argument.
#[test]
fn spacing_tells_a_prefixed_name_from_a_named_argument() {
    let prefixed = |prefix: &str, name: &str| {
        TermKind::Constant(Identifier::Prefixed {
            prefix: prefix.to_owned(),
            name: name.to_owned(),
        })
    };
    let kinds = |fact: &Fact| -> Vec<TermKind> {
        fact.terms.iter().map(|term| term.kind.clone()).collect()
    };

    // A name, a `:` and a local name without spaces between them are a prefixed name.
    // The local name may start with a digit, and it is empty if the `:` ends the term.
    let fact = Fact::parse("in(wd:42, wd:4-2a, age:30, ex:, ex:) .").expect("the fact parses");
    assert_eq!(fact.labels, None);
    assert_eq!(
        kinds(&fact),
        [
            prefixed("wd", "42"),
            prefixed("wd", "4-2a"),
            prefixed("age", "30"),
            prefixed("ex", ""),
            prefixed("ex", ""),
        ]
    );
    assert_eq!(fact.to_string(), "in(wd:42, wd:4-2a, age:30, ex:, ex:) .");

    // A space after the `:` makes a named argument.
    let fact = Fact::parse("in(wd: 42, ex: Q1) .").expect("the fact parses");
    assert_eq!(fact.labels, Some(vec!["wd".to_owned(), "ex".to_owned()]));
    assert_eq!(fact.to_string(), "in(wd: 42, ex: Q1) .");

    // Among named arguments, a prefixed name gets a hint that it lacks its space.
    let (_, diagnostics) = Program::parse("person(name: \"Ann\", age:30) .");
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    assert_eq!(diagnostics[0].code(), crate::codes::MIXED_ARGUMENTS);
    assert_eq!(
        diagnostics[0].hints(),
        ["write `age: 30` with a space for a named argument"]
    );

    // A prefixed name with an empty local name may also end an embedded expression.
    let (_, diagnostics) = Program::parse("p(f\"{ex:}x\") .");
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
}

/// A dot belongs to a local name only if a character of the name follows it,
/// and otherwise ends the statement.
#[test]
fn a_dot_joins_a_local_name_only_inside_it() {
    let source = "p(?x) :- q(?x), ?x = ex:a.\nr(ex:v1.2).% the end\n";
    let (program, diagnostics) = Program::parse(source);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    assert_eq!(
        program.to_string(),
        "p(?x) :- q(?x), ?x = ex:a .\nr(ex:v1.2) ."
    );

    // After a name and a space, a `:` separates the label of a named argument from its value.
    let fact = Fact::parse("in(name :42) .").expect("the fact parses");
    assert_eq!(fact.labels, Some(vec!["name".to_owned()]));
    assert!(matches!(fact.terms[0].kind, TermKind::Number(_)));
}

/// Escapes in strings are decoded as in Turtle, except `\b`, which is kept as written,
/// and escapes in IRIs are decoded too.
#[test]
fn escapes_decode_as_turtle_reads_them_except_a_word_boundary() {
    let text = |source: &str| match Term::parse(source).expect(source).kind {
        TermKind::String(text) => text,
        other => panic!("{source} reads as {other:?}"),
    };
    assert_eq!(text(r#""a\fb\'c""#), "a\u{C}b'c");
    assert_eq!(text(r#""\bcat\b""#), r"\bcat\b");
    assert_eq!(text(r#""\d+""#), r"\d+");

    let iri = Term::parse(r"<http://e.org/caf\u00E9>").expect("the IRI parses");
    assert_eq!(
        iri.kind,
        TermKind::Constant(Identifier::Iri("http://e.org/café".to_owned()))
    );
    assert_eq!(iri.to_string(), "<http://e.org/café>");
}

/// An IRI that contains U+0000, written directly or as an escape, is an error.
#[test]
fn an_iri_cannot_spell_the_character_u0000() {
    for source in [
        "<\\u0000unit>(5) .",
        "<a\\U00000000b>(5) .",
        "@prefix p: <\\u0000> .",
        "<a\u{0}b>(5) .",
    ] {
        let (_, diagnostics) = Program::parse(source);
        assert_eq!(diagnostics.len(), 1, "{source:?}: {diagnostics:?}");
        assert_eq!(
            diagnostics[0].code(),
            crate::codes::NUL_IN_IRI,
            "{source:?}"
        );
    }

    // Other control characters are allowed,
    // so that every IRI that term syntax writes is parsed back.
    let (_, diagnostics) = Program::parse("<a\\u0009b\\u007Fc\u{85}d>(5) .");
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
}
