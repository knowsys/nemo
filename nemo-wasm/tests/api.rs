//! The wasm API, end to end:
//! real `wasm_bindgen_test`s, run with `wasm-pack test --node nemo-wasm`
//! (or headless browsers in CI).

#![cfg(target_arch = "wasm32")]

use js_sys::{Array, Reflect};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::wasm_bindgen_test;

use nemo_wasm::{NemoEngine, NemoProgram};

/// The ancestor program every test uses.
const ANCESTORS: &str = "
    parent(alice, bob) . parent(bob, carol) .
    ancestor(?x, ?y) :- parent(?x, ?y) .
    ancestor(?x, ?z) :- parent(?x, ?y), ancestor(?y, ?z) .
";

/// A loaded, reasoned engine over the ancestor program.
fn reasoned() -> NemoEngine {
    let program = NemoProgram::parse(String::from(ANCESTORS), None);
    let mut engine = NemoEngine::new();
    engine.load(&program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    engine
}

#[wasm_bindgen_test]
fn programs_parse_and_inspect() {
    let program = NemoProgram::parse(String::from(ANCESTORS), None);
    assert!(program.ok());
    assert_eq!(program.statement_count(), 4);
    assert_eq!(program.predicates().length(), 2);
}

#[wasm_bindgen_test]
fn parse_is_total_and_reports() {
    let program = NemoProgram::parse(String::from("p(?x :- q(?x) ."), None);
    assert!(!program.ok());
    let diagnostics: Array = program.diagnostics().into();
    let first = diagnostics.get(0);
    let code = Reflect::get(&first, &JsValue::from_str("code")).expect("a code");
    assert_eq!(code.as_string().as_deref(), Some("N0202"));
}

#[wasm_bindgen_test]
fn reasoning_and_rows() {
    let mut engine = reasoned();
    let rows = engine.rows("ancestor").expect("the predicate exists");
    assert_eq!(rows.length(), 3);
    assert_eq!(engine.count("ancestor").expect("registered"), 3);
    // Cells are tagged IRI objects.
    let row: Array = rows.get(0).into();
    let cell = row.get(0);
    let kind = Reflect::get(&cell, &JsValue::from_str("kind")).expect("a kind");
    assert_eq!(kind.as_string().as_deref(), Some("iri"));
}

#[wasm_bindgen_test]
fn substrings_reach_past_32_bit_counts() {
    // `usize` has 32 bits here, fewer than a window without a length spans.
    let program = NemoProgram::parse(
        String::from(
            r#"tail(SUBSTR("abcdef", 3)) . far(SUBSTR("abcdef", 5000000000)) .
               window(SUBSTR("abcdef", 3, 5000000000)) ."#,
        ),
        None,
    );
    let mut engine = NemoEngine::new();
    engine.load(&program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    for (predicate, expected) in [("tail", "cdef"), ("far", ""), ("window", "cdef")] {
        let rows = engine.rows(predicate).expect("the predicate exists");
        let row: Array = rows.get(0).into();
        assert_eq!(
            row.get(0).as_string().as_deref(),
            Some(expected),
            "{predicate}"
        );
    }
}

#[wasm_bindgen_test]
fn the_clock_and_the_draws_come_from_javascript() {
    let program = NemoProgram::parse(
        String::from(
            "seed(1) . seed(2) .
             drawn(?s, ?t, ?r, ?u) :- seed(?s), ?t = NOW(), ?r = RAND(), ?u = UUID() .",
        ),
        None,
    );
    let mut engine = NemoEngine::new();
    engine.load(&program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(engine.count("drawn").expect("registered"), 2);

    // The instant is JavaScript's, not the epoch of a missing clock.
    let row: Array = engine
        .rows("drawn")
        .expect("the predicate exists")
        .get(0)
        .into();
    let lexical = Reflect::get(&row.get(1), &JsValue::from_str("lexical")).expect("a lexical");
    assert!(!lexical.as_string().expect("a string").starts_with("1970"));
}

#[wasm_bindgen_test]
fn cursors_page_through_the_rows() {
    let mut engine = reasoned();
    let mut cursor = engine.cursor("ancestor").expect("the predicate exists");
    assert_eq!(cursor.remaining(), 3);
    let mut delivered = Vec::new();
    loop {
        let rows = engine
            .read_chunk(&mut cursor, Some(2))
            .expect("the read succeeds");
        if rows.length() == 0 {
            break;
        }
        delivered.extend(rows.iter());
    }
    assert_eq!(delivered.len(), 3);
    assert!(cursor.done());

    // Each row carries its entry id, its tagged values, and the same
    // values in term syntax.
    let field = |row: &JsValue, name: &str| Reflect::get(row, &name.into()).expect("the field");
    for (position, row) in delivered.iter().enumerate() {
        assert_eq!(field(row, "entryId").as_f64(), Some(position as f64));
    }
    let values: Array = field(&delivered[0], "values").into();
    let kind = Reflect::get(&values.get(0), &"kind".into()).expect("a kind");
    assert_eq!(kind.as_string().as_deref(), Some("iri"));
    let terms: Array = field(&delivered[0], "terms").into();
    let text = terms.get(0).as_string().expect("a term");
    // A bare name is written without angle brackets.
    assert!(["alice", "bob"].contains(&text.as_str()), "{text}");
}

#[wasm_bindgen_test]
fn imported_rows_round_trip() {
    // One value per conversion-table row, through a rule and back.
    let values = Array::new();
    values.push(&JsValue::from_str("plain"));
    values.push(&JsValue::from_bool(true));
    values.push(&JsValue::from_f64(42.0));
    values.push(&JsValue::from_f64(2.5));
    values.push(
        &js_sys::BigInt::new(&JsValue::from_str("12345678901234567890123"))
            .unwrap()
            .into(),
    );
    // The extremes of the engine's integers cross as BigInts too.
    for extreme in [i64::MIN, i64::MAX] {
        values.push(&js_sys::BigInt::from(extreme).into());
    }
    for tagged in [
        r#"{"kind": "iri", "value": "alice"}"#,
        r#"{"kind": "lang", "text": "hallo", "language": "de"}"#,
        r#"{"kind": "float", "value": 2.5}"#,
        r#"{"kind": "decimal", "lexical": "1.5"}"#,
        r#"{"kind": "datetime", "lexical": "2024-03-01T12:30:00Z"}"#,
        r#"{"kind": "date", "lexical": "2024-03-01+05:30"}"#,
        r#"{"kind": "time", "lexical": "12:30:00.25"}"#,
        r#"{"kind": "duration", "lexical": "P1Y2M"}"#,
        r#"{"kind": "literal", "lexical": "5cm", "datatype": "http://example.org/unit"}"#,
        r#"{"kind": "tuple", "label": "point", "elements": [1, 2]}"#,
        r#"{"kind": "map", "pairs": [[{"kind": "iri", "value": "port"}, 8080]]}"#,
    ] {
        values.push(&js_sys::JSON::parse(tagged).expect("the sample parses"));
    }
    let nested = Array::of2(&JsValue::from_f64(1.0), &JsValue::from_f64(2.0));
    values.push(&nested.into());

    let rows: Array = values
        .iter()
        .map(|value| JsValue::from(Array::of1(&value)))
        .collect();
    let mut engine = NemoEngine::new();
    engine.import_rows("p", &rows.into()).expect("rows import");
    let program = NemoProgram::parse(String::from("q(?x) :- p(?x) ."), None);
    engine.load(&program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    let back = engine.rows("q").expect("the predicate exists");
    assert_eq!(back.length(), values.length());
    // Every cell comes back in the JS form the conversion table
    // promises, since a kind that changed on the way would keep the count.
    let mut forms: Vec<String> = back
        .iter()
        .map(|row| describe(&Array::from(&row).get(0)))
        .collect();
    forms.sort();
    let mut expected: Vec<String> = [
        r#""plain""#,
        "true",
        "42",
        "2.5",
        "12345678901234567890123n",
        "-9223372036854775808n",
        "9223372036854775807n",
        r#"{kind:"iri",value:"alice"}"#,
        r#"{kind:"lang",text:"hallo",language:"de"}"#,
        r#"{kind:"float",value:2.5}"#,
        r#"{kind:"decimal",lexical:"1.5"}"#,
        r#"{kind:"datetime",lexical:"2024-03-01T12:30:00Z"}"#,
        r#"{kind:"date",lexical:"2024-03-01+05:30"}"#,
        r#"{kind:"time",lexical:"12:30:00.25"}"#,
        r#"{kind:"duration",lexical:"P1Y2M"}"#,
        r#"{kind:"literal",lexical:"5cm",datatype:"http://example.org/unit"}"#,
        r#"{kind:"tuple",label:"point",elements:[1,2]}"#,
        r#"{kind:"map",pairs:[[{kind:"iri",value:"port"},8080]]}"#,
        "[1,2]",
    ]
    .map(String::from)
    .to_vec();
    expected.sort();
    assert_eq!(forms, expected);
    // Feeding the results back derives the identical table, the
    // round-trip check on the engine's side.
    let mut again = NemoEngine::new();
    again
        .import_rows("p", &back.into())
        .expect("results re-import");
    let program = NemoProgram::parse(String::from("q(?x) :- p(?x) ."), None);
    again.load(&program).expect("the program loads");
    again.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(
        again.count("q").expect("registered"),
        values.length() as usize
    );
}

/// A JS value spelled out for comparison:
/// JSON, except that a BigInt, which `JSON.stringify` cannot serialize, is written as `<digits>n`.
fn describe(value: &JsValue) -> String {
    if let Ok(big) = value.clone().dyn_into::<js_sys::BigInt>() {
        let digits = big.to_string(10).expect("radix 10 is valid");
        return format!("{}n", String::from(digits));
    }
    if let Some(text) = value.as_string() {
        return format!("{text:?}");
    }
    if let Some(flag) = value.as_bool() {
        return flag.to_string();
    }
    if let Some(number) = value.as_f64() {
        return number.to_string();
    }
    if Array::is_array(value) {
        let elements: Vec<String> = Array::from(value)
            .iter()
            .map(|element| describe(&element))
            .collect();
        return format!("[{}]", elements.join(","));
    }
    let object = js_sys::Object::from(value.clone());
    let fields: Vec<String> = js_sys::Object::keys(&object)
        .iter()
        .map(|key| {
            let field = Reflect::get(value, &key).expect("a listed key reads");
            format!(
                "{}:{}",
                key.as_string().expect("keys are strings"),
                describe(&field)
            )
        })
        .collect();
    format!("{{{}}}", fields.join(","))
}

#[wasm_bindgen_test]
fn registered_modules_are_what_a_use_reads() {
    let mut engine = NemoEngine::new();
    engine
        .add_module(
            "lib::tc",
            String::from(
                "@input edge . @output reach . reach(?x, ?y) :- edge(?x, ?y) . \
                 reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .",
            ),
        )
        .expect("the module registers");
    let program = NemoProgram::parse(
        String::from(
            "link(1, 2) . link(2, 3) . @use lib::tc { edge = link } as t . \
             out(?x, ?y) :- t::reach(?x, ?y) .",
        ),
        None,
    );
    engine.load(&program).expect("the module links");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(engine.count("out").expect("registered"), 3);
}

#[wasm_bindgen_test]
fn registered_resources_feed_imports() {
    let mut engine = NemoEngine::new();
    engine
        .add_resource("data.csv", &JsValue::from_str("a,1\nb,2\nc,3\n"))
        .expect("the resource registers");
    let program = NemoProgram::parse(
        String::from("@import p from csv { resource = \"data.csv\" } . q(?x) :- p(?x, ?y) ."),
        None,
    );
    engine.load(&program).expect("the import reads the bytes");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(engine.count("q").expect("registered"), 3);
}

/// The code of the first diagnostic a thrown error carries.
fn thrown_code(thrown: &JsValue) -> Option<String> {
    let diagnostics: Array = Reflect::get(thrown, &JsValue::from_str("diagnostics"))
        .expect("the error carries diagnostics")
        .into();
    Reflect::get(&diagnostics.get(0), &JsValue::from_str("code"))
        .expect("a code")
        .as_string()
}

#[wasm_bindgen_test]
fn remote_imports_suspend_the_load_until_the_caller_responds() {
    let mut engine = NemoEngine::new();
    let program = NemoProgram::parse(
        String::from(
            "@import p from csv { resource = \"https://example.org/data.csv\", \
             http_headers = { Authorization = \"Bearer t\" } } . q(?x) :- p(?x, ?y) .",
        ),
        None,
    );
    // Nothing fetches in a wasm build: the load suspends.
    let suspended = engine.load(&program).expect_err("the load suspends");
    assert_eq!(thrown_code(&suspended).as_deref(), Some("N0491"));
    let requests = engine.pending_requests().expect("the requests list");
    assert_eq!(requests.length(), 1);
    let entry = requests.get(0);
    let field = |name: &str| Reflect::get(&entry, &JsValue::from_str(name)).expect("the field");
    assert_eq!(
        field("url").as_string().as_deref(),
        Some("https://example.org/data.csv")
    );
    assert_eq!(field("method").as_string().as_deref(), Some("GET"));
    assert_eq!(field("accept").as_string().as_deref(), Some("text/csv"));
    let headers: Array = field("headers").into();
    let header: Array = headers.get(0).into();
    assert_eq!(header.get(0).as_string().as_deref(), Some("Authorization"));
    assert_eq!(header.get(1).as_string().as_deref(), Some("Bearer t"));
    assert!(field("form").is_null());

    let id = u64::try_from(field("id")).expect("the id is a BigInt");
    assert!(
        engine
            .respond(id, &JsValue::from_str("a,1\nb,2\n"))
            .expect("the body is taken")
    );
    assert_eq!(engine.pending_requests().expect("the list").length(), 0);
    engine.resume().expect("the load completes");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(engine.count("q").expect("registered"), 2);
}

#[wasm_bindgen_test]
fn a_failed_response_fails_the_load() {
    let mut engine = NemoEngine::new();
    let program = NemoProgram::parse(
        String::from(
            "@import p from csv { resource = \"https://example.org/data.csv\" } . \
             q(?x) :- p(?x, ?y) .",
        ),
        None,
    );
    engine.load(&program).expect_err("the load suspends");
    let entry = engine.pending_requests().expect("the list").get(0);
    let id = u64::try_from(Reflect::get(&entry, &JsValue::from_str("id")).expect("an id"))
        .expect("the id is a BigInt");
    assert!(
        engine
            .respond_failure(id, Some(404), String::from("Not Found"))
            .expect("not busy")
    );
    let failed = engine.resume().expect_err("the import fails");
    assert_eq!(thrown_code(&failed).as_deref(), Some("N0111"));
    assert_eq!(engine.pending_requests().expect("the list").length(), 0);
}

#[wasm_bindgen_test]
fn predicates_report_their_roles_and_the_declared_outputs() {
    let mut engine = NemoEngine::new();
    let program = NemoProgram::parse(
        String::from("edge(1, 2) . reach(?x, ?y) :- edge(?x, ?y) . @output reach ."),
        None,
    );
    engine.load(&program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    let outputs = engine.output_predicates();
    assert_eq!(outputs.length(), 1);
    assert_eq!(outputs.get(0).as_string().as_deref(), Some("reach"));
    let predicates = engine.predicates();
    assert_eq!(predicates.length(), 2);
    let first: Array = predicates.get(0).into();
    assert_eq!(first.get(0).as_string().as_deref(), Some("edge"));
    assert_eq!(first.get(1).as_f64(), Some(2.0));
    assert_eq!(first.get(2).as_string().as_deref(), Some("extensional"));
    // A cursor knows the entry id of its next row.
    let mut cursor = engine.cursor("reach").expect("the predicate exists");
    assert_eq!(cursor.position(), 0);
    engine
        .read_chunk(&mut cursor, Some(1))
        .expect("the read succeeds");
    assert_eq!(cursor.position(), 1);
}

#[wasm_bindgen_test]
fn exports_serialize_to_bytes() {
    let mut engine = NemoEngine::new();
    let program = NemoProgram::parse(
        String::from("p(1, alice) . p(2, bob) . @export p to csv ."),
        None,
    );
    engine.load(&program).expect("the program loads");
    engine.reason().expect("reasoning reaches the fixpoint");
    let tables = engine.export_tables().expect("exports serialize");
    let content = Reflect::get(&tables, &JsValue::from_str("p.csv")).expect("the file exists");
    let bytes: js_sys::Uint8Array = content.into();
    let text = String::from_utf8(bytes.to_vec()).expect("CSV is text");
    let sorted = |text: &str| {
        let mut lines: Vec<String> = text.lines().map(str::to_owned).collect();
        lines.sort_unstable();
        lines
    };
    assert_eq!(sorted(&text), ["1,alice", "2,bob"]);
    // Any predicate serializes on request, declared export or not.
    let bytes = engine
        .export_predicate("p")
        .expect("a held predicate serializes");
    let text = String::from_utf8(bytes.to_vec()).expect("CSV is text");
    assert_eq!(sorted(&text), ["1,alice", "2,bob"]);
    assert!(engine.export_predicate("nowhere").is_err());
}

#[wasm_bindgen_test]
fn the_engine_continues_across_stages() {
    // Post-fixpoint rows are deltas; an extension recompiles over the
    // stored data; a reload replaces the program and recomputes.
    let mut engine = reasoned();
    let row = Array::of1(&JsValue::from(Array::of2(
        &js_sys::JSON::parse(r#"{"kind": "iri", "value": "carol"}"#).expect("parses"),
        &js_sys::JSON::parse(r#"{"kind": "iri", "value": "dave"}"#).expect("parses"),
    )));
    engine
        .import_rows("parent", &row.into())
        .expect("rows join a loaded engine");
    engine.reason().expect("reasoning continues");
    assert_eq!(engine.count("ancestor").expect("registered"), 6);

    let extension = NemoProgram::parse(String::from("forebear(?x) :- ancestor(?x, ?y) ."), None);
    engine.extend(&extension).expect("the extension compiles");
    engine.reason().expect("the new rule reasons");
    assert_eq!(engine.count("forebear").expect("registered"), 3);

    let replacement = NemoProgram::parse(
        String::from("parent(alice, bob) . ancestor(?x, ?y) :- parent(?x, ?y) ."),
        None,
    );
    engine.reload(&replacement).expect("the replacement loads");
    engine.reason().expect("recomputation reasons");
    assert_eq!(engine.count("ancestor").expect("registered"), 1);
}

#[wasm_bindgen_test]
fn load_failures_throw_structured_diagnostics() {
    let mut engine = NemoEngine::new();
    let program = NemoProgram::parse(String::from("q(?x, ?y) :- p(?x) ."), None);
    let thrown = engine.load(&program).expect_err("the rule is unsafe");
    let diagnostics: Array = Reflect::get(&thrown, &JsValue::from_str("diagnostics"))
        .expect("the error carries diagnostics")
        .into();
    let code = Reflect::get(&diagnostics.get(0), &JsValue::from_str("code")).expect("a code");
    assert_eq!(code.as_string().as_deref(), Some("N0204"));
}

#[wasm_bindgen_test]
fn tracing_returns_trees_and_query_responses() {
    let mut engine = reasoned();

    // The simple trace: the protocol response as a JS object.
    let response = engine
        .trace(vec![String::from("ancestor(alice, carol)")])
        .expect("tracing succeeds");
    let facts: Array = Reflect::get(&response, &"facts".into())
        .expect("the response holds facts")
        .into();
    let traced = facts.get(0);
    assert_eq!(
        Reflect::get(&traced, &"holds".into()).expect("holds"),
        JsValue::TRUE
    );
    let tree = Reflect::get(&traced, &"tree".into()).expect("a proof tree");
    assert_eq!(
        Reflect::get(&tree, &"kind".into()).expect("kind"),
        JsValue::from_str("derived")
    );

    // The ASCII convenience renders an instantiated rule.
    let ascii = engine
        .trace_ascii(vec![String::from("ancestor(alice, bob)")])
        .expect("tracing succeeds");
    assert!(ascii.contains(":-"), "{ascii}");

    // The explanation view's tree query expands server-side.
    let query = js_sys::JSON::parse(
        r#"{"predicate": "ancestor", "tableEntries": {"queries": ["alice, carol"]}}"#,
    )
    .expect("the query parses");
    let response = engine.tree_for_table(query).expect("the query succeeds");
    let expansion = Reflect::get(&response, &"childInformation".into()).expect("an expansion");
    let rule = Reflect::get(&expansion, &"rule".into()).expect("a rule");
    let id = Reflect::get(&rule, &"id".into())
        .expect("an id")
        .as_f64()
        .expect("a number");
    let children: Array = Reflect::get(&expansion, &"children".into())
        .expect("children")
        .into();
    assert_eq!(children.length(), 2, "one per premise");

    // An editor jumps to the rule by its id.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let found = engine.rule(id as usize).expect("the rule is found");
    let source = Reflect::get(&found, &"source".into()).expect("a source");
    let line = Reflect::get(&source, &"line".into()).expect("a line");
    assert_eq!(line.as_f64(), Some(4.0), "the recursive rule's line");
    assert!(engine.rule(0).expect("the lookup succeeds").is_undefined());

    // The node query returns the nodes of the shape it is given, flat.
    let query = js_sys::JSON::parse(
        r#"{"predicate": "ancestor", "childInformation": {"rule": 2, "headIndex": 0,
            "children": [{"tableEntries": {"queries": ["alice, bob"]}}]}}"#,
    )
    .expect("the query parses");
    let nodes: Array = engine
        .table_entries_for_tree_nodes(query)
        .expect("the query succeeds")
        .into();
    assert_eq!(nodes.length(), 2);
    let address = Reflect::get(&nodes.get(1), &"addressInTree".into()).expect("an address");
    assert_eq!(Array::from(&address).get(0).as_f64(), Some(0.0));
}

#[wasm_bindgen_test]
fn retained_imports_trace_at_their_written_width() {
    let program = NemoProgram::parse(
        String::from(
            "@import p from csv { resource = \"data.csv\", format = (string, string, int) } .
             big(?x) :- p(?x, ?label, ?n), ?n > 5 . @output big .",
        ),
        None,
    );
    let query = || {
        js_sys::JSON::parse(r#"{"predicate": "p", "tableEntries": {"queries": [0]}}"#)
            .expect("the query parses")
    };
    let mut pruned = NemoEngine::new();
    pruned
        .add_resource("data.csv", &JsValue::from_str("a,label,7\nb,other,2\n"))
        .expect("the resource registers");
    pruned.load(&program).expect("the program loads");
    pruned.reason().expect("reasoning reaches the fixpoint");
    // The rules never read `?label`: the import stores without it.
    assert!(pruned.tree_for_table(query()).is_err());

    let mut retained = NemoEngine::new();
    retained.retain_imports().expect("not busy");
    retained
        .add_resource("data.csv", &JsValue::from_str("a,label,7\nb,other,2\n"))
        .expect("the resource registers");
    retained.load(&program).expect("the program loads");
    retained.reason().expect("reasoning reaches the fixpoint");
    let response = retained
        .tree_for_table(query())
        .expect("the query succeeds");
    let entries = Reflect::get(&response, &"tableEntries".into())
        .and_then(|entries| Reflect::get(&entries, &"entries".into()))
        .expect("entries");
    let terms = Reflect::get(&Array::from(&entries).get(0), &"termTuple".into()).expect("terms");
    assert_eq!(Array::from(&terms).length(), 3);
}

/// Replaces the global `fetch` with a function of `url` and `init` written in JavaScript.
///
/// `globalThis.fetched` counts its calls.
fn install_fetch(body: &str) {
    let fake = js_sys::Function::new_with_args(
        "url, init",
        &format!("globalThis.fetched = (globalThis.fetched ?? 0) + 1; {body}"),
    );
    Reflect::set(&js_sys::global(), &"fetch".into(), &fake).expect("the global sets");
    Reflect::set(&js_sys::global(), &"fetched".into(), &JsValue::from(0)).expect("the global sets");
}

/// How often the replaced `fetch` was called.
fn fetched() -> f64 {
    Reflect::get(&js_sys::global(), &"fetched".into())
        .expect("the global reads")
        .as_f64()
        .unwrap_or(0.0)
}

#[wasm_bindgen_test]
async fn async_methods_fetch_what_a_bound_import_requests() {
    // A fake endpoint: the parent of every node the query binds.
    install_fetch(
        r#"
        const query = new URL(url).searchParams.get("query")
            ?? new URLSearchParams(init.body ?? "").get("query");
        const parents = { "urn:a": "urn:b", "urn:b": "urn:c" };
        let body = "?c\t?p\n";
        for (const [, node] of query.matchAll(/<(urn:[a-z])>/g)) {
            if (parents[node]) body += `<${node}>\t<${parents[node]}>\n`;
        }
        return Promise.resolve(new Response(body, { status: 200 }));
        "#,
    );
    let program = NemoProgram::parse(
        String::from(
            r#"@import parent(?c, ?p) from sparql {
                endpoint = <http://example.org/sparql>,
                query = "SELECT ?c ?p WHERE { ?c <urn:parent> ?p }"
            } where anc(?c) .
            anc(<urn:a>) .
            anc(?p) :- parent(?c, ?p) ."#,
        ),
        None,
    );
    let engine = NemoEngine::new();
    JsFuture::from(engine.load_async(&program).expect("not busy"))
        .await
        .expect("the program loads");
    let reasoning = engine.reason_async().expect("not busy");
    // The engine is busy until the fixpoint, but still serves reads.
    assert!(engine.count("anc").is_ok());
    JsFuture::from(reasoning)
        .await
        .expect("reasoning reaches the fixpoint");
    assert_eq!(engine.count("anc").expect("registered"), 3);
    assert!(fetched() >= 2.0, "the bindings grew twice");
}

#[wasm_bindgen_test]
async fn async_methods_send_a_long_query_as_a_form() {
    // A fake endpoint that, as Wikidata's does, takes a POST not labeled as a
    // form for an update and refuses it.
    install_fetch(
        r#"
        const request = new Request(url, init);
        const type = request.headers.get("content-type") ?? "";
        if (request.method === "POST" && !type.startsWith("application/x-www-form-urlencoded")) {
            return Promise.resolve(new Response("Not writable", { status: 405 }));
        }
        globalThis.method = request.method;
        return Promise.resolve(new Response("?c\n<urn:a>\n", { status: 200 }));
        "#,
    );
    // A query too long for a URL goes out as a POST form.
    let filler = "x".repeat(2_000);
    let program = NemoProgram::parse(
        format!(
            "@import p from sparql {{ endpoint = <http://example.org/sparql>, \
             query = \"SELECT ?c WHERE {{ ?c <urn:p> '{filler}' }}\" }} ."
        ),
        None,
    );
    let engine = NemoEngine::new();
    JsFuture::from(engine.load_async(&program).expect("not busy"))
        .await
        .expect("the program loads");
    let method = Reflect::get(&js_sys::global(), &"method".into()).expect("the global reads");
    assert_eq!(method.as_string().as_deref(), Some("POST"));
    assert_eq!(engine.count("p").expect("registered"), 1);
}

#[wasm_bindgen_test]
async fn async_methods_retry_an_overloaded_server_and_reject_a_missing_resource() {
    install_fetch(
        r#"
        if (url.endsWith("missing.csv")) return Promise.resolve(new Response("no", { status: 404 }));
        if (globalThis.fetched === 1) {
            return Promise.resolve(new Response("busy", { status: 503, headers: { "Retry-After": "0" } }));
        }
        return Promise.resolve(new Response("a,1\nb,2\n", { status: 200 }));
        "#,
    );
    let program = NemoProgram::parse(
        String::from(
            "@import p from csv { resource = \"https://example.org/data.csv\" } . \
             q(?x) :- p(?x, ?y) .",
        ),
        None,
    );
    let mut engine = NemoEngine::new();
    let loading = engine.load_async(&program).expect("not busy");
    // A method that would abandon the suspended one throws meanwhile.
    assert!(engine.reason().is_err(), "the engine is busy");
    JsFuture::from(loading).await.expect("the retry succeeds");
    assert_eq!(fetched(), 2.0, "one 503, one success");
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(engine.count("q").expect("registered"), 2);

    let program = NemoProgram::parse(
        String::from(
            "@import p from csv { resource = \"https://example.org/missing.csv\" } . \
             q(?x) :- p(?x, ?y) .",
        ),
        None,
    );
    let engine = NemoEngine::new();
    let failed = JsFuture::from(engine.load_async(&program).expect("not busy"))
        .await
        .expect_err("a 404 is final");
    assert_eq!(thrown_code(&failed).as_deref(), Some("N0111"));
}

/// The language server runs in the browser: an editor opens a
/// document over the message channel and the diagnostics come back
/// as a server-initiated message.
#[wasm_bindgen_test]
async fn the_language_server_responds_over_messages() {
    use nemo_wasm::{LspClientChannel, language_server_handles};

    let (mut channel, mut requests, _responses) = language_server_handles();

    let send = |channel: &mut LspClientChannel, json: &str| {
        let message = js_sys::JSON::parse(json).expect("the message parses");
        channel.send_request(message).expect("a request")
    };
    let initialized = JsFuture::from(send(
        &mut channel,
        r#"{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"capabilities": {}}}"#,
    ))
    .await
    .expect("initialize responds");
    let capabilities = Reflect::get(&initialized, &JsValue::from_str("result"))
        .and_then(|result| Reflect::get(&result, &JsValue::from_str("capabilities")))
        .expect("the result carries capabilities");
    assert!(!capabilities.is_undefined());

    let opened = JsFuture::from(send(
        &mut channel,
        r#"{"jsonrpc": "2.0", "method": "textDocument/didOpen", "params": {"textDocument": {
            "uri": "untitled:one", "languageId": "nemo", "version": 1,
            "text": "p(?x :- q(?x) ."}}}"#,
    ))
    .await
    .expect("a notification is accepted");
    assert!(opened.is_null(), "a notification has no response");

    let published = requests
        .next_request()
        .await
        .expect("the server publishes diagnostics");
    let method = Reflect::get(&published, &JsValue::from_str("method")).expect("a method");
    assert_eq!(
        method.as_string().as_deref(),
        Some("textDocument/publishDiagnostics")
    );
    let code = Reflect::get(&published, &JsValue::from_str("params"))
        .and_then(|params| Reflect::get(&params, &JsValue::from_str("diagnostics")))
        .map(|diagnostics| Array::from(&diagnostics).get(0))
        .and_then(|first| Reflect::get(&first, &JsValue::from_str("code")))
        .expect("the first diagnostic carries a code");
    assert_eq!(code.as_string().as_deref(), Some("N0202"));
}
