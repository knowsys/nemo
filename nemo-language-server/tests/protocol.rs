//! One session with the real binary, over the real protocol.
//!
//! The unit tests cover what the server knows,
//! and this covers that it can be *asked*:
//! framing, capabilities, and the shape of what comes back.
//! Responses are parsed, never matched as text,
//! since the field order of a serialized message is not something a test should depend on.

use std::{
    io::{BufRead, BufReader, Read, Write},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
};

use serde_json::{Value, json};

/// A running server, driven message by message.
struct Session {
    /// The process.
    process: Child,
    /// Its input.
    input: ChildStdin,
    /// Its output, buffered for header reading.
    output: BufReader<ChildStdout>,
    /// The id the next request takes.
    next: u32,
}

impl Session {
    /// Starts the server and initializes it.
    fn start() -> Self {
        let mut process = Command::new(env!("CARGO_BIN_EXE_nemo-language-server"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("the server runs");
        let input = process.stdin.take().expect("stdin was piped");
        let output = BufReader::new(process.stdout.take().expect("stdout was piped"));
        let mut session = Self {
            process,
            input,
            output,
            next: 1,
        };

        let response = session.request(
            "initialize",
            json!({ "capabilities": {}, "processId": Value::Null }),
        );
        let capabilities = &response["result"]["capabilities"];
        for advertised in [
            "hoverProvider",
            "definitionProvider",
            "referencesProvider",
            "documentSymbolProvider",
            "textDocumentSync",
        ] {
            assert!(
                !capabilities[advertised].is_null(),
                "the server advertises {advertised}: {capabilities}"
            );
        }
        session.notify("initialized", json!({}));
        session
    }

    /// Sends one message, framed as the protocol wants it.
    fn send(&mut self, message: &Value) {
        let body = message.to_string();
        write!(self.input, "Content-Length: {}\r\n\r\n{body}", body.len())
            .expect("the server takes input");
        self.input.flush().expect("the server takes input");
    }

    /// Reads one message.
    fn receive(&mut self) -> Value {
        let mut length = 0;
        loop {
            let mut header = String::new();
            let read = self
                .output
                .read_line(&mut header)
                .expect("the server responds");
            assert_ne!(read, 0, "the server closed its output");
            let header = header.trim();
            if header.is_empty() {
                break;
            }
            if let Some(value) = header.strip_prefix("Content-Length: ") {
                length = value.parse().expect("the length is a number");
            }
        }
        let mut body = vec![0; length];
        self.output
            .read_exact(&mut body)
            .expect("the body follows its header");
        serde_json::from_slice(&body).expect("the protocol carries JSON")
    }

    /// Sends a notification.
    fn notify(&mut self, method: &str, params: Value) {
        self.send(&json!({ "jsonrpc": "2.0", "method": method, "params": params }));
    }

    /// Sends a request and returns the message carrying its response,
    /// skipping whatever the server published on its own account.
    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next;
        self.next += 1;
        self.send(&json!({
            "jsonrpc": "2.0", "id": id, "method": method, "params": params
        }));
        loop {
            let message = self.receive();
            if message["id"].as_u64() == Some(u64::from(id)) {
                return message;
            }
        }
    }

    /// Opens a document and returns the diagnostics published for it.
    fn open(&mut self, uri: &str, text: &str) -> Vec<Value> {
        self.notify(
            "textDocument/didOpen",
            json!({ "textDocument": {
                "uri": uri, "languageId": "nemo", "version": 1, "text": text
            }}),
        );
        let published = self.receive();
        assert_eq!(
            published["method"], "textDocument/publishDiagnostics",
            "opening publishes: {published}"
        );
        assert_eq!(
            published["params"]["version"], 1,
            "the version comes along: {published}"
        );
        published["params"]["diagnostics"]
            .as_array()
            .expect("diagnostics are a list")
            .clone()
    }

    /// Asks about a place in a document.
    fn ask(&mut self, method: &str, uri: &str, line: u32, character: u32) -> Value {
        self.request(
            method,
            json!({
                "textDocument": { "uri": uri },
                "position": { "line": line, "character": character },
                "context": { "includeDeclaration": true },
            }),
        )
    }

    /// Asks what could be completed at a place,
    /// which takes a context of its own, not the one a reference query carries.
    fn complete(&mut self, uri: &str, line: u32, character: u32) -> Value {
        self.request(
            "textDocument/completion",
            json!({
                "textDocument": { "uri": uri },
                "position": { "line": line, "character": character },
                "context": { "triggerKind": 1 },
            }),
        )
    }

    /// Shuts the session down and asserts it ended cleanly.
    fn finish(mut self) {
        let _ = self.request("shutdown", Value::Null);
        self.notify("exit", Value::Null);
        let status = self.process.wait().expect("the server exits");
        assert!(status.success(), "the server exited with {status}");
    }
}

/// The labels a completion response offers.
fn labels(response: &Value) -> Vec<String> {
    response["result"]
        .as_array()
        .unwrap_or_else(|| panic!("completion returns a list: {response}"))
        .iter()
        .map(|item| {
            item["label"]
                .as_str()
                .expect("an item has a label")
                .to_owned()
        })
        .collect()
}

/// The URI every test opens its document under.
const URI: &str = "file:///demo.rls";

/// A program with a fact and a rule reading it.
const PROGRAM: &str = "edge(1, 2) .\nreach(?x, ?y) :- edge(?x, ?y) .\n";

#[test]
fn a_program_that_parses_publishes_no_diagnostics() {
    let mut session = Session::start();
    let diagnostics = session.open(URI, PROGRAM);
    assert!(diagnostics.is_empty(), "nothing to report: {diagnostics:?}");
    session.finish();
}

#[test]
fn a_parse_error_is_published_with_its_code_and_place() {
    let mut session = Session::start();
    let diagnostics = session.open(URI, "p(1) .\nq(?x :- p(?x) .\n");
    let report = diagnostics.first().expect("the tail does not parse");
    assert_eq!(report["code"], "N0202", "{report}");
    assert_eq!(report["source"], "nemo", "{report}");
    assert_eq!(report["severity"], 1, "an error, not a warning: {report}");
    assert_eq!(
        report["range"]["start"]["line"], 1,
        "it points at the second line: {report}"
    );
    session.finish();
}

#[test]
fn the_outline_lists_the_statements_of_the_file() {
    let mut session = Session::start();
    session.open(URI, PROGRAM);
    let response = session.request(
        "textDocument/documentSymbol",
        json!({ "textDocument": { "uri": URI } }),
    );
    let names: Vec<&str> = response["result"]
        .as_array()
        .expect("an outline is a list")
        .iter()
        .map(|symbol| symbol["name"].as_str().expect("a symbol has a name"))
        .collect();
    assert_eq!(names, vec!["edge", "reach"]);
    session.finish();
}

/// The Markdown a hover at a position shows.
fn hovered(session: &mut Session, line: u32, character: u32) -> String {
    let response = session.ask("textDocument/hover", URI, line, character);
    response["result"]["contents"]["value"]
        .as_str()
        .unwrap_or_else(|| panic!("hover carries markup: {response}"))
        .to_owned()
}

#[test]
fn hovering_a_predicate_describes_it() {
    let mut session = Session::start();
    session.open(URI, PROGRAM);
    // Line 1, character 19: inside the body's `edge`.
    let text = hovered(&mut session, 1, 19);
    assert_eq!(text, "```nemo\nedge(int, int)\n```\n\nfacts");
    // `reach` is derived, and with no outputs declared, rules' predicates are written out.
    let text = hovered(&mut session, 1, 0);
    assert!(text.ends_with("\n\nderived · output"), "{text}");
    session.finish();
}

#[test]
fn a_predicate_is_documented_by_the_comments_above_what_derives_it() {
    let mut session = Session::start();
    session.open(
        URI,
        "edge(1, 2) .\n% Every node reachable from another.\nreach(?x, ?y) :- edge(?x, ?y) .\n\n\
         % Not this one: a blank line parts it.\n\n\
         reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .\n@output edge, reach .\n",
    );
    // Line 2, character 0: the head's `reach`.
    let text = hovered(&mut session, 2, 0);
    assert!(
        text.ends_with("derived · output\n\nEvery node reachable from another."),
        "{text}"
    );
    // Line 7, character 14: `reach` in `@output edge, reach .`, not `edge` before it.
    let text = hovered(&mut session, 7, 14);
    assert!(text.starts_with("```nemo\nreach(int, int)\n"), "{text}");
    session.finish();
}

#[test]
fn hovering_a_variable_shows_its_type() {
    let mut session = Session::start();
    session.open(URI, PROGRAM);
    // Line 1, character 7: the head's `?x`.
    let text = hovered(&mut session, 1, 7);
    assert_eq!(text, "```nemo\n?x: int\n```");
    session.finish();
}

#[test]
fn hovering_a_builtin_shows_how_to_call_it() {
    let mut session = Session::start();
    session.open(
        URI,
        "p(\"ab\") .\nq(?n, ?m) :- p(?s), ?n = STRLEN(?s), ?m = ?n + 1 .\n",
    );
    // Line 1, character 27: inside `STRLEN`.
    let text = hovered(&mut session, 1, 27);
    assert!(
        text.starts_with("```nemo\nSTRLEN(text: string) -> int\n```\n\n"),
        "{text}"
    );
    // Line 1, character 45: the `+`.
    let text = hovered(&mut session, 1, 45);
    assert!(text.contains("number + number -> number"), "{text}");
    session.finish();
}

#[test]
fn hovering_a_declared_label_shows_its_declaration() {
    let mut session = Session::start();
    session.open(
        URI,
        "% An angle in degrees.\n@type angle(decimal) .\nspin(angle(1.5)) .\n",
    );
    // Line 2, character 6: inside `angle`.
    let text = hovered(&mut session, 2, 6);
    assert_eq!(
        text,
        "```nemo\n@type angle(decimal)\n```\n\nAn angle in degrees."
    );
    session.finish();
}

#[test]
fn going_to_a_predicates_definition_lands_where_it_is_derived() {
    let mut session = Session::start();
    session.open(URI, PROGRAM);
    let response = session.ask("textDocument/definition", URI, 1, 19);
    let locations = response["result"].as_array().expect("a list of places");
    assert_eq!(locations.len(), 1, "{response}");
    // The fact on the first line derives `edge`: `edge(1, 2)`, ten
    // characters, without the terminator that ends the statement.
    let range = &locations[0]["range"];
    assert_eq!(
        range["start"],
        json!({ "line": 0, "character": 0 }),
        "{response}"
    );
    assert_eq!(
        range["end"],
        json!({ "line": 0, "character": 10 }),
        "{response}"
    );
    session.finish();
}

#[test]
fn an_imported_predicate_is_derived_where_it_is_imported() {
    let mut session = Session::start();
    session.open(
        URI,
        "@import edge from csv { resource = \"e.csv\" } .\nreach(?x) :- edge(?x, 1) .\n",
    );
    // The rows come from the import, so that is the definition.
    let response = session.ask("textDocument/definition", URI, 1, 14);
    let locations = response["result"].as_array().expect("a list of places");
    assert_eq!(locations.len(), 1, "{response}");
    assert_eq!(locations[0]["range"]["start"]["line"], 0, "{response}");

    let text = hovered(&mut session, 1, 14);
    assert!(
        text.ends_with("\n\nimported from csv"),
        "an import derives it: {text}"
    );
    session.finish();
}

#[test]
fn positions_are_counted_in_utf16_across_the_wire() {
    let mut session = Session::start();
    // `𝄞` is two UTF-16 units and four bytes; a client counting either
    // characters or bytes would disagree with these numbers.
    let source = "p(\"𝄞\") .\nq(?x) :- p(?x) .\n";
    session.open(URI, source);
    let response = session.ask("textDocument/hover", URI, 0, 0);
    let range = &response["result"]["range"];
    assert_eq!(
        range["start"],
        json!({ "line": 0, "character": 0 }),
        "{response}"
    );
    // `p("𝄞")` is p ( " 𝄞 " ) = 5 characters and 7 UTF-16 units.
    assert_eq!(
        range["end"],
        json!({ "line": 0, "character": 7 }),
        "{response}"
    );
    session.finish();
}

#[test]
fn a_document_that_changes_is_reparsed() {
    let mut session = Session::start();
    session.open(URI, PROGRAM);
    session.notify(
        "textDocument/didChange",
        json!({
            "textDocument": { "uri": URI, "version": 2 },
            "contentChanges": [{ "text": "p(?x :- q(?x) .\n" }],
        }),
    );
    let published = session.receive();
    assert_eq!(published["params"]["version"], 2, "{published}");
    let diagnostics = published["params"]["diagnostics"]
        .as_array()
        .expect("diagnostics are a list");
    assert!(!diagnostics.is_empty(), "the new text does not parse");
    session.finish();
}

#[test]
fn an_edit_sent_as_a_range_does_not_replace_the_document() {
    let mut session = Session::start();
    session.open(URI, PROGRAM);
    // This server syncs whole documents; a range's text is a fragment,
    // and taking it for the document would destroy the file.
    session.notify(
        "textDocument/didChange",
        json!({
            "textDocument": { "uri": URI, "version": 2 },
            "contentChanges": [{
                "range": {
                    "start": { "line": 0, "character": 0 },
                    "end": { "line": 0, "character": 4 },
                },
                "text": "node",
            }],
        }),
    );
    // The document is untouched, so hovering still describes `edge`.
    let text = hovered(&mut session, 1, 19);
    assert!(text.contains("edge(int, int)"), "{text}");
    session.finish();
}

#[test]
fn completion_offers_what_fits_where_the_cursor_is() {
    let mut session = Session::start();
    session.open(
        URI,
        "edge(1, 2) .
reach(?x, ?y) :- edge(?x, ?y) .
",
    );

    // In ordinary term position: the file's predicates and the builtins.
    let response = session.complete(URI, 1, 0);
    let labels = labels(&response);
    assert!(labels.contains(&"edge".to_string()), "{labels:?}");
    assert!(labels.contains(&"reach".to_string()), "{labels:?}");
    assert!(
        labels.contains(&"CONCAT".to_string()),
        "the builtins: {labels:?}"
    );

    session.finish();
}

#[test]
fn a_marker_decides_what_completion_offers() {
    let mut session = Session::start();
    session.open(
        URI,
        "@output p .
reach(?x, ?y) :- edge(?x, ?y) .
",
    );

    // After `@`, directives and nothing else.
    let directives = labels(&session.complete(URI, 0, 1));
    assert!(
        directives.contains(&"@import".to_string()),
        "{directives:?}"
    );
    assert!(
        !directives.iter().any(|label| label == "CONCAT"),
        "a directive is not a builtin: {directives:?}"
    );

    // After `?`, the variables of the rule the cursor is in.
    let variables = labels(&session.complete(URI, 1, 7));
    assert!(variables.contains(&"?x".to_string()), "{variables:?}");
    assert!(variables.contains(&"?y".to_string()), "{variables:?}");
    assert!(
        !variables.iter().any(|label| label == "edge"),
        "a variable is not a predicate: {variables:?}"
    );

    session.finish();
}

#[test]
fn renaming_a_predicate_edits_every_occurrence() {
    let mut session = Session::start();
    session.open(
        URI,
        "edge(1, 2) .
@output edge .
reach(?x) :- edge(?x, 1) .
",
    );
    let response = session.request(
        "textDocument/rename",
        json!({
            "textDocument": { "uri": URI },
            "position": { "line": 0, "character": 0 },
            "newName": "link",
        }),
    );
    let edits = response["result"]["changes"][URI]
        .as_array()
        .expect("edits for this file");
    // The fact, the directive, and the body atom.
    assert_eq!(edits.len(), 3, "{response}");
    for edit in edits {
        assert_eq!(edit["newText"], "link", "{response}");
        let range = &edit["range"];
        let width = range["end"]["character"].as_u64().expect("a number")
            - range["start"]["character"].as_u64().expect("a number");
        assert_eq!(width, 4, "only the name moves, not the atom: {edit}");
    }
    session.finish();
}

#[test]
fn preparing_a_rename_names_what_would_move() {
    let mut session = Session::start();
    session.open(URI, PROGRAM);
    let response = session.ask("textDocument/prepareRename", URI, 0, 1);
    assert_eq!(response["result"]["placeholder"], "edge", "{response}");
    session.finish();
}

#[test]
fn formatting_normalizes_statements_and_keeps_what_is_between_them() {
    let mut session = Session::start();
    session.open(
        URI,
        "% what this is
p(1,2)   .

q(?x):-p(?x,1) .
",
    );
    let response = session.request(
        "textDocument/formatting",
        json!({
            "textDocument": { "uri": URI },
            "options": { "tabSize": 4, "insertSpaces": true },
        }),
    );
    let edits = response["result"].as_array().expect("a list of edits");
    let texts: Vec<&str> = edits
        .iter()
        .map(|edit| edit["newText"].as_str().expect("text"))
        .collect();
    assert_eq!(
        texts,
        vec!["p(1, 2) .", "q(?x) :- p(?x, 1) ."],
        "{response}"
    );
    // The comment is on line 0 and nothing touches it.
    assert!(
        edits
            .iter()
            .all(|edit| edit["range"]["start"]["line"].as_u64() != Some(0)),
        "the comment is not the model's to rewrite: {response}"
    );
    session.finish();
}

#[test]
fn a_document_that_does_not_parse_is_not_formatted() {
    let mut session = Session::start();
    session.open(
        URI,
        "p(1,2) .
q(?x :- p(?x) .
",
    );
    let response = session.request(
        "textDocument/formatting",
        json!({
            "textDocument": { "uri": URI },
            "options": { "tabSize": 4, "insertSpaces": true },
        }),
    );
    // Printing back what parsed would delete the rest of the file.
    assert!(response["result"].is_null(), "{response}");
    session.finish();
}

#[test]
fn highlighting_tells_where_a_predicate_is_written_from_where_it_is_read() {
    let mut session = Session::start();
    session.open(URI, PROGRAM);
    let response = session.ask("textDocument/documentHighlight", URI, 1, 19);
    let highlights = response["result"].as_array().expect("a list");
    assert_eq!(highlights.len(), 2, "{response}");
    let kinds: Vec<u64> = highlights
        .iter()
        .map(|highlight| highlight["kind"].as_u64().expect("a kind"))
        .collect();
    // 3 is Write, 2 is Read.
    assert_eq!(kinds, vec![3, 2], "the fact writes it, the rule reads it");
    session.finish();
}

#[test]
fn asking_about_a_document_that_is_not_open_returns_nothing() {
    let mut session = Session::start();
    session.open(URI, "p(1) .\n");
    let response = session.ask("textDocument/hover", "file:///other.rls", 0, 0);
    assert!(response["result"].is_null(), "{response}");
    session.finish();
}

#[test]
fn a_closed_document_is_forgotten() {
    let mut session = Session::start();
    session.open(URI, PROGRAM);
    session.notify(
        "textDocument/didClose",
        json!({ "textDocument": { "uri": URI } }),
    );
    let cleared = session.receive();
    assert_eq!(cleared["method"], "textDocument/publishDiagnostics");
    assert_eq!(
        cleared["params"]["diagnostics"].as_array().map(Vec::len),
        Some(0),
        "closing clears what was reported"
    );
    let response = session.ask("textDocument/hover", URI, 1, 19);
    assert!(response["result"].is_null(), "{response}");
    session.finish();
}
