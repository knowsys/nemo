"""The Python API, end to end: parse, load, reason, read back.

Runs against a built `nmo` module (`maturin develop`, or a wheel on the
path)."""

import datetime
import decimal

import pytest

import nmo

ANCESTORS = """
    parent(alice, bob) . parent(bob, carol) .
    ancestor(?x, ?y) :- parent(?x, ?y) .
    ancestor(?x, ?z) :- parent(?x, ?y), ancestor(?y, ?z) .
"""


def reasoned():
    engine = nmo.Engine()
    engine.load(nmo.Program.parse(ANCESTORS))
    engine.reason()
    return engine


def test_programs_parse_and_inspect():
    program = nmo.Program.parse(ANCESTORS, name="ancestors.rls")
    assert program.ok
    assert len(program) == 4
    assert program.predicates == [("ancestor", 2), ("parent", 2)]


def test_parse_is_total_and_reports():
    program = nmo.Program.parse("p(?x :- q(?x) .")
    assert not program.ok
    report = program.diagnostics[0]
    assert report.severity == "error"
    assert report.code == "N0202"
    # The rendering quotes the source.
    assert "p(?x :- q(?x) ." in report.rendered


def test_reasoning_and_rows():
    engine = reasoned()
    rows = engine.rows("ancestor")
    assert len(rows) == 3
    assert (nmo.Iri("alice"), nmo.Iri("carol")) in rows
    assert engine.count("ancestor") == 3
    assert engine.derived_facts == 3
    assert engine.loaded_facts == 2
    assert engine.column_types("ancestor") == ["iri", "iri"]


def test_cursors_page_and_report_progress():
    engine = reasoned()
    cursor = engine.cursor("ancestor")
    assert cursor.remaining == 3
    assert set(engine.rows("ancestor")) == set(cursor)


def test_loading_a_broken_program_raises_diagnostics():
    engine = nmo.Engine()
    with pytest.raises(nmo.DiagnosticsError) as raised:
        engine.load(nmo.Program.parse("q(?x, ?y) :- p(?x) ."))
    report = raised.value.diagnostics[0]
    assert report.code == "N0204"
    assert "not bound" in report.message
    label = report.labels[0]
    assert (label.line, label.column) == (1, 7)


def test_check_collects_without_loading():
    engine = nmo.Engine()
    reports = engine.check(nmo.Program.parse("q(?x, ?y) :- p(?x) ."))
    assert any(report.code == "N0204" for report in reports)


VALUES = [
    "plain text",
    'with "quotes" and \\ escapes',
    nmo.Iri("alice"),
    nmo.Iri("http://example.org/entry?a=1&b=2"),
    nmo.LanguageString("hallo", "de"),
    True,
    False,
    -42,
    10**25,
    decimal.Decimal("1.5"),
    2.5,
    nmo.Float(2.5),
    datetime.datetime(2024, 3, 1, 12, 30, 0, 250000, tzinfo=datetime.timezone.utc),
    datetime.date(2024, 3, 1),
    datetime.time(12, 30, 0, 250000),
    datetime.time(12, 30, tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30))),
    datetime.timedelta(days=1, hours=2, microseconds=5),
    -datetime.timedelta(hours=5),
    nmo.Literal("2024-03-01Z", "http://www.w3.org/2001/XMLSchema#date"),
    nmo.Literal("P1Y2M", "http://www.w3.org/2001/XMLSchema#yearMonthDuration"),
    (1, 2),
    ("solo",),
    (1, (2, 3)),
    {nmo.Iri("port"): 8080},
    nmo.Labeled("point", (1, 2)),
    nmo.Labeled("config", {nmo.Iri("port"): 80}),
    nmo.Literal("5cm", "http://example.org/unit"),
]


def test_the_conversion_table_round_trips():
    engine = nmo.Engine()
    engine.import_rows("p", [(value,) for value in VALUES])
    engine.load(nmo.Program.parse("q(?x) :- p(?x) ."))
    engine.reason()
    back = [row[0] for row in engine.rows("q")]
    assert len(back) == len(VALUES)
    for value in VALUES:
        assert value in back, value


def test_python_equality_mirrors_term_identity():
    # An IRI is not the string of its characters; a 32-bit float is not
    # the double of the same value; 1 is not 1.0 — and hashes agree.
    assert nmo.Iri("a") != "a"
    assert nmo.Float(2.5) != 2.5
    assert len({nmo.Iri("a"), "a"}) == 2
    assert nmo.Iri("a") == nmo.Iri("a")
    assert hash(nmo.Iri("a")) == hash(nmo.Iri("a"))


def test_values_render_canonical_term_syntax():
    assert str(nmo.Iri("alice")) == "alice"
    assert str(nmo.LanguageString("hallo", "de")) == '"hallo"@de'
    assert str(nmo.Float(2.5)) == "2.5f"
    assert str(nmo.Null(3)) == "_:3"


def test_nulls_do_not_import():
    engine = nmo.Engine()
    with pytest.raises(ValueError):
        engine.import_rows("p", [(nmo.Null(1),)])


def test_map_keys_that_collapse_in_python_raise():
    # `1` and `true` are distinct terms but one dict key in Python; a map
    # holding both cannot cross without losing a pair, so it raises
    # instead of returning the wrong map.
    engine = nmo.Engine()
    engine.load(nmo.Program.parse('m({1 = "a", true = "b"}) .'))
    engine.reason()
    with pytest.raises(ValueError, match="`true` and `1`"):
        engine.rows("m")


def test_registered_resources_shadow_the_filesystem():
    engine = nmo.Engine()
    engine.add_resource("data.csv", "a,1\nb,2\nc,3\n")
    engine.load(
        nmo.Program.parse('@import p from csv { resource = "data.csv" } . q(?x) :- p(?x, ?y) .')
    )
    engine.reason()
    assert engine.count("q") == 3



CLOSURE = """
    @input edge .
    @output reach .
    reach(?x, ?y) :- edge(?x, ?y) .
    reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .
"""

USING_CLOSURE = """
    link(1, 2) . link(2, 3) .
    @use {name} {{ edge = link }} as t .
    out(?x, ?y) :- t::reach(?x, ?y) .
"""


def test_registered_modules_shadow_the_filesystem():
    engine = nmo.Engine()
    engine.add_module("lib::tc", CLOSURE)
    engine.load(nmo.Program.parse(USING_CLOSURE.format(name="lib::tc")))
    engine.reason()
    assert engine.count("out") == 3
    with pytest.raises(nmo.DiagnosticsError):
        engine.add_module("lib/tc.rls", CLOSURE)


def test_a_module_root_names_the_modules_in_its_directory(tmp_path):
    (tmp_path / "closure.rls").write_text(CLOSURE)
    engine = nmo.Engine()
    engine.add_module_root("shared", str(tmp_path))
    engine.load(nmo.Program.parse(USING_CLOSURE.format(name="shared::closure")))
    engine.reason()
    assert engine.count("out") == 3
    with pytest.raises(nmo.DiagnosticsError):
        engine.add_module_root("a::b", str(tmp_path))

@pytest.fixture
def server():
    """A local HTTP server serving `/data.csv`, and 404 elsewhere."""
    import http.server
    import threading

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/data.csv":
                body = b"a,1\nb,2\n"
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_error(404)

        def log_message(self, *args):
            pass

    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def test_remote_imports_are_fetched_by_the_load(server):
    engine = nmo.Engine()
    engine.load(
        nmo.Program.parse(
            f'@import p from csv {{ resource = "{server}/data.csv" }} . q(?x) :- p(?x, ?y) .'
        )
    )
    engine.reason()
    assert engine.count("q") == 2


def test_failed_fetches_fail_the_load_unless_the_import_warns(server):
    missing = f'resource = "{server}/missing.csv"'
    with pytest.raises(nmo.DiagnosticsError) as raised:
        nmo.Engine().load(nmo.Program.parse(f"@import p from csv {{ {missing} }} . q(?x) :- p(?x) ."))
    assert raised.value.diagnostics[0].code == "N0111"

    engine = nmo.Engine()
    engine.load(
        nmo.Program.parse(f"@import p from csv {{ {missing}, on_error = warn }} . q(?x) :- p(?x) .")
    )
    engine.reason()
    assert engine.count("q") == 0


def test_relative_imports_resolve_beside_the_program_file(tmp_path):
    (tmp_path / "data.csv").write_text("1\n2\n")
    path = tmp_path / "program.rls"
    path.write_text('@import p from csv { resource = "data.csv" } . q(?x) :- p(?x) .')
    engine = nmo.Engine()
    engine.load(nmo.Program.parse_file(str(path)))
    engine.reason()
    assert engine.count("q") == 2


def test_the_import_directory_resolves_every_relative_import(tmp_path):
    (tmp_path / "rows.csv").write_text("1\n")
    engine = nmo.Engine()
    engine.set_import_directory(str(tmp_path))
    engine.load(nmo.Program.parse('@import p from csv { resource = "rows.csv" } . q(?x) :- p(?x) .'))
    engine.reason()
    assert engine.count("q") == 1


def test_programs_parse_from_files_and_predicates_have_roles(tmp_path):
    path = tmp_path / "program.rls"
    path.write_text("edge(1, 2) . reach(?x, ?y) :- edge(?x, ?y) .")
    program = nmo.Program.parse_file(str(path))
    assert program.ok
    engine = nmo.Engine()
    engine.load(program)
    engine.reason()
    assert engine.predicates() == [("edge", 2, "extensional"), ("reach", 2, "derived")]


def test_arrow_stream_types_uniform_columns():
    pytest.importorskip("pyarrow")
    engine = nmo.Engine()
    engine.import_rows("m", [(i, float(i) / 2, f"row{i}") for i in range(100)])
    engine.load(nmo.Program.parse("out(?a, ?b, ?c) :- m(?a, ?b, ?c) ."))
    engine.reason()
    table = engine.to_arrow("out")
    assert [str(field.type) for field in table.schema] == ["int64", "double", "string"]
    assert table.num_rows == 100


def test_arrow_stream_counts_the_row_of_a_nullary_predicate():
    pytest.importorskip("pyarrow")
    engine = nmo.Engine()
    engine.load(nmo.Program.parse("flag() ."))
    engine.reason()
    assert engine.count("flag") == 1
    assert len(engine.arrow_stream("flag")) == 1
    assert engine.to_arrow("flag").num_rows == 1


def test_rows_import_after_the_fixpoint():
    engine = reasoned()
    engine.import_rows("parent", [(nmo.Iri("carol"), nmo.Iri("dave"))])
    engine.reason()
    assert engine.count("ancestor") == 6
    with pytest.raises(nmo.DiagnosticsError):
        engine.import_rows("unknown", [(1,)])


def test_extend_adds_rules_and_facts():
    engine = reasoned()
    engine.extend(
        nmo.Program.parse("parent(dave, erin) . forebear(?x) :- ancestor(?x, ?y) .")
    )
    engine.reason()
    assert engine.count("forebear") == 3
    with pytest.raises(nmo.DiagnosticsError):
        engine.extend(nmo.Program.parse("parent(too, many, columns) ."))


def test_reload_replaces_the_program():
    engine = reasoned()
    # Without the recursive rule, only the direct pairs remain.
    engine.reload(
        nmo.Program.parse(
            "parent(alice, bob) . parent(bob, carol) . ancestor(?x, ?y) :- parent(?x, ?y) ."
        )
    )
    engine.reason()
    assert engine.count("ancestor") == 2


def test_cancel_from_another_thread():
    import threading

    source = " ".join(f"edge({i}, {i + 1}) ." for i in range(120))
    source += " path(?x, ?y) :- edge(?x, ?y) . path(?x, ?z) :- edge(?x, ?y), path(?y, ?z) ."
    engine = nmo.Engine()
    engine.load(nmo.Program.parse(source))
    threading.Timer(0.01, engine.cancel).start()
    # Either the cancellation landed mid-run (N0207) or the run finished
    # first; resuming reaches the same fixpoint either way.
    try:
        engine.reason()
    except nmo.DiagnosticsError as raised:
        assert raised.diagnostics[0].code == "N0207"
        engine.reason()
    assert engine.count("path") == 121 * 120 // 2


def test_trace_returns_the_protocol_dict():
    engine = reasoned()
    response = engine.trace("ancestor(alice, carol)")
    assert response["version"] == 1
    [traced] = response["facts"]
    assert traced["holds"]
    tree = traced["tree"]
    assert tree["kind"] == "derived"
    assert tree["fact"]["predicate"] == "ancestor"
    # The chain bottoms out in asserted parents.
    kinds = set()

    def walk(node):
        kinds.add(node["kind"])
        for premise in node.get("premises", []):
            walk(premise)

    walk(tree)
    assert kinds == {"derived", "asserted"}
    # An underivable fact reports so, without a tree.
    [missing] = engine.trace(["ancestor(carol, alice)"])["facts"]
    assert not missing["holds"]
    assert "tree" not in missing


def test_trace_ascii_renders_trees():
    engine = reasoned()
    ascii_tree = engine.trace_ascii("ancestor(alice, bob)")
    assert ":-" in ascii_tree and "parent" in ascii_tree


def test_trace_raises_on_a_dirty_engine():
    engine = reasoned()
    # Strings would import as plain strings; the program's constants
    # are IRIs.
    engine.import_rows("parent", [(nmo.Iri("carol"), nmo.Iri("dan"))])
    with pytest.raises(nmo.DiagnosticsError):
        engine.trace("ancestor(alice, carol)")
    engine.reason()
    assert engine.trace("ancestor(alice, dan)")["facts"][0]["holds"]


def test_tree_queries_expand_server_side():
    engine = reasoned()
    response = engine.tree_for_table(
        {"predicate": "ancestor", "tableEntries": {"queries": ["alice, carol"]}}
    )
    assert response["predicate"] == "ancestor"
    assert len(response["tableEntries"]["entries"]) == 1
    assert not response["tableEntries"]["pagination"]["moreEntriesExist"]
    expansion = response["childInformation"]
    rule = expansion["rule"]
    assert rule["id"] == 3
    assert [atom["name"] for atom in rule["bodyPredicates"]] == ["parent", "ancestor"]
    # Some node bottoms out in an asserted parent fact.
    predicates = set()
    pending = [response]
    while pending:
        node = pending.pop()
        predicates.add(node["predicate"])
        pending.extend(node.get("childInformation", {}).get("children", []))
    assert predicates == {"ancestor", "parent"}
    # The rule's source, for an editor to jump to.
    assert engine.rule(rule["id"])["text"] == rule["stringRepresentation"]
    assert engine.rule(0) is None


def test_node_queries_return_every_node_of_the_given_shape():
    engine = reasoned()
    nodes = engine.table_entries_for_tree_nodes(
        {
            "predicate": "ancestor",
            "childInformation": {
                "rule": 2,
                "headIndex": 0,
                "children": [{"tableEntries": {"queries": ["alice, bob"]}}],
            },
        }
    )
    assert [node["addressInTree"] for node in nodes] == [[], [0]]
    assert [entry["termTuple"] for entry in nodes[0]["tableEntries"]["entries"]] == [
        ["alice", "bob"]
    ]


def test_retained_imports_trace_at_their_written_width():
    program = nmo.Program.parse(
        '@import p from csv { resource = "data.csv", format = (string, string, int) } .\n'
        "big(?x) :- p(?x, ?label, ?n), ?n > 5 . @output big ."
    )
    query = {"predicate": "p", "tableEntries": {"queries": [0]}}

    pruned = nmo.Engine()
    pruned.add_resource("data.csv", "a,label,7\nb,other,2\n")
    pruned.load(program)
    pruned.reason()
    # The rules never read `?label`: the import stores without it.
    with pytest.raises(nmo.DiagnosticsError):
        pruned.tree_for_table(query)

    retained = nmo.Engine()
    retained.retain_imports()
    retained.add_resource("data.csv", "a,label,7\nb,other,2\n")
    retained.load(program)
    retained.reason()
    response = retained.tree_for_table(query)
    assert len(response["tableEntries"]["entries"][0]["termTuple"]) == 3
