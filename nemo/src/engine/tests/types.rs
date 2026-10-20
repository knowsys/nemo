//! Types: the analysis, declarations, `@type`, and named arguments.

use super::*;

#[test]
fn a_type_conflict_hints_at_a_near_builtin() {
    // Only a conflict earns the hint: `loh(?x)` into a declared
    // double column is probably a misspelled `LOG`.
    let (program, _) = Program::parse(
        "@declare out(v: double) . n(1) . out(loh(?x)) :- n(?x) .",
        FileId::new(0),
    );
    let problem = Engine::new()
        .load(program)
        .expect_err("a tuple is not a double");
    assert!(
        problem.hints().iter().any(|hint| hint.contains("LOG")),
        "{problem:?}"
    );

    // A dead condition over the mistyped value hints too.
    let engine = run("
        n(1) .
        q(?y) :- n(?x), ?y = loh(?x), ?y > 1 .
    ");
    let [warning] = engine.reports() else {
        panic!("expected the one warning: {:?}", engine.reports());
    };
    assert_eq!(warning.code(), codes::NEVER_FIRES, "{warning:?}");
    assert!(
        warning.hints().iter().any(|hint| hint.contains("LOG")),
        "{warning:?}"
    );

    // Without type information, a near-builtin name is just a name.
    let engine = run("
        n(\"a\") .
        p(strlem(?x)) :- n(?x) .
    ");
    assert!(engine.reports().is_empty(), "{:?}", engine.reports());
}

#[test]
fn iri_and_langstring_declare_like_any_base_type() {
    // Every inferred kind has a declaration spelling; a fact
    // provably outside the declared kind is an error.
    let program = parse(
        "@declare said(who: iri, what: langstring) .
         said(alice, \"hi\"@en) .
         said(bob, 5) .",
    );
    let problem = Engine::new()
        .load(program)
        .expect_err("5 is not a langstring");
    assert_eq!(problem.code(), codes::FACT_OUTSIDE_DECLARATION);
    assert_eq!(problem.field("declared"), Some("langstring"));

    // The declared types read back as declared: the display and the
    // declaration use the same names.
    let (program, _) = Program::parse(
        "@declare said(who: iri, what: langstring) . said(alice, \"hi\"@en) .",
        FileId::new(0),
    );
    let mut engine = Engine::new();
    engine.load(program).expect("the program loads");
    let types = engine.column_types("said").expect("registered");
    let rendered: Vec<String> = types
        .iter()
        .map(|datatype| TypeSyntax::of(datatype).to_string())
        .collect();
    assert_eq!(rendered, ["iri", "langstring"]);
}

#[test]
fn a_union_admits_the_values_of_its_members() {
    // `null` names the nulls an existential rule invents, and `number`
    // every number.
    let mut engine = run("
        @declare manager(iri, iri | null) .
        @declare measure(number | string) .
        employee(alice) .
        manager(?e, !m) :- employee(?e) .
        measure(1) . measure(1.5) . measure(1.5E0) . measure(\"none\") .
    ");
    assert!(engine.reports().is_empty(), "{:?}", engine.reports());
    assert_eq!(count(&mut engine, "manager"), 1);

    // A value outside every member is an error, and the message writes
    // the union as declared.
    let problem = Engine::new()
        .load(parse("@declare measure(number | string) . measure(true) ."))
        .expect_err("a boolean is neither a number nor a string");
    assert_eq!(problem.code(), codes::FACT_OUTSIDE_DECLARATION);
    assert_eq!(problem.field("declared"), Some("string | number"));

    // Shapes of one arity under different labels stay apart, so a third
    // label is outside their union.
    let problem = Engine::new()
        .load(parse(
            "@declare drawn(point(int, int) | size(int, int)) .
             drawn(point(1, 2)) . drawn(size(3, 4)) . drawn(other(5, 6)) .",
        ))
        .expect_err("`other` is neither a point nor a size");
    assert_eq!(problem.code(), codes::FACT_OUTSIDE_DECLARATION);
    assert_eq!(
        problem.field("declared"),
        Some("point(int, int) | size(int, int)")
    );
}

#[test]
fn conditions_narrow_what_a_rule_writes() {
    let rendered = |engine: &Engine, predicate: &str| -> Vec<String> {
        engine
            .column_types(predicate)
            .expect("registered")
            .iter()
            .map(|datatype| TypeSyntax::of(datatype).to_string())
            .collect()
    };

    // A type test, a comparison, and a label each narrow the column they read.
    let engine = run("
        p(1) . p(\"x\") . p(point(1, 2)) . p(size(3, 4)) .
        integers(?x) :- p(?x), ISINTEGER(?x) .
        numbers(?x) :- p(?x), ?x > 0 .
        points(?x) :- p(?x), LABEL(?x) = point .
        others(?x) :- p(?x), NOT(ISINTEGER(?x)) .
    ");
    assert!(engine.reports().is_empty(), "{:?}", engine.reports());
    assert_eq!(rendered(&engine, "integers"), ["int"]);
    assert_eq!(rendered(&engine, "numbers"), ["int"]);
    assert_eq!(rendered(&engine, "points"), ["point(int, int)"]);
    assert_eq!(
        rendered(&engine, "others"),
        ["string | point(int, int) | size(int, int)"]
    );

    // Conditions that no value meets at once prove the rule dead.
    let engine = run("
        p(1) . p(\"x\") .
        q(?x) :- p(?x), ISINTEGER(?x), ISSTRING(?x) .
    ");
    let [warning] = engine.reports() else {
        panic!("expected the one warning: {:?}", engine.reports());
    };
    assert_eq!(warning.code(), codes::NEVER_FIRES, "{warning:?}");
    assert!(
        format!("{warning:?}").contains("no value of `?x` satisfies the rule's conditions"),
        "{warning:?}"
    );
}

#[test]
fn base_type_names_cannot_be_declared_with_type() {
    let (program, _) = Program::parse("@type langstring(int) .", FileId::new(0));
    let problem = Engine::new()
        .load(program)
        .expect_err("base-type names are reserved");
    assert_eq!(
        problem.code(),
        codes::TYPE_LABEL_IS_BASE_TYPE,
        "{problem:?}"
    );
    assert_eq!(problem.field("name"), Some("langstring"));
}

#[test]
fn a_disjoint_join_is_warned_and_still_runs() {
    let mut engine = run("
        word(\"a\") . value(1) .
        dead(?x) :- word(?x), value(?x) .
    ");
    assert_eq!(count(&mut engine, "dead"), 0);
    let [warning] = engine.reports() else {
        panic!("expected the one warning: {:?}", engine.reports());
    };
    assert_eq!(warning.code(), codes::NEVER_FIRES);
    assert!(
        warning.labels()[0].message.contains("would have to be"),
        "{warning:?}"
    );
}

#[test]
fn a_condition_that_cannot_hold_is_warned() {
    let engine = run("
        word(\"a\") .
        big(?x) :- word(?x), ?x > 1 .
    ");
    let [warning] = engine.reports() else {
        panic!("expected the one warning: {:?}", engine.reports());
    };
    assert_eq!(
        warning.labels()[0].message,
        "this condition always fails to evaluate",
        "{warning:?}"
    );
}

#[test]
fn the_analysis_stays_silent_where_a_value_could_fit() {
    // `?x` can be an integer, so nothing is provable, and provable is
    // the only thing reported.
    let engine = run("
        p(1) . p(\"a\") .
        q(?x) :- p(?x), ?x > 0 .
    ");
    assert!(engine.reports().is_empty(), "{:?}", engine.reports());
}

#[test]
fn a_fact_violating_its_declaration_is_an_error() {
    let (program, _) = Program::parse("@declare p(int) . p(\"a\") .", FileId::new(0));
    let problem = Engine::new()
        .load(program)
        .expect_err("the value is outside the declared type");
    assert_eq!(
        problem.code(),
        codes::FACT_OUTSIDE_DECLARATION,
        "{problem:?}"
    );
}

#[test]
fn a_declared_shape_checks_fact_elements() {
    // The declaration reaches inside the tuple: a string where an
    // integer element is declared is an error.
    let (program, _) = Program::parse(
        "@declare city(location: (int, int), name: string) .\n\
         city((\"x\", 13), \"Dresden\") .",
        FileId::new(0),
    );
    let problem = Engine::new()
        .load(program)
        .expect_err("the value is outside the declared type at depth");
    assert_eq!(
        problem.code(),
        codes::FACT_OUTSIDE_DECLARATION,
        "{problem:?}"
    );
    assert_eq!(problem.field("declared"), Some("(int, int)"));
}

#[test]
fn a_pattern_against_a_declared_shape_proves_dead() {
    // The declared arity is two; a three-element pattern can never
    // match, and the guard's conjunction is provably false.
    let mut engine = run("
        @declare city(location: (int, int), name: string) .
        city((51, 13), \"Dresden\") .
        q(?a, ?b, ?c) :- city((?a, ?b, ?c), ?n) .
    ");
    assert_eq!(count(&mut engine, "q"), 0);
    let [warning] = engine.reports() else {
        panic!("expected the one warning: {:?}", engine.reports());
    };
    assert_eq!(warning.code(), codes::NEVER_FIRES, "{warning:?}");
    assert_eq!(
        warning.labels()[0].message,
        "this pattern never matches a value of type (int, int)",
        "{warning:?}"
    );
}

#[test]
fn a_pattern_against_the_facts_shape_proves_dead() {
    // No declaration: the facts alone fix the arity, and the
    // mismatched pattern is proven dead the same way.
    let engine = run("
        p((1, 2)) . p((3, 4)) .
        q(?x, ?y, ?z) :- p((?x, ?y, ?z)) .
    ");
    let [warning] = engine.reports() else {
        panic!("expected the one warning: {:?}", engine.reports());
    };
    assert_eq!(warning.code(), codes::NEVER_FIRES, "{warning:?}");
}

#[test]
fn a_label_mismatch_proves_dead() {
    let engine = run("
        p(point(1, 2)) .
        q(?x, ?y) :- p(pixel(?x, ?y)) .
    ");
    let [warning] = engine.reports() else {
        panic!("expected the one warning: {:?}", engine.reports());
    };
    assert_eq!(warning.code(), codes::NEVER_FIRES, "{warning:?}");
}

#[test]
fn element_types_flow_through_accessors() {
    // The element type reaches the condition: a string function
    // over a provably integer element can never evaluate.
    let engine = run("
        @declare city(location: (int, int), name: string) .
        city((51, 13), \"Dresden\") .
        dead(?c) :- city(?c, _), STRLEN(?c[0]) = 1 .
    ");
    let [warning] = engine.reports() else {
        panic!("expected the one warning: {:?}", engine.reports());
    };
    assert_eq!(warning.code(), codes::NEVER_FIRES, "{warning:?}");
}

#[test]
fn mixed_shapes_widen_and_stay_quiet() {
    // Two arities in one column: the shape widens to the bare
    // kind, and provable is the only thing reported.
    let mut engine = run("
        p((1, 2)) . p((1, 2, 3)) .
        q(?x, ?y, ?z) :- p((?x, ?y, ?z)) .
    ");
    assert_eq!(count(&mut engine, "q"), 1);
    assert!(engine.reports().is_empty(), "{:?}", engine.reports());
}

#[test]
fn an_unlabeled_tuple_type_is_exactly_unlabeled() {
    // `(int, int)` and `point(int, int)` are different values, so
    // they are different types.
    let (program, _) = Program::parse("@declare p((int, int)) . p(point(1, 2)) .", FileId::new(0));
    let problem = Engine::new()
        .load(program)
        .expect_err("the label's value is outside its declared type");
    assert_eq!(
        problem.code(),
        codes::FACT_OUTSIDE_DECLARATION,
        "{problem:?}"
    );
}

#[test]
fn bare_structure_types_declare_the_kind() {
    let (program, _) = Program::parse("@declare p(tuple) . p({a = 1}) .", FileId::new(0));
    let problem = Engine::new()
        .load(program)
        .expect_err("a map is not a tuple");
    assert_eq!(
        problem.code(),
        codes::FACT_OUTSIDE_DECLARATION,
        "{problem:?}"
    );

    let mut engine = run("
        @declare q(map) .
        q({a = 1}) .
    ");
    assert_eq!(count(&mut engine, "q"), 1);
    assert!(engine.reports().is_empty(), "{:?}", engine.reports());
}

#[test]
fn a_type_declaration_checks_ground_constructions() {
    // The wrapper type: `angle(decimal)` takes decimals (integers
    // canonicalize into them), anywhere in a value.
    let mut engine = run("
        @type angle(decimal) .
        a(angle(1.5)) . a(angle(2)) .
        nested((angle(0.5), 1)) .
    ");
    assert_eq!(count(&mut engine, "a"), 2);
    for bad in [
        "@type angle(decimal) . a(angle(\"x\")) .",
        "@type angle(decimal) . a(angle(1.5, 2.5)) .",
        "@type angle(decimal) . nested((angle(\"x\"), 1)) .",
    ] {
        let (program, _) = Program::parse(bad, FileId::new(0));
        let problem = Engine::new()
            .load(program)
            .expect_err("the value is outside the declared type");
        assert_eq!(
            problem.code(),
            codes::VALUE_OUTSIDE_TYPE,
            "{bad}: {problem:?}"
        );
    }
}

#[test]
fn a_type_declaration_checks_rule_constructions() {
    // What is static at the site (the arity and the ground parts)
    // errors before anything runs, each as its own kind.
    for (bad, code) in [
        (
            "@type point(int, int) . n(1) . q(point(?x, ?x, ?x)) :- n(?x) .",
            codes::CONSTRUCTION_SIZE,
        ),
        (
            "@type point(int, int) . n(1) . q(point(?x, \"a\")) :- n(?x) .",
            codes::TUPLE_FIELD_OUTSIDE_TYPE,
        ),
        (
            "@type config{ port: int } . n(1) . q(config{host = ?x}) :- n(?x) .",
            codes::UNKNOWN_FIELD,
        ),
    ] {
        let (program, _) = Program::parse(bad, FileId::new(0));
        let problem = Engine::new()
            .load(program)
            .expect_err("the value is outside the declared type");
        assert_eq!(problem.code(), code, "{bad}: {problem:?}");
    }
    // A conforming construction runs.
    let mut engine = run("
        @type point(int, int) .
        n(1) . n(2) .
        q(point(?x, 0)) :- n(?x) .
    ");
    assert_eq!(count(&mut engine, "q"), 2);
}

#[test]
fn a_type_declaration_checks_what_variables_build() {
    // The values a variable can hold flow into the fields it builds, so a
    // construction no value can fit errors, though `q` declares nothing.
    for bad in [
        "@type angle(decimal) . p(\"x\") . q(angle(?v)) :- p(?v) .",
        "@type angle(decimal) . p(\"x\") . q((angle(?v), 1)) :- p(?v) .",
        "@type config{ port: int } . p(\"x\") . q(config{port = ?v}) :- p(?v) .",
    ] {
        let (program, _) = Program::parse(bad, FileId::new(0));
        let problem = Engine::new()
            .load(program)
            .expect_err("the built value is outside the declared type");
        assert_eq!(
            problem.code(),
            codes::RULE_BUILDS_OUTSIDE_TYPE,
            "{bad}: {problem:?}"
        );
    }
    // Into a declared column, the declaration of the column reports it.
    let (program, _) = Program::parse(
        "@type angle(decimal) . @declare q(angle) . p(\"x\") . q(angle(?v)) :- p(?v) .",
        FileId::new(0),
    );
    let problem = Engine::new()
        .load(program)
        .expect_err("the column excludes the built value");
    assert_eq!(problem.code(), codes::RULE_WRITES_OUTSIDE, "{problem:?}");
    // Values that may not fit are an error too: every construction must provably fit.
    let problem = Engine::new()
        .load(parse(
            "@type angle(decimal) . p(1.5) . p(\"x\") . q(angle(?v)) :- p(?v) .",
        ))
        .expect_err("the built value is not shown to fit");
    assert_eq!(
        problem.code(),
        codes::RULE_MAY_BUILD_OUTSIDE_TYPE,
        "{problem:?}"
    );
    // A condition establishes the type.
    let mut engine = run("
        @type angle(decimal) .
        p(1.5) . p(\"x\") .
        q(angle(?v)) :- p(?v), ISDECIMAL(?v) .
    ");
    assert_eq!(count(&mut engine, "q"), 1);
}

#[test]
fn a_declared_label_is_a_type() {
    // The name can be used in type expressions, including later `@type`
    // declarations, and is checked at depth like any declared type.
    let mut engine = run("
        @type point(int, int) .
        @type segment(point, point) .
        @declare road(span: segment, name: string) .
        road(segment(point(1, 2), point(3, 4)), \"a\") .
    ");
    assert_eq!(count(&mut engine, "road"), 1);
    assert!(engine.reports().is_empty(), "{:?}", engine.reports());

    // A bare decimal is not an angle, since the wrapper is distinct.
    let (program, _) = Program::parse(
        "@type angle(decimal) . @declare a(turn: angle) . a(1.5) .",
        FileId::new(0),
    );
    let problem = Engine::new()
        .load(program)
        .expect_err("the wrapper is not its content");
    assert_eq!(
        problem.code(),
        codes::FACT_OUTSIDE_DECLARATION,
        "{problem:?}"
    );
}

#[test]
fn a_recursive_type_is_checked_by_its_label() {
    // A value of the wrong kind is no tree.
    let problem = Engine::new()
        .load(parse(
            "@type tree(int, tree, tree) . @declare t(root: tree) . t(5) .",
        ))
        .expect_err("an integer is not a tree");
    assert_eq!(
        problem.code(),
        codes::FACT_OUTSIDE_DECLARATION,
        "{problem:?}"
    );

    // A list built by recursion fits by its label, at any length.
    let lists = "
        @type list(int, list | nil) .
        @type nil() .
        @declare l(list | nil) .
        item(1) . item(2) .
        l(nil()) .
        l(list(?x, ?t)) :- item(?x), l(?t) .
    ";
    let mut engine = Engine::new();
    engine.load(parse(lists)).expect("the program loads");
    assert!(engine.reports().is_empty(), "{:?}", engine.reports());
    let types = engine.column_types("l").expect("registered");
    assert_eq!(TypeSyntax::of(&types[0]).to_string(), "list | nil");

    // A fact is checked through every level, past any depth.
    let deep = "@type list(int, list | nil) . @type nil() .
        l(list(1, list(2, list(3, list(4, list(\"x\", nil())))))) .";
    let problem = Engine::new()
        .load(parse(deep))
        .expect_err("the fifth element is a string");
    assert_eq!(problem.code(), codes::VALUE_OUTSIDE_TYPE, "{problem:?}");
}

#[test]
fn a_write_not_shown_to_fit_is_an_error() {
    // One value of `p` fits and one does not, so the write is not shown to fit.
    let problem = Engine::new()
        .load(parse(
            "@declare q(int) . p(1) . p(\"x\") . q(?x) :- p(?x) .",
        ))
        .expect_err("`p` may hold a string");
    assert_eq!(problem.code(), codes::RULE_MAY_WRITE_OUTSIDE, "{problem:?}");
    assert_eq!(problem.field("written"), Some("string | int"));
    assert_eq!(problem.field("declared"), Some("int"));
    assert!(
        problem
            .notes()
            .iter()
            .any(|note| note == "`?x` is read from column 1 of `p`, which holds string | int"),
        "{problem:?}"
    );
    assert!(
        problem.hints().iter().any(
            |hint| hint == "add a condition such as `ISINTEGER(?x)`, or convert with `INT(?x)`"
        ),
        "{problem:?}"
    );

    // The condition the hint names establishes the type.
    let mut engine = run("
        @declare q(int) .
        p(1) . p(\"x\") .
        q(?x) :- p(?x), ISINTEGER(?x) .
    ");
    assert_eq!(count(&mut engine, "q"), 1);
    assert!(engine.reports().is_empty(), "{:?}", engine.reports());

    // A value of a declared label narrows by its label.
    let problem = Engine::new()
        .load(parse(
            "@type point(int, int) . @declare at(point) .
             p(point(1, 2)) . p(1) . at(?x) :- p(?x) .",
        ))
        .expect_err("`p` may hold an integer");
    assert!(
        problem
            .hints()
            .iter()
            .any(|hint| hint == "add a condition such as `LABEL(?x) = point`"),
        "{problem:?}"
    );
}

#[test]
fn a_construction_not_shown_to_fit_says_where_its_fields_come_from() {
    let problem = Engine::new()
        .load(parse(
            "@type angle(decimal) . p(1.5) . p(\"x\") . q(angle(?v)) :- p(?v) .",
        ))
        .expect_err("the built value is not shown to fit");
    assert_eq!(
        problem.code(),
        codes::RULE_MAY_BUILD_OUTSIDE_TYPE,
        "{problem:?}"
    );
    assert!(
        problem
            .notes()
            .iter()
            .any(|note| note == "`?v` is read from column 1 of `p`, which holds string | decimal"),
        "{problem:?}"
    );
    assert!(
        problem
            .hints()
            .iter()
            .any(|hint| hint == "add a condition such as `ISDECIMAL(?v)`"),
        "{problem:?}"
    );
}

#[test]
fn a_computed_value_not_shown_to_fit_says_where_its_inputs_come_from() {
    // A string cannot reach the sum, whose evaluation drops its row,
    // so only the decimals make the sum not shown to be an integer.
    let problem = Engine::new()
        .load(parse(
            "@declare q(int) . p(1) . p(1.5) . p(\"x\") . q(?x + 1) :- p(?x) .",
        ))
        .expect_err("the sum may be a decimal");
    assert_eq!(problem.code(), codes::RULE_MAY_WRITE_OUTSIDE, "{problem:?}");
    assert!(
        problem
            .notes()
            .iter()
            .any(|note| note == "`?x` is read from column 1 of `p`, which holds string | decimal"),
        "{problem:?}"
    );
    assert!(
        problem.hints().iter().any(
            |hint| hint == "add a condition such as `ISINTEGER(?x)`, or convert with `INT(?x)`"
        ),
        "{problem:?}"
    );
}

#[test]
fn a_union_is_established_by_tests_combined() {
    let problem = Engine::new()
        .load(parse(
            "@declare q(iri | null) .
             p(a) . p(1) . s(1) .
             p(!n) :- s(?y) .
             q(?x) :- p(?x) .",
        ))
        .expect_err("`p` may hold an integer");
    assert!(
        problem
            .hints()
            .iter()
            .any(|hint| hint == "add a condition such as `OR(ISIRI(?x), ISNULL(?x))`"),
        "{problem:?}"
    );
}

#[test]
fn a_pattern_against_a_declared_label_is_proved_dead_by_its_declaration() {
    // The column holds points, which the label's declaration gives two
    // fields, so a pattern of three never matches.
    let engine = run("
        @type point(int, int) .
        p(point(1, 2)) .
        q(?x) :- p(point(?x, ?y, ?z)) .
    ");
    let [warning] = engine.reports() else {
        panic!("expected the one warning: {:?}", engine.reports());
    };
    assert_eq!(warning.code(), codes::NEVER_FIRES, "{warning:?}");
}

#[test]
fn arithmetic_and_aggregates_keep_their_types() {
    // Integers add to integers, and sum to one.
    let engine = run("
        @declare next(int) .
        @declare total(int) .
        @declare mean(decimal) .
        n(1) . n(2) .
        next(?x + 1) :- n(?x) .
        total(#sum(?x)) :- n(?x) .
        mean(#avg(?x)) :- n(?x) .
    ");
    assert!(engine.reports().is_empty(), "{:?}", engine.reports());

    // A quotient of integers is exact, but not an integer.
    let problem = Engine::new()
        .load(parse("@declare half(int) . n(1) . half(?x / 2) :- n(?x) ."))
        .expect_err("a quotient may be a decimal");
    assert_eq!(problem.code(), codes::RULE_MAY_WRITE_OUTSIDE, "{problem:?}");
}

#[test]
fn a_declared_label_is_named_rather_than_written_out() {
    let problem = Engine::new()
        .load(parse(
            "@type point(int, int) . @declare at(point(int, int)) .",
        ))
        .expect_err("`point` has a `@type`");
    assert_eq!(
        problem.code(),
        codes::DECLARED_LABEL_WRITTEN_OUT,
        "{problem:?}"
    );
    assert!(
        problem.hints().iter().any(|hint| hint == "write `point`"),
        "{problem:?}"
    );
}

#[test]
fn rows_added_after_a_load_keep_the_declarations() {
    let rendered = |engine: &Engine, predicate: &str| -> Vec<String> {
        engine
            .column_types(predicate)
            .expect("registered")
            .iter()
            .map(|datatype| TypeSyntax::of(datatype).to_string())
            .collect()
    };
    let program = "@declare q(int) . p(1) . q(?x) :- p(?x) .";

    // Rows within the inferred types keep the analysis the program was loaded with.
    let mut engine = run(program);
    engine
        .import_rows("p", [vec![DataValue::integer(2)]])
        .expect("an integer fits");
    engine.reason().expect("reasoning continues");
    assert_eq!(count(&mut engine, "q"), 2);

    // A row that widens `p` would let `q` store a string, so the call is refused,
    // and the engine stays as it was.
    let problem = engine
        .import_rows("p", [vec![DataValue::string("x")]])
        .expect_err("a string would reach `q`");
    assert_eq!(problem.code(), codes::RULE_MAY_WRITE_OUTSIDE, "{problem:?}");
    assert!(
        problem.notes().iter().any(|note| note
            == "`?x` is read from column 1 of `p`, which holds string | int, and held int \
                before this change"),
        "{problem:?}"
    );
    assert!(
        problem
            .notes()
            .iter()
            .any(|note| note == "the rows added through `import_rows` widen `p`"),
        "{problem:?}"
    );
    assert_eq!(count(&mut engine, "p"), 2);
    assert_eq!(rendered(&engine, "p"), ["int"]);

    // Without a declaration in the way, the rows widen the types of everything
    // derived from them.
    let mut engine = run("p(1) . q(?x) :- p(?x) .");
    engine
        .import_rows("p", [vec![DataValue::string("x")]])
        .expect("nothing is declared");
    assert_eq!(rendered(&engine, "q"), ["string | int"]);
}

#[test]
fn an_extension_that_breaks_a_declaration_is_refused() {
    let mut engine = run("@declare q(int) . p(1) . q(?x) :- p(?x) .");
    let problem = engine
        .extend(parse("r(\"x\") . p(?x) :- r(?x) ."))
        .expect_err("the extension adds a string to `p`");
    assert_eq!(problem.code(), codes::RULE_MAY_WRITE_OUTSIDE, "{problem:?}");
    assert!(
        problem.notes().iter().any(|note| note
            == "`?x` is read from column 1 of `p`, which holds string | int, and held int \
                before this change"),
        "{problem:?}"
    );
    // The loaded program is unchanged and runs on.
    engine.reason().expect("reasoning continues");
    assert_eq!(count(&mut engine, "q"), 1);
}

/// Debug builds hold every stored block to the inferred column types,
/// so a value outside them, which would be a bug in the analysis, stops the run.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "the analysis inferred")]
fn a_stored_value_outside_the_inferred_types_panics_in_debug_builds() {
    use std::collections::HashMap;

    use nemo_physical::{
        storage::StorageValue,
        trie::RowBuffer,
        value::{Type, ValueDomain},
    };

    let engine = run("p(1) .");
    let &Registered { table, .. } = engine.predicates.get("p").expect("registered");
    let types = HashMap::from([(table, vec![Type::of(ValueDomain::String)])]);
    let mut rows = RowBuffer::new(1);
    let one = DataValue::integer(1);
    rows.push_row(&[StorageValue::from_value(one.as_ref()).expect("inline")]);
    engine.assert_stored_types(&types, table, &rows);
}

#[test]
fn type_declaration_names_are_checked() {
    for (bad, expected) in [
        ("@type int(string) .", codes::TYPE_LABEL_IS_BASE_TYPE),
        ("@type strlen(string) .", codes::TYPE_LABEL_IS_BUILTIN),
        ("@type _(int) .", codes::TYPE_UNNAMED),
        ("@type (int, int) .", codes::TYPE_UNNAMED),
        (
            "@type point(int) . @type point(int, int) .",
            codes::DUPLICATE_TYPE,
        ),
        ("@declare p(location: whatever) .", codes::UNKNOWN_TYPE),
    ] {
        let program = parse(bad);
        let problem = Engine::new().load(program).expect_err(bad);
        assert_eq!(problem.code(), expected, "{bad}: {problem:?}");
    }
}

#[test]
fn a_rule_writing_a_wrong_shape_is_an_error() {
    // The head builds a three-element tuple into a column declared
    // as a pair, which the check at depth catches.
    let (program, _) = Program::parse(
        "@declare city(location: (int, int), name: string) .\n\
         src(1) .\n\
         city((?x, ?x, ?x), \"n\") :- src(?x) .",
        FileId::new(0),
    );
    let problem = Engine::new()
        .load(program)
        .expect_err("the value is outside the declared type at depth");
    assert_eq!(problem.code(), codes::RULE_WRITES_OUTSIDE, "{problem:?}");
}

#[test]
fn a_rule_writing_outside_a_declaration_is_an_error() {
    let (program, _) = Program::parse(
        "@declare out(int) . src(\"a\") . out(?x) :- src(?x) .",
        FileId::new(0),
    );
    let problem = Engine::new()
        .load(program)
        .expect_err("the head provably writes a string");
    assert_eq!(problem.code(), codes::RULE_WRITES_OUTSIDE, "{problem:?}");
}

#[test]
fn a_declaration_fixes_the_arity() {
    let (program, _) = Program::parse("@declare p(int, int) . p(1) .", FileId::new(0));
    let problem = Engine::new()
        .load(program)
        .expect_err("one column is not two");
    assert_eq!(problem.code(), codes::DECLARED_ARITY, "{problem:?}");
}

#[test]
fn a_predicate_has_one_arity() {
    // A name is one predicate; a second use with a different number
    // of columns is refused where it happens.
    let program = parse("p(1) . p(1, 2) .");
    let problem = Engine::new()
        .load(program)
        .expect_err("one predicate, two widths");
    assert_eq!(problem.code(), codes::ARITY_MISMATCH, "{problem:?}");
    assert_eq!(problem.field("arity"), Some("2"));
    assert_eq!(problem.field("registered"), Some("1"));
}

#[test]
fn named_arguments_are_errors_where_they_cannot_resolve() {
    for (source, gap) in [
        ("p(name: 1) .", "no declaration"),
        (
            "@declare p(name: string) . p(other: \"a\") .",
            "unknown column",
        ),
        (
            "@declare p(name: string, age: int) . p(name: \"a\") .",
            "a fact leaves no column out",
        ),
        (
            "@declare p(name: string, age: int) . q(?x) :- p(name: ?x, name: ?x) .",
            "a column given twice",
        ),
    ] {
        let program = parse(source);
        assert!(Engine::new().load(program).is_err(), "{gap} went unrefused");
    }
}

#[test]
fn inferred_types_are_readable() {
    use nemo_physical::value::{Type, ValueDomain};

    let engine = run("
        edge(1, 2) . name(\"a\") .
        reach(?x, ?y) :- edge(?x, ?y) .
        reach(?x, ?y) :- edge(?x, ?z), reach(?z, ?y) .
        located(point(?x, ?y)) :- edge(?x, ?y) .
    ");
    let types = engine
        .column_types("reach")
        .expect("the predicate is known");
    assert_eq!(
        types,
        [
            Type::of(ValueDomain::Integer),
            Type::of(ValueDomain::Integer)
        ]
    );
    // A structured column reads back with its shape.
    let [located] = engine
        .column_types("located")
        .expect("the predicate is known")
    else {
        panic!("one column");
    };
    assert_eq!(TypeSyntax::of(located).to_string(), "point(int, int)");
}

#[test]
fn a_typed_parameter_rejects_a_value_outside_its_type() {
    let (program, _) = Program::parse("@parameter $limit: int = \"ten\" .\np(1) .", FileId::new(0));
    let problem = Engine::new()
        .load(program)
        .expect_err("a string is not an int");
    assert_eq!(problem.code(), codes::PARAMETER_OUTSIDE_TYPE, "{problem:?}");
}

/// The program reading two columns of a three-column import, so the middle one is pruned.
const PRUNED_IMPORT: &str = "@import c from csv { resource = \"c.csv\", format = (string, int, \
                             string) } .\nq(?s, ?z) :- c(?s, _, ?z) .\n@output q .";

/// A row given to `import_rows` for an import stored without a column is checked at its
/// columns as written, the pruned one included.
#[test]
fn rows_of_a_pruned_import_are_checked_at_their_written_columns() {
    let data = [("c.csv", "a,1,b\nc,2,d\n")];
    let declared = format!("@declare c(a: string, b: int, c: string) .\n{PRUNED_IMPORT}");
    let mut engine = load_resources(&declared, &data);
    assert_eq!(stored(&engine, "c").1, 2, "the middle column is pruned");
    engine
        .import_rows(
            "c",
            vec![vec![
                DataValue::string("x"),
                DataValue::integer(5),
                DataValue::string("y"),
            ]],
        )
        .expect("the row fits the declaration");
    let refusal = engine
        .import_rows(
            "c",
            vec![vec![
                DataValue::string("x"),
                DataValue::string("no"),
                DataValue::string("y"),
            ]],
        )
        .expect_err("the pruned column is checked too");
    assert_eq!(
        refusal.code(),
        codes::FACT_OUTSIDE_DECLARATION,
        "{refusal:?}"
    );
    assert_eq!(refusal.field("column"), Some("2"));
    engine.reason().expect("reasoning reaches the fixpoint");
    assert_eq!(count(&mut engine, "q"), 3);
}

/// A declaration an extension gives an import stored without a column is checked against
/// the stored columns, each at its column as written.
#[test]
fn a_late_declaration_checks_a_pruned_import_at_its_written_columns() {
    let data = [("c.csv", "a,1,b\nc,2,d\n")];
    let mut engine = load_resources(PRUNED_IMPORT, &data);
    assert_eq!(stored(&engine, "c").1, 2, "the middle column is pruned");
    engine
        .extend(parse("@declare c(a: string, b: int, c: string) ."))
        .expect("the stored columns fit the declaration");

    let mut engine = load_resources(PRUNED_IMPORT, &data);
    let refusal = engine
        .extend(parse("@declare c(a: string, b: int, c: int) ."))
        .expect_err("the third column stores strings");
    assert_eq!(refusal.code(), codes::STORED_VALUES_EXCLUDED, "{refusal:?}");
    assert_eq!(refusal.field("column"), Some("3"));
    assert_eq!(refusal.field("stored"), Some("string"));
}

/// The note on where a write's values come from names the column as written,
/// also where the loader leaves out columns before it.
#[test]
fn origin_notes_name_the_written_column_of_a_pruned_import() {
    let mut engine = Engine::new();
    engine.add_resource("c.csv", "a,1,b\n".as_bytes().to_vec());
    let problem = engine
        .load(parse(
            "@declare q(v: int) .\n@import c from csv { resource = \"c.csv\" } .\n\
             q(?z) :- c(_, _, ?z) .",
        ))
        .expect_err("the third column may hold anything");
    assert_eq!(problem.code(), codes::RULE_MAY_WRITE_OUTSIDE, "{problem:?}");
    assert!(
        problem
            .notes()
            .iter()
            .any(|note| note == "`?z` is read from column 3 of `c`, which holds any"),
        "{problem:?}"
    );
}

/// A rule writing values that cannot fit a declaration is told where they come from,
/// and given no hint: no condition makes them fit.
#[test]
fn a_write_that_cannot_fit_names_its_origin_without_a_hint() {
    let problem = Engine::new()
        .load(parse(
            "@type point(int, int) .\np(\"a\") .\nq(point(?x, 1)) :- p(?x) .",
        ))
        .expect_err("a string never fits an int field");
    assert_eq!(
        problem.code(),
        codes::RULE_BUILDS_OUTSIDE_TYPE,
        "{problem:?}"
    );
    assert!(
        problem
            .notes()
            .iter()
            .any(|note| note == "`?x` is read from column 1 of `p`, which holds string"),
        "{problem:?}"
    );
    assert!(problem.hints().is_empty(), "{problem:?}");
}

/// A dead rule's labels name what rules out every row, at the part of the rule
/// written for it.
#[test]
fn never_firing_rules_are_labeled_at_their_cause() {
    // Rows dropped at run time are reported too, so the warning is looked up by its code.
    let label = |source: &str| -> String {
        let engine = run(source);
        let warning = engine
            .reports()
            .iter()
            .find(|report| report.code() == codes::NEVER_FIRES)
            .unwrap_or_else(|| panic!("no warning: {:?}", engine.reports()));
        warning.labels()[0].message.clone()
    };
    assert_eq!(
        label("p(1, 2) .\nr(?x) :- p(?x, \"a\") ."),
        "column 2 of `p` holds int, never this constant"
    );
    assert_eq!(
        label("p(1, \"a\") .\nr(?x) :- p(?x, ?x) ."),
        "`?x` would have to be int and string at once"
    );
    assert_eq!(
        label("p(1) .\nr(?y) :- p(?x), ?y = STRLEN(?x) ."),
        "`?y` can never be computed"
    );
    assert_eq!(
        label("p(1) .\nr(?x + \"a\") :- p(?x) ."),
        "an expression in the head can never be computed"
    );
    assert_eq!(
        label("p(1) . q(\"b\") .\nr(?x) :- p(?x), q(STRLEN(?x)) ."),
        "this argument can never be computed"
    );
    assert_eq!(
        label("p(1) .\nr(?x) :- p(?x), q(_) ."),
        "`q` is always empty"
    );
    // The values a dead pattern would extract are not reported again.
    let engine = run("p(1) .\nr(?a) :- p((?a, _)) .");
    let [warning] = engine.reports() else {
        panic!("expected the one warning: {:?}", engine.reports());
    };
    assert_eq!(
        warning.labels()[0].message,
        "this pattern never matches a value of type int"
    );
    assert!(warning.notes().is_empty(), "{warning:?}");
}

/// A value outside its label's type is reported at the innermost label it violates.
#[test]
fn the_innermost_violated_label_is_reported() {
    let problem = Engine::new()
        .load(parse(
            "@type point(int, int) .\n@type segment(point, point) .\n\
             p(segment(point(\"a\", 1), point(1, 2))) .",
        ))
        .expect_err("the first point holds a string");
    assert_eq!(problem.code(), codes::VALUE_OUTSIDE_TYPE, "{problem:?}");
    assert_eq!(problem.field("label"), Some("point"));
}
