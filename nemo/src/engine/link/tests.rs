//! Tests of linking: modules that compute over their inputs, compose, and keep their
//! names private.

use super::{files::identity, *};

use crate::{engine::Engine, model::Program, testing::TempDirectory};

/// Links, loads and reasons over a program whose modules are given in memory,
/// under the names that their `@use`s write.
fn run(program: &str, modules: &[(&str, &str)]) -> Result<Engine, Diagnostic> {
    let mut engine = load(program, modules)?;
    engine.reason()?;
    Ok(engine)
}

/// Links and loads a program whose modules are given in memory.
fn load(program: &str, modules: &[(&str, &str)]) -> Result<Engine, Diagnostic> {
    let mut engine = Engine::new();
    for (name, text) in modules {
        engine
            .add_module(name, *text)
            .expect("the name is a module name");
    }
    let file = engine
        .add_source("main.rls", program)
        .expect("the text is small");
    let (parsed, diagnostics) = Program::parse_with_file(program, file);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let linked = engine.link(parsed)?;
    engine.load(linked)?;
    Ok(engine)
}

/// Links, loads, and reasons over a program in an engine with registered module roots and
/// texts.
fn run_in(mut engine: Engine, program: &str) -> Result<Engine, Diagnostic> {
    let file = engine
        .add_source("main.rls", program)
        .expect("the text is small");
    let (parsed, diagnostics) = Program::parse_with_file(program, file);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let linked = engine.link(parsed)?;
    engine.load(linked)?;
    engine.reason()?;
    Ok(engine)
}

/// Links a program as the file `main.rls` in a directory.
fn link_in(directory: &Path, program: &str) -> Result<Program, Diagnostic> {
    let mut engine = Engine::new();
    let file = engine
        .add_source(directory.join("main.rls").to_string_lossy(), program)
        .expect("the text is small");
    let (parsed, diagnostics) = Program::parse_with_file(program, file);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    engine.link(parsed)
}

/// Creates a temporary directory holding the given files.
fn directory_with(files: &[(&str, &str)]) -> TempDirectory {
    let directory = TempDirectory::new();
    for (path, text) in files {
        let path = directory.path().join(path);
        fs::create_dir_all(path.parent().expect("a file lies in a directory"))
            .expect("the directory is created");
        fs::write(&path, text).expect("the file is written");
    }
    directory
}

/// Returns a module over one input that passes the input to the module `name`
/// and offers what that module derives.
fn forwarding(name: &str) -> String {
    format!(
        "@input edge .\n@output reach .\n@use {name} {{ edge = edge }} as c .\n\
         reach(?x, ?y) :- c::reach(?x, ?y) ."
    )
}

/// Returns the row count of a predicate, or 0 if it has no table.
fn count(engine: &Engine, predicate: &str) -> usize {
    match engine.predicate(predicate) {
        Some(_) => engine.count(predicate).expect("the predicate has a table"),
        None => 0,
    }
}

/// A module with a name that it does not offer.
const SECRETIVE: &str = "
    @input edge .
    @output reach .
    secret(99, 99) .
    reach(?x, ?y) :- edge(?x, ?y) .
";

/// A transitive-closure module over one input.
const CLOSURE: &str = "
    @input edge .
    @output reach .
    reach(?x, ?y) :- edge(?x, ?y) .
    reach(?x, ?z) :- reach(?x, ?y), edge(?y, ?z) .
";

/// A warning about a rule of a module used twice is reported for each instance,
/// labeled with the instance's alias.
#[test]
fn a_warning_in_a_module_used_twice_names_each_instance() {
    let engine = load(
        "
        word(\"a\") . value(1) .
        @use dead { w = word, v = value } as one .
        @use dead { w = word, v = value } as two .
        ",
        &[(
            "dead",
            "@input w . @input v . @output dead .
dead(?x) :- w(?x), v(?x) .",
        )],
    )
    .expect("the program loads");
    let instances: Vec<&str> = engine
        .reports()
        .iter()
        .filter(|report| report.code() == crate::codes::NEVER_FIRES)
        .map(|report| {
            report
                .labels()
                .last()
                .expect("the warning is labeled")
                .message
                .as_str()
        })
        .collect();
    assert_eq!(
        instances,
        ["in module instance `one`", "in module instance `two`"],
        "{:?}",
        engine.reports()
    );
}

/// A reference to a name that a module does not offer fails.
#[test]
fn a_name_a_module_keeps_to_itself_cannot_be_reached() {
    let problem = run(
        "
        link(1, 2) .
        @use tc { edge = link } as t .
        answer(?x, ?y) :- t::hidden(?x, ?y) .
        ",
        &[(
            "tc",
            "
            @input edge .
            @output reach .
            hidden(?x, ?y) :- edge(?x, ?y) .
            reach(?x, ?y) :- hidden(?x, ?y) .
            ",
        )],
    )
    .expect_err("`hidden` is not offered");
    assert_eq!(problem.code(), crate::codes::NOT_OFFERED, "{problem:?}");
    assert_eq!(problem.field("module"), Some("t"));
    assert_eq!(problem.field("name"), Some("hidden"));
}

/// A program that writes a module's renamed name as an IRI, to read or to write it,
/// fails as using an owned name, even for a name the module offers.
#[test]
fn a_private_name_cannot_be_reached_by_writing_it_either() {
    // Writing the renamed name as an IRI would otherwise read what
    // the module keeps to itself and, as a fact, write to it.
    let module = "
        @input edge .
        @output reach .
        secret(99, 99) .
        reach(?x, ?y) :- edge(?x, ?y) .
    ";
    let problem = load(
        "
        link(1, 2) .
        @use tc { edge = link } as t .
        spy(?x, ?y) :- <t::secret>(?x, ?y) .
        ",
        &[("tc", module)],
    )
    .expect_err("`t::secret` is the module's name");
    assert_eq!(problem.code(), crate::codes::OWNED_NAME, "{problem:?}");
    assert_eq!(problem.field("alias"), Some("t"));

    let injected = load(
        "
        link(1, 2) .
        @use tc { edge = link } as t .
        <t::reach>(7, 7) .
        ",
        &[("tc", module)],
    )
    .expect_err("even a public name is the instantiation's to write");
    assert_eq!(injected.code(), crate::codes::OWNED_NAME, "{injected:?}");
    assert_eq!(injected.field("alias"), Some("t"));
}

/// A name that a prefix expands into a module's namespace fails as an owned name,
/// whatever the prefix is called.
#[test]
fn a_prefix_cannot_form_a_module_name_either() {
    // The check is on what a name *resolves* to: a prefix whose
    // expansion is the instance's is the same name written shorter.
    let problem = load(
        "
        @prefix t: <t::> .
        link(1, 2) .
        @use tc { edge = link } as t .
        spy(?x, ?y) :- t:secret(?x, ?y) .
        ",
        &[("tc", SECRETIVE)],
    )
    .expect_err("`t:secret` resolves to the module's name");
    assert_eq!(problem.code(), crate::codes::OWNED_NAME, "{problem:?}");
    assert_eq!(problem.field("alias"), Some("t"));

    // Nor does the prefix have to be named after the alias.
    let renamed = load(
        "
        @prefix zz: <t::> .
        link(1, 2) .
        @use tc { edge = link } as t .
        spy(?x, ?y) :- zz:secret(?x, ?y) .
        ",
        &[("tc", SECRETIVE)],
    )
    .expect_err("what it is called changes nothing");
    assert_eq!(renamed.code(), crate::codes::OWNED_NAME, "{renamed:?}");
    assert_eq!(renamed.field("alias"), Some("t"));
}

/// A `@base` inside a module's namespace makes the bare names of the program fail as owned
/// names.
#[test]
fn a_base_cannot_put_the_program_inside_a_module() {
    // A `@base` moves every bare name of the file at once, which
    // would merge the whole program into the instantiation.
    let problem = load(
        "
        @base <t::> .
        link(1, 2) .
        @use tc { edge = link } as t .
        spy(?x, ?y) :- secret(?x, ?y) .
        ",
        &[("tc", SECRETIVE)],
    )
    .expect_err("every name here resolves into the module's namespace");
    assert_eq!(problem.code(), crate::codes::OWNED_NAME, "{problem:?}");
    assert_eq!(problem.field("alias"), Some("t"));
}

/// A predicate that a `@use` binds is checked for ownership like any other name that the
/// using program writes.
#[test]
fn what_a_use_passes_in_is_owned_like_anything_else() {
    // Binding another module's input to a module's private name reads
    // that name just as surely as mentioning it in a rule.
    let problem = load(
        "
        link(1, 2) .
        @use tc { edge = link } as t .
        @use id { src = <t::secret> } as leak .
        answer(?x, ?y) :- leak::copy(?x, ?y) .
        ",
        &[
            ("tc", SECRETIVE),
            (
                "id",
                "@input src . @output copy . copy(?x, ?y) :- src(?x, ?y) .",
            ),
        ],
    )
    .expect_err("`t::secret` is the module's name wherever it is written");
    assert_eq!(problem.code(), crate::codes::OWNED_NAME, "{problem:?}");
    assert_eq!(problem.field("alias"), Some("t"));
}

/// A module parameter without a default value, which the `@use` gives no value, is an error.
#[test]
fn a_module_parameter_without_a_value_is_an_error() {
    // A module's parameters are its own, so only the `@use` can give one a value,
    // and this `@use` gives none.
    let problem = run(
        "@use needy {} as n .",
        &[(
            "needy",
            "@output p . @parameter $limit . p(?x) :- ?x = $limit .",
        )],
    )
    .expect_err("nothing can give `$limit` a value");
    assert_eq!(
        problem.code(),
        crate::codes::MODULE_PARAMETER_WITHOUT_VALUE,
        "{problem:?}"
    );
    assert_eq!(problem.field("name"), Some("limit"));
}

/// A value binding of a `@use` for a parameter that the module does not declare is an error.
#[test]
fn a_value_binding_must_name_a_declared_parameter() {
    let problem = run(
        "n(1) . @use small { number = n, $nonsense = 3 } as s .",
        &[(
            "small",
            "@input number . @output small . small(?x) :- number(?x) .",
        )],
    )
    .expect_err("the module declares no `$nonsense`");
    assert_eq!(
        problem.code(),
        crate::codes::NO_SUCH_PARAMETER,
        "{problem:?}"
    );
    assert_eq!(problem.field("name"), Some("nonsense"));
}

/// A module that declares an input twice is an error.
#[test]
fn an_input_declared_twice_is_an_error() {
    let problem = run(
        "@use two { edge = d } as t . d(1, 2) .",
        &[(
            "two",
            "@input edge .
@output r .
@input edge .
r(?x, ?y) :- edge(?x, ?y) .",
        )],
    )
    .expect_err("a module declares each input once");
    assert_eq!(problem.code(), crate::codes::DUPLICATE_INPUT, "{problem:?}");
    assert_eq!(problem.field("predicate"), Some("edge"));
}

/// A module's import is resolved against the module's directory,
/// not against the engine's import directory.
#[test]
fn a_modules_import_does_not_take_the_using_programs_base() {
    // The module writes `data.csv`. Whatever directory the program is
    // loaded against, this refers to the file next to the module.
    let mut engine = Engine::new();
    engine
        .add_module(
            "lib::loader",
            "@output rows . @import rows from csv { resource = \"data.csv\" } .",
        )
        .expect("the text is small");
    let source = "@use lib::loader {} as l .";
    let file = engine
        .add_source("main.rls", source)
        .expect("the text is small");
    let (program, diagnostics) = Program::parse_with_file(source, file);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    engine.set_import_directory("somewhere/else");
    let problem = engine
        .link(program)
        .and_then(|linked| engine.load(linked))
        .expect_err("there is no such file");
    let resource = problem
        .field("resource")
        .expect("the problem names the file");
    assert!(
        resource.contains("lib") && resource.contains("data.csv"),
        "{resource}"
    );
    assert!(
        !resource.contains("somewhere"),
        "the engine's import directory is not the module's: {resource}"
    );
}

/// A module that writes a name of a module it uses fails as using an owned name.
#[test]
fn a_module_cannot_write_the_names_of_the_module_it_uses() {
    let problem = load(
        "
        link(1, 2) .
        @use outer { source = link } as o .
        answer(?x) :- o::out(?x) .
        ",
        &[
            ("inner", "@input src . @output p . p(?x) :- src(?x, _) ."),
            (
                "outer",
                "
                @input source .
                @output out .
                @use inner { src = source } as sub .
                <sub::p>(1000) .
                out(?x) :- sub::p(?x) .
                ",
            ),
        ],
    )
    .expect_err("`sub::p` is the nested instantiation's name");
    assert_eq!(problem.code(), crate::codes::OWNED_NAME, "{problem:?}");
    assert_eq!(problem.field("alias"), Some("sub"));
}

/// The error for an owned name notes what the name, as the file writes it, resolves to,
/// unless it is written as it resolves.
#[test]
fn an_owned_name_is_quoted_as_the_file_writes_it() {
    // Inside a module, the note names the prefixed name the module wrote.
    let problem = load(
        "@use outer as o .",
        &[
            ("inner", "@output p . p(1) ."),
            (
                "outer",
                "
                @prefix s: <sub::> .
                @output out .
                @use inner as sub .
                out(?x) :- s:p(?x) .
                ",
            ),
        ],
    )
    .expect_err("`s:p` resolves to the nested instantiation's name");
    assert_eq!(problem.code(), crate::codes::OWNED_NAME, "{problem:?}");
    assert!(
        problem
            .notes()
            .iter()
            .any(|note| note == "`s:p` resolves to `sub::p`"),
        "{problem:?}"
    );

    // A name written as it resolves needs no note.
    let problem = load(
        "
        link(1, 2) .
        @use tc { edge = link } as t .
        spy(?x, ?y) :- <t::secret>(?x, ?y) .
        ",
        &[("tc", SECRETIVE)],
    )
    .expect_err("`t::secret` is the module's name");
    assert!(problem.notes().is_empty(), "{problem:?}");
}

/// A module cannot read a parameter of the using program.
#[test]
fn a_module_cannot_read_the_using_programs_globals() {
    let problem = run(
        "
        @parameter $limit = 100 .
        n(1) .
        @use leaky { number = n } as s .
        ",
        &[(
            "leaky",
            "
            @input number .
            @output small .
            small(?x) :- number(?x), ?x < $limit .
            ",
        )],
    )
    .expect_err("the module declares no `$limit` of its own");
    assert_eq!(
        problem.code(),
        crate::codes::UNDEFINED_PARAMETER,
        "{problem:?}"
    );
    assert_eq!(problem.field("name"), Some("s::limit"));
}

/// A module that uses itself is an error.
#[test]
fn a_module_that_uses_itself_is_an_error() {
    let problem = run(
        "
        d(1) .
        @use loop { x = d } as l .
        ",
        &[(
            "loop",
            "
            @input x .
            @output p .
            @use loop { x = q } as self .
            p(?a) :- self::p(?a) .
            q(1) .
            ",
        )],
    )
    .expect_err("the module uses itself");
    assert_eq!(problem.code(), crate::codes::USED_IN_CYCLE, "{problem:?}");
    assert_eq!(problem.field("module"), Some("loop"));
}

/// An input bound to the module's own offer of that input is an error.
#[test]
fn an_input_bound_to_its_own_offer_is_a_cycle() {
    // The module offers its input, which the `@use` binds to that very offer,
    // so following the name never reaches a predicate.
    let problem = run(
        "
        @use pass { inp = a::inp } as a .
        q(?x) :- a::inp(?x) .
        ",
        &[("pass", "@input inp . @output inp .")],
    )
    .expect_err("the binding leads back to itself");
    assert_eq!(problem.code(), crate::codes::PASSED_IN_CYCLE, "{problem:?}");
    assert_eq!(problem.field("module"), Some("a"));
    assert_eq!(problem.field("name"), Some("inp"));
}

/// Two instances whose inputs are bound to each other's offers are an error.
#[test]
fn inputs_bound_to_each_others_offers_are_a_cycle() {
    let problem = run(
        "
        @use pass { inp = b::inp } as a .
        @use pass { inp = a::inp } as b .
        q(?x) :- a::inp(?x) .
        ",
        &[("pass", "@input inp . @output inp .")],
    )
    .expect_err("the bindings lead back to each other");
    assert_eq!(problem.code(), crate::codes::PASSED_IN_CYCLE, "{problem:?}");
}

/// A `@use` that leaves an input of the module unbound is an error.
#[test]
fn every_parameter_must_be_bound() {
    let problem = run("@use tc {} as t .", &[("tc", CLOSURE)]).expect_err("`edge` is unbound");
    assert_eq!(problem.code(), crate::codes::INPUTS_UNBOUND, "{problem:?}");
    assert_eq!(problem.field("inputs"), Some("`edge`"));
}

/// A qualified name in a program without a `@use` is reported as a name without modules.
#[test]
fn a_reference_to_no_module_is_reported_as_one() {
    // Without a `@use`, linking changes nothing,
    // and translation is what finds the name.
    let problem = run("answer(?x) :- nope::p(?x) .", &[]).expect_err("there is no module `nope`");
    assert_eq!(
        problem.code(),
        crate::codes::QUALIFIED_WITHOUT_MODULES,
        "{problem:?}"
    );
    assert_eq!(problem.field("module"), Some("nope"));
}

/// A reference to an alias that the program does not declare is an error that lists the
/// aliases in use.
#[test]
fn a_reference_to_an_alias_the_program_does_not_have_is_reported() {
    let problem = run(
        "
        link(1, 2) .
        @use tc { edge = link } as t .
        answer(?x, ?y) :- other::reach(?x, ?y) .
        ",
        &[("tc", CLOSURE)],
    )
    .expect_err("there is no module `other`");
    assert_eq!(problem.code(), crate::codes::UNKNOWN_MODULE, "{problem:?}");
    assert_eq!(problem.field("module"), Some("other"));
    assert!(
        problem
            .notes()
            .iter()
            .any(|note| note.contains("the aliases in use are: t")),
        "{problem:?}"
    );
}

/// A module text registered with [add_module](Engine::add_module) is read instead of a file.
#[test]
fn a_module_registered_in_memory_is_used_instead_of_the_disk() {
    // Nothing is on disk here at all.
    let engine = run(
        "
        link(1, 2) . link(2, 3) .
        @use nowhere::tc { edge = link } as t .
        ",
        &[("nowhere::tc", CLOSURE)],
    )
    .expect("the module came from memory");
    assert_eq!(count(&engine, "t::reach"), 3);
}

/// The name of a module file drops `.` steps and separates the parts by `/`.
#[test]
fn the_identity_of_a_path_ignores_the_steps_that_lead_nowhere() {
    assert_eq!(identity(Path::new("./tc.rls")), "tc.rls");
    assert_eq!(
        identity(&PathBuf::from(".").join("lib/tc.rls")),
        "lib/tc.rls"
    );
    // A rooted path keeps exactly one separator after its root.
    assert_eq!(identity(Path::new("/usr/lib/tc.rls")), "/usr/lib/tc.rls");
    #[cfg(windows)]
    assert_eq!(
        identity(Path::new(r"C:\tmp\lib\tc.rls")),
        "C:/tmp/lib/tc.rls"
    );
}

/// A module's import of a relative resource reads the file next to the module.
#[test]
fn a_modules_import_reads_from_where_the_module_is() {
    let mut engine = Engine::new();
    engine
        .add_module(
            "lib::loader",
            "@output rows . @import rows from csv { resource = \"data.csv\" } .",
        )
        .expect("the text is small");
    let source = "@use lib::loader {} as l .\nseen(?x) :- l::rows(?x) .";
    let file = engine
        .add_source("main.rls", source)
        .expect("the text is small");
    let (program, diagnostics) = Program::parse_with_file(source, file);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let problem = engine
        .link(program)
        .and_then(|linked| engine.load(linked))
        .expect_err("there is no such file");
    // What matters is *which* path it looked for.
    assert_eq!(problem.code(), crate::codes::UNREADABLE, "{problem:?}");
    assert!(
        problem
            .field("resource")
            .is_some_and(|resource| resource.ends_with("lib/data.csv")),
        "{problem:?}"
    );
}

/// A chain of modules deeper than [MAX_DEPTH] is an error.
#[test]
fn a_module_chain_deeper_than_the_limit_is_an_error() {
    let mut engine = Engine::new();
    for level in 0..MAX_DEPTH + 2 {
        engine
            .add_module(
                &format!("m{level}"),
                format!(
                    "@output p . @use m{} {{}} as n .\np(?x) :- n::p(?x) .",
                    level + 1
                ),
            )
            .expect("the text is small");
    }
    let source = "@use m0 {} as m .";
    let file = engine
        .add_source("main.rls", source)
        .expect("the text is small");
    let (program, diagnostics) = Program::parse_with_file(source, file);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let problem = engine.link(program).expect_err("the chain is too deep");
    assert_eq!(
        problem.code(),
        crate::codes::MODULES_TOO_DEEP,
        "{problem:?}"
    );
}

/// A name that a module offers but never writes is reported with a warning.
#[test]
fn a_public_name_the_module_never_writes_is_reported() {
    let engine = run(
        "
        @use promises {} as g .
        answer(?x) :- g::real(?x) .
        ",
        &[("promises", "@output real, ghost . real(1) .")],
    )
    .expect("the program still loads");
    assert!(
        engine
            .reports()
            .iter()
            .any(|report| report.code() == crate::codes::OFFERS_UNWRITTEN
                && report.field("names") == Some("`ghost`")),
        "{:?}",
        engine.reports()
    );
}

/// A name that a module offers but only reads is reported as never written.
#[test]
fn a_public_name_the_module_only_reads_is_reported() {
    // Reading a predicate does not fill it.
    let engine = run(
        "
        @use echo {} as e .
        answer(?x) :- e::copy(?x) .
        ",
        &[("echo", "@output copy, echo . copy(?x) :- echo(?x) .")],
    )
    .expect("the program still loads");
    assert!(
        engine
            .reports()
            .iter()
            .any(|report| report.code() == crate::codes::OFFERS_UNWRITTEN
                && report.field("names") == Some("`echo`")),
        "{:?}",
        engine.reports()
    );
}

/// The names that a module used twice offers but never writes are reported once.
#[test]
fn a_module_used_twice_reports_its_unwritten_names_once() {
    let engine = run(
        "
        @use promises {} as g .
        @use promises {} as h .
        ",
        &[("promises", "@output real, ghost . real(1) .")],
    )
    .expect("the program still loads");
    let reports = engine
        .reports()
        .iter()
        .filter(|report| report.code() == crate::codes::OFFERS_UNWRITTEN)
        .count();
    assert_eq!(reports, 1, "{:?}", engine.reports());
}

/// The error for unbound inputs names them as a `@use` binds them, not as they resolve.
#[test]
fn inputs_are_listed_as_a_use_binds_them() {
    // The module's base makes `edge` an IRI, but a `@use` still binds it as `edge`.
    let problem = run(
        "@use based {} as b .",
        &[(
            "based",
            "@base <https://example.org/> . @input edge . @output reach . \
             reach(?x) :- edge(?x) .",
        )],
    )
    .expect_err("`edge` is unbound");
    assert_eq!(problem.code(), crate::codes::INPUTS_UNBOUND, "{problem:?}");
    assert_eq!(problem.field("inputs"), Some("`edge`"));
}

/// A module that declares two base IRIs is an error.
#[test]
fn a_module_with_two_base_iris_is_an_error() {
    let problem = load(
        "@use based {} as b .",
        &[(
            "based",
            "@base <https://example.org/> . @base <https://example.com/> . @output p . p(1) .",
        )],
    )
    .expect_err("a file declares one base IRI");
    assert_eq!(
        problem.code(),
        crate::codes::CONFLICTING_BASE,
        "{problem:?}"
    );
}

/// Loading a program that is not linked fails, with the `@use` labeled.
#[test]
fn an_unlinked_program_is_an_error() {
    let mut engine = Engine::new();
    engine.add_module("tc", CLOSURE).expect("the text is small");
    let source = "link(1, 2) .\n@use tc { edge = link } as t .";
    let file = engine
        .add_source("main.rls", source)
        .expect("the text is small");
    let (program, diagnostics) = Program::parse_with_file(source, file);
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    let problem = engine.load(program).expect_err("the program is not linked");
    assert_eq!(problem.code(), crate::codes::NOT_LINKED);
    assert_eq!(problem.labels().len(), 1, "the `@use` is labeled");
}

/// Linking returns a program without a `@use` unchanged.
#[test]
fn a_program_without_modules_links_to_itself() {
    let mut engine = Engine::new();
    let (program, _) = Program::parse("p(1) . q(?x) :- p(?x) .");
    let linked = engine.link(program.clone()).expect("nothing to link");
    assert_eq!(linked, program);
}

/// A warning about a rule of a nested module is labeled with every enclosing instance.
#[test]
fn a_warning_in_a_nested_module_names_every_instance() {
    let engine = load(
        "
        word(\"a\") . value(1) .
        @use outer { w = word, v = value } as one .
        ",
        &[
            (
                "outer",
                "@input w . @input v . @output dead .
@use dead { w = w, v = v } as two .
dead(?x) :- two::dead(?x) .",
            ),
            (
                "dead",
                "@input w . @input v . @output dead .
dead(?x) :- w(?x), v(?x) .",
            ),
        ],
    )
    .expect("the program loads");
    let report = engine
        .reports()
        .iter()
        .find(|report| report.code() == crate::codes::NEVER_FIRES)
        .expect("the inner rule never fires");
    let labels: Vec<&str> = report
        .labels()
        .iter()
        .map(|label| label.message.as_str())
        .filter(|message| message.starts_with("in module instance"))
        .collect();
    assert_eq!(
        labels,
        ["in module instance `one`", "in module instance `two`"],
        "{report:?}"
    );
}

/// The error for a missing module with a hyphen in its name notes the file looked for.
#[test]
fn a_missing_hyphenated_module_is_named_by_its_file() {
    let problem = run("@use my-lib {} as m .", &[]).expect_err("there is no module");
    assert!(
        problem
            .notes()
            .iter()
            .any(|note| note == "looked for `my-lib.rls`"),
        "{problem:?}"
    );
}

/// Outside a module root, `super` can lead above the program's directory.
#[test]
fn super_can_lead_above_the_programs_directory() {
    // Within the program's tree nothing bounds `super`, so a module
    // checked on its own links as it does when used.
    let engine = run(
        "
        link(1, 2) .
        @use super::shared::tc { edge = link } as t .
        ",
        &[("super::shared::tc", CLOSURE)],
    )
    .expect("the module links");
    assert_eq!(count(&engine, "t::reach"), 1);
}

/// A module root's modules can be named from a module in another directory.
#[test]
fn a_root_names_its_modules_from_anywhere() {
    let mut engine = Engine::new();
    engine
        .add_module_root("shared", "nowhere")
        .expect("`shared` names a root");
    engine
        .add_module("shared::closure", CLOSURE)
        .expect("the name is a module name");
    engine
        .add_module("lib::tc", forwarding("shared::closure"))
        .expect("the name is a module name");
    let engine = run_in(
        engine,
        "link(1, 2) . link(2, 3) .\n@use lib::tc { edge = link } as t .",
    )
    .expect("the modules link");
    assert_eq!(count(&engine, "t::reach"), 3);
}

/// A module name of a single part names the module next to the program, even if it is the
/// name of a root.
#[test]
fn a_single_name_never_names_a_root() {
    let mut engine = Engine::new();
    engine
        .add_module_root("shared", "nowhere")
        .expect("`shared` names a root");
    engine
        .add_module("shared", "@output p . p(1) .")
        .expect("the name is a module name");
    let engine = run_in(engine, "@use shared {} as s .\nq(?x) :- s::p(?x) .")
        .expect("`shared` is the module next to the program");
    assert_eq!(count(&engine, "q"), 1);
}

/// A `super` step that leads out of a module root is an error.
#[test]
fn super_cannot_lead_out_of_a_root() {
    let mut engine = Engine::new();
    engine
        .add_module_root("shared", "nowhere")
        .expect("`shared` names a root");
    engine
        .add_module(
            "shared::tc",
            "@output p . @use super::outside {} as o . p(?x) :- o::p(?x) .",
        )
        .expect("the name is a module name");
    let problem = run_in(engine, "@use shared::tc {} as t .").expect_err("`super` leaves the root");
    assert_eq!(problem.code(), crate::codes::LEAVES_ROOT, "{problem:?}");
    assert_eq!(problem.field("module"), Some("super::outside"));
    assert_eq!(problem.field("root"), Some("shared"));
}

/// A module name that begins with a root is an error where a directory of the root's name
/// lies next to the `@use`.
#[test]
fn a_root_and_a_directory_of_its_name_are_ambiguous() {
    let mut engine = Engine::new();
    engine
        .add_module_root("shared", "nowhere")
        .expect("`shared` names a root");
    for (name, text) in [
        ("shared::closure", CLOSURE.to_owned()),
        ("lib::shared::closure", CLOSURE.to_owned()),
        ("lib::tc", forwarding("shared::closure")),
    ] {
        engine
            .add_module(name, text)
            .expect("the name is a module name");
    }
    let problem = run_in(engine, "link(1, 2) .\n@use lib::tc { edge = link } as t .")
        .expect_err("`shared` is the root and a directory next to `lib/tc.rls`");
    assert_eq!(problem.code(), crate::codes::AMBIGUOUS_ROOT, "{problem:?}");
    assert_eq!(problem.field("root"), Some("shared"));
}

/// A module root must be named by one plain name other than `super`,
/// and a registered module text by a module name.
#[test]
fn a_root_is_named_by_one_plain_name() {
    let mut engine = Engine::new();
    for name in ["a::b", "super", "super::a", "", "\"a\""] {
        let problem = engine
            .add_module_root(name, "nowhere")
            .expect_err("it is not a root name");
        assert_eq!(problem.code(), crate::codes::NOT_A_ROOT_NAME, "{name:?}");
    }
    let problem = engine
        .add_module("lib/tc.rls", CLOSURE)
        .expect_err("a path is not a module name");
    assert_eq!(problem.code(), crate::codes::EXPECTED, "{problem:?}");
}

/// Registering a module text again under the same name replaces the text.
#[test]
fn registering_a_module_again_replaces_its_text() {
    let mut engine = Engine::new();
    engine
        .add_module("constants", "@output pi . pi(3) .")
        .expect("the name is a module name");
    engine
        .add_module("constants", "@output pi . pi(3) . pi(4) .")
        .expect("the name is a module name");
    let engine = run_in(
        engine,
        "@use constants {} as c .\nanswer(?x) :- c::pi(?x) .",
    )
    .expect("the program loads");
    assert_eq!(count(&engine, "answer"), 2);
}

/// The error for a missing module names it as written and notes the path looked for.
#[test]
fn a_missing_module_is_named_as_written_and_located_by_its_path() {
    let problem = run("@use lib::missing {} as m .", &[]).expect_err("there is no module");
    assert_eq!(problem.code(), crate::codes::NO_SUCH_MODULE, "{problem:?}");
    assert_eq!(problem.field("module"), Some("lib::missing"));
    assert!(
        problem
            .notes()
            .iter()
            .any(|note| note == "looked for `lib/missing.rls`"),
        "{problem:?}"
    );
}

/// A module name finds a file only if every part matches a directory or the file exactly,
/// case included, and a directory is not a module.
#[test]
fn module_names_match_file_names_exactly() {
    let temporary = directory_with(&[("lib/tc.rls", CLOSURE), ("graph/x.rls", "")]);
    let directory = temporary.path();

    // A part differing in case names no module, whatever the filesystem.
    for (name, found) in [("lib::Tc", "`tc.rls`"), ("Lib::tc", "`lib`")] {
        let problem = link_in(
            directory,
            &format!("link(1, 2) .\n@use {name} {{ edge = link }} as t ."),
        )
        .expect_err(name);
        assert_eq!(problem.code(), crate::codes::NO_SUCH_MODULE, "{problem:?}");
        assert!(
            problem.notes().iter().any(|note| note.contains(found)),
            "{problem:?}"
        );
    }

    // Named exactly, the module is found.
    link_in(
        directory,
        "link(1, 2) .\n@use lib::tc { edge = link } as t .",
    )
    .expect("the module is there");

    // A directory is no module, and the note says how to name one in it.
    let problem = link_in(directory, "@use graph {} as g .").expect_err("it is a directory");
    assert_eq!(problem.code(), crate::codes::NO_SUCH_MODULE, "{problem:?}");
    assert!(
        problem
            .notes()
            .iter()
            .any(|note| note.contains("`graph::name`")),
        "{problem:?}"
    );
}

/// A module root reads its modules from its directory on disk.
#[test]
fn a_root_reads_its_modules_from_its_directory() {
    let library = directory_with(&[
        ("closure.rls", CLOSURE),
        ("graph/tc.rls", &forwarding("super::closure")),
    ]);
    let program = directory_with(&[]);
    let link = |source: &str| {
        let mut engine = Engine::new();
        engine
            .add_module_root("shared", library.path())
            .expect("`shared` names a root");
        let file = engine
            .add_source(program.path().join("main.rls").to_string_lossy(), source)
            .expect("the text is small");
        let (parsed, diagnostics) = Program::parse_with_file(source, file);
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        engine.link(parsed)
    };
    let source = "link(1, 2) .\n@use shared::graph::tc { edge = link } as t .";
    link(source).expect("the root's modules link");

    // A directory of the root's name next to the program makes the name ambiguous.
    fs::create_dir_all(program.path().join("shared")).expect("the directory is created");
    let problem = link(source).expect_err("`shared` is a directory here too");
    assert_eq!(problem.code(), crate::codes::AMBIGUOUS_ROOT, "{problem:?}");
}

/// The modules of a program whose text is not registered get file ids of their own,
/// so that each `@use` of the program is resolved against the working directory.
#[test]
fn an_unregistered_program_shares_no_file_id_with_its_modules() {
    let mut engine = Engine::new();
    for name in ["sub::a", "sub::b"] {
        engine
            .add_module(name, CLOSURE)
            .expect("the name is a module name");
    }
    // The program is parsed with the file id 0, and its text is not registered.
    let (parsed, diagnostics) = Program::parse(
        "link(1, 2) .\n@use sub::a { edge = link } as x .\n@use sub::b { edge = link } as y .",
    );
    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    engine.link(parsed).expect("the modules link");

    let names: Vec<&str> = engine.sources().map(|(_, name, _)| name).collect();
    assert!(names.contains(&"sub/a.rls"), "{names:?}");
    assert!(names.contains(&"sub/b.rls"), "{names:?}");
    assert_eq!(engine.file_name(FileId::new(0)), None);
}
