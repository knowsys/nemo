use assert_cmd::prelude::*; // Add methods on commands
use assert_fs::prelude::*;
use predicates::prelude::*;
use stage2::physical::tabular::table_types::trie::Trie; // Used for writing assertions
use std::{fs::read_to_string, path::PathBuf, process::Command}; // Run programs
use test_log::test;

#[cfg_attr(miri, ignore)]
#[test]
fn cli_argument_parsing() -> Result<(), Box<dyn std::error::Error>> {
    let bin = "stage2";
    let mut cmd = Command::cargo_bin(bin)?;
    cmd.arg("-vvv").arg("Non-existing-file.rls");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("No such file or directory"));

    cmd = Command::cargo_bin(bin)?;
    cmd.arg("-h");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Print help"));

    cmd = Command::cargo_bin(bin)?;
    cmd.arg("--version");
    cmd.assert().success().stdout(predicate::str::contains(bin));

    cmd = Command::cargo_bin(bin)?;
    cmd.arg("-v").arg("-q");
    cmd.assert().failure().stderr(predicate::str::contains(
        "argument '--verbose...' cannot be used with '--quiet'",
    ));

    cmd = Command::cargo_bin(bin)?;
    cmd.arg("-v").arg("-q");
    cmd.assert().failure().stderr(predicate::str::contains(
        "argument '--verbose...' cannot be used with '--quiet'",
    ));

    cmd = Command::cargo_bin(bin)?;
    cmd.arg("-v").arg("--log").arg("error");
    cmd.assert().failure().stderr(predicate::str::contains(
        "argument '--verbose...' cannot be used with '--log <LOG_LEVEL>'",
    ));

    cmd = Command::cargo_bin(bin)?;
    cmd.arg("--log").arg("cats");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("'--log <LOG_LEVEL>'"));
    Ok(())
}

struct Source {
    name: String,
    arity: usize,
    content: String,
}

impl Source {
    fn new(name: &str, arity: usize, content: &str) -> Self {
        Self {
            name: String::from(name),
            arity,
            content: String::from(content),
        }
    }

    fn to_source_statment(&self, path: &str) -> String {
        format!(
            "@source {}[{}]: load-csv(\"{}\").\n",
            self.name, self.arity, path
        )
    }
}

struct Target {
    name: String,
    content: String,
}

impl Target {
    fn new(name: &str, content: &str) -> Self {
        Self {
            name: String::from(name),
            content: String::from(content),
        }
    }
}

fn run_test(
    sources: Vec<Source>,
    rules: &str,
    targets: Vec<Target>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("stage2")?;

    let output_directory = assert_fs::TempDir::new()?;

    let rule_file = assert_fs::NamedTempFile::new("rule_file.rls")?;
    let mut rule_content = String::new();
    let mut source_files = Vec::new();

    for source in &sources {
        let csv_file = assert_fs::NamedTempFile::new(format!("{}.csv", source.name))?;
        csv_file.write_str(&source.content)?;
        let csv_path = csv_file.path().as_os_str().to_str().unwrap();

        rule_content += &source.to_source_statment(csv_path);

        source_files.push(csv_file);
    }

    rule_content += rules;
    rule_file.write_str(&rule_content)?;

    cmd.arg("-vvv")
        .arg("-s")
        .arg("-o")
        .arg(output_directory.path())
        .arg(rule_file.path());
    cmd.assert().success();

    for target in targets {
        let target_file = PathBuf::from(
            format!(
                "{}/{}.csv",
                output_directory
                    .to_path_buf()
                    .into_os_string()
                    .to_str()
                    .unwrap(),
                target.name
            )
            .as_str(),
        );

        let target_computed = read_to_string(target_file)?;
        assert_eq!(target_computed, target.content);
    }

    Ok(())
}

#[cfg_attr(miri, ignore)]
#[test]
fn reasoning_symmetry_transitive_closure() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("stage2")?;

    let rule_file = assert_fs::NamedTempFile::new("rule_file.rls")?;
    let csv_file_1 = assert_fs::NamedTempFile::new("csv1.csv")?;
    let csv_file_2 = assert_fs::NamedTempFile::new("csv2.csv")?;
    let output_directory = assert_fs::TempDir::new()?;

    let csv_path_1 = csv_file_1.path().as_os_str().to_str().unwrap();
    let csv_path_2 = csv_file_2.path().as_os_str().to_str().unwrap();
    let rules = "connected(?X,?Y) :- city(?X), city(?Y), conn(?X,?Y).\nconn(?X,?Y) :- conn(?Y,?X).\nconnected(?X,?Y) :- city(?X), city(?XY), city(?Y), connected(?X,?XY), conn(?XY, ?Y).\n";
    let rule_content = format!(
        "@source city[1]: load-csv(\"{csv_path_1}\"). \n@source conn[2]: load-csv(\"{csv_path_2}\"). \n{rules}"
    );
    println!("{rule_content}");
    rule_file.write_str(&rule_content)?;
    csv_file_1.write_str("Vienna\nBerlin\nParis\nBasel\nRome")?;
    csv_file_2.write_str("Vienna,Berlin\nVienna,Rome\nVienna,Zurich\nBerlin,Paris")?;

    cmd.arg("-vvv")
        .arg("-s")
        .arg("-o")
        .arg(output_directory.path())
        .arg(rule_file.path());
    cmd.assert().success();

    let outputfile = PathBuf::from(
        format!(
            "{}/connected.csv",
            output_directory
                .to_path_buf()
                .into_os_string()
                .to_str()
                .unwrap()
        )
        .as_str(),
    );
    output_directory.read_dir()?.for_each(|file| {
        let file = file.unwrap();
        println!("{}", file.file_name().to_str().unwrap());
        println!("{}", read_to_string(file.path()).unwrap())
    });
    let result = read_to_string(outputfile)?;
    println!("{result}");
    let lines = result.trim().split('\n');
    let mut expected_results = vec![
        "Vienna,Vienna",
        "Vienna,Berlin",
        "Vienna,Paris",
        "Vienna,Rome",
        "Berlin,Vienna",
        "Berlin,Berlin",
        "Berlin,Paris",
        "Berlin,Rome",
        "Paris,Vienna",
        "Paris,Berlin",
        "Paris,Paris",
        "Paris,Rome",
        "Rome,Vienna",
        "Rome,Berlin",
        "Rome,Paris",
        "Rome,Rome",
    ];

    lines.clone().for_each(|line| {
        assert!(expected_results.contains(&line));
        println!(
            "{line}\t{}\t{:?}",
            expected_results[0],
            line.eq(expected_results[0])
        );
        let index = expected_results
            .iter()
            .position(|&elem| elem.eq(line))
            .expect("Result should exist");
        expected_results.remove(index);
    });

    assert!(expected_results.is_empty());
    assert_eq!(lines.count(), 16);
    Ok(())
}

#[cfg_attr(miri, ignore)]
#[test]
fn test_datalog_basic_project() -> Result<(), Box<dyn std::error::Error>> {
    let sources = vec![Source::new(
        "source",
        3,
        "A,B,C\n\
        B,B,D\n\
        A,E,F\n\
        C,D,D\n",
    )];

    let rules = "\
        A(?X, ?Z) :- source(?X, ?Y, ?Z) .\n\
        B(?Y, ?X) :- A(?X, ?Y) .\n\
        C(?Y) :- B(?VariableThatIsNotNeeded, ?Y) .\n\
        D(?Y, ?Z) :- source(?X, ?Y, ?Z) .\n\
        E(?F, ?E) :- D(?E, ?F) .\
    ";

    let targets = vec![
        Target::new(
            "A",
            "A,C\n\
            A,F\n\
            B,D\n\
            C,D\n",
        ),
        Target::new(
            "B",
            "C,A\n\
            D,B\n\
            D,C\n\
            F,A\n",
        ),
        Target::new(
            "C",
            "A\n\
            B\n\
            C\n",
        ),
        Target::new(
            "D",
            "B,C\n\
            B,D\n\
            D,D\n\
            E,F\n",
        ),
        Target::new(
            "E",
            "C,B\n\
            D,B\n\
            D,D\n\
            F,E\n",
        ),
    ];

    run_test(sources, rules, targets)
}

#[cfg_attr(miri, ignore)]
#[test]
fn test_datalog_basic_union() -> Result<(), Box<dyn std::error::Error>> {
    let sources = vec![
        Source::new(
            "sourceA",
            3,
            "A,B,C\n\
            B,B,D\n\
            A,E,F\n\
            C,D,D\n",
        ),
        Source::new(
            "sourceB",
            3,
            "B,B,D\n\
            A,C,F\n\
            A,G,Q\n\
            Q,Q,A\n\
            C,D,D\n",
        ),
        Source::new(
            "sourceC",
            3,
            "Q,Q,A\n\
            Z,Z,Z\n",
        ),
    ];

    let rules = "\
        U(?X, ?Y, ?Z) :- sourceA(?X, ?Y, ?Z) .\n\
        U(?X, ?Y, ?Z) :- sourceB(?X, ?Y, ?Z) .\n\
        U(?X, ?Y, ?Z) :- sourceC(?X, ?Y, ?Z) .\n\
        u(?X, ?Y, ?Z) :- sourceA(?X, ?Y, ?Z) .\n\
        u(?X, ?Y, ?Z) :- sourceB(?X, ?Y, ?Z) .\n\
    ";

    let targets = vec![
        Target::new(
            "U",
            "A,B,C\n\
            A,C,F\n\
            A,E,F\n\
            A,G,Q\n\
            B,B,D\n\
            C,D,D\n\
            Q,Q,A\n\
            Z,Z,Z\n",
        ),
        Target::new(
            "u",
            "A,B,C\n\
            A,C,F\n\
            A,E,F\n\
            A,G,Q\n\
            B,B,D\n\
            C,D,D\n\
            Q,Q,A\n",
        ),
    ];

    run_test(sources, rules, targets)
}

#[cfg_attr(miri, ignore)]
#[test]
fn test_datalog_basic_join() -> Result<(), Box<dyn std::error::Error>> {
    let sources = vec![
        Source::new(
            "sourceA",
            3,
            "A,B,C\n\
            B,B,D\n\
            A,E,F\n\
            C,D,D\n",
        ),
        Source::new(
            "sourceB",
            3,
            "B,B,D\n\
            B,D,A\n\
            A,C,F\n\
            A,G,Q\n\
            Q,Q,A\n\
            C,D,D\n",
        ),
        Source::new(
            "sourceC",
            3,
            "Q,Q,A\n\
            Z,Z,Z\n\
            F,E,D\n\
            D,B,Q\n\
            D,B,Z\n\
            D,B,F\n",
        ),
    ];

    let rules = "\
        J1(?X, ?Y, ?Z) :- sourceA(?X, ?Z, ?Y), sourceB(?X, ?Y, ?T) .\n\
        J2(?X, ?Y, ?Z) :- sourceA(?Z, ?Y, ?X), sourceC(?X, ?Y, ?T) .\n\
        J3(?X, ?Y, ?W) :- sourceA(?T, ?Y, ?X), sourceB(?T, ?Y, ?X), sourceC(?X, ?Y, ?W) .\n\
    ";

    let targets = vec![
        Target::new(
            "J1",
            "A,C,B\n\
            B,D,B\n\
            C,D,D\n",
        ),
        Target::new(
            "J2",
            "D,B,B\n\
            F,E,A\n",
        ),
        Target::new(
            "J3",
            "D,B,F\n\
            D,B,Q\n\
            D,B,Z\n",
        ),
    ];

    run_test(sources, rules, targets)
}

#[cfg_attr(miri, ignore)]
#[test]
fn test_datalog_repeat_vars() -> Result<(), Box<dyn std::error::Error>> {
    let sources = vec![
        Source::new(
            "sourceA",
            3,
            "A,B,C\n\
            B,B,D\n\
            A,E,F\n\
            C,D,D\n",
        ),
        Source::new(
            "sourceB",
            3,
            "B,B,D\n\
            B,D,A\n\
            A,C,F\n\
            A,G,Q\n\
            A,Q,Q\n\
            C,D,D\n",
        ),
        Source::new(
            "sourceC",
            3,
            "Q,Q,A\n\
            Z,Z,Z\n\
            F,E,D\n\
            D,B,Q\n\
            D,B,Z\n\
            D,B,F\n",
        ),
    ];

    let rules = "\
        RepeatBody(?R, ?S) :- sourceA(?X, ?X, ?R), sourceB(?S, ?Y, ?Y) .\n\
        RepeatHead(?X, ?Y, ?X, ?Y, ?Z, ?Z, ?X) :- sourceA(?X, ?Z, ?Y), sourceB(?X, ?Y, ?T) .\n
        RepeatAll(?X, ?X, ?X, ?X) :- sourceC(?X, ?X, ?X) .
        RepeatAlternative(?R, ?S) :- sourceA(?R, ?X, ?X), sourceB(?S, ?Y, ?Y) .\n\
    ";

    let targets = vec![
        Target::new(
            "RepeatBody",
            "D,A\n\
            D,C\n",
        ),
        Target::new(
            "RepeatAlternative",
            "C,A\n\
            C,C\n",
        ),
        Target::new("RepeatAll", "Z,Z,Z,Z\n"),
        Target::new(
            "RepeatHead",
            "A,C,A,C,B,B,A\n\
            B,D,B,D,B,B,B\n\
            C,D,C,D,D,D,C\n",
        ),
    ];

    run_test(sources, rules, targets)
}

#[cfg_attr(miri, ignore)]
#[test]
fn test_datalog_constants() -> Result<(), Box<dyn std::error::Error>> {
    let sources = vec![
        Source::new(
            "sourceA",
            3,
            "<A>,<B>,<C>\n\
            <B>,<B>,<D>\n\
            <A>,<E>,<F>\n\
            <C>,<D>,<D>\n",
        ),
        Source::new(
            "sourceB",
            3,
            "<B>,<B>,<D>\n\
            <B>,<D>,<A>\n\
            <A>,<C>,<F>\n\
            <A>,<G>,<Q>\n\
            <A>,<Q>,<Q>\n\
            <C>,<D>,<D>\n\
            <C>,<D>,<A>\n",
        ),
    ];

    let rules = "\
        ConstantBodyXY(?X, ?Y) :- sourceA(?X, ?Y, D) .\n\
        ConstantBodyYZ(?Y, ?Z) :- sourceA(A, ?Y, ?Z) .\n\
        ConstantBodyXZ(?X, ?Z) :- sourceA(?X, B, ?Z) .\n

        ConstantBodyX(?X) :- sourceB(?X, D, A) .\n\
        ConstantBodyY(?Y) :- sourceB(A, ?Y, Q) .\n\
        ConstantBodyZ(?Z) :- sourceB(C, D, ?Z) .\n

        Exist(?X, ?Y) :- sourceA(?X, ?X, ?Y), sourceB(A, Q, Q).\n\
        NotExist(?X, ?Y) :- sourceA(?X, ?X, ?Y), sourceB(D, D, D).\n

        ConstantHeadAfter(?X, ?Y, A, B, Z) :- sourceA(?X, ?Y, ?I) .\n\
        ConstantHeadBefore(A, Z, B, ?X, ?Y) :- sourceA(?X, ?I, ?Y) .\n\
        ConstantHeadEverywhere(A, B, ?X, ?Z, ?X, C, ?Y, E, F) :- sourceA(?X, ?Y, ?Z) .\n

        ConstantBodyHead(Q, ?Y, A, B, ?X, Z) :- sourceA(A, ?X, ?Y), sourceB(?X, D, A) .\n
    ";

    let targets = vec![
        Target::new(
            "ConstantBodyXY",
            "<B>,<B>\n\
            <C>,<D>\n",
        ),
        Target::new(
            "ConstantBodyYZ",
            "<B>,<C>\n\
            <E>,<F>\n",
        ),
        Target::new(
            "ConstantBodyXZ",
            "<A>,<C>\n\
            <B>,<D>\n",
        ),
        Target::new(
            "ConstantBodyX",
            "<B>\n\
            <C>\n",
        ),
        Target::new(
            "ConstantBodyY",
            "<G>\n\
            <Q>\n",
        ),
        Target::new(
            "ConstantBodyZ",
            "<A>\n\
            <D>\n",
        ),
        Target::new("Exist", "<B>,<D>\n"),
        Target::new("NotExist", ""),
        Target::new(
            "ConstantHeadAfter",
            "<A>,<B>,<A>,<B>,<Z>\n\
            <A>,<E>,<A>,<B>,<Z>\n\
            <B>,<B>,<A>,<B>,<Z>\n\
            <C>,<D>,<A>,<B>,<Z>\n",
        ),
        Target::new(
            "ConstantHeadBefore",
            "<A>,<Z>,<B>,<A>,<C>\n\
            <A>,<Z>,<B>,<A>,<F>\n\
            <A>,<Z>,<B>,<B>,<D>\n\
            <A>,<Z>,<B>,<C>,<D>\n",
        ),
        Target::new(
            "ConstantHeadEverywhere",
            "<A>,<B>,<A>,<C>,<A>,<C>,<B>,<E>,<F>\n\
            <A>,<B>,<A>,<F>,<A>,<C>,<E>,<E>,<F>\n\
            <A>,<B>,<B>,<D>,<B>,<C>,<B>,<E>,<F>\n\
            <A>,<B>,<C>,<D>,<C>,<C>,<D>,<E>,<F>\n",
        ),
        Target::new("ConstantBodyHead", "<Q>,<C>,<A>,<B>,<B>,<Z>\n"),
    ];

    run_test(sources, rules, targets)
}

struct NullPrinter {
    current_null: u64,
}

impl NullPrinter {
    fn new() -> Self {
        Self {
            current_null: 1 << 63,
        }
    }

    fn print_next(&mut self) -> String {
        self.current_null += 1;

        Trie::format_null(self.current_null)
    }

    fn print_back(&mut self, back: u64) -> String {
        self.current_null -= back;

        self.print_next()
    }

    fn print_skip(&mut self, skip: u64) -> String {
        self.current_null += skip;

        self.print_next()
    }
}

#[cfg_attr(miri, ignore)]
#[test]
fn test_restricted() -> Result<(), Box<dyn std::error::Error>> {
    let sources = vec![
        Source::new(
            "sourceR",
            2,
            "<A>,<A>\n\
            <B>,<C>\n\
            <C>,<D>\n",
        ),
        Source::new("sourceA", 2, "<A>,<B>\n"),
        Source::new("sourceB", 2, "<B>,<C>\n"),
    ];

    let rules = "\
        Simple(?X, !V) :- sourceR(?X, ?Y) .\n

        Block(?X, ?X) :- sourceR(?X, ?X) .\n\
        Block(?X, !V) :- sourceR(?X, ?Y) .\n

        HeadConstant(!V, X), HeadConstant(?X, !V) :- sourceR(?X, ?Y) .\n

        DatalogEx(?X, ?X) :- sourceR(?X, ?X) .\n\
        Datalog(?X), DatalogEx(?X, !V) :- sourceR(?X, ?Y) .\n

        MultiHeadNull(!V), MultiHead(!V, ?X), MultiHead(?X, !V) :- sourceR(?X, ?Y) .\n

        MultiNulls(?X, ?X) :- sourceR(?X, ?X) .\n\
        MultiNulls(!W, ?X), MultiNulls(?X, !V) :- sourceR(?X, ?Y) .\n

        MultiPieces(?X, ?X) :- sourceR(?X, ?X) .\n\
        MultiPieces(!W, ?X), MultiPieces(?Y, !V) :- sourceR(?X, ?Y) .\n

        RecA(?X, ?Y) :- sourceA(?X, ?Y) .\n\
        RecB(?X, ?Y) :- sourceB(?X, ?Y) .\n\
        RecB(?X, !V), RecA(!V, ?Y) :- RecA(?X, ?Z), RecB(?Z, ?Y) .\n
    ";

    let mut null = NullPrinter::new();

    let targets = vec![
        Target::new(
            "Simple",
            &format!(
                "<A>,{}\n\
                <B>,{}\n\
                <C>,{}\n",
                null.print_next(),
                null.print_next(),
                null.print_next()
            ),
        ),
        Target::new(
            "Block",
            &format!(
                "<A>,<A>\n\
                <B>,{}\n\
                <C>,{}\n",
                null.print_next(),
                null.print_next(),
            ),
        ),
        Target::new(
            "HeadConstant",
            &format!(
                "<A>,{0}\n\
                 <B>,{1}\n\
                 <C>,{2}\n\
                 {0},<X>\n\
                 {1},<X>\n\
                 {2},<X>\n",
                null.print_next(),
                null.print_next(),
                null.print_next(),
            ),
        ),
        Target::new(
            "DatalogEx",
            &format!(
                "<A>,<A>\n\
                <A>,{}\n\
                <B>,{}\n\
                <C>,{}\n",
                null.print_next(),
                null.print_next(),
                null.print_next(),
            ),
        ),
        Target::new(
            "Datalog",
            "<A>\n\
            <B>\n\
            <C>\n",
        ),
        Target::new(
            "MultiHead",
            &format!(
                "<A>,{0}\n\
                 <B>,{1}\n\
                 <C>,{2}\n\
                 {0},<A>\n\
                 {1},<B>\n\
                 {2},<C>\n",
                null.print_next(),
                null.print_next(),
                null.print_next(),
            ),
        ),
        Target::new(
            "MultiHeadNull",
            &format!(
                "{}\n\
                {}\n\
                {}\n",
                null.print_back(3),
                null.print_next(),
                null.print_next(),
            ),
        ),
        // Sadly, this concrete nulls assigned here are random
        // TODO: Think about a solution
        // Target::new(
        //     "MultiNulls",
        //     &format!(
        //         "<A>,<X>\n\
        //          <B>,{0}\n\
        //          <C>,{2}\n\
        //          {1},<B>\n\
        //          {3},<C>\n",
        //         null.print_next(),
        //         null.print_next(),
        //         null.print_next(),
        //         null.print_next(),
        //     ),
        // ),
        // Target::new(
        //     "MultiPieces",
        //     &format!(
        //         "<A>,<X>\n\
        //          <B>,{0}\n\
        //          <C>,{2}\n\
        //          {1},<C>\n\
        //          {3},<D>\n",
        //         null.print_skip(1),
        //         null.print_next(),
        //         null.print_next(),
        //         null.print_next(),
        //     ),
        // ),
        Target::new(
            "RecA",
            &format!(
                "<A>,<B>\n\
                {},<C>\n",
                null.print_skip(8),
            ),
        ),
        Target::new(
            "RecB",
            &format!(
                "<A>,{}\n\
                <B>,<C>\n",
                null.print_back(1),
            ),
        ),
    ];

    run_test(sources, rules, targets)
}
