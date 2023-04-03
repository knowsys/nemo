use assert_cmd::prelude::*; // Add methods on commands
use assert_fs::prelude::*;
use predicates::prelude::*; // Used for writing assertions
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
        "1,2,3\n\
        2,2,5\n\
        1,4,7\n\
        3,5,5",
    )];

    let rules = "\
        A(?X, ?Z) :- source(?X, ?Y, ?Z) .\n\
        B(?Y, ?X) :- A(?X, ?Y) .\n\
        C(?Y) :- B(?VariableThatIsNotNeeded, ?Y) .\n\
        D(?Y, ?Z) :- source(?X, ?Y, ?Z) .\n\
        E(?F, ?E) :- D(?E, ?F) .\
    ";

    let targets = vec![Target::new(
        "A",
        "1,3\n\
        1,7\n\
        2,5\n\
        3,5\n",
    )];

    run_test(sources, rules, targets)
}
