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
    let type_declarations = [
        "@decl city(GenericEverything) .",
        "@decl conn(GenericEverything, GenericEverything) .",
        "@decl connected(GenericEverything, GenericEverything) .",
    ];
    rule_file.write_str(&(type_declarations.join("\n") + "\n" + &rule_content))?;
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
