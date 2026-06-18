use assert_cmd::cargo_bin_cmd; // Add methods on commands
use predicates::prelude::*;
#[cfg(not(miri))]
use test_log::test;

#[cfg_attr(miri, ignore)]
#[test]
fn cli_argument_parsing() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = cargo_bin_cmd!("nmo");
    cmd.arg("-vvv").arg("Non-existing-file.rls");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("No such file or directory"));

    cmd = cargo_bin_cmd!("nmo");
    cmd.arg("-h");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Print help"));

    cmd = cargo_bin_cmd!("nmo");
    cmd.arg("--version");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("nemo"));

    cmd = cargo_bin_cmd!("nmo");
    cmd.arg("-v").arg("-q");
    cmd.assert().failure().stderr(predicate::str::contains(
        "argument '--verbose...' cannot be used with '--quiet'",
    ));

    cmd = cargo_bin_cmd!("nmo");
    cmd.arg("-v").arg("--log").arg("error");
    cmd.assert().failure().stderr(predicate::str::contains(
        "argument '--verbose...' cannot be used with '--log <LOG_LEVEL>'",
    ));

    cmd = cargo_bin_cmd!("nmo");
    cmd.arg("--log").arg("cats");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("'--log <LOG_LEVEL>'"));

    cmd = cargo_bin_cmd!("nmo");
    cmd.arg("--print-facts").arg("keep");
    cmd.assert().failure().stderr(predicate::str::contains(
        "invalid value 'keep' for '--print-facts <PRINT_FACTS_SETTING>'",
    ));
    cmd = cargo_bin_cmd!("nmo");
    cmd.arg("--print-facts").arg("none");
    cmd.assert().failure().stderr(predicate::str::contains(
        "the following required arguments were not provided",
    ));
    Ok(())
}
