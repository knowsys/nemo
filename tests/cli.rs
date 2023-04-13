use assert_cmd::prelude::*; // Add methods on commands
use predicates::prelude::*;
use std::process::Command; // Run programs
use test_log::test;

#[cfg_attr(miri, ignore)]
#[test]
fn cli_argument_parsing() -> Result<(), Box<dyn std::error::Error>> {
    let bin = "nemo";
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
