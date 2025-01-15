#![cfg(not(miri))]

use std::{
    fs::{read_dir, read_to_string},
    path::PathBuf,
    str::FromStr,
};

use assert_cmd::Command;
use assert_fs::{prelude::*, TempDir};
use dir_test::{dir_test, Fixture};

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases",
    glob: "**/*/*.rls",
)]
fn test(fixture: Fixture<&str>) {
    let path = PathBuf::from_str(fixture.path())
        .unwrap()
        .canonicalize()
        .unwrap();
    assert!(path.exists());

    // _ = env_logger::builder().is_test(true).try_init();
    //
    // let test_case = TestCase::test_from_rule_file(path).unwrap();
    // test_case.run().unwrap();
}
