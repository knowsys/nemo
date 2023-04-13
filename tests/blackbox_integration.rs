#![cfg(not(miri))]
/// Test methods to execute code in the test builder.
/// code appears to be dead as the utilising code is generated during build time
use std::{
    fs::{read_dir, read_to_string},
    path::PathBuf,
    str::FromStr,
};

use assert_cmd::Command;
use assert_fs::{prelude::*, TempDir};
use dir_test::{dir_test, Fixture};
/// Testcase-generator. Each set of testcases needs a directory in resources/testcases.
/// Every testcase is represented by a single .rls file in the testcases directory
/// To check the correctness of the output, the expected output (in csv-export format) needs to be in a directory with the same name (without extension) as the rule-file
/// e.g.:
/// resources/testcases
/// |- lcs-diff
/// | |- len5.rls
/// | |- len5
/// | | |- docAend.csv
/// | | |- docBend.csv
/// | | |- doPlus.csv
/// | | |- edge.csv
/// | | |- eq.csv
/// | | |- furthestPath.csv
/// | | |- gather.csv
/// | | |- ge.csv
/// | | |- infDocA.csv
/// | | |- infDocB.csv
/// | | |- lcs.csv
/// | | |- lt.csv
/// | | |- ltLetter.csv
/// | | |- max.csv
/// | | |- min.csv
/// | | |- minus.csv
/// | | |- neq.csv
/// | | |- nonfinal.csv
/// | | |- path.csv
/// | | |- s.csv
/// | | |- s2.csv
/// | | |- startPathComp.csv
/// |- transitive-closure
/// | |- test1.rls
/// | |- test1
/// | | |- conn.csv
/// | |- test2.rls
/// | |- test2
/// | | |- conn.csv
///
/// will generate the following tests:
/// lcs_diff_len5
/// transitive_closure_test1
/// transitive_closure_test2

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/resources/testcases",
    glob: "**/*/*.rls",
)]
fn test(fixture: Fixture<&str>) {
    let path = PathBuf::from_str(fixture.path())
        .unwrap()
        .canonicalize()
        .unwrap();
    assert!(path.exists());

    let test_case = TestCase::test_from_rule_file(path).unwrap();
    test_case.run().unwrap();
}

#[derive(Debug)]
struct TestCase {
    rule_file: PathBuf,
    output_dir: TempDir,
    test_dir: PathBuf,
    expected_result: PathBuf,
}

impl TestCase {
    fn test_from_rule_file(rule_file: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let mut directory = rule_file.clone();
        directory.pop();
        let mut expected_result = directory.clone();
        expected_result.push(rule_file.file_stem().unwrap());
        assert!(directory.is_dir());
        assert!(expected_result.is_dir());
        Ok(Self {
            rule_file,
            output_dir: TempDir::new()?,
            test_dir: directory,
            expected_result,
        })
    }

    fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("nemo")?;

        cmd.current_dir(self.test_dir.as_path())
            .arg("-s")
            .arg("-o")
            .arg(self.output_dir.path())
            .arg(self.rule_file.as_path())
            .assert()
            .success();

        read_dir(&self.expected_result)?.for_each(|entry| {
            let expected_file = entry.unwrap().path();
            let expected_name = expected_file.file_name().and_then(|s| s.to_str()).unwrap();
            let output_file =
                PathBuf::from_str(self.output_dir.child(expected_name).to_str().unwrap()).unwrap();
            assert!(output_file.exists());
            let mut output_lines = read_to_string(output_file)
                .unwrap()
                .trim()
                .lines()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            let mut expected_lines = read_to_string(expected_file)
                .unwrap()
                .trim()
                .lines()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            output_lines.sort();
            expected_lines.sort();
            assert_eq!(output_lines, expected_lines);
        });
        Ok(())
    }
}
