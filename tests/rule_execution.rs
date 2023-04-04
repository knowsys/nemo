/// Test methods to execute code in the test builder.
/// code appears to be dead as the utilising code is generated during build time
use std::{
    fs::{read_dir, read_to_string},
    path::PathBuf,
    str::FromStr,
};

use assert_cmd::Command;
use assert_fs::{prelude::*, TempDir};

/// The folder Structure of one test case is as follows
/// testcase
/// |- problem ... a folder, containing the rule file to be executed and all other resources. stage2 will be run in this folder
/// |- expected ... contains all output files to be compared to the computed output
#[derive(Debug)]
struct TestCase {
    #[allow(dead_code)]
    rule_file: PathBuf,
    #[allow(dead_code)]
    output_dir: TempDir,
    #[allow(dead_code)]
    test_dir: TempDir,
    #[allow(dead_code)]
    expected_result: PathBuf,
}

impl TestCase {
    #[allow(dead_code)]
    /// Expects a path to the test, case and the name of the file to run in the problem subfolder
    fn generate_test(
        rule_file: &str,
        test_directory: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let test_dir = PathBuf::from_str(&format!("{test_directory}/problem"))?;
        let tmp_test_dir = TempDir::new()?;
        Command::new("cp")
            .arg("-r")
            .arg(&format!("{}/.", test_dir.as_os_str().to_str().unwrap()))
            .arg(tmp_test_dir.path())
            .output()?;
        let rule_file = format!("{}/{rule_file}", tmp_test_dir.as_os_str().to_string_lossy());
        log::error!("{rule_file}");
        let result = Self {
            rule_file: PathBuf::from_str(&rule_file)?,
            output_dir: TempDir::new()?,
            test_dir: tmp_test_dir,
            expected_result: PathBuf::from_str(&format!("{test_directory}/expected"))?,
        };
        assert!(result.expected_result.is_dir());
        assert!(result.rule_file.exists());
        assert!(!result.rule_file.is_dir());
        Ok(result)
    }

    #[allow(dead_code)]
    fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("stage2")?;

        cmd.current_dir(self.test_dir.path())
            .arg("-s")
            .arg("-o")
            .arg(self.output_dir.path())
            .arg(self.rule_file.as_path())
            .assert()
            .success();

        read_dir(&self.expected_result)?
            .into_iter()
            .for_each(|entry| {
                let expected_file = entry.unwrap().path();
                let expected_name = expected_file.file_name().and_then(|s| s.to_str()).unwrap();
                let output_file =
                    PathBuf::from_str(self.output_dir.child(expected_name).to_str().unwrap())
                        .unwrap();
                assert!(output_file.exists());
                let mut output_lines = read_to_string(output_file)
                    .unwrap()
                    .trim()
                    .split('\n')
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                let mut expected_lines = read_to_string(expected_file)
                    .unwrap()
                    .trim()
                    .split('\n')
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                output_lines.sort();
                expected_lines.sort();
                assert_eq!(output_lines, expected_lines);
            });
        Ok(())
    }
}
