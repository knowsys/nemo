use std::{fs::read_to_string, path::PathBuf, str::FromStr};

use assert_cmd::Command;
use assert_fs::{prelude::*, NamedTempFile, TempDir};
use test_log::test;

#[cfg_attr(miri, ignore)]
#[test]
fn symmetry_transitive_closure() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let rule_file = format!("{manifest}/tests/testfiles/stc_symmetry_transitive_closure.rls");
    let files = vec![
        format!("{manifest}/tests/testfiles/stc_csv1.csv"),
        format!("{manifest}/tests/testfiles/stc_csv2.csv"),
    ];
    let tuples = vec![
        (
            format!("{manifest}/tests/testfiles/stc_csv1.csv"),
            1,
            "city",
        ),
        (
            format!("{manifest}/tests/testfiles/stc_csv2.csv"),
            2,
            "conn",
        ),
    ];
    let conn = format!("{manifest}/tests/testfiles/stc_result.csv");
    let test_case =
        TestCase::generate_test_set(&rule_file, tuples, vec![("connected.csv", conn.as_str())])?;

    test_case.run()?;
    Ok(())
}

#[derive(Debug)]
struct TestCase {
    rule_file: NamedTempFile,
    _csv_files: Vec<NamedTempFile>,
    output_dir: TempDir,
    result_files: Vec<(String, PathBuf)>,
}

impl TestCase {
    fn generate_test_set(
        rule_file: &str,
        csv_files: Vec<(String, usize, &str)>,
        result_files: Vec<(&str, &str)>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rule_file = PathBuf::from_str(rule_file).unwrap();
        assert!(rule_file.exists());
        let rule_file_name = rule_file.file_name().expect("Filename should exist");

        let mut import = String::new();

        let temp_csv_files = csv_files
            .iter()
            .map(|(file, arity, pred_name)| {
                let file_buf = PathBuf::from_str(file).unwrap();
                assert!(file_buf.exists());
                let file_name = file_buf.file_name().expect("File should exist");
                let temp = NamedTempFile::new(file_name.to_str().unwrap()).unwrap();
                import = format!(
                    "{import}\n@source {pred_name}[{arity}]: load-csv(\"{}\").",
                    temp.as_os_str().to_str().unwrap()
                );
                temp.write_file(&file_buf).unwrap();
                temp
            })
            .collect::<Vec<_>>();

        let temp_rule_file = NamedTempFile::new(rule_file_name).unwrap();
        temp_rule_file
            .write_str(format!("{import}{}", read_to_string(&rule_file).unwrap()).as_str())
            .unwrap();
        Ok(Self {
            rule_file: temp_rule_file,
            _csv_files: temp_csv_files,
            output_dir: TempDir::new().unwrap(),
            result_files: result_files
                .iter()
                .map(|(name, path)| (name.to_string(), PathBuf::from_str(path).unwrap()))
                .collect::<Vec<_>>(),
        })
    }

    fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("stage2")?;
        cmd.arg("-s")
            .arg("-o")
            .arg(self.output_dir.path())
            .arg(self.rule_file.path());
        cmd.assert().success();
        self.result_files.iter().try_for_each(
            |(filename, expected_result_path)| -> Result<(), Box<dyn std::error::Error>> {
                let output = PathBuf::from(
                    format!(
                        "{}/{filename}",
                        self.output_dir
                            .to_path_buf()
                            .into_os_string()
                            .to_str()
                            .unwrap()
                    )
                    .as_str(),
                );
                let mut output_lines = read_to_string(output)?
                    .trim()
                    .split('\n')
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                let mut expected_lines = read_to_string(expected_result_path)?
                    .trim()
                    .split('\n')
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                output_lines.sort();
                expected_lines.sort();
                assert_eq!(output_lines, expected_lines);
                Ok(())
            },
        )?;
        Ok(())
    }
}
