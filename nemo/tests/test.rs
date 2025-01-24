#![cfg(not(miri))]
use nemo::static_checks::rule_set::RuleSet;
use nemo::static_checks::rules_properties::RulesProperties;

use std::io::{Error, ErrorKind};

use std::{assert_eq, fs::read_to_string, path::Path, path::PathBuf, str::FromStr};

use dir_test::{dir_test, Fixture};

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isDatalog",
    glob: "*.rls",
    postfix: "datalog_negative",
)]
fn datalog_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_datalog)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isDatalog",
    glob: "*.rls",
    postfix: "datalog_positive",
)]
fn datalog_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_datalog)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isDomainRestricted",
    glob: "*.rls",
    postfix: "domain_restricted_negative",
)]
fn domain_restricted_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_domain_restricted)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isDomainRestricted",
    glob: "*.rls",
    postfix: "domain_restricted_positive",
)]
fn domain_restricted_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_domain_restricted)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isFrontierGuarded",
    glob: "*.rls",
    postfix: "frontier_guarded_negative",
)]
fn frontier_guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_frontier_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isFrontierGuarded",
    glob: "*.rls",
    postfix: "frontier_guarded_positive",
)]
fn frontier_guarded_positive(fixture: Fixture<&str>) {
    let path = PathBuf::from_str(fixture.path())
        .unwrap()
        .canonicalize()
        .unwrap();
    if !path.ends_with("cq-entailment.rls") {
        return;
    }
    test(fixture, true, &RulesProperties::is_frontier_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isGuarded",
    glob: "*.rls",
    postfix: "guarded_negative",
)]
fn guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isGuarded",
    glob: "*.rls",
    postfix: "guarded_positive",
)]
fn guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isJoinless",
    glob: "*.rls",
    postfix: "joinless_negative",
)]
fn joinless_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_joinless)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isJoinless",
    glob: "*.rls",
    postfix: "joinless_positive",
)]
fn joinless_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_joinless)
}

struct TestCase<'a> {
    expected_result: bool,
    rule_set: RuleSet,
    static_check: &'a dyn Fn(&RuleSet) -> bool,
}

impl<'a> TestCase<'a> {
    fn test_from_rule_file(
        rule_file: PathBuf,
        expected_result: bool,
        static_check: &'a dyn Fn(&RuleSet) -> bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            expected_result,
            rule_set: run_until_rule_model(rule_file)?,
            static_check,
        })
    }

    fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let func = self.static_check;
        assert_eq!(self.expected_result, func(&self.rule_set));
        Ok(())
    }
}

fn run_until_rule_model(rule_file: PathBuf) -> Result<RuleSet, Box<dyn std::error::Error>> {
    // TimedCode::instance().start();
    // TimedCode::instance().sub("Reading & Preprocessing").start();

    log::info!("Parsing rules ...");

    // if cli.rules.len() > 1 {
    //     return Err(CliError::MultipleFilesNotImplemented);
    // }

    // let program_file = cli.rules.pop().ok_or(CliError::NoInput)?;
    let program_filename = rule_file.to_string_lossy().to_string();
    let program_content = read_to_string(rule_file.clone())?;
    //     .map_err(|err| CliError::IoReading {
    //     error: err,
    //     filename: program_filename.clone(),
    // })?;

    let program_ast = match nemo::parser::Parser::initialize(
        &program_content,
        program_filename.clone(),
    )
    .parse()
    {
        Ok(program) => program,
        Err((_program, report)) => {
            report.eprint()?;
            return Err(Box::new(Error::new(ErrorKind::Other, "error1")));
        }
    };

    let program = match nemo::rule_model::translation::ASTProgramTranslation::initialize(
        &program_content,
        program_filename.clone(),
    )
    .translate(&program_ast)
    {
        Ok(program) => program,
        Err(report) => {
            report.eprint()?;
            return Err(Box::new(Error::new(ErrorKind::Other, "error2")));
            // return Err(CliError::ProgramParsing {
            //     filename: program_filename,
            // });
        }
    };

    // override_exports(&mut program, cli.output.export_setting);
    // log::info!("Rules parsed");
    let rule_set: RuleSet = program.rules().cloned().collect();
    Ok(rule_set)
}

fn path_canonicalized(path: &str) -> PathBuf {
    PathBuf::from_str(path).unwrap().canonicalize().unwrap()
}

fn test(fixture: Fixture<&str>, expected_result: bool, static_check: &dyn Fn(&RuleSet) -> bool) {
    let path = path_canonicalized(fixture.path());
    assert!(path.exists());
    _ = env_logger::builder().is_test(true).try_init();
    let test_case = TestCase::test_from_rule_file(path, expected_result, static_check).unwrap();
    test_case.run().unwrap();
}
