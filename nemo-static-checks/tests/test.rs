#![cfg(not(miri))]
use nemo_static_checks::static_checks::rule_set::RuleSet;
use nemo_static_checks::static_checks::rules_properties::RulesProperties;

use nemo::{
    execution::{
        execution_engine::ExecutionEngine, execution_parameters::ExecutionParameters,
        DefaultExecutionEngine,
    },
    rule_file::RuleFile,
    rule_model::programs::program::Program,
};

use std::{assert_eq, fs::read_to_string, path::PathBuf, str::FromStr};

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
    test(fixture, true, &RulesProperties::is_frontier_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isFrontierOne",
    glob: "*.rls",
    postfix: "frontier_one_negative",
)]
fn frontier_one_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_frontier_one)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isFrontierOne",
    glob: "*.rls",
    postfix: "frontier_one_positive",
)]
fn frontier_one_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_frontier_one)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isGlutFrontierGuarded",
    glob: "*.rls",
    postfix: "glut_frontier_guarded_negative",
)]
fn glut_frontier_guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_glut_frontier_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isGlutFrontierGuarded",
    glob: "*.rls",
    postfix: "glut_frontier_guarded_positive",
)]
fn glut_frontier_guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_glut_frontier_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isGlutGuarded",
    glob: "*.rls",
    postfix: "glut_guarded_negative",
)]
fn glut_guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_glut_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isGlutGuarded",
    glob: "*.rls",
    postfix: "glut_guarded_positive",
)]
fn glut_guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_glut_guarded)
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

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isJointlyAcyclic",
    glob: "*.rls",
    postfix: "jointly_acyclic_negative",
)]
fn jointly_acyclic_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_jointly_acyclic)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isJointlyAcyclic",
    glob: "*.rls",
    postfix: "jointly_acyclic_positive",
)]
fn jointly_acyclic_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_jointly_acyclic)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isJointlyFrontierGuarded",
    glob: "*.rls",
    postfix: "jointly_frontier_guarded_negative",
)]
fn jointly_frontier_guarded_negative(fixture: Fixture<&str>) {
    test(
        fixture,
        false,
        &RulesProperties::is_jointly_frontier_guarded,
    )
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isJointlyFrontierGuarded",
    glob: "*.rls",
    postfix: "jointly_frontier_guarded_positive",
)]
fn jointly_frontier_guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_jointly_frontier_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isJointlyGuarded",
    glob: "*.rls",
    postfix: "jointly_guarded_negative",
)]
fn jointly_guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_jointly_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isJointlyGuarded",
    glob: "*.rls",
    postfix: "jointly_guarded_positive",
)]
fn jointly_guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_jointly_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isLinear",
    glob: "*.rls",
    postfix: "linear_negative",
)]
fn linear_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_linear)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isLinear",
    glob: "*.rls",
    postfix: "linear_positive",
)]
fn linear_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_linear)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isMonadic",
    glob: "*.rls",
    postfix: "monadic_negative",
)]
fn monadic_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_monadic)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isMonadic",
    glob: "*.rls",
    postfix: "monadic_positive",
)]
fn monadic_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_monadic)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isShy",
    glob: "*.rls",
    postfix: "shy_negative",
)]
fn shy_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_shy)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isShy",
    glob: "*.rls",
    postfix: "shy_positive",
)]
fn shy_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_shy)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isSticky",
    glob: "*.rls",
    postfix: "sticky_negative",
)]
fn sticky_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_sticky)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isSticky",
    glob: "*.rls",
    postfix: "sticky_positive",
)]
fn sticky_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_sticky)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isWeaklyAcyclic",
    glob: "*.rls",
    postfix: "weakly_acyclic_negative",
)]
fn weakly_acyclic_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_weakly_acyclic)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isWeaklyAcyclic",
    glob: "*.rls",
    postfix: "weakly_acyclic_positive",
)]
fn weakly_acyclic_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_weakly_acyclic)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isWeaklyFrontierGuarded",
    glob: "*.rls",
    postfix: "weakly_frontier_guarded_negative",
)]
fn weakly_frontier_guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_weakly_frontier_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isWeaklyFrontierGuarded",
    glob: "*.rls",
    postfix: "weakly_frontier_guarded_positive",
)]
fn weakly_frontier_guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_weakly_frontier_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isWeaklyGuarded",
    glob: "*.rls",
    postfix: "weakly_guarded_negative",
)]
fn weakly_guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_weakly_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isWeaklyGuarded",
    glob: "*.rls",
    postfix: "weakly_guarded_positive",
)]
fn weakly_guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_weakly_guarded)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isWeaklySticky",
    glob: "*.rls",
    postfix: "weakly_sticky_negative",
)]
fn weakly_sticky_negative(fixture: Fixture<&str>) {
    test(fixture, false, &RulesProperties::is_weakly_sticky)
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isWeaklySticky",
    glob: "*.rls",
    postfix: "weakly_sticky_positive",
)]
fn weakly_sticky_positive(fixture: Fixture<&str>) {
    test(fixture, true, &RulesProperties::is_weakly_sticky)
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

fn run_until_rule_model(file_path: PathBuf) -> Result<RuleSet, Box<dyn std::error::Error>> {
    let rule_file: RuleFile = RuleFile::load(file_path)?;
    let execution_parameters: ExecutionParameters = ExecutionParameters::default();
    let (engine, _) =
        DefaultExecutionEngine::from_file(rule_file, execution_parameters)?.into_pair();
    let program: &Program = engine.program();
    let rule_set: RuleSet = RuleSet(program.all_rules());
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
