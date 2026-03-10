#![cfg(not(miri))]

use assert_cmd::Command;
use std::{assert_eq, path::PathBuf, str::FromStr};

use dir_test::{Fixture, dir_test};

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isDatalog",
    glob: "*.rls",
    postfix: "datalog_negative",
)]
fn datalog_negative(fixture: Fixture<&str>) {
    test(fixture, false, "datalog")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isDatalog",
    glob: "*.rls",
    postfix: "datalog_positive",
)]
fn datalog_positive(fixture: Fixture<&str>) {
    test(fixture, true, "datalog")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isDomainRestricted",
    glob: "*.rls",
    postfix: "domain_restricted_negative",
)]
fn domain_restricted_negative(fixture: Fixture<&str>) {
    test(fixture, false, "domain-restricted")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isDomainRestricted",
    glob: "*.rls",
    postfix: "domain_restricted_positive",
)]
fn domain_restricted_positive(fixture: Fixture<&str>) {
    test(fixture, true, "domain-restricted")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isFrontierGuarded",
    glob: "*.rls",
    postfix: "frontier_guarded_negative",
)]
fn frontier_guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, "frontier-guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isFrontierGuarded",
    glob: "*.rls",
    postfix: "frontier_guarded_positive",
)]
fn frontier_guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, "frontier-guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isFrontierOne",
    glob: "*.rls",
    postfix: "frontier_one_negative",
)]
fn frontier_one_negative(fixture: Fixture<&str>) {
    test(fixture, false, "frontier-one")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isFrontierOne",
    glob: "*.rls",
    postfix: "frontier_one_positive",
)]
fn frontier_one_positive(fixture: Fixture<&str>) {
    test(fixture, true, "frontier-one")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isGlutFrontierGuarded",
    glob: "*.rls",
    postfix: "glut_frontier_guarded_negative",
)]
fn glut_frontier_guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, "glut-frontier-guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isGlutFrontierGuarded",
    glob: "*.rls",
    postfix: "glut_frontier_guarded_positive",
)]
fn glut_frontier_guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, "glut-frontier-guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isGlutGuarded",
    glob: "*.rls",
    postfix: "glut_guarded_negative",
)]
fn glut_guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, "glut-guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isGlutGuarded",
    glob: "*.rls",
    postfix: "glut_guarded_positive",
)]
fn glut_guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, "glut-guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isGuarded",
    glob: "*.rls",
    postfix: "guarded_negative",
)]
fn guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, "guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isGuarded",
    glob: "*.rls",
    postfix: "guarded_positive",
)]
fn guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, "guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isJoinless",
    glob: "*.rls",
    postfix: "joinless_negative",
)]
fn joinless_negative(fixture: Fixture<&str>) {
    test(fixture, false, "joinless")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isJoinless",
    glob: "*.rls",
    postfix: "joinless_positive",
)]
fn joinless_positive(fixture: Fixture<&str>) {
    test(fixture, true, "joinless")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isJointlyAcyclic",
    glob: "*.rls",
    postfix: "jointly_acyclic_negative",
)]
fn jointly_acyclic_negative(fixture: Fixture<&str>) {
    test(fixture, false, "jointly-acyclic")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isJointlyAcyclic",
    glob: "*.rls",
    postfix: "jointly_acyclic_positive",
)]
fn jointly_acyclic_positive(fixture: Fixture<&str>) {
    test(fixture, true, "jointly-acyclic")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isJointlyFrontierGuarded",
    glob: "*.rls",
    postfix: "jointly_frontier_guarded_negative",
)]
fn jointly_frontier_guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, "jointly-frontier-guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isJointlyFrontierGuarded",
    glob: "*.rls",
    postfix: "jointly_frontier_guarded_positive",
)]
fn jointly_frontier_guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, "jointly-frontier-guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isJointlyGuarded",
    glob: "*.rls",
    postfix: "jointly_guarded_negative",
)]
fn jointly_guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, "jointly-guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isJointlyGuarded",
    glob: "*.rls",
    postfix: "jointly_guarded_positive",
)]
fn jointly_guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, "jointly-guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isLinear",
    glob: "*.rls",
    postfix: "linear_negative",
)]
fn linear_negative(fixture: Fixture<&str>) {
    test(fixture, false, "linear")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isLinear",
    glob: "*.rls",
    postfix: "linear_positive",
)]
fn linear_positive(fixture: Fixture<&str>) {
    test(fixture, true, "linear")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isDmfa",
    glob: "*.rls",
    postfix: "mfa_negative",
)]
fn mfa_negative(fixture: Fixture<&str>) {
    // let file_names = [
    // "r01.r",
    // "r02.rls",
    // "c28.rls",
    // "higherCyclicTermDeptchRequired.rls",
    // "inf_chain.rls",
    // "joinless_non_terminating.rls",
    //     "joint-acyclicity-guarded-talk-guadedness.rls",
    // ];
    // if !file_names.contains(
    //     &path_canonicalized(fixture.path())
    //         .file_name()
    //         .unwrap()
    //         .to_str()
    //         .unwrap(),
    // ) {
    //     return;
    // }
    test(fixture, false, "mfa")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isDmfa",
    glob: "*.rls",
    postfix: "mfa_positive",
)]
fn mfa_positive(fixture: Fixture<&str>) {
    test(fixture, true, "mfa")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isMonadic",
    glob: "*.rls",
    postfix: "monadic_negative",
)]
fn monadic_negative(fixture: Fixture<&str>) {
    test(fixture, false, "monadic")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isMonadic",
    glob: "*.rls",
    postfix: "monadic_positive",
)]
fn monadic_positive(fixture: Fixture<&str>) {
    test(fixture, true, "monadic")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isShy",
    glob: "*.rls",
    postfix: "shy_negative",
)]
fn shy_negative(fixture: Fixture<&str>) {
    test(fixture, false, "shy")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isShy",
    glob: "*.rls",
    postfix: "shy_positive",
)]
fn shy_positive(fixture: Fixture<&str>) {
    test(fixture, true, "shy")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isSticky",
    glob: "*.rls",
    postfix: "sticky_negative",
)]
fn sticky_negative(fixture: Fixture<&str>) {
    test(fixture, false, "sticky")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isSticky",
    glob: "*.rls",
    postfix: "sticky_positive",
)]
fn sticky_positive(fixture: Fixture<&str>) {
    test(fixture, true, "sticky")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isWeaklyAcyclic",
    glob: "*.rls",
    postfix: "weakly_acyclic_negative",
)]
fn weakly_acyclic_negative(fixture: Fixture<&str>) {
    test(fixture, false, "weakly-acyclic")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isWeaklyAcyclic",
    glob: "*.rls",
    postfix: "weakly_acyclic_positive",
)]
fn weakly_acyclic_positive(fixture: Fixture<&str>) {
    test(fixture, true, "weakly-acyclic")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isWeaklyFrontierGuarded",
    glob: "*.rls",
    postfix: "weakly_frontier_guarded_negative",
)]
fn weakly_frontier_guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, "weakly-frontier-guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isWeaklyFrontierGuarded",
    glob: "*.rls",
    postfix: "weakly_frontier_guarded_positive",
)]
fn weakly_frontier_guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, "weakly-frontier-guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isWeaklyGuarded",
    glob: "*.rls",
    postfix: "weakly_guarded_negative",
)]
fn weakly_guarded_negative(fixture: Fixture<&str>) {
    test(fixture, false, "weakly-guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isWeaklyGuarded",
    glob: "*.rls",
    postfix: "weakly_guarded_positive",
)]
fn weakly_guarded_positive(fixture: Fixture<&str>) {
    test(fixture, true, "weakly-guarded")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isWeaklySticky",
    glob: "*.rls",
    postfix: "weakly_sticky_negative",
)]
fn weakly_sticky_negative(fixture: Fixture<&str>) {
    test(fixture, false, "weakly-sticky")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isWeaklySticky",
    glob: "*.rls",
    postfix: "weakly_sticky_positive",
)]
fn weakly_sticky_positive(fixture: Fixture<&str>) {
    test(fixture, true, "weakly-sticky")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/negative/isDmfa",
    glob: "*.rls",
    postfix: "msa_negative",
)]
fn msa_negative(fixture: Fixture<&str>) {
    test(fixture, false, "msa")
}

#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/testcases_static_checks/tests/positive/isDmfa",
    glob: "*.rls",
    postfix: "msa_positive",
)]
fn msa_positive(fixture: Fixture<&str>) {
    test(fixture, true, "msa")
}

struct TestCase<'a> {
    file: PathBuf,
    exp_res: String,
    check: &'a str,
}

impl<'a> TestCase<'a> {
    fn new(file: PathBuf, exp_res: String, check: &'a str) -> Self {
        Self {
            file,
            exp_res,
            check,
        }
    }

    fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut cmd = Command::cargo_bin("nemo-static-checks")?;

        let output = cmd
            .current_dir("/Users/louis/kbs/nemo/")
            .arg("-c")
            .arg(self.check)
            .arg(self.file.as_path())
            .output()
            .expect("failed to execute");

        // println!("{}", str::from_utf8(&output.stderr).expect(""));
        // println!("hello");

        assert_eq!(
            str::from_utf8(&output.stdout).expect("").trim(),
            self.exp_res.trim()
        );

        Ok(())
    }
}

fn test(fixture: Fixture<&str>, exp_bool: bool, check: &str) {
    let path = path_canonicalized(fixture.path());
    let test_case = TestCase::new(path, format!("{check}: {}\n", exp_bool), check);
    test_case.run().unwrap();
}

fn path_canonicalized(path: &str) -> PathBuf {
    PathBuf::from_str(path).unwrap().canonicalize().unwrap()
}
