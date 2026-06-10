//! Tests for individual reliance computations.
//! A subset of the tests for positive/(self-)restraint reliances originally stem from VLog. See also:
//! <https://github.com/knowsys/2022-ISWC-reliances/tree/master/reproduce/VLog/examples/reliances>

use std::path::PathBuf;

use crate::{
    execution::{
        ExecutionEngine,
        execution_parameters::ExecutionParameters,
        planning::normalization::rule::NormalizedRule,
        selection_strategy::{
            strategy_full_chain_stratification::{
                reliance_memoization::RuleMemoization,
                reliances::{
                    aggr::is_aggregation_reliance, negr::is_negation_reliance,
                    posr::is_positive_reliance, restr::is_restraint_reliance,
                    self_restr::is_self_restraint_reliance,
                },
                util::extend::Reliance,
            },
            strategy_round_robin::StrategyRoundRobin,
        },
    },
    rule_file::RuleFile,
};

use dir_test::{Fixture, dir_test};

/// Auxiliary function to read a rule file and parse a vector of formalized rules.
fn aux_parse_rules(path: PathBuf) -> (String, Vec<NormalizedRule>) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let rule_file = RuleFile::load(path).expect("rule file should have loaded");
    let content = rule_file.content().to_string();

    // make sure to not use StrategyFullChainStratification here, as reliance would otherwise be computed twice
    let engine: ExecutionEngine<StrategyRoundRobin> = rt
        .block_on(ExecutionEngine::from_file(
            rule_file,
            //RuleFile::new(rules_str.to_string(), "test_file".to_string()),
            ExecutionParameters::default(),
        ))
        .expect("rule file should be parseable")
        .into_pair()
        .0;
    (content, engine.chase_program().rules().clone())
}

/// Auxiliary function for self-restraint check to get same interface as other relchecks.
fn aux_is_self_restraint_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    _rule2_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    is_self_restraint_reliance(mem, rule1_index, previous_opt)
}

/// Auxiliary function to perform a reliance check and compare the expected result as per the comment given in the file's first line.
fn aux_check_reliance(path: PathBuf) {
    let (content, rules) = aux_parse_rules(path.clone());

    let expected = content.lines().next().expect("first line should be there");
    assert!(expected.starts_with("%"), "first line should be comment");
    let parts = expected[1..].split(":").collect::<Vec<_>>();
    assert_eq!(parts.len(), 2);
    let relcheck = match parts[0].trim() {
        "posr" => is_positive_reliance,
        "restr" => is_restraint_reliance,
        "self_restr" => aux_is_self_restraint_reliance,
        "negr" => is_negation_reliance,
        "aggr" => is_aggregation_reliance,
        _ => panic!(),
    };
    let is_rel: bool = parts[1].trim().parse().unwrap();

    assert_eq!(
        relcheck(
            &mut RuleMemoization::new(&rules.iter().collect()),
            0,
            1,
            None,
        )
        .is_some(),
        is_rel,
        "File: {}\n\n{}",
        path.display(),
        content
    );
}

#[allow(dead_code)] // code appears to be dead as the utilising code is generated during build time
/// Testcase-generator. Each testcases needs a single .rls file in resources/reliance-testcases.
/// To check the correctness of the reliance computation, the expected result is in a comment in the first line of the file like `% posr: true`
#[dir_test(
    dir: "$CARGO_MANIFEST_DIR/../resources/reliance-testcases",
    glob: "**/*.rls",
)]
fn test(fixture: Fixture<&str>) {
    let path = PathBuf::from(fixture.path())
        //.unwrap()
        .canonicalize()
        .unwrap();
    assert!(path.exists());

    _ = env_logger::builder().is_test(true).try_init();

    aux_check_reliance(path);
}
