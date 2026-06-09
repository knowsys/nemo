use crate::execution::planning::normalization::{atom::head::HeadAtom, rule::NormalizedRule};
use crate::rule_model::substitution::Substitution;

use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::extend::{AtomMapping, CheckResult, Reliance, extend_init};

fn check_self_restr(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    todo!()
}

pub fn is_self_restraint_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    extend_init::<HeadAtom>(mem, rule_index, rule_index, check_self_restr, previous_opt)
}

/*
#[cfg(test)]
mod test {
    #![allow(non_snake_case)] // to preserve the original test case names

    // see also:
    // <https://github.com/knowsys/2022-ISWC-reliances/tree/master/reproduce/VLog/examples/reliances>

    #[test]
    fn test_res_self_larry_1() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("q(X,V),q(V,X) :- p(X,Y)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_10() {
        // vlog_posr = [], vlog_restr = [(0, 0)]

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("q(X,V,W), q(X,X,W), r(V) :- p(X)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_11() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("q(X,U,V), q(X,W,V), r(U,V,W) :- p(X)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_12() {
        // vlog_posr = [], vlog_restr = [(0, 0)]

        let rule1: Rule =
            ParsedRule::<VLogStyle>::from_str("q(X,U,V), q(Y,V,U), q(Z,V,W) :- p(X,Y,Z)")
                .unwrap()
                .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_13() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("p(X,Z) :- p(X,Y)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_14() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("p(U,V) :- p(X,Y)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_15() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("q(X,U), q(Y,U), q(Z,U) :- p(X,Y,Z)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            true /* (hard-coded, deviation from VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_16() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("q(X,Y,V), q(X,V,Y) :- p(X,Y)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_2() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("q(V) :- p(X)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_3() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("q(X,V) :- p(X)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_4() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("q(X,V), r(V) :- p(X)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_5() {
        // vlog_posr = [], vlog_restr = [(0, 0)]

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("q(X,V),q(V,W) :- p(X,Y)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_6() {
        // vlog_posr = [], vlog_restr = [(0, 0)]

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("q(V,X),q(X,W) :- p(X)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_7() {
        // vlog_posr = [], vlog_restr = [(0, 0)]

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("q(V,X), q(X,Y) :- p(X,Y)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_8() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("q(X,Z), q(Y,Z) :- p(X,Y)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            true /* (hard-coded, deviation from VLog) */
        )
    }

    #[test]
    fn test_res_self_larry_9() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("q(X,V,V),r(V,W) :- p(X)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_self_Im() {
        // vlog_posr = [], vlog_restr = [(0, 0)]

        let rule1: Rule =
            ParsedRule::<VLogStyle>::from_str("R(X, X, W), R(X, V, W), R(X, V, T) :- B(X)")
                .unwrap()
                .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_res_self_markusX() {
        // vlog_posr = [], vlog_restr = [(0, 0)]

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str(
            "D(W, Q), C(V, T), R(X, V, W), R(X, X, W), A(V) :- B(X)",
        )
        .unwrap()
        .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_res_self_markus() {
        // vlog_posr = [], vlog_restr = [(0, 0)]

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("R(X, X, W), R(X, V, W), A(V) :- B(X)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_res_self_trivial() {
        // vlog_posr = [], vlog_restr = [(0, 0)]

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("H(X, V), R(X) :- B(X)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_res_self_twice() {
        // vlog_posr = [], vlog_restr = [(0, 0)]

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("E(X, V, W), E(X, Y, V) :- D(X, Y)")
            .unwrap()
            .into();

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_self_restraint_dependency(0, TEST_VERBOSITY, &rule1)
                && trace_check_self_restraint_reliance(0, TEST_VERBOSITY, &rule1, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }
}
*/
