use crate::execution::planning::normalization::{atom::head::HeadAtom, rule::NormalizedRule};
use crate::rule_model::substitution::Substitution;

use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::extend::{AtomMapping, CheckResult, Reliance, extend_init};

fn check_restr(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    todo!()
}

pub fn is_restraint_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    extend_init::<HeadAtom>(mem, rule1_index, rule2_index, check_restr, previous_opt)
}

/*
#[cfg(test)]
mod test {
    #![allow(non_snake_case)] // to preserve the original test case names

    // see also:
    // <https://github.com/knowsys/2022-ISWC-reliances/tree/master/reproduce/VLog/examples/reliances>

    #[test]
    fn test_res_altPresent() {
        // vlog_posr = [(1, 1), (1, 0)], vlog_restr = [(1, 0)]

        let rule1: Rule =
            ParsedRule::<VLogStyle>::from_str("H(X, Y, V, V), H(Y, X, V, V) :- A(X, Y)")
                .unwrap()
                .into();
        let rule2: Rule =
            ParsedRule::<VLogStyle>::from_str("H(X, Y, X, Y), C(X, X), A(X, X) :- C(X, Y)")
                .unwrap()
                .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_restraint_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_restraint_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_basic_1() {
        // vlog_posr = [], vlog_restr = [(1, 0)]

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("H(X, V) :- B(X)")
            .unwrap()
            .into();
        let rule2: Rule = ParsedRule::<VLogStyle>::from_str("H(X, Y) :- A(X, Y)")
            .unwrap()
            .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_restraint_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_restraint_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_basic_2() {
        // vlog_posr = [], vlog_restr = [(1, 0)]

        let rule1: Rule =
            ParsedRule::<VLogStyle>::from_str("H(X, V, W), B(Y, W, const) :- A(X, Y)")
                .unwrap()
                .into();
        let rule2: Rule =
            ParsedRule::<VLogStyle>::from_str("H(X, Y, W), B(X, W, const) :- C(X, Y)")
                .unwrap()
                .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_restraint_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_restraint_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_null_1() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("H(X, V, W) :- B(X)")
            .unwrap()
            .into();
        let rule2: Rule = ParsedRule::<VLogStyle>::from_str("H(V, X, Y) :- A(X, Y)")
            .unwrap()
            .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_restraint_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_restraint_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_res_null_2() {
        // vlog_posr = [], vlog_restr = [(0, 1)]

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("H(X, Y, V) :- A(X, Y)")
            .unwrap()
            .into();
        let rule2: Rule = ParsedRule::<VLogStyle>::from_str("H(X, V, W) :- B(X)")
            .unwrap()
            .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_restraint_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_restraint_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            true /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_null_3() {
        // vlog_posr = [], vlog_restr = [(0, 0), (0, 1), (1, 1)]

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("Q(X), H(X, W, W) :- A(X, Y)")
            .unwrap()
            .into();
        let rule2: Rule = ParsedRule::<VLogStyle>::from_str("R(X), H(Y, V, W) :- B(X)")
            .unwrap()
            .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_restraint_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_restraint_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            true /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_null_4() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("H(X, V, W), B(Y, W) :- A(X, Y)")
            .unwrap()
            .into();
        let rule2: Rule = ParsedRule::<VLogStyle>::from_str("H(X, V, V) :- C(X)")
            .unwrap()
            .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_restraint_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_restraint_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_res_psi1Ibm_phi1phi2() {
        // vlog_posr = [(1, 1), (1, 0)], vlog_restr = [(1, 1)]

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("H(X, Y, Y, V) :- A(X, Y)")
            .unwrap()
            .into();
        let rule2: Rule =
            ParsedRule::<VLogStyle>::from_str("H(X, X, Y, V), C(W, W), A(X, X) :- C(X, Y)")
                .unwrap()
                .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_restraint_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_restraint_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_psi1Ibm_phi1phi2psi22() {
        // vlog_posr = [(1, 1), (1, 0)], vlog_restr = [(1, 0)]

        let rule1: Rule =
            ParsedRule::<VLogStyle>::from_str("R(V), H(X, Y, V, V), H(Y, X, V, V) :- A(X, Y)")
                .unwrap()
                .into();
        let rule2: Rule =
            ParsedRule::<VLogStyle>::from_str("R(X), H(X, Y, X, Y), C(X, X), A(X, X) :- C(X, Y)")
                .unwrap()
                .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_restraint_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_restraint_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_psi1Ibm_psi2() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("A(X, V, V) :- B(X)")
            .unwrap()
            .into();
        let rule2: Rule = ParsedRule::<VLogStyle>::from_str("A(X, Z, Y) :- A(X, Y, Z)")
            .unwrap()
            .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_restraint_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_restraint_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_res_psi2Iam_phi2() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = ParsedRule::<VLogStyle>::from_str("B(Y, X, V) :- B(X, Y, Z)")
            .unwrap()
            .into();
        let rule2: Rule = ParsedRule::<VLogStyle>::from_str("B(X, X, X) :- A(X)")
            .unwrap()
            .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_restraint_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_restraint_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }
}
*/
