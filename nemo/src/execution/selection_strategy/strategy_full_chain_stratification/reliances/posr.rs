use std::collections::HashSet;

use crate::execution::planning::normalization::{atom::body::BodyAtom, rule::NormalizedRule};
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::atom::Atom;
use crate::rule_model::substitution::Substitution;

use crate::execution::selection_strategy::strategy_full_chain_stratification::reliance_memoization::RuleMemoization;
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::extend::{AtomMapping, CheckResult, Reliance, extend_init};
use crate::execution::selection_strategy::strategy_full_chain_stratification::util::database::{RepresentativeDatabase, RepresentativeAtom};

fn check_posr(
    rule1: &NormalizedRule,
    rule2: &NormalizedRule,
    mu: &AtomMapping,
    eta: &Substitution,
) -> CheckResult {
    let r1_universals_eta = eta
        .substitute_variables(rule1.universals())
        .collect::<HashSet<_>>();
    let r1_existentials = rule1.existentials();

    // mu failed if eta assigned a variable in rule1.body to an existential
    if r1_existentials.iter().any(|v| eta.contains_variable(*v)) {
        log::trace!("existential of rule1 mapped");
        return CheckResult::Reject;
    }
    if !r1_universals_eta.is_disjoint(&r1_existentials) {
        log::trace!(
            "universals of rule1 under eta are not disjoint from existentials of rule1 => mu failed"
        );
        return CheckResult::Reject;
    }

    let rule2_body_mapped_set = mu.mapped(&rule2.positive()).collect::<HashSet<_>>();

    let mu_maxidx = mu.maxidx();

    let rule2_body_unmapped_left = &rule2.positive()[..mu_maxidx]
        .iter()
        .collect::<HashSet<_>>()
        .difference(&rule2_body_mapped_set)
        .cloned()
        .collect::<HashSet<_>>();

    // mu failed if eta assigned a variable in the left unmapped portion of rule2.body to an existential
    let rule2_body_unmapped_left_vars_eta = eta
        .substitute_variables(
            rule2_body_unmapped_left
                .iter()
                .flat_map(|atom| atom.variables()),
        )
        .collect::<HashSet<_>>();
    if !rule2_body_unmapped_left_vars_eta.is_disjoint(&r1_existentials) {
        log::trace!(
            "variables of the left unmapped part of rule2 under eta are not disjoint from existentials of rule1 => mu failed"
        );
        return CheckResult::Reject;
    }

    let rule2_body_unmapped_right = &rule2.positive()[mu_maxidx..]
        .iter()
        .collect::<HashSet<_>>()
        .difference(&rule2_body_mapped_set)
        .cloned()
        .collect::<HashSet<_>>();

    // mu has to be extended if eta assigned a variable from the right unmapped portion of rule2.body to an existential
    let rule2_body_unmapped_right_vars_eta = eta
        .substitute_variables(
            rule2_body_unmapped_right
                .iter()
                .flat_map(|atom| atom.variables()),
        )
        .collect::<HashSet<_>>();
    if !rule2_body_unmapped_right_vars_eta.is_disjoint(&r1_existentials) {
        log::trace!(
            "variables of the right unmapped part of rule2 under eta are disjoint from existentials of rule1 => mu must be extended"
        );
        return CheckResult::Extend;
    }

    let rule1_body_eta =
        RepresentativeAtom::substitute_atoms(eta, rule1.positive()).collect::<HashSet<_>>();

    let rule2_body_unmapped_eta = RepresentativeAtom::substitute_atoms(
        eta,
        rule2
            .positive()
            .iter()
            .collect::<HashSet<_>>()
            .difference(&rule2_body_mapped_set)
            .copied(),
    )
    .collect();

    // construct representative interpretation I_a
    let rule1_body_eta_cup_rule2_body_unmapped_eta = rule1_body_eta
        .union(&rule2_body_unmapped_eta)
        .collect::<HashSet<_>>();
    log::trace!(
        "I_a = {}",
        RepresentativeDatabase::display(
            rule1_body_eta_cup_rule2_body_unmapped_eta.iter().copied(),
            None
        )
    );
    let interpretation_a_db =
        RepresentativeDatabase::new(rule1_body_eta_cup_rule2_body_unmapped_eta.into_iter());

    // mu has to be extended if rule1 under eta is satisfied on I_a
    let rule1_head_eta =
        RepresentativeAtom::substitute_atoms(eta, rule1.head()).collect::<HashSet<_>>();
    if interpretation_a_db.entails(&r1_existentials, &rule1_head_eta) {
        log::trace!("I_a models the head of rule1 under eta => mu must be extended");
        return CheckResult::Extend;
    }

    let rule2_body_eta =
        RepresentativeAtom::substitute_atoms(eta, rule2.positive()).collect::<HashSet<_>>();

    // mu has to be extended if rule2_body under eta is fully contained in I_a
    if interpretation_a_db.contains(&rule2_body_eta) {
        log::trace!("body of rule2 under eta already contained in I_a => mu must be extended");
        return CheckResult::Extend;
    }

    // construct representative interpretation I_b
    log::trace!(
        "I_b = I_a U {}",
        RepresentativeDatabase::display(&rule1_head_eta, Some(&interpretation_a_db))
    );
    let interpretation_b_db = interpretation_a_db.add_facts(&rule1_head_eta);

    // mu has failed if rule2 under eta is satisfied in I_b
    let r2_existentials = eta.substitute_variables(rule2.existentials()).collect();
    let rule2_head_eta =
        RepresentativeAtom::substitute_atoms(eta, rule2.head()).collect::<HashSet<_>>();
    if interpretation_b_db.entails(&r2_existentials, &rule2_head_eta) {
        log::trace!("I_b models head of rule2 under eta => mu failed");
        return CheckResult::Reject;
    }

    let r2_universals_eta = eta
        .substitute_variables(rule2.universals())
        .collect::<HashSet<_>>();
    for n in rule2.negative() {
        let n = RepresentativeAtom::from_atom_with_substitution(eta, n);
        let existentials = n
            .variables()
            .filter(|v| !r2_universals_eta.contains(v))
            .collect();
        if interpretation_b_db.entails(&existentials, [&n]) {
            log::trace!("I_b is not disjoint from negative body of rule2 under eta => mu failed");
            return CheckResult::Reject;
        }
    }

    log::trace!("=> rule1 <+ rule2",);
    CheckResult::Accept
}

pub fn is_positive_reliance<'b, 'a: 'b>(
    mem: &'b mut RuleMemoization<'a>,
    rule1_index: usize,
    rule2_index: usize,
    previous_opt: Option<&Reliance>,
) -> Option<Reliance> {
    extend_init::<BodyAtom>(mem, rule1_index, rule2_index, check_posr, previous_opt)
}

#[cfg(test)]
mod test {
    #![allow(non_snake_case)] // to preserve the original test case names

    use crate::{
        execution::{
            DefaultExecutionStrategy, ExecutionEngine, execution_parameters::ExecutionParameters, planning::normalization::rule::NormalizedRule, selection_strategy::strategy_full_chain_stratification::{
                reliance_memoization::RuleMemoization, reliances::posr::is_positive_reliance,
            }
        },
        rule_file::RuleFile,
    };

    fn aux_parse_rules(rules_str: &str) -> Vec<NormalizedRule> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let engine: ExecutionEngine<DefaultExecutionStrategy> = rt
            .block_on(ExecutionEngine::from_file(
                RuleFile::new(rules_str.to_string(), "test_file".to_string()),
                ExecutionParameters::default(),
            ))
            .unwrap()
            .into_pair()
            .0;
        engine.chase_program().rules().clone()
    }

    // see also:
    // <https://github.com/knowsys/2022-ISWC-reliances/tree/master/reproduce/VLog/examples/reliances>

    #[test]
    fn test_long() {
        // vlog_posr = [(0, 1)], vlog_restr = [(0, 0)]

        let rules = aux_parse_rules(
            "
            <http://purl.org/obo/owl/CL#CL_0000766>(?x), <http://purl.org/obo/owl/has_part>(?x, !y1), <http://purl.org/obo/owl/GO#GO_0030141>(!y1), <http://purl.org/obo/owl/obo#capable_of>(?x, !y2), <http://purl.org/obo/owl/GO#GO_0008015>(!y2), <http://purl.org/obo/owl/obo#has_plasma_membrane_part>(?x, !y3), <http://purl.org/obo/owl/PRO#PRO_000001012>(!y3), <http://purl.org/obo/owl/obo#has_plasma_membrane_part>(?x, !y4), <http://purl.org/obo/owl/PRO#PRO_000001332>(!y4), <http://purl.org/obo/owl/obo#has_plasma_membrane_part>(?x, !y5), <http://purl.org/obo/owl/PRO#PRO_000001969>(!y5), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, !y6), <http://purl.org/obo/owl/PRO#PRO_000001002>(!y6), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, !y7), <http://purl.org/obo/owl/PRO#PRO_000001020>(!y7), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, !y8), <http://purl.org/obo/owl/PRO#PRO_000001024>(!y8), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, !y9), <http://purl.org/obo/owl/PRO#PRO_000001084>(!y9) :- <http://purl.org/obo/owl/CL#CL_0000094>(?x).
            <http://purl.org/obo/owl/CL#CL_0000037>(?x) :- <http://purl.org/obo/owl/CL#CL_0000988>(?x), <http://purl.org/obo/owl/obo#capable_of>(?x, ?y1), <http://purl.org/obo/owl/GO#GO_0002244>(?y1), <http://purl.org/obo/owl/obo#capable_of>(?x, ?y2), <http://purl.org/obo/owl/GO#GO_0048103>(?y2), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, ?y3), <http://purl.org/obo/owl/PRO#PRO_000001002>(?y3), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, ?y4), <http://purl.org/obo/owl/PRO#PRO_000001004>(?y4), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, ?y5), <http://purl.org/obo/owl/PRO#PRO_000001012>(?y5), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, ?y6), <http://purl.org/obo/owl/PRO#PRO_000001020>(?y6), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, ?y7), <http://purl.org/obo/owl/PRO#PRO_000001024>(?y7), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, ?y8), <http://purl.org/obo/owl/PRO#PRO_000001083>(?y8), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, ?y9), <http://purl.org/obo/owl/PRO#PRO_000001084>(?y9), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, ?y10), <http://purl.org/obo/owl/PRO#PRO_000001289>(?y10), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, ?y11), <http://purl.org/obo/owl/PRO#PRO_000001839>(?y11), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, ?y12), <http://purl.org/obo/owl/PRO#PRO_000001889>(?y12), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, ?y13), <http://purl.org/obo/owl/PRO#PRO_000002978>(?y13), <http://purl.org/obo/owl/obo#lacks_plasma_membrane_part>(?x, ?y14), <http://purl.org/obo/owl/PRO#PRO_000002981>(?y14).
            "
        );

        assert_eq!(
            is_positive_reliance(&mut RuleMemoization::new(&rules.iter().collect()), 0, 1, None).is_some(),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    /*
    #[test]
    fn test_pos_basic() {
        // vlog_posr = [(0, 1)], vlog_restr = []

        let rule1: Rule = aux_parse_rule("A(X, V) :- B(X, Y)").unwrap().into();
        let rule2: Rule = aux_parse_rule("C(X, Y) :- A(X, Y)").unwrap().into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_pos_basic_2() {
        // vlog_posr = [(0, 1)], vlog_restr = []

        let rule1: Rule = aux_parse_rule("A(X, X), C(X, X) :- B(X, Y)")
            .unwrap()
            .into();
        let rule2: Rule = aux_parse_rule("D(X, Y) :- A(X, Y), C(Y, X)")
            .unwrap()
            .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_pos_null_1() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = aux_parse_rule("H(X, Y) :- B(X)").unwrap().into();
        let rule2: Rule = aux_parse_rule("C(Y) :- H(Y, Y)").unwrap().into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_pos_null_2() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = aux_parse_rule("H(X, V) :- B(X)").unwrap().into();
        let rule2: Rule = aux_parse_rule("D(X, Y) :- H(X, Y), C(Y)").unwrap().into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_pos_phi2Ia() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = aux_parse_rule("A(X, Y), R(X) :- A(X, Y)").unwrap().into();
        let rule2: Rule = aux_parse_rule("B(X, Y) :- A(X, Y)").unwrap().into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_pos_phi2Ia_ext() {
        // vlog_posr = [(0, 1)], vlog_restr = []

        let rule1: Rule = aux_parse_rule("A(X, X), R(X) :- B(X)").unwrap().into();
        let rule2: Rule = aux_parse_rule("C(X, Y) :- A(X, Y), A(Y, X)")
            .unwrap()
            .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_pos_psi1Ia_ext() {
        // vlog_posr = [(0, 1)], vlog_restr = [(0, 0)]

        let rule1: Rule = aux_parse_rule("H(X, X), H(X, V), C(V, X) :- C(X, X), B(X)")
            .unwrap()
            .into();
        let rule2: Rule = aux_parse_rule("D(X, Y) :- H(X, Y), C(Y, X), H(X, X)")
            .unwrap()
            .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_pos_psi1Ia_phi1() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = aux_parse_rule("A(V, V) :- A(X, X)").unwrap().into();
        let rule2: Rule = aux_parse_rule("C(Y) :- A(Y, Y)").unwrap().into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_pos_psi1Ia_phi1phi22() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = aux_parse_rule("H(X, V), A(V, V) :- A(X, X), B(X)")
            .unwrap()
            .into();
        let rule2: Rule = aux_parse_rule("C(X) :- H(X, X), H(X, Y)").unwrap().into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_pos_psi1Ia_phi22() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = aux_parse_rule("H(X, V) :- B(X)").unwrap().into();
        let rule2: Rule = aux_parse_rule("C(X) :- H(X, X), H(X, Y)").unwrap().into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_pos_psi2Ib_phi1() {
        // vlog_posr = [(1, 0)], vlog_restr = []

        let rule1: Rule = aux_parse_rule("B(X, Y) :- A(X, Y)").unwrap().into();
        let rule2: Rule = aux_parse_rule("A(X, V) :- B(X, Y)").unwrap().into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_pos_psi2Ib_phi22() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = aux_parse_rule("H(X, X) :- A(X)").unwrap().into();
        let rule2: Rule = aux_parse_rule("B(X, X) :- B(X, Y), H(X, Y)")
            .unwrap()
            .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_pos_psi2Ib_psi1() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = aux_parse_rule("H(X, V), T(Y, V) :- A(X, Y)")
            .unwrap()
            .into();
        let rule2: Rule = aux_parse_rule("T(W, Y) :- H(X, Y)").unwrap().into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_pos_thesis() {
        // vlog_posr = [(0, 1)], vlog_restr = []

        let rule1: Rule = aux_parse_rule("P(X1, V), Q(V, Y1) :- A(X1, Y1)")
            .unwrap()
            .into();
        let rule2: Rule = aux_parse_rule("B(W, W) :- P(X2, X2), P(X2, Y2), Q(Y2, X2)")
            .unwrap()
            .into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            true /* (suggestive test-case name + according to VLog) */
        )
    }

    #[test]
    fn test_pos_unif_1() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = aux_parse_rule("H(X, X, const) :- B(X)").unwrap().into();
        let rule2: Rule = aux_parse_rule("C(Y) :- H(Y, other, Y)").unwrap().into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }

    #[test]
    fn test_pos_unif_2() {
        // vlog_posr = [], vlog_restr = []

        let rule1: Rule = aux_parse_rule("H(X, V, const) :- B(X)").unwrap().into();
        let rule2: Rule = aux_parse_rule("C(Y, Z) :- H(Y, Z, Z)").unwrap().into();
        let rule1 = rule1.prime(1);

        let mut result = RelianceResult::default();

        assert_eq!(
            trace_check_positive_dependency(0, TEST_VERBOSITY, &rule1, &rule2)
                && trace_check_positive_reliance(0, TEST_VERBOSITY, &rule1, &rule2, &mut result),
            false /* (according to VLog) */
        )
    }
    */
}
