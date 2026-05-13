use nemo::rule_model::{
    components::{
        IterableVariables,
        atom::Atom,
        fact::Fact,
        rule::Rule,
        tag::Tag,
        term::{
            Cyclic, Term,
            function::FunctionTerm,
            primitive::{Primitive, variable::Variable},
        },
    },
    pipeline::transformations::{
        crit_instance::TransformationCriticalInstance, skolem::TransformationSkolemize,
    },
    programs::{ProgramRead, handle::ProgramHandle},
};

use crate::static_checks::cyclicity_checks::{
    Assignment, CoreReasoner, CyclicityStrategy, StrategySelector, body_for_assignment,
    build_var_index_for_rules, head_for_assignment, predicates_ref, union,
};

use crate::static_checks::rule_properties::RuleProperties;
use crate::static_checks::rule_set::{RuleRefs, RuleSet};

use std::collections::{HashMap, HashSet};

pub struct MFCStrategy;

impl CyclicityStrategy for MFCStrategy {
    fn is_blocked(&self, _rule: &Rule, _ass: &Assignment) -> bool {
        false
    }
}

pub fn mfc_handle(handle: ProgramHandle) -> ProgramHandle {
    handle
        .transform(TransformationSkolemize::default())
        .expect("TransformationSkolemize Error")
}

pub async fn check_cyclicity(handle: ProgramHandle, strat: StrategySelector) -> bool {
    let rule_set: RuleSet = RuleSet(handle.rules().cloned().collect());
    let det_rules: Vec<&Rule> = rule_set.0.iter().collect();

    // let det_rules: Vec<&Rule> = rules
    //     .iter()
    //     .filter(|rule| rule.is_deterministic())
    //     .copied()
    //     .collect();

    for rule in det_rules.iter() {
        if check_cyclicity_for_rule(rule, &det_rules, &strat).await {
            return true;
        }
    }
    false
}

pub async fn check_cyclicity_for_rule(
    rule: &Rule,
    rule_set: &Vec<&Rule>,
    strat: &StrategySelector,
) -> bool {
    let unique_ass: Assignment = unique_ass(rule);

    let mfc_set = union(
        head_for_assignment(rule, &unique_ass),
        body_for_assignment(rule, &unique_ass),
    );

    let preds: HashSet<&Tag> = predicates_ref(rule_set);
    let var_per_atom_idx_pos_idx_per_rule = build_var_index_for_rules(rule_set);

    let strat: &dyn CyclicityStrategy = match strat {
        StrategySelector::MFC => &MFCStrategy,
        _ => unreachable!(),
    };

    let mut reasoner: CoreReasoner =
        CoreReasoner::new(&preds, rule_set, &var_per_atom_idx_pos_idx_per_rule, strat);

    reasoner.run_saturating(mfc_set);

    todo!();
}

fn unique_ass(rule: &'_ Rule) -> Assignment<'_> {
    let count = 0;
    rule.variables().fold(Assignment::new(), |mut ass, var| {
        let fresh_const = Term::from(format!("__FR_CONST_{count}__"));
        ass.insert(var, fresh_const);
        ass
    })
}
