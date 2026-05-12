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
    Assignment, body_for_assignment, head_for_assignment, union,
};

use crate::static_checks::rule_properties::RuleProperties;
use crate::static_checks::rule_set::{RuleRefs, RuleSet};

use std::collections::{HashMap, HashSet};

#[derive(Clone)]
pub enum CyclicityVariant {
    SkolemMFC,
    SkolemDMFC,
    SkolemRPC,
    SkolemDRPC,
    // SkolemRestricted(&'b HashSet<&'a Tag>, &'b Vec<&'a Rule>),
}

pub fn mfc_handle(handle: ProgramHandle) -> ProgramHandle {
    handle
        .transform(TransformationSkolemize::default())
        .expect("TransformationSkolemize Error")
}

pub async fn check_cyclicity(handle: ProgramHandle, variant: CyclicityVariant) -> bool {
    let rule_set: RuleSet = RuleSet(handle.rules().cloned().collect());
    let rules: Vec<&Rule> = rule_set.0.iter().collect();

    let det_rules: Vec<&Rule> = rules
        .iter()
        .filter(|rule| rule.is_deterministic())
        .copied()
        .collect();

    for rule in det_rules.iter() {
        if check_cyclicity_for_rule(rule, &det_rules, &variant).await {
            return true;
        }
    }
    false
}

pub async fn check_cyclicity_for_rule(
    rule: &Rule,
    rule_set: &[&Rule],
    variant: &CyclicityVariant,
) -> bool {
    let unique_ass: Assignment = unique_ass(rule);

    let mfc_set = union(
        head_for_assignment(rule, &unique_ass),
        body_for_assignment(rule, &unique_ass),
    );

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
