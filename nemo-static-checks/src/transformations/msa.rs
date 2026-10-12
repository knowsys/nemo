//! This module defines [TransformationMSA].
use itertools::Itertools;
use std::collections::HashSet;

use nemo::rule_model::{
    components::{
        atom::Atom,
        literal::Literal,
        rule::Rule,
        statement::Statement,
        tag::Tag,
        term::{
            Term,
            primitive::{Primitive, variable::Variable},
        },
    },
    error::ValidationReport,
    pipeline::{commit::ProgramCommit, transformations::ProgramTransformation},
    programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
    substitution::Substitution,
};

/// Program transformation used to reduce model-summarizing acyclicity to fact entailment.
#[derive(Debug, Default, Clone, Copy)]
pub struct TransformationMSA {}

fn function_predicates<'a>(
    existential_variables: impl Iterator<Item = &'a Variable>,
    rule_index: usize,
) -> impl Iterator<Item = Tag> {
    existential_variables
        .enumerate()
        .map(move |(variable_index, _)| Tag::new(format!("_msa_F_{rule_index}_{variable_index}")))
}

fn modified_msa_rule<'a>(
    rule: &Rule,
    rule_index: usize,
    s_pred: &Tag,
    f_preds: impl Iterator<Item = Tag>,
    existential_variables: impl Iterator<Item = &'a Variable>,
) -> Rule {
    let mut ret_val: Rule = rule.clone();

    let head_of_rule_mut: &mut Vec<Atom> = ret_val.head_mut();
    let frontier_vars: HashSet<&Variable> = rule.frontier_variables().collect();
    let mut msa_sub_for_rule: Substitution = Substitution::default();

    f_preds.zip(existential_variables).enumerate().for_each(
        |(variable_index, (f_pred, ex_var))| {
            let ex_term: Term = Term::from((*ex_var).clone());
            let f_atom: Atom = Atom::new(f_pred, Vec::from([ex_term.clone()]));
            head_of_rule_mut.push(f_atom);
            frontier_vars.iter().for_each(|var| {
                let var_as_term: Term = Term::from((*var).clone());
                let s_atom: Atom =
                    Atom::new(s_pred.clone(), Vec::from([var_as_term, ex_term.clone()]));
                head_of_rule_mut.push(s_atom);
            });

            let cons_name: String = format!("_msa_c_{rule_index}_{variable_index}");
            let cons: Term = Term::from(cons_name);
            let var_as_prim: Primitive = Primitive::from((*ex_var).clone());
            msa_sub_for_rule.insert(var_as_prim, cons);
        },
    );

    msa_sub_for_rule.apply(&mut ret_val);
    ret_val
}

fn generic_rules(s_pred: &Tag, d_pred: &Tag, x1_term: &Term, x2_term: &Term) -> (Rule, Rule) {
    let x3_var: Variable = Variable::universal("_msa_x3");
    let x3_term: Term = Term::from(x3_var);

    let s_atom_1_2: Atom = Atom::new(
        s_pred.clone(),
        Vec::from([x1_term.clone(), x2_term.clone()]),
    );
    let s_lit_1_2: Literal = Literal::Positive(s_atom_1_2);

    let s_atom_2_3: Atom = Atom::new(
        s_pred.clone(),
        Vec::from([x2_term.clone(), x3_term.clone()]),
    );
    let s_lit_2_3: Literal = Literal::Positive(s_atom_2_3);

    let d_atom_1_2: Atom = Atom::new(
        d_pred.clone(),
        Vec::from([x1_term.clone(), x2_term.clone()]),
    );
    let d_lit_1_2: Literal = Literal::Positive(d_atom_1_2.clone());

    let d_atom_1_3: Atom = Atom::new(
        d_pred.clone(),
        Vec::from([x1_term.clone(), x3_term.clone()]),
    );

    let rule_s_to_d: Rule = Rule::new(Vec::from([d_atom_1_2]), Vec::from([s_lit_1_2]));
    let rule_d_s_to_d: Rule = Rule::new(Vec::from([d_atom_1_3]), Vec::from([d_lit_1_2, s_lit_2_3]));
    (rule_s_to_d, rule_d_s_to_d)
}

fn f_msa_rules(
    f_preds: impl Iterator<Item = Tag>,
    d_pred: &Tag,
    x1_term: &Term,
    x2_term: &Term,
) -> impl Iterator<Item = Rule> {
    let null_term: Term = Term::from("nullaryPredsNotAllowed");
    let c_pred: Tag = Tag::from("_msa_C");
    let c_atom: Atom = Atom::new(c_pred, Vec::from([null_term]));

    f_preds.map(move |f_pred| {
        let f_atom_1: Atom = Atom::new(f_pred.clone(), Vec::from([x1_term.clone()]));
        let f_lit_1: Literal = Literal::Positive(f_atom_1);
        let f_atom_2: Atom = Atom::new(f_pred, Vec::from([x2_term.clone()]));
        let f_lit_2: Literal = Literal::Positive(f_atom_2);
        let d_atom_1_2: Atom = Atom::new(
            d_pred.clone(),
            Vec::from([x1_term.clone(), x2_term.clone()]),
        );
        let d_lit_1_2: Literal = Literal::Positive(d_atom_1_2);
        Rule::new(
            Vec::from([c_atom.clone()]),
            Vec::from([f_lit_1, d_lit_1_2, f_lit_2]),
        )
    })
}

impl ProgramTransformation for TransformationMSA {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit: ProgramCommit = program.fork();

        let s_pred: Tag = Tag::from("_msa_S");

        let d_pred: Tag = Tag::from("_msa_D");

        let x1_var: Variable = Variable::universal("_msa_x1");
        let x1_term: Term = Term::from(x1_var);
        let x2_var: Variable = Variable::universal("_msa_x2");
        let x2_term: Term = Term::from(x2_var);

        let (rule_s_to_d, rule_d_s_to_d): (Rule, Rule) =
            generic_rules(&s_pred, &d_pred, &x1_term, &x2_term);
        commit.add_rule(rule_s_to_d);
        commit.add_rule(rule_d_s_to_d);

        for (i, statement) in program.statements().enumerate() {
            let Statement::Rule(rule) = statement else {
                commit.keep(statement);
                continue;
            };
            if rule.existential_variables().next().is_none() {
                commit.keep(statement);
                continue;
            }
            let f_preds = function_predicates(rule.existential_variables().unique(), i);
            let (f_preds_1, f_preds_2) = f_preds.tee();

            let modified_msa_rule: Rule = modified_msa_rule(
                rule,
                i,
                &s_pred,
                f_preds_1,
                rule.existential_variables().unique(),
            );
            commit.add_rule(modified_msa_rule);

            f_msa_rules(f_preds_2, &d_pred, &x1_term, &x2_term).for_each(|f_msa_rule| {
                commit.add_rule(f_msa_rule);
            });
        }

        commit.submit()
    }
}
