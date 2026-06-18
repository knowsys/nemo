use super::ProgramTransformation;
use crate::rule_model::components::{
    atom::Atom,
    literal::Literal,
    rule::Rule,
    statement::Statement,
    tag::Tag,
    term::{
        Term,
        primitive::{Primitive, variable::Variable},
    },
};
use crate::rule_model::error::ValidationReport;
use crate::rule_model::pipeline::commit::ProgramCommit;
use crate::rule_model::programs::handle::ProgramHandle;
use crate::rule_model::programs::{ProgramRead, ProgramWrite};
use crate::rule_model::substitution::Substitution;

use std::collections::HashSet;

#[derive(Debug, Default, Clone, Copy)]
pub struct TransformationMSA {}

fn f_preds_for_ex_vars(ex_vars_of_rule: &HashSet<&Variable>, rule_name: usize) -> Vec<Tag> {
    ex_vars_of_rule
        .iter()
        .enumerate()
        .fold(Vec::<Tag>::new(), |mut f_preds, (i, _)| {
            let f_name: String = format!("_msa_F_{}_{}", rule_name, i);
            let f_pred: Tag = Tag::new(f_name);
            f_preds.push(f_pred);
            f_preds
        })
}

fn ex_vars_as_terms(ex_vars_of_rule: &HashSet<&Variable>) -> Vec<Term> {
    ex_vars_of_rule
        .iter()
        .fold(Vec::<Term>::new(), |mut terms, var| {
            let term: Term = Term::from((*var).clone());
            terms.push(term);
            terms
        })
}

fn modified_msa_rule(
    rule: &Rule,
    rule_name: usize,
    s_pred: &Tag,
    f_preds: &[Tag],
    ex_vars_of_rule: HashSet<&Variable>,
) -> Rule {
    let mut ret_val: Rule = rule.clone();
    let head_of_rule_mut: &mut Vec<Atom> = ret_val.head_mut();
    let ex_vars_as_terms: Vec<Term> = ex_vars_as_terms(&ex_vars_of_rule);
    let preds_zip_terms: Vec<(Tag, Term)> = f_preds.iter().cloned().zip(ex_vars_as_terms).collect();
    let frontier_vars: HashSet<&Variable> = rule.frontier_variables();

    preds_zip_terms.into_iter().for_each(|(f_pred, ex_term)| {
        let f_atom: Atom = Atom::new(f_pred, Vec::from([ex_term.clone()]));
        head_of_rule_mut.push(f_atom);
        frontier_vars.iter().for_each(|var| {
            let var_as_term: Term = Term::from((*var).clone());
            let s_atom: Atom = Atom::new(s_pred.clone(), Vec::from([var_as_term, ex_term.clone()]));
            head_of_rule_mut.push(s_atom);
        });
    });
    let mut msa_sub_for_rule: Substitution = Substitution::default();
    ex_vars_of_rule.iter().enumerate().for_each(|(i, var)| {
        let cons_name: String = format!("_msa_c_{}_{}", rule_name, i);
        let cons: Term = Term::from(cons_name);
        let var_as_prim: Primitive = Primitive::from((*var).clone());
        msa_sub_for_rule.insert(var_as_prim, cons);
    });
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

fn f_msa_rules(f_preds: &[Tag], d_pred: &Tag, x1_term: &Term, x2_term: &Term) -> Vec<Rule> {
    let null_term: Term = Term::from("nullaryPredsNotAllowed");
    let c_pred: Tag = Tag::from("_msa_C");
    let c_atom: Atom = Atom::new(c_pred, Vec::from([null_term]));

    f_preds
        .iter()
        .fold(Vec::<Rule>::new(), |mut f_msa_rules, f_pred| {
            let f_atom_1: Atom = Atom::new(f_pred.clone(), Vec::from([x1_term.clone()]));
            let f_lit_1: Literal = Literal::Positive(f_atom_1);
            let f_atom_2: Atom = Atom::new(f_pred.clone(), Vec::from([x2_term.clone()]));
            let f_lit_2: Literal = Literal::Positive(f_atom_2);
            let d_atom_1_2: Atom = Atom::new(
                d_pred.clone(),
                Vec::from([x1_term.clone(), x2_term.clone()]),
            );
            let d_lit_1_2: Literal = Literal::Positive(d_atom_1_2);
            let f_msa_rule: Rule = Rule::new(
                Vec::from([c_atom.clone()]),
                Vec::from([f_lit_1, d_lit_1_2, f_lit_2]),
            );
            f_msa_rules.push(f_msa_rule);
            f_msa_rules
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
            if let Statement::Rule(rule) = statement {
                let ex_vars_of_rule: HashSet<&Variable> = rule.existential_variables();
                if ex_vars_of_rule.is_empty() {
                    commit.keep(statement);
                    continue;
                }
                let f_preds: Vec<Tag> = f_preds_for_ex_vars(&ex_vars_of_rule, i);
                let modified_msa_rule: Rule =
                    modified_msa_rule(rule, i, &s_pred, &f_preds, ex_vars_of_rule);
                commit.add_rule(modified_msa_rule);
                let f_msa_rules: Vec<Rule> = f_msa_rules(&f_preds, &d_pred, &x1_term, &x2_term);
                f_msa_rules.into_iter().for_each(|f_msa_rule| {
                    commit.add_rule(f_msa_rule);
                });
            } else {
                commit.keep(statement);
            }
        }

        commit.submit()
    }
}
