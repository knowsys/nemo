//! This module defines [TransformationMSA].

use std::collections::HashSet;

use super::ProgramTransformation;
use crate::rule_model::{
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
    pipeline::commit::ProgramCommit,
    programs::{ProgramRead, ProgramWrite, handle::ProgramHandle},
    substitution::Substitution,
};

/// Program transformation used to reduce model-summarizing acyclicity to fact entailment.
#[derive(Debug, Default, Clone, Copy)]
pub struct TransformationMSA;

fn function_predicates(existential_variables: &[&Variable], rule_index: usize) -> Vec<Tag> {
    existential_variables
        .iter()
        .enumerate()
        .map(|(variable_index, _)| Tag::new(format!("_msa_F_{rule_index}_{variable_index}")))
        .collect()
}

fn modified_msa_rule(
    rule: &Rule,
    rule_index: usize,
    s_predicate: &Tag,
    existential_variables: &[&Variable],
) -> (Rule, Vec<Tag>) {
    let mut result = rule.clone();
    let function_predicates = function_predicates(existential_variables, rule_index);
    let frontier_variables = rule.frontier_variables().collect::<HashSet<_>>();

    for (function_predicate, existential_variable) in
        function_predicates.iter().zip(existential_variables.iter())
    {
        let existential_term = Term::from((*existential_variable).clone());
        result.head_mut().push(Atom::new(
            function_predicate.clone(),
            [existential_term.clone()],
        ));

        for frontier_variable in &frontier_variables {
            result.head_mut().push(Atom::new(
                s_predicate.clone(),
                [
                    Term::from((*frontier_variable).clone()),
                    existential_term.clone(),
                ],
            ));
        }
    }

    let mut substitution = Substitution::default();
    for (variable_index, variable) in existential_variables.iter().enumerate() {
        substitution.insert(
            Primitive::from((*variable).clone()),
            Term::from(format!("_msa_c_{rule_index}_{variable_index}")),
        );
    }
    substitution.apply(&mut result);

    (result, function_predicates)
}

fn generic_rules(s_predicate: &Tag, d_predicate: &Tag, x1: &Term, x2: &Term) -> [Rule; 2] {
    let x3 = Term::from(Variable::universal("_msa_x3"));

    let s_x1_x2 = Literal::Positive(Atom::new(s_predicate.clone(), [x1.clone(), x2.clone()]));
    let s_x2_x3 = Literal::Positive(Atom::new(s_predicate.clone(), [x2.clone(), x3.clone()]));
    let d_x1_x2 = Atom::new(d_predicate.clone(), [x1.clone(), x2.clone()]);
    let d_x1_x3 = Atom::new(d_predicate.clone(), [x1.clone(), x3]);

    [
        Rule::new([d_x1_x2.clone()].into(), [s_x1_x2].into()),
        Rule::new(
            [d_x1_x3].into(),
            [Literal::Positive(d_x1_x2), s_x2_x3].into(),
        ),
    ]
}

fn function_rules(
    function_predicates: &[Tag],
    d_predicate: &Tag,
    x1: &Term,
    x2: &Term,
) -> impl Iterator<Item = Rule> {
    let c_atom = Atom::new(Tag::from("_msa_C"), [Term::from("nullaryPredsNotAllowed")]);

    function_predicates.iter().map(move |function_predicate| {
        let f_x1 = Literal::Positive(Atom::new(function_predicate.clone(), [x1.clone()]));
        let f_x2 = Literal::Positive(Atom::new(function_predicate.clone(), [x2.clone()]));
        let d_x1_x2 = Literal::Positive(Atom::new(d_predicate.clone(), [x1.clone(), x2.clone()]));

        Rule::new([c_atom.clone()].into(), [f_x1, d_x1_x2, f_x2].into())
    })
}

impl ProgramTransformation for TransformationMSA {
    fn apply(self, program: &ProgramHandle) -> Result<ProgramHandle, ValidationReport> {
        let mut commit: ProgramCommit = program.fork();

        let s_predicate = Tag::from("_msa_S");
        let d_predicate = Tag::from("_msa_D");
        let x1 = Term::from(Variable::universal("_msa_x1"));
        let x2 = Term::from(Variable::universal("_msa_x2"));

        for rule in generic_rules(&s_predicate, &d_predicate, &x1, &x2) {
            commit.add_rule(rule);
        }

        for (rule_index, statement) in program.statements().enumerate() {
            let Statement::Rule(rule) = statement else {
                commit.keep(statement);
                continue;
            };

            let existential_variables = rule.existential_variables().collect::<Vec<_>>();
            if existential_variables.is_empty() {
                commit.keep(statement);
                continue;
            }

            let (modified_rule, function_predicates) =
                modified_msa_rule(rule, rule_index, &s_predicate, &existential_variables);
            commit.add_rule(modified_rule);

            for rule in function_rules(&function_predicates, &d_predicate, &x1, &x2) {
                commit.add_rule(rule);
            }
        }

        commit.submit()
    }
}
