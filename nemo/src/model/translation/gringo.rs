use std::collections::{HashMap, HashSet};

use nemo_physical::datavalues::AnyDataValue;

use crate::{
    model::{
        chase_model::{ChaseAtom, ChaseProgram, ChaseRule, VariableAtom},
        Identifier, PrimitiveTerm, Variable,
    },
    program_analysis::analysis::{ProgramAnalysis, RuleAnalysis},
};

use super::{RuleTranslation, TranslationResult};

#[derive(Debug, Copy, Clone)]
pub(crate) struct GringoTranslation {}

struct TermPosition {
    pub rule: usize,
    pub atom: usize,
    pub term: usize,
}

impl TermPosition {
    pub fn new(rule: usize, atom: usize, term: usize) -> Self {
        Self { rule, atom, term }
    }
}

struct SkolemTerm {
    pub predicate: Identifier,
    pub position: TermPosition,
    pub frontier: Vec<Variable>,
}

enum RewrittenTerm {
    Primitive(PrimitiveTerm),
    Skolem(SkolemTerm),
}

struct RewrittenAtom {
    pub predicate: Identifier,
    pub terms: Vec<RewrittenTerm>,
}

impl GringoTranslation {
    fn existential_positions(program: &ChaseProgram) -> HashMap<Identifier, Vec<TermPosition>> {
        let mut result = HashMap::new();

        for (rule_index, rule) in program.rules().iter().enumerate() {
            for (atom_index, atom) in rule.head().iter().enumerate() {
                for (term_index, term) in atom.terms().iter().enumerate() {
                    if let PrimitiveTerm::Variable(Variable::Existential(_variable)) = term {
                        let position_list =
                            result.entry(atom.predicate().clone()).or_insert(Vec::new());
                        position_list.push(TermPosition::new(rule_index, atom_index, term_index));
                    }
                }
            }
        }

        result
    }

    fn skolem_function_name(
        predicate: &str,
        rule_index: usize,
        atom_index: usize,
        term_index: usize,
    ) -> String {
        format!("{predicate}_{rule_index}_{atom_index}_{term_index}")
    }

    fn is_existential_position(
        positions: &HashMap<Identifier, Vec<TermPosition>>,
        predicate: &Identifier,
        term_position: usize,
    ) -> bool {
        if let Some(existential_positions) = positions.get(predicate) {
            for position in existential_positions {
                if position.term == term_position {
                    return true;
                }
            }
        }

        false
    }

    fn write_variable(variable: &Variable) -> String {
        if let Some(name) = variable.name() {
            name.to_owned().to_ascii_uppercase()
        } else {
            String::from("_")
        }
    }

    fn write_unused_variable(variable: &Variable) -> String {
        if let Some(name) = variable.name() {
            format!("_{}", name.to_owned().to_ascii_uppercase())
        } else {
            String::from("_")
        }
    }

    fn write_groundterm(term: &AnyDataValue) -> String {
        term.to_string()
    }

    fn write_primitive(term: &PrimitiveTerm) -> String {
        match term {
            PrimitiveTerm::GroundTerm(term) => Self::write_groundterm(term),
            PrimitiveTerm::Variable(variable) => Self::write_variable(variable),
        }
    }

    fn write_skolemterm(term: &SkolemTerm) -> String {
        let mut result = String::from("");

        result += &Self::skolem_function_name(
            &term.predicate.name().to_ascii_lowercase(),
            term.position.rule,
            term.position.atom,
            term.position.term,
        );

        if term.frontier.is_empty() {
            result += "_constant";
            return result;
        }

        result += "(";
        for (index, frontier) in term.frontier.iter().enumerate() {
            result += &Self::write_variable(frontier);

            if index < term.frontier.len() - 1 {
                result += ", ";
            }
        }

        result += ")";

        result
    }

    fn write_rewritten(term: &RewrittenTerm) -> String {
        match term {
            RewrittenTerm::Primitive(term) => Self::write_primitive(term),
            RewrittenTerm::Skolem(term) => Self::write_skolemterm(term),
        }
    }

    fn write_variable_atom(atom: &VariableAtom) -> String {
        let mut result = String::new();

        result += &atom.predicate().name().to_ascii_lowercase();
        result += "(";

        for (index, variable) in atom.terms().iter().enumerate() {
            result += &Self::write_variable(variable);

            if index < atom.terms().len() - 1 {
                result += ", ";
            }
        }

        result += ")";
        result
    }

    fn write_rewritten_atom(atom: &RewrittenAtom) -> String {
        let mut result = String::new();

        result += &atom.predicate.name().to_ascii_lowercase();
        result += "(";

        for (index, term) in atom.terms.iter().enumerate() {
            result += &Self::write_rewritten(term);

            if index < atom.terms.len() - 1 {
                result += ", ";
            }
        }

        result += ")";
        result
    }
}

impl GringoTranslation {
    fn statement_rule(
        rule_index: usize,
        rule: &ChaseRule,
        analysis: &RuleAnalysis,
        existential_positions: &HashMap<Identifier, Vec<TermPosition>>,
    ) -> String {
        let mut result = String::from("");
        let frontier = analysis
            .frontier
            .iter()
            .cloned()
            .collect::<HashSet<_>>()
            .intersection(&analysis.positive_body_variables)
            .cloned()
            .collect::<Vec<_>>();

        for (atom_index, head_atom) in rule.head().iter().enumerate() {
            let predicate = head_atom.predicate().clone();
            let rewritten_terms = head_atom
                .terms()
                .iter()
                .enumerate()
                .map(|(index, term)| {
                    if let PrimitiveTerm::Variable(Variable::Existential(_)) = term {
                        let position = TermPosition::new(rule_index, atom_index, index);
                        RewrittenTerm::Skolem(SkolemTerm {
                            predicate: predicate.clone(),
                            position,
                            frontier: frontier.clone(),
                        })
                    } else {
                        RewrittenTerm::Primitive(term.clone())
                    }
                })
                .collect::<Vec<_>>();

            result += &Self::write_rewritten_atom(&RewrittenAtom {
                predicate: predicate.clone(),
                terms: rewritten_terms,
            });

            result += " :- ";

            for (atom_index, body_atom) in rule.positive_body().iter().enumerate() {
                result += &Self::write_variable_atom(body_atom);

                if atom_index < rule.positive_body().len() - 1 {
                    result += ", ";
                }
            }

            result += " .";

            if atom_index < rule.head().len() - 1 {
                result += "\n";
            }
        }

        result
    }

    fn statement_output(predicate: &str) -> String {
        format!(".output {predicate}(IO=file, filename=\"results/{predicate}.csv, delimiter=\",\")")
    }
}

impl GringoTranslation {
    fn rule_statements(
        program: &ChaseProgram,
        analysis: &ProgramAnalysis,
        existential_positions: &HashMap<Identifier, Vec<TermPosition>>,
        result_rules: &mut TranslationResult,
    ) {
        for (rule_index, (rule, analysis)) in program
            .rules()
            .iter()
            .zip(analysis.rule_analysis.iter())
            .enumerate()
        {
            result_rules.push_statement(Self::statement_rule(
                rule_index,
                rule,
                analysis,
                existential_positions,
            ))
        }
    }

    fn output_statements(
        _program: &ChaseProgram,
        analysis: &ProgramAnalysis,
        result_rules: &mut TranslationResult,
    ) {
        for predicate in &analysis.derived_predicates {
            result_rules.push_statement(Self::statement_output(&predicate.name()));
        }
    }
}

impl RuleTranslation for GringoTranslation {
    fn translate(
        program: &ChaseProgram,
        analysis: &ProgramAnalysis,
    ) -> Option<Vec<TranslationResult>> {
        let mut result_rules = TranslationResult::new("program.lp");

        let existential_positions = Self::existential_positions(program);

        Self::rule_statements(program, analysis, &existential_positions, &mut result_rules);

        Some(vec![result_rules])
    }
}
