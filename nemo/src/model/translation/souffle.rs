use std::{
    collections::{HashMap, HashSet},
    fmt::format,
};

use nemo_physical::datavalues::{AnyDataValue, DataValue};

use crate::{
    io::formats::import_export::ImportExportHandlers,
    model::{
        chase_model::{ChaseAtom, ChaseProgram, ChaseRule, PrimitiveAtom, VariableAtom},
        FileFormat, Identifier, PrimitiveTerm, Program, Term, Variable,
    },
    program_analysis::analysis::{self, ProgramAnalysis, RuleAnalysis},
};

use super::{util::number_to_letters, RuleTranslation, TranslationResult};

#[derive(Debug, Copy, Clone)]
pub(crate) struct SouffleTranslation {}

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
    Base(Identifier, PrimitiveTerm),
}

struct RewrittenAtom {
    pub predicate: Identifier,
    pub terms: Vec<RewrittenTerm>,
}

impl SouffleTranslation {
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

    fn symbol_list(elements: usize) -> String {
        let mut result = String::new();

        for argument_index in 0..elements {
            result += &number_to_letters(argument_index);
            result += ": symbol";

            if argument_index < elements - 1 {
                result += ", ";
            }
        }

        result
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
            name.to_owned()
        } else {
            String::from("_")
        }
    }

    fn write_unused_variable(variable: &Variable) -> String {
        if let Some(name) = variable.name() {
            format!("_{}", name.to_owned())
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
        let mut result = String::from("$");

        result += &Self::skolem_function_name(
            &term.predicate.name(),
            term.position.rule,
            term.position.atom,
            term.position.term,
        );
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

    fn write_skolem_baseterm(predicate: &Identifier, term: &PrimitiveTerm) -> String {
        let mut result = format!("${predicate}_base(");
        result += &Self::write_primitive(term);
        result += ")";

        result
    }

    fn write_rewritten(term: &RewrittenTerm) -> String {
        match term {
            RewrittenTerm::Primitive(term) => Self::write_primitive(term),
            RewrittenTerm::Skolem(term) => Self::write_skolemterm(term),
            RewrittenTerm::Base(predicate, term) => Self::write_skolem_baseterm(predicate, term),
        }
    }

    fn write_variable_atom(atom: &VariableAtom, used: &HashSet<Variable>) -> String {
        let mut result = String::new();

        result += &atom.predicate().name();
        result += "(";

        for (index, variable) in atom.terms().iter().enumerate() {
            if used.contains(variable) {
                result += &Self::write_variable(variable);
            } else {
                result += &Self::write_unused_variable(variable);
            }

            if index < atom.terms().len() - 1 {
                result += ", ";
            }
        }

        result += ")";
        result
    }

    fn write_rewritten_atom(atom: &RewrittenAtom) -> String {
        let mut result = String::new();

        result += &atom.predicate.name();
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

impl SouffleTranslation {
    fn statement_declare(predicate: &str, arity: usize) -> String {
        let mut result = format!(".decl {predicate}(");

        result += &Self::symbol_list(arity);

        result += ")";
        result
    }

    fn statement_input(predicate: &str, filename: &str, delimiter: Option<String>) -> String {
        let mut result = format!(".input {predicate}(IO=file, filename=\"{filename}\"");

        if let Some(delimiter) = delimiter {
            result += &format!(", delimiter=\"{delimiter}\"");
        }

        result += ")";
        result
    }

    fn statement_type(predicate: &str, arity: usize, positions: &[TermPosition]) -> String {
        let mut result = format!(".type Skolem_{predicate} = ");

        for position in positions {
            result +=
                &Self::skolem_function_name(predicate, position.rule, position.atom, position.term);
            result += " { ";
            result += &Self::symbol_list(arity);
            result += " } | ";
        }

        result += &format!(" {predicate}_base {{ base: symbol }}");
        result
    }

    fn statement_rule(
        rule_index: usize,
        rule: &ChaseRule,
        analysis: &RuleAnalysis,
        existential_positions: &HashMap<Identifier, Vec<TermPosition>>,
    ) -> String {
        let mut result = String::from("");

        let frontier = analysis
            .head_variables
            .intersection(&analysis.positive_body_variables)
            .cloned()
            .collect::<Vec<_>>();

        let mut variable_count = HashMap::<Variable, usize>::new();
        for body_atom in rule.positive_body() {
            for variable in body_atom.terms() {
                let counter = variable_count.entry(variable.clone()).or_insert(0);
                *counter += 1;
            }
        }
        let mut used_variables = variable_count
            .iter()
            .filter_map(|(variable, count)| {
                if *count > 1 {
                    Some(variable.clone())
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>();
        for variable in &frontier {
            used_variables.insert(variable.clone());
        }

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
                    } else if Self::is_existential_position(
                        existential_positions,
                        &predicate,
                        index,
                    ) {
                        RewrittenTerm::Base(predicate.clone(), term.clone())
                    } else {
                        RewrittenTerm::Primitive(term.clone())
                    }
                })
                .collect::<Vec<_>>();

            result += &Self::write_rewritten_atom(&RewrittenAtom {
                predicate: predicate.clone(),
                terms: rewritten_terms,
            });

            if atom_index < rule.head().len() - 1 {
                result += ", ";
            }
        }

        result += " :- ";

        for (atom_index, body_atom) in rule.positive_body().iter().enumerate() {
            result += &Self::write_variable_atom(body_atom, &used_variables);

            if atom_index < rule.positive_body().len() - 1 {
                result += ", ";
            }
        }

        result += " .";

        result
    }

    fn statement_output(predicate: &str) -> String {
        format!(".output {predicate}(IO=file, filename=\"results/{predicate}.csv, delimiter=\",\")")
    }
}

impl SouffleTranslation {
    fn declare_statements(
        _program: &ChaseProgram,
        analysis: &ProgramAnalysis,
        result_rules: &mut TranslationResult,
    ) {
        for (predicate, arity) in &analysis.all_predicates {
            if predicate
                .name()
                .starts_with("FRESH_HEAD_MATCHES_IDENTIFIER_FOR_RULE_")
            {
                continue;
            }

            result_rules.push_statement(Self::statement_declare(&predicate.name(), *arity));
        }
    }

    fn import_statements(
        program: &ChaseProgram,
        _analysis: &ProgramAnalysis,
        result_rules: &mut TranslationResult,
    ) -> Option<()> {
        for (predicate, handler) in program.imports() {
            let filename = handler.resource()?;

            let delimiter = match handler.file_format() {
                FileFormat::CSV => Some(String::from(",")),
                FileFormat::DSV => None,
                FileFormat::TSV => Some(String::from("\t")),
                FileFormat::RDF(_) => None,
            };

            result_rules.push_statement(Self::statement_input(
                &predicate.name(),
                &filename,
                delimiter,
            ));
        }

        Some(())
    }

    fn type_statements(
        _program: &ChaseProgram,
        analysis: &ProgramAnalysis,
        existential_positions: &HashMap<Identifier, Vec<TermPosition>>,
        result_rules: &mut TranslationResult,
    ) {
        for (predicate, positions) in existential_positions {
            let arity = *analysis
                .all_predicates
                .get(predicate)
                .expect("Prediate must be known");
            result_rules.push_statement(Self::statement_type(&predicate.name(), arity, positions))
        }
    }

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

impl RuleTranslation for SouffleTranslation {
    fn translate(
        program: &ChaseProgram,
        analysis: &ProgramAnalysis,
    ) -> Option<Vec<TranslationResult>> {
        let mut result_rules = TranslationResult::new("program.dl");

        let existential_positions = Self::existential_positions(program);

        Self::type_statements(program, analysis, &existential_positions, &mut result_rules);
        result_rules.empty_line();
        Self::declare_statements(program, analysis, &mut result_rules);
        result_rules.empty_line();
        Self::import_statements(program, analysis, &mut result_rules)?;
        result_rules.empty_line();
        Self::rule_statements(program, analysis, &existential_positions, &mut result_rules);
        result_rules.empty_line();
        Self::output_statements(program, analysis, &mut result_rules);

        Some(vec![result_rules])
    }
}
