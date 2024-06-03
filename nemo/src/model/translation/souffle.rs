use std::{
    collections::{HashMap, HashSet},
    env::var,
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

#[derive(Debug, Clone)]
struct SkolemPredicate {
    pub predicate: Identifier,
    pub rule: usize,
    pub atom: usize,
    pub term: usize,
}

impl SkolemPredicate {
    pub fn new(predicate: Identifier, rule: usize, atom: usize, term: usize) -> Self {
        Self {
            predicate,
            rule,
            atom,
            term,
        }
    }
}

#[derive(Debug, Clone)]
struct SkolemTerm {
    pub predicate: SkolemPredicate,
    pub frontier: Vec<RewrittenTerm>,
}

#[derive(Debug, Clone)]
enum RewrittenTerm {
    Primitive(PrimitiveTerm),
    Skolem(SkolemTerm),
    Base(PrimitiveTerm),
}

struct RewrittenAtom {
    pub predicate: Identifier,
    pub terms: Vec<RewrittenTerm>,
}

impl SouffleTranslation {
    fn find_variable(atoms: &[VariableAtom], variable: &Variable) -> Option<(Identifier, usize)> {
        for atom in atoms {
            for (variable_index, atom_variable) in atom.terms().iter().enumerate() {
                if variable == atom_variable {
                    return Some((atom.predicate(), variable_index));
                }
            }
        }

        None
    }

    fn find_variable_primitive(
        atoms: &[PrimitiveAtom],
        variable: &Variable,
    ) -> Option<(Identifier, usize)> {
        for atom in atoms {
            for (variable_index, atom_variable) in atom.terms().iter().enumerate() {
                if PrimitiveTerm::Variable(variable.clone()) == *atom_variable {
                    return Some((atom.predicate(), variable_index));
                }
            }
        }

        None
    }

    fn existential_positions(program: &ChaseProgram) -> HashSet<(Identifier, usize)> {
        let mut result = HashSet::new();
        for rule in program.rules() {
            for atom in rule.head() {
                for (term_index, term) in atom.terms().iter().enumerate() {
                    // if let PrimitiveTerm::Variable(Variable::Existential(_variable)) = term {
                    result.insert((atom.predicate(), term_index));
                    // }
                }
            }
        }

        // let mut previous_size = result.len() + 1;

        // while result.len() != previous_size {
        //     previous_size = result.len();
        //     for rule in program.rules() {
        //         for atom in rule.head() {
        //             for (term_index, term) in atom.terms().iter().enumerate() {
        //                 if let PrimitiveTerm::Variable(variable) = term {
        //                     if let Some(body_position) =
        //                         Self::find_variable(&rule.positive_body(), variable)
        //                     {
        //                         if result.contains(&body_position) {
        //                             result.insert((atom.predicate().clone(), term_index));
        //                         }
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // }

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

    fn argument_declaration(
        predicate: &Identifier,
        arity: usize,
        existential_positions: &HashSet<(Identifier, usize)>,
    ) -> String {
        let mut result = String::new();

        for argument_index in 0..arity {
            result += &number_to_letters(argument_index);

            if existential_positions.contains(&(predicate.clone(), argument_index)) {
                result += ": Skolem";
            } else {
                result += ": symbol";
            }

            // result += ": Existential";

            if argument_index < arity - 1 {
                result += ", ";
            }
        }

        result
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
            &term.predicate.predicate.name(),
            term.predicate.rule,
            term.predicate.atom,
            term.predicate.term,
        );
        result += "(";
        for (index, frontier) in term.frontier.iter().enumerate() {
            result += &Self::write_rewritten(frontier);

            if index < term.frontier.len() - 1 {
                result += ", ";
            }
        }

        result += ")";

        result
    }

    fn write_skolem_baseterm(term: &PrimitiveTerm) -> String {
        let mut result = format!("$SkolemBase(");
        // let mut result = String::from("");
        result += &Self::write_primitive(term);
        result += ")";

        result
    }

    fn write_rewritten(term: &RewrittenTerm) -> String {
        match term {
            RewrittenTerm::Primitive(term) => Self::write_primitive(term),
            RewrittenTerm::Skolem(term) => Self::write_skolemterm(term),
            RewrittenTerm::Base(term) => Self::write_skolem_baseterm(term),
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
    fn statement_declare(
        predicate: &Identifier,
        arity: usize,
        existential_positions: &HashSet<(Identifier, usize)>,
    ) -> String {
        let mut result = format!(".decl {predicate}(");

        result += &Self::argument_declaration(predicate, arity, existential_positions);

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

    fn statement_type(
        predicate: &Identifier,
        position_predicate: &Identifier,
        num_frontier: usize,
        rule_index: usize,
        atom_index: usize,
        term_index: usize,
        existential_positions: &HashSet<(Identifier, usize)>,
    ) -> String {
        let mut result =
            Self::skolem_function_name(&predicate.name(), rule_index, atom_index, term_index);

        result += " { ";
        result +=
            &Self::argument_declaration(position_predicate, num_frontier, existential_positions);
        result += " }";

        result
    }

    fn statement_rule(
        rule_index: usize,
        rule: &ChaseRule,
        analysis: &RuleAnalysis,
        existential_positions: &HashSet<(Identifier, usize)>,
    ) -> String {
        let mut result = String::from("");

        let frontier = analysis.frontier.clone();

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

        let frontier = frontier
            .into_iter()
            .map(|variable| {
                if let Some(body_position) = Self::find_variable(&rule.positive_body(), &variable) {
                    if let Some(head_position) =
                        Self::find_variable_primitive(&rule.head(), &variable)
                    {
                        if !existential_positions.contains(&body_position)
                            && existential_positions.contains(&head_position)
                        {
                            return RewrittenTerm::Base(PrimitiveTerm::Variable(variable));
                        }
                    }
                }

                RewrittenTerm::Primitive(PrimitiveTerm::Variable(variable))
            })
            .collect::<Vec<_>>();

        for (atom_index, head_atom) in rule.head().iter().enumerate() {
            let predicate = head_atom.predicate().clone();
            let rewritten_terms = head_atom
                .terms()
                .iter()
                .enumerate()
                .map(|(index, term)| {
                    if let PrimitiveTerm::Variable(Variable::Existential(_)) = term {
                        RewrittenTerm::Skolem(SkolemTerm {
                            predicate: SkolemPredicate::new(
                                predicate.clone(),
                                rule_index,
                                atom_index,
                                index,
                            ),
                            frontier: frontier.clone(),
                        })
                    } else if existential_positions.contains(&(predicate.clone(), index)) {
                        if let PrimitiveTerm::Variable(variable) = term {
                            if let Some(body_position) =
                                Self::find_variable(&rule.positive_body(), variable)
                            {
                                if let Some(head_position) =
                                    Self::find_variable_primitive(&rule.head(), variable)
                                {
                                    if !existential_positions.contains(&body_position)
                                        && existential_positions.contains(&head_position)
                                    {
                                        return RewrittenTerm::Base(term.clone());
                                    }
                                }
                            }
                        }

                        RewrittenTerm::Primitive(term.clone())
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
        format!(
            ".output {predicate}(IO=file, filename=\"results/{predicate}.csv\", delimiter=\",\")"
        )
    }
}

impl SouffleTranslation {
    fn declare_statements(
        _program: &ChaseProgram,
        analysis: &ProgramAnalysis,
        existential_positions: &HashSet<(Identifier, usize)>,
        result_rules: &mut TranslationResult,
    ) {
        for (predicate, arity) in &analysis.all_predicates {
            if predicate
                .name()
                .starts_with("FRESH_HEAD_MATCHES_IDENTIFIER_FOR_RULE_")
            {
                continue;
            }

            result_rules.push_statement(Self::statement_declare(
                predicate,
                *arity,
                existential_positions,
            ));
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
        program: &ChaseProgram,
        analysis: &ProgramAnalysis,
        existential_positions: &HashSet<(Identifier, usize)>,
        result_rules: &mut TranslationResult,
    ) {
        let mut skolem_statement = String::from(".type Skolem");
        let mut append = false;
        let mut existential = false;

        for (rule_index, (rule, analysis)) in program
            .rules()
            .iter()
            .zip(analysis.rule_analysis.iter())
            .enumerate()
        {
            if analysis.is_existential {
                existential = true;
            }

            let frontier = analysis.frontier.clone();

            let dummy_identifier = Identifier::new(String::from(""));
            let mut frontier_existential = HashSet::new();
            for (frontier_index, variable) in frontier.iter().enumerate() {
                if let Some(frontier_position) =
                    Self::find_variable_primitive(&rule.head(), variable)
                {
                    if existential_positions.contains(&frontier_position) {
                        frontier_existential.insert((dummy_identifier.clone(), frontier_index));
                    }
                }
            }

            for (head_index, head_atom) in rule.head().iter().enumerate() {
                for (term_index, term) in head_atom.terms().iter().enumerate() {
                    if let PrimitiveTerm::Variable(Variable::Existential(_)) = term {
                        skolem_statement += "\n";

                        if append {
                            skolem_statement += "   | ";
                        } else {
                            skolem_statement += "   = ";
                            append = true;
                        }

                        skolem_statement += &Self::statement_type(
                            &head_atom.predicate(),
                            &dummy_identifier,
                            frontier.len(),
                            rule_index,
                            head_index,
                            term_index,
                            &frontier_existential,
                        );
                    }
                }
            }
        }

        skolem_statement += "\n   | SkolemBase { base: symbol }";

        if existential {
            result_rules.push_statement(skolem_statement);
        }
    }

    fn rule_statements(
        program: &ChaseProgram,
        analysis: &ProgramAnalysis,
        existential_positions: &HashSet<(Identifier, usize)>,
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
        Self::declare_statements(program, analysis, &existential_positions, &mut result_rules);
        result_rules.empty_line();
        Self::import_statements(program, analysis, &mut result_rules)?;
        result_rules.empty_line();
        Self::rule_statements(program, analysis, &existential_positions, &mut result_rules);
        result_rules.empty_line();
        Self::output_statements(program, analysis, &mut result_rules);

        Some(vec![result_rules])
    }
}
