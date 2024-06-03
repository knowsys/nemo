use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use nemo_physical::datavalues::AnyDataValue;

use crate::{
    model::{
        chase_model::{ChaseAtom, ChaseProgram, ChaseRule, PrimitiveAtom, VariableAtom},
        FileFormat, Identifier, PrimitiveTerm, Variable,
    },
    program_analysis::analysis::{ProgramAnalysis, RuleAnalysis},
};

use super::{RuleTranslation, TranslationResult};

#[derive(Debug, Copy, Clone)]
pub(crate) struct VLogTranslation {}

impl VLogTranslation {
    fn write_variable(variable: &Variable) -> String {
        if let Some(name) = variable.name() {
            format!("V{}", name)
        } else {
            unimplemented!("Unnamed variables not supported for VLog");
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

    fn write_variable_atom(atom: &VariableAtom) -> String {
        let mut result = String::new();

        result += &atom.predicate().name();
        result += "(";

        for (index, variable) in atom.terms().iter().enumerate() {
            result += &Self::write_variable(variable);

            if index < atom.terms().len() - 1 {
                result += ",";
            }
        }

        result += ")";
        result
    }

    fn write_primitive_atom(atom: &PrimitiveAtom) -> String {
        let mut result = String::new();

        result += &atom.predicate().name();
        result += "(";

        for (index, term) in atom.terms().iter().enumerate() {
            result += &Self::write_primitive(term);

            if index < atom.terms().len() - 1 {
                result += ",";
            }
        }

        result += ")";
        result
    }
}

impl VLogTranslation {
    fn statement_rule(rule: &ChaseRule, analysis: &RuleAnalysis) -> String {
        let mut result = String::from("");

        for (atom_index, head_atom) in rule.head().iter().enumerate() {
            result += &Self::write_primitive_atom(head_atom);

            if atom_index < rule.head().len() - 1 {
                result += ",";
            }
        }

        result += " :- ";

        for (atom_index, body_atom) in rule.positive_body().iter().enumerate() {
            result += &Self::write_variable_atom(body_atom);

            if atom_index < rule.positive_body().len() - 1 {
                result += ",";
            }
        }

        result
    }

    fn statement_config(count: usize, predicate: &str, directory: &str, filename: &str) -> String {
        let mut result = String::from("");

        result += &format!("EDB{count}_predname={predicate}\n");
        result += &format!("EDB{count}_type=INMEMORY\n");
        result += &format!("EDB{count}_param0={directory}\n");
        result += &format!("EDB{count}_param1={filename}\n");

        result
    }
}

impl VLogTranslation {
    fn rule_statements(
        program: &ChaseProgram,
        analysis: &ProgramAnalysis,
        result_rules: &mut TranslationResult,
    ) {
        for (rule, analysis) in program.rules().iter().zip(analysis.rule_analysis.iter()) {
            result_rules.push_statement(Self::statement_rule(rule, analysis))
        }
    }

    fn config_statements(
        program: &ChaseProgram,
        analysis: &ProgramAnalysis,
        result_rules: &mut TranslationResult,
    ) -> Option<()> {
        for (import_index, (predicate, handler)) in program.imports().enumerate() {
            let resource = handler.resource()?;
            let path = Path::new(&resource);
            let directory = path.parent().map(|p| p.to_string_lossy().to_string())?;
            let filename = path.file_name().and_then(|name| {
                name.to_str()
                    .and_then(|s| s.split('.').next())
                    .map(String::from)
            })?;

            result_rules.push_statement(Self::statement_config(
                import_index,
                &predicate.name(),
                &directory,
                &filename,
            ));
        }

        Some(())
    }
}

impl RuleTranslation for VLogTranslation {
    fn translate(
        program: &ChaseProgram,
        analysis: &ProgramAnalysis,
    ) -> Option<Vec<TranslationResult>> {
        let mut result_rules = TranslationResult::new("program.dlog");
        Self::rule_statements(program, analysis, &mut result_rules);

        let mut result_config = TranslationResult::new("edb.conf");
        Self::config_statements(program, analysis, &mut result_config)?;

        Some(vec![result_rules, result_config])
    }
}
