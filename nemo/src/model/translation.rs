//! Module that defines translations from our internal rule representation
//! to rule formats accepted by other reasoners.

use crate::program_analysis::analysis::ProgramAnalysis;

use super::{chase_model::ChaseProgram, Program};

mod util;

pub mod gringo;
pub mod souffle;
pub mod vlog;

#[derive(Debug, Copy, Clone)]
pub enum TranslationFormat {
    Souffle,
    VLog,
    Rulewerk,
    Gringo,
}

#[derive(Debug)]
pub struct TranslationResult {
    filename: String,
    result: String,
}

impl TranslationResult {
    pub fn new(filename: &str) -> Self {
        Self {
            filename: filename.to_owned(),
            result: String::default(),
        }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn result(&self) -> &str {
        &self.result
    }
    pub fn set_result(&mut self, result: String) {
        self.result = result
    }

    pub fn push_statement(&mut self, statement: String) {
        self.result += &format!("\n{statement}");
    }

    pub fn empty_line(&mut self) {
        self.result += "\n";
    }
}

pub trait RuleTranslation {
    fn translate(
        program: &ChaseProgram,
        analysis: &ProgramAnalysis,
    ) -> Option<Vec<TranslationResult>>;
}
