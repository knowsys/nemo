//! Bundles functionaly for checking the satisfiablity of a formula.

use super::{
    common::{Interpretation, VariableAssignment},
    rules::Formula,
};

pub(super) struct FormulaSatisfiser {
    formula: Formula,
    interpretation: Interpretation,
}

impl FormulaSatisfiser {
    fn new(formula: Formula, interpretation: Interpretation) -> Self {
        Self {
            formula,
            interpretation,
        }
    }
}
