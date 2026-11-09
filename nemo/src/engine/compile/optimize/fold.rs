//! Constant folding over compiled rules.

use nemo_physical::{diagnostics::Diagnostic, function::EvaluationContext};

use crate::{
    diagnostics::TypeError,
    engine::{compile::compiled::CompiledRule, compile::optimize::ProgramPass},
    model::Span,
};

/// The program pass that evaluates every ground condition of a rule once.
///
/// The condition is then removed, and if it is false or fails, the rule never fires.
/// A failing condition is also reported as a rule that never fires,
/// since it was presumably meant to hold.
/// A false one is not reported, since a parameter may switch a rule off that way.
/// A condition that calls a nondeterministic function, such as `RAND()`, is not folded,
/// and is evaluated per row.
#[derive(Debug)]
pub(crate) struct ConstantFold;

impl ProgramPass for ConstantFold {
    fn name(&self) -> &'static str {
        "constant-fold"
    }

    fn run(
        &self,
        rules: &mut [CompiledRule],
        spans: &[Option<Span>],
        reports: &mut Vec<Diagnostic>,
    ) {
        for (rule, &span) in rules.iter_mut().zip(spans) {
            let never_fires = &mut rule.never_fires;
            rule.filters.retain(|filter| {
                if !filter.inputs.is_empty() || filter.program.is_nondeterministic() {
                    return true;
                }
                match filter
                    .program
                    .filter(EvaluationContext::now(), &mut Vec::new(), &[])
                {
                    Ok(true) => {}
                    Ok(false) => *never_fires = true,
                    Err(error) => {
                        *never_fires = true;
                        let warning = TypeError::NeverFires.diagnostic();
                        reports.push(match (filter.origin.span(), span) {
                            (Some(condition), _) => warning.with_label(
                                condition,
                                format!("this condition always fails to evaluate: {error}"),
                            ),
                            (None, Some(rule)) => warning.with_label(
                                rule,
                                format!("a condition always fails to evaluate: {error}"),
                            ),
                            (None, None) => warning.with_note(format!(
                                "a condition always fails to evaluate: {error}"
                            )),
                        });
                    }
                }
                false
            });
        }
    }
}
