//! The `--param` settings: parameter values given on the command line.

use nemo::{
    engine::Engine,
    model::{Directive, FileId, Program, Statement},
};
use nemo_physical::diagnostics::Diagnostic;

use crate::{error::CliError, run::Run};

/// A `--param name=value` setting: the parameter it names, and the
/// source its value was registered as, so a bad value points at itself.
#[derive(Debug)]
pub(super) struct Parameter {
    /// The setting as it was written, for reporting it back.
    setting: String,
    /// The parameter's name, without the `$`.
    name: String,
    /// The synthetic source holding the declaration to parse.
    file: FileId,
}

/// Returns the diagnostic for a `--param` setting that does not read as one
/// parameter taking one term.
fn malformed_setting(setting: &str) -> Diagnostic {
    CliError::InvalidSetting {
        setting: setting.to_owned(),
    }
    .diagnostic()
}

impl Run<'_> {
    /// Registers a `name=value` setting as a source holding the
    /// declaration it stands for.
    ///
    /// The name may be written with its `$`, as the program writes it.
    pub(super) fn parameter(
        &self,
        engine: &mut Engine,
        setting: &str,
    ) -> Result<Parameter, Diagnostic> {
        let Some((name, value)) = setting.split_once('=') else {
            return Err(malformed_setting(setting));
        };
        let name = name.trim();
        let name = name.strip_prefix('$').unwrap_or(name);
        if name.is_empty() {
            return Err(malformed_setting(setting));
        }
        let file = engine.add_source(
            format!("--param {name}"),
            format!("@parameter ${name} = {value} ."),
        )?;
        Ok(Parameter {
            setting: setting.to_string(),
            name: name.to_string(),
            file,
        })
    }

    /// Gives a declared parameter the value the command line supplied.
    ///
    /// The value is read as the declaration it would have been written as,
    /// so it is one Nemo term and nothing else,
    /// and a malformed one is shown against the setting it came from.
    pub(super) fn supply(
        &self,
        engine: &Engine,
        program: &mut Program,
        parameter: &Parameter,
    ) -> Result<(), Diagnostic> {
        let (parsed, diagnostics) = engine.parse_source(parameter.file);
        if let Some(problem) = diagnostics.into_iter().next() {
            return Err(problem);
        }
        // The setting must read as exactly the declaration it stands
        // for: one parameter, named as asked, taking one term. Anything
        // else (a name that is not a name, a value carrying further
        // statements) is a malformed setting, not a program.
        let value = match <[Statement; 1]>::try_from(parsed.statements) {
            Ok([Statement::Directive(Directive::Parameter(directive))])
                if directive.variable.name == parameter.name =>
            {
                directive.value
            }
            _ => None,
        };
        let Some(value) = value else {
            return Err(malformed_setting(&parameter.setting));
        };

        let mut supplied = false;
        for statement in &mut program.statements {
            if let Statement::Directive(Directive::Parameter(directive)) = statement
                && directive.variable.name == parameter.name
            {
                directive.value = Some(value.clone());
                supplied = true;
            }
        }
        if supplied {
            return Ok(());
        }

        let declared: Vec<&str> = program
            .statements
            .iter()
            .filter_map(|statement| match statement {
                Statement::Directive(Directive::Parameter(directive)) => {
                    Some(directive.variable.name.as_str())
                }
                _ => None,
            })
            .collect();
        let mut problem = CliError::UndeclaredParameter {
            name: parameter.name.clone(),
        }
        .diagnostic();
        if !declared.is_empty() {
            problem = problem.with_note(format!(
                "the program declares: `${}`",
                declared.join("`, `$")
            ));
        }
        Err(problem)
    }
}
