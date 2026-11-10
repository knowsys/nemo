//! The `--param` settings: parameter values given on the command line.

use nemo::{
    engine::Engine,
    model::{Directive, FileId, Statement},
};
use nemo_physical::diagnostics::Diagnostic;

use crate::{error::CliError, run::Run};

/// A `--param name=value` setting, registered as a source of its own.
///
/// The source contains the declaration `@parameter $name = value .`,
/// so that a diagnostic about a bad value points into the setting.
#[derive(Debug)]
pub(super) struct Parameter {
    /// The setting as it was written, for error messages.
    setting: String,
    /// The parameter's name, without the `$`.
    name: String,
    /// The registered source that contains the declaration.
    file: FileId,
}

/// Returns the diagnostic for a `--param` setting that is not one parameter name
/// with one term as its value.
fn malformed_setting(setting: &str) -> Diagnostic {
    CliError::InvalidSetting {
        setting: setting.to_owned(),
    }
    .diagnostic()
}

impl Run<'_> {
    /// Registers a `name=value` setting as a source that contains the matching `@parameter`
    /// declaration.
    ///
    /// The name may be written with its `$`, as in the program.
    /// Fails for a setting without `=` or without a name.
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

    /// Sets the engine's value of `parameter` to the value from the command line,
    /// which the load gives the program's declaration of the parameter.
    ///
    /// The value is parsed as part of the registered declaration, so it must be one term,
    /// and a diagnostic about it points into the setting.
    /// Fails for a setting that does not parse as one declaration of the named parameter.
    pub(super) fn supply(
        &self,
        engine: &mut Engine,
        parameter: &Parameter,
    ) -> Result<(), Diagnostic> {
        let (parsed, diagnostics) = engine.parse_source(parameter.file);
        if let Some(problem) = diagnostics.into_iter().next() {
            return Err(problem);
        }
        // The setting must parse as exactly one declaration of the named parameter, with a
        // value. Anything else, such as a name that is not a valid name or a value followed
        // by further statements, is a malformed setting.
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
        engine.set_parameter(&parameter.name, value);
        Ok(())
    }
}
