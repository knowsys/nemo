//! Nemo API to call the reasoning engine and compute results
//!
//! # Examples
//! ```
//! # use std::fs::read_to_string;
//! # use assert_fs::TempDir;
//! # #[cfg(miri)]
//! # fn main() {}
//! # #[cfg(not(miri))]
//! # fn main() {
//! use nemo::api::{load, reason, output_predicates};
//! std::env::set_current_dir("../resources/testcases/lcs-diff-computation/").unwrap();
//! let mut engine = load("run-lcs-10.rls".into()).unwrap();
//! // reasoning on the rule file
//! reason(&mut engine).unwrap();
//! // write the results to a temporary directory
//! let temp_dir = TempDir::new().unwrap();
//! let predicates = output_predicates(&engine);
//! // TODO: Write API disabled due to refactoring.
//! // write(temp_dir.to_str().unwrap().to_string(), &mut engine, predicates).unwrap();
//! # }
//! ```

use std::{fs::read_to_string, path::PathBuf};

use crate::{
    error::{Error, ReadingError, report::ProgramReport},
    execution::{
        DefaultExecutionEngine, ExecutionEngine, execution_parameters::ExecutionParameters,
    },
    rule_file::RuleFile,
    rule_model::{
        components::tag::Tag,
        pipeline::transformations::{
            default::TransformationDefault, validate::TransformationValidate,
        },
        programs::{handle::ProgramHandle, program::Program},
    },
};
use nemo_physical::resource::Resource;

/// Reasoning Engine exposed by the API
pub type Engine = DefaultExecutionEngine;

/// Load the given `file` and load the program from the file.
///
/// For details see [load_string]
pub fn load(file: PathBuf) -> Result<Engine, Error> {
    let input = read_to_string(file.clone())
        .map_err(|err| ReadingError::from(err).with_resource(Resource::Path(file)))?;
    load_string(input)
}

/// Parse a program in the given `input`-String and return an [Engine].
///
/// The program will be parsed and checked for unsupported features.
///
/// # Error
/// Returns an appropriate [Error] variant on parsing and feature check issues.
pub fn load_string(input: String) -> Result<Engine, Error> {
    let parameters = ExecutionParameters::default();
    let file = RuleFile::new(input, String::default());

    Ok(ExecutionEngine::from_file(file, parameters)?.into_object())
}

/// Parse a program in the given `input`-string and return a [Program].
///
/// # Error
/// Returns a [ProgramReport] if parsing or validation fails.
pub fn load_program(input: String, label: String) -> Result<Program, ProgramReport> {
    let file = RuleFile::new(input, label);
    let parameters = ExecutionParameters::default();

    let handle = ProgramHandle::from_file(&file);
    let report = ProgramReport::new(file);

    let (program, report) = report.merge_program_parser_report(handle)?;
    let (program, report) = report.merge_validation_report(
        &program,
        program.transform(TransformationDefault::new(&parameters)),
    )?;

    report.result(program.materialize())
}

/// Validate the `input` and create a [ProgramReport] for error reporting
pub fn validate(input: String, label: String) -> ProgramReport {
    let file = RuleFile::new(input, label);
    let handle = ProgramHandle::from_file(&file);
    let report = ProgramReport::new(file);

    let (program, report) = match report.merge_program_parser_report(handle) {
        Ok(result) => result,
        Err(report) => return report,
    };

    let (_program, report) = match report.merge_validation_report(
        &program,
        program.transform(TransformationValidate::default()),
    ) {
        Ok(result) => result,
        Err(report) => return report,
    };

    report
}

/// Executes the reasoning process of the [Engine].
///
/// # Note
/// If there are `@source` or `@import` directives in the
/// parsed rules, all relative paths are resolved with the current
/// working directory
pub fn reason(engine: &mut Engine) -> Result<(), Error> {
    engine.execute()
}

/// Get a [Vec] of all output predicates that are computed by the engine.
pub fn output_predicates(engine: &Engine) -> Vec<Tag> {
    engine
        .chase_program()
        .exports()
        .iter()
        .map(|export| export.predicate())
        .cloned()
        .collect()
}

// TODO: Disabled write API. This API is designed in a way that does not fit how Nemo controls exporting.
// One could take a list of export directives instead of a list of predicate names, and one could also
// have a method that works with the directives from the program.
//
// /// Writes all result [predicates][Identifier] in the vector `predicates` into the directory specified in `path`.
// pub fn write(path: String, engine: &mut Engine, predicates: Vec<Identifier>) -> Result<(), Error> {
//     let output_dir = PathBuf::from(path);
//     let export_manager = ExportManager::new()
//         .set_base_path(output_dir.clone())
//         .overwrite(true)
//         .compress(false);

//     let output_predicates = engine
//         .output_predicates()
//         .map(|export_spec| (export_spec.predicate().clone(), export_spec))
//         .collect::<HashMap<_, _>>();

//     for predicate in &predicates {
//         if let Some(arity) = engine.predicate_arity(predicate) {
//             export_manager.export_table(
//                 output_predicates
//                     .get(predicate)
//                     .expect("export spec should be present"),
//                 engine.predicate_rows(predicate)?,
//                 arity,
//             )?;
//         }
//     }

//     Ok(())
// }

#[cfg(test)]
mod test {
    use assert_fs::TempDir;

    use super::*;

    #[cfg_attr(miri, ignore)]
    #[test]
    fn reason() {
        std::env::set_current_dir("../resources/testcases/lcs-diff-computation/").unwrap();
        let mut engine = load("run-lcs-10.rls".into()).unwrap();
        super::reason(&mut engine).unwrap();

        // writing only the results where the predicates contain an "i"
        let results = output_predicates(&engine)
            .into_iter()
            .filter(|pred| pred.to_string().contains('i'))
            .collect::<Vec<_>>();

        assert_eq!(results.len(), 4);
        let _temp_dir = TempDir::new().unwrap();
        // Disabled:
        // write(temp_dir.to_str().unwrap().to_string(), &mut engine, results).unwrap();
    }
}
