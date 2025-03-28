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
    error::{Error, ReadingError},
    execution::{DefaultExecutionEngine, ExecutionEngine},
    io::{resource_providers::ResourceProviders, ImportManager},
    parser::Parser,
    rule_model::{components::tag::Tag, translation::ASTProgramTranslation},
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
    let program_ast = Parser::initialize(&input, String::default())
        .parse()
        .map_err(|_| Error::ProgramParseError)?;
    let program = ASTProgramTranslation::initialize(&input, String::default())
        .translate(&program_ast)
        .map_err(|_| Error::ProgramParseError)?;

    ExecutionEngine::initialize(program, ImportManager::new(ResourceProviders::default()))
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
        .program()
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
