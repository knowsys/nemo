//! Nemo API to call the reasoning engine and compute results
//!
//! # Examples
//! ```
//! # use std::fs::read_to_string;
//! # use assert_fs::TempDir;
//! use nemo::api::{load,reason,write};
//! # let path = String::from("./resources/testcases/lcs-diff-computation/run-lcs-10.rls");
//! // assume path is a string with the path to a rules file
//! let mut engine = load(path).unwrap();
//! # let cur_dir = std::env::current_dir().unwrap();
//! # std::env::set_current_dir("./resources/testcases/lcs-diff-computation/").unwrap();
//! // reasoning on the rule file
//! let results = reason(&mut engine).unwrap();
//! # std::env::set_current_dir(cur_dir).unwrap();
//! // write the results to a temporary directory
//! let temp_dir = TempDir::new().unwrap();
//! write(temp_dir.to_str().unwrap().to_string(), &mut engine, results).unwrap();
//! ```

use std::{fs::read_to_string, path::PathBuf};

use crate::{
    error::Error,
    io::{
        parser::{all_input_consumed, RuleParser},
        OutputFileManager, RecordWriter,
    },
    logical::{
        execution::{
            selection_strategy::{
                dependency_graph::graph_positive::GraphConstructorPositive,
                strategy_graph::StrategyDependencyGraph, strategy_round_robin::StrategyRoundRobin,
            },
            ExecutionEngine,
        },
        model::Identifier,
    },
    physical::{dictionary::value_serializer::TrieSerializer, management::database::TableId},
};

/// Reasoning Engine exposed by the API
#[derive(Debug)]
pub struct Engine(
    ExecutionEngine<StrategyDependencyGraph<GraphConstructorPositive, StrategyRoundRobin>>,
);

/// Predicate representation for results
#[derive(Debug, PartialEq, Eq)]
pub struct Predicate {
    name: Identifier,
    table_id: TableId,
}

impl ToString for Predicate {
    fn to_string(&self) -> String {
        self.name.name()
    }
}

/// Load the given `file` and load the program from the file.
///
/// For details see [`load_string`]
pub fn load(file: String) -> Result<Engine, Error> {
    let input = read_to_string(file.clone()).map_err(|err| Error::IOReading {
        error: err,
        filename: file,
    })?;
    load_string(input)
}

/// Parse a program in the given `input`-String and return an [`Engine`].
///
/// The program will be parsed and checked for unsupported features.
///
/// # Error
/// Returns an appropriate [`Error`][crate::error::Error] variant on parsing and feature check issues.
pub fn load_string(input: String) -> Result<Engine, Error> {
    let program = all_input_consumed(RuleParser::new().parse_program())(&input)?;
    program.check_for_unsupported_features()?;
    ExecutionEngine::initialize(program).map(Engine)
}

/// Executes the reasoning process of the [`Engine`] and returns a vector of result [`predicates`][Predicate].
///
/// # Note
/// If there have been `@source` routines in the parsed rules, all relative paths are resolved with the current working directory
pub fn reason(engine: &mut Engine) -> Result<Vec<Predicate>, Error> {
    engine.0.execute()?;
    let results = engine.0.combine_results()?;
    Ok(results
        .into_iter()
        .filter(|(_id, table_id)| table_id.is_some())
        .map(|(id, table_id)| Predicate {
            name: id,
            table_id: table_id.expect("filtered all None values"),
        })
        .collect::<Vec<_>>())
}

/// Writes all result [`predicates`][Predicate] in the vector `predicates` into the directory specified in `path`.
pub fn write(path: String, engine: &mut Engine, predicates: Vec<Predicate>) -> Result<(), Error> {
    let output_dir = PathBuf::from(path);
    let file_manager = OutputFileManager::try_new(&output_dir, true, false)?;

    for predicate in predicates {
        let mut writer = file_manager.create_file_writer(&predicate.name)?;
        let mut serializer = engine.0.table_serializer(predicate.table_id);

        while let Some(record) = serializer.next_record() {
            writer.write_record(record)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use assert_fs::TempDir;

    use super::*;

    #[test]
    fn reason() {
        let mut engine =
            load("./resources/testcases/lcs-diff-computation/run-lcs-10.rls".to_string()).unwrap();
        let cur_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir("./resources/testcases/lcs-diff-computation/").unwrap();
        let results = super::reason(&mut engine).unwrap();
        std::env::set_current_dir(cur_dir).unwrap();
        assert_eq!(results.len(), 20); // 2 empty tables, which are omitted

        // writing only the results where the predicates contain an "i"
        let results = results
            .into_iter()
            .filter(|pred| pred.to_string().contains('i'))
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 5);
        let temp_dir = TempDir::new().unwrap();
        write(temp_dir.to_str().unwrap().to_string(), &mut engine, results).unwrap();
    }
}
