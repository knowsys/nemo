//! api
use std::path::PathBuf;

use crate::{
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

use thiserror::Error;

/// Exposed error for API
#[derive(Error, Debug)]
pub enum Error {
    /// nemo error
    #[error(transparent)]
    Nemo(#[from] crate::error::Error),
    /// API error
    #[error("API error {0} occurred: {1}")]
    API(usize, String),
    /// API direnv error
    #[error(transparent)]
    DirEnv(#[from] std::io::Error),
}

/// NemoControl
#[derive(Debug, Default)]
pub struct NemoControl {
    engine: Option<
        ExecutionEngine<StrategyDependencyGraph<GraphConstructorPositive, StrategyRoundRobin>>,
    >,
    results: Vec<(Identifier, Option<TableId>)>,
}

impl NemoControl {
    /// load
    pub fn load(&mut self, input_program: String) -> Result<(), Error> {
        let program = all_input_consumed(RuleParser::new().parse_program())(&input_program)
            .map_err(|err| -> crate::error::Error { err.into() })?;
        //let program = program.map_err(|err| -> Error { err.into() });
        program
            .check_for_unsupported_features()
            .map_err(|err| -> crate::error::Error { err.into() })?;
        self.engine = Some(ExecutionEngine::initialize(program)?);
        Ok(())
    }

    /// reasoning
    pub fn reason(&mut self) -> Result<(), Error> {
        match &mut self.engine {
            Some(engine) => Ok(engine.execute()?),
            None => Err(Error::API(
                1,
                "Engine has not been initialized with a valid loaded program. Use load first."
                    .to_string(),
            )),
        }
    }

    /// write predicate
    pub fn write(&mut self, predicates: Vec<String>, directory: String) -> Result<(), Error> {
        match &mut self.engine {
            Some(engine) => {
                let output_dir = PathBuf::from(directory);
                let file_manager = OutputFileManager::try_new(&output_dir, true, false)?;

                if self.results.is_empty() {
                    self.results = engine.combine_results()?;
                }
                for (pred, table_id) in &self.results {
                    if predicates.contains(&pred.name()) {
                        let mut writer = file_manager.create_file_writer(pred)?;

                        if let Some(id) = table_id {
                            let mut serializer = engine.table_serializer(*id);
                            while let Some(record) = serializer.next_record() {
                                writer.write_record(record)?;
                            }
                        }
                    }
                }
                Ok(())
            }
            None => Err(Error::API(
                1,
                "Engine has not been initialized with a valid loaded program. Use load first."
                    .to_string(),
            )),
        }
    }

    /// reset
    pub fn reset(&mut self) {
        self.engine = None;
        self.results = Vec::new();
    }
}

#[cfg(test)]
mod test {
    use std::fs::read_to_string;

    use assert_fs::TempDir;
    use serial_test::serial;
    use test_log::test;

    use super::*;

    #[cfg_attr(miri, ignore)]
    #[test]
    #[serial]
    fn reason() {
        let cur_dir = std::env::current_dir().unwrap();
        let mut nemo_ctl = NemoControl::default();
        let bytes = include_bytes!("../../resources/testcases/lcs-diff-computation/run-lcs-10.rls");
        let input = String::from_utf8(bytes.to_vec()).unwrap();

        let temp_dir = TempDir::new().unwrap();

        nemo_ctl.load(input).unwrap();
        std::env::set_current_dir("./resources/testcases/lcs-diff-computation/").unwrap();
        nemo_ctl.reason().unwrap();

        nemo_ctl
            .write(
                vec!["furthestPath".to_string(), "lcs".to_string()],
                temp_dir.to_str().unwrap().to_string(),
            )
            .unwrap();

        nemo_ctl
            .write(
                vec!["edge".to_string(), "docAend".to_string()],
                temp_dir.to_str().unwrap().to_string(),
            )
            .unwrap();

        let expected_result1 = read_to_string("./run-lcs-10/furthestPath.csv").unwrap();
        let expected_result2 = read_to_string("./run-lcs-10/lcs.csv").unwrap();
        let expected_result3 = read_to_string("./run-lcs-10/edge.csv").unwrap();
        let expected_result4 = read_to_string("./run-lcs-10/docAend.csv").unwrap();
        let result1 = read_to_string(temp_dir.path().join("furthestPath.csv")).unwrap();
        let result2 = read_to_string(temp_dir.path().join("lcs.csv")).unwrap();
        let result3 = read_to_string(temp_dir.path().join("edge.csv")).unwrap();
        let result4 = read_to_string(temp_dir.path().join("docAend.csv")).unwrap();

        assert_eq!(result1, expected_result1);
        assert_eq!(result2, expected_result2);
        assert_eq!(result3, expected_result3);
        assert_eq!(result4, expected_result4);

        std::env::set_current_dir(cur_dir).unwrap();
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    #[serial]
    fn reason_with_working_dir() {
        let cur_dir = std::env::current_dir().unwrap();
        let mut nemo_ctl = NemoControl::default();
        let bytes = include_bytes!("../../resources/testcases/lcs-diff-computation/run-lcs-10.rls");
        let input = String::from_utf8(bytes.to_vec()).unwrap();
        let bytes = include_bytes!("../../resources/testcases/load_multiple/run.rls");
        let input2 = String::from_utf8(bytes.to_vec()).unwrap();

        let temp_dir = TempDir::new().unwrap();

        std::env::set_current_dir("./resources/testcases/lcs-diff-computation/").unwrap();
        nemo_ctl.load(input).unwrap();
        nemo_ctl.reason().unwrap();

        nemo_ctl
            .write(
                vec!["furthestPath".to_string(), "lcs".to_string()],
                temp_dir.to_str().unwrap().to_string(),
            )
            .unwrap();

        nemo_ctl
            .write(
                vec!["edge".to_string(), "docAend".to_string()],
                temp_dir.to_str().unwrap().to_string(),
            )
            .unwrap();

        let expected_result1 = read_to_string("./run-lcs-10/furthestPath.csv").unwrap();
        let expected_result2 = read_to_string("./run-lcs-10/lcs.csv").unwrap();
        let expected_result3 = read_to_string("./run-lcs-10/edge.csv").unwrap();
        let expected_result4 = read_to_string("./run-lcs-10/docAend.csv").unwrap();
        let result1 = read_to_string(temp_dir.path().join("furthestPath.csv")).unwrap();
        let result2 = read_to_string(temp_dir.path().join("lcs.csv")).unwrap();
        let result3 = read_to_string(temp_dir.path().join("edge.csv")).unwrap();
        let result4 = read_to_string(temp_dir.path().join("docAend.csv")).unwrap();

        assert_eq!(result1, expected_result1);
        assert_eq!(result2, expected_result2);
        assert_eq!(result3, expected_result3);
        assert_eq!(result4, expected_result4);

        nemo_ctl.reset();

        std::env::set_current_dir("../load_multiple/").unwrap();
        nemo_ctl.load(input2).unwrap();
        nemo_ctl.reason().unwrap();

        nemo_ctl
            .write(
                vec!["combined".to_string(), "constant".to_string()],
                temp_dir.to_str().unwrap().to_string(),
            )
            .unwrap();
        let mut expected_result1 = read_to_string("./run/combined.csv")
            .unwrap()
            .trim()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let mut expected_result2 = read_to_string("./run/constant.csv")
            .unwrap()
            .trim()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let mut result1 = read_to_string(temp_dir.path().join("combined.csv"))
            .unwrap()
            .trim()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let mut result2 = read_to_string(temp_dir.path().join("constant.csv"))
            .unwrap()
            .trim()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        result1.sort();
        result2.sort();
        expected_result1.sort();
        expected_result2.sort();

        assert_eq!(result1, expected_result1);
        assert_eq!(result2, expected_result2);
        std::env::set_current_dir(cur_dir).unwrap();
    }
}
