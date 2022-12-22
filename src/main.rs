use stage2::io::parser::RuleParser;
use stage2::logical::execution::ExecutionEngine;
use stage2::meta::timing::TimedDisplay;
use stage2::meta::TimedCode;
use stage2::physical::tabular::traits::table::Table;
use std::fs::{read_to_string, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

fn main() {
    env_logger::init();

    TimedCode::instance().start();
    TimedCode::instance().sub("Reading & Preprocessing").start();

    // let mut input: String = "".to_string();
    // stdin().read_to_string(&mut input).unwrap();
    // let input = read_to_string("test-files/snomed-el-noconst.rls").unwrap();
    // let input = read_to_string("test-files/medmed-el-noconst.rls").unwrap();
    // let input = read_to_string("test-files/galen-el-without-constants.rls").unwrap();
    // let input = read_to_string("test-files/snomed-el-noaux.rls").unwrap();

    let input = read_to_string("test-files/galen-el-noaux.rls").unwrap();
    let save_results = false;

    let parser = RuleParser::new();
    let mut parser_function = parser.parse_program();
    let program = parser_function(&input).unwrap().1;

    log::info!("Parsed program.");
    log::debug!("{:?}", program);

    let mut exec_engine = ExecutionEngine::initialize(program);

    TimedCode::instance().sub("Reading & Preprocessing").stop();
    TimedCode::instance().sub("Reasoning").start();

    log::info!("Executing ...");

    exec_engine.execute();

    TimedCode::instance().sub("Reasoning").stop();
    TimedCode::instance().sub("Save").start();

    let dict = exec_engine.get_dict().clone();
    if save_results {
        log::info!("Results:");
        for (predicate, trie) in exec_engine.get_results() {
            let predicate_string = parser
                .resolve_identifier(&predicate)
                .expect("should have been interned");

            log::info!("{}: {} entries", predicate_string, trie.row_num());

            let predicate_file_name = predicate_string.replace(['/', ':'], "_");

            let path = PathBuf::from(format!("out/{predicate_file_name}.csv"));
            match OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path.clone())
            {
                Ok(mut file) => {
                    write!(file, "{}", trie.debug(&dict)).expect("should succeed");
                    log::info!("wrote {path:?}");
                }
                Err(e) => log::warn!("error writing: {e:?}"),
            }
        }
    }

    TimedCode::instance().sub("Save").stop();
    TimedCode::instance().stop();

    println!(
        "\n{}",
        TimedCode::instance().create_tree_string(
            "stage2",
            &[
                TimedDisplay::default(),
                TimedDisplay::default(),
                TimedDisplay::new(stage2::meta::timing::TimedSorting::LongestTime, 0)
            ]
        )
    );
}
