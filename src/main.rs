use stage2::io::parser::RuleParser;
use stage2::logical::execution_engine::RuleExecutionEngine;
use stage2::logical::model::{Fact, Identifier, NumericLiteral, Term};
use stage2::logical::table_manager::TableManagerStrategy;
use stage2::meta::timing::TimedDisplay;
use stage2::meta::TimedCode;
use stage2::physical::datatypes::{DataTypeName, DataValueT};
use stage2::physical::tables::{Table, TableSchema, Trie, TrieSchema, TrieSchemaEntry};
use std::collections::HashSet;
use std::fs::{read_to_string, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

fn main() {
    env_logger::init();

    TimedCode::instance().start();

    TimedCode::instance().sub("Reading & Parsing").start();

    // let mut input: String = "".to_string();
    // stdin().read_to_string(&mut input).unwrap();
    let input = read_to_string("test-files/snomed-el-noconst.rls").unwrap();
    // let input = read_to_string("test-files/medmed-el-noconst.rls").unwrap();
    // let input = read_to_string("test-files/galen-el-without-constants.rls").unwrap();
    // let input = read_to_string("test-files/galen-el-noaux.rls").unwrap();

    let parser = RuleParser::new();
    let mut parser_function = parser.parse_program();
    let program = parser_function(&input).unwrap().1;

    log::info!("parsed program");
    log::debug!("{:?}", program);

    let facts: Vec<&Fact> = program.facts().collect();
    let fact_preds: HashSet<(Identifier, usize)> = facts
        .iter()
        .map(|f| (f.0.predicate(), f.0.terms.len()))
        .collect();
    let head_preds: HashSet<(Identifier, usize)> = program
        .rules()
        .flat_map(|rule| rule.head().map(|atom| (atom.predicate(), atom.terms.len())))
        .collect();
    let all_preds: HashSet<(Identifier, usize)> = fact_preds.union(&head_preds).copied().collect();

    log::info!("collected predicates");
    all_preds.iter().for_each(|(identifier, arity)| {
        log::info!(
            "predicate {identifier:?}: {}[{arity}]",
            parser
                .resolve_identifier(identifier)
                .expect("should have been interned")
        )
    });

    let tries: Vec<(Identifier, Trie)> = fact_preds
        .iter()
        .map(|(p, _)| {
            let pred_facts: Vec<&Fact> = facts
                .iter()
                .copied()
                .filter(|f| f.0.predicate() == *p)
                .collect();
            let datatypes: Vec<DataTypeName> = pred_facts[0]
                .0
                .terms()
                .map(|_t| DataTypeName::U64)
                .collect(); // TODO: should depend on type but for now we only consider numeric literal integers that we case to u64

            let mut schema_entries = vec![];

            for (label, datatype) in datatypes.into_iter().enumerate() {
                schema_entries.push(TrieSchemaEntry { label, datatype });
            }

            let schema = TrieSchema::new(schema_entries);

            let contents: Vec<Vec<DataValueT>> = pred_facts
                .iter()
                .map(|f| {
                    f.0.terms()
                        .map(|t| match t {
                            Term::NumericLiteral(nl) => match nl {
                                NumericLiteral::Integer(i) => {
                                    DataValueT::U64((*i).try_into().unwrap())
                                }
                                _ => unimplemented!(),
                            },
                            Term::Constant(identifier) => {
                                DataValueT::U64(identifier.to_constant_u64())
                            }
                            _ => unimplemented!(),
                        })
                        .collect()
                })
                .collect();

            (*p, Trie::from_rows(schema, contents))
        })
        .collect();

    log::info!("assembled tries");

    let mut exec_engine = RuleExecutionEngine::new(
        TableManagerStrategy::Unlimited,
        program,
        parser.clone_dict(),
    );

    // for (pred, trie) in &tries {
    //     eprintln!("{:?}", pred);
    //     eprintln!("{}", trie);
    // }

    for (pred, trie) in tries {
        exec_engine.add_trie(pred, 0..1, (0..trie.schema().arity()).collect(), 0, trie);
    }

    TimedCode::instance().sub("Reading & Parsing").stop();
    TimedCode::instance().sub("Reasoning").start();

    log::info!("executing …");

    exec_engine.execute();
    let dict = exec_engine.table_manager.dictionary.clone();

    log::info!("Results:");
    for (pred, arity) in all_preds {
        log::info!(
            "{:?}: {}[{arity}]",
            pred,
            parser
                .resolve_identifier(&pred)
                .expect("should have been interned")
        );
        let trie_option = exec_engine.get_final_table(pred, (0..arity).collect());

        if let Some(trie) = trie_option {
            let predicate = parser
                .resolve_identifier(&pred)
                .expect("should have been interned");
            let sanitised = predicate.replace("/", "_").replace(":", "_");
            log::info!("{predicate}: {} rows", trie.row_num());
            let path = PathBuf::from(format!("out/{sanitised}.csv"));
            match OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path.clone())
            {
                Ok(mut file) => write!(file, "{}", trie.debug(&dict)).expect("should succeed"),
                Err(e) => log::warn!("error writing: {e:?}"),
            }
            log::info!("writing to {path:?}");

            log::info!("wrote {path:?}");
        }
    }

    TimedCode::instance().sub("Reasoning").stop();
    TimedCode::instance().stop();

    log::info!(
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
