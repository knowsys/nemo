use stage2::io::parser::RuleParser;
use stage2::logical::execution_engine::RuleExecutionEngine;
use stage2::logical::model::{Fact, Identifier, NumericLiteral, Term};
use stage2::logical::table_manager::TableManagerStrategy;
use stage2::physical::datatypes::{DataTypeName, DataValueT};
use stage2::physical::tables::{Table, TableSchema, Trie, TrieSchema, TrieSchemaEntry};
use std::collections::HashSet;
use std::io::{stdin, Read};

fn main() {
    env_logger::init();

    let mut input: String = "".to_string();
    stdin().read_to_string(&mut input).unwrap();
    let parser = RuleParser::new();
    let mut parser_function = parser.parse_program();
    let program = parser_function(&input).unwrap().1;

    eprintln!("{:?}", program);

    let facts: Vec<&Fact> = program.facts().collect();
    let preds: HashSet<Identifier> = facts.iter().map(|f| f.0.predicate()).collect();

    let tries: Vec<(Identifier, Trie)> = preds
        .iter()
        .map(|p| {
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
                            _ => unimplemented!(),
                        })
                        .collect()
                })
                .collect();

            (*p, Trie::from_rows(schema, contents))
        })
        .collect();

    for (_, trie) in &tries {
        eprintln!("{}", trie);
    }

    let mut exec_engine = RuleExecutionEngine::new(TableManagerStrategy::Unlimited, program);

    for (pred, trie) in tries {
        exec_engine.add_trie(pred, 0..1, (0..trie.schema().arity()).collect(), 0, trie);
    }

    exec_engine.execute();

    println!("{:?}", exec_engine.table_manager.get_info(0));
    println!("{:?}", exec_engine.table_manager.get_info(1));
    println!("{:?}", exec_engine.table_manager.get_info(2));
    println!("{:?}", exec_engine.table_manager.get_info(3));
    println!("{:?}", exec_engine.table_manager.get_info(4));
    println!("{:?}", exec_engine.table_manager.get_info(5));
    println!("{:?}", exec_engine.table_manager.get_info(6));
}
