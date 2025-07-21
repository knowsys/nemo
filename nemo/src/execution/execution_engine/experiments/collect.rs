//! This module defines procedures to setup experiments for the AAAI submission

use std::{
    collections::HashMap,
    fs::{create_dir_all, File},
    io::Write,
    path::Path,
};

use itertools::Itertools;

use crate::{
    chase_model::GroundAtom,
    execution::{
        selection_strategy::strategy::RuleSelectionStrategy,
        tracing::{node_query::TableEntriesForTreeNodesQuery, trace::ExecutionTraceTree},
        ExecutionEngine,
    },
    rule_model::components::{fact::Fact, tag::Tag, term::Term},
};

const COLLECT_OUTPUT_DIR: &'static str = "output";

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    /// Collect all facts derived during the chase.
    fn all_facts(&mut self) -> Vec<Fact> {
        let mut facts = Vec::default();

        let predicates = self.chase_program().idb_predicates();
        for predicate in predicates {
            if let Ok(Some(row_iterator)) = self.predicate_rows(&predicate) {
                for row in row_iterator {
                    let subterms = row.into_iter().map(|value| Term::ground(value));
                    facts.push(Fact::new(predicate.clone(), subterms));
                }
            }
        }

        facts
    }

    fn all_subtrees(tree: &ExecutionTraceTree) -> Vec<ExecutionTraceTree> {
        let tree_empty =
            ExecutionTraceTree::Fact(GroundAtom::new(Tag::new(String::default()), vec![]));

        let (rule_application, subtree_list) = match tree {
            ExecutionTraceTree::Fact(_) => return vec![tree.clone()],
            ExecutionTraceTree::Rule(rule_application, subtrees) => {
                let mut subtree_list = Vec::default();

                for subtree in subtrees {
                    let mut choices = Self::all_subtrees(subtree);
                    choices.push(tree_empty.clone());

                    subtree_list.push(choices);
                }

                (rule_application, subtree_list)
            }
        };

        let mut results = Vec::default();

        for combination in subtree_list.into_iter().multi_cartesian_product() {
            let new_tree = ExecutionTraceTree::Rule(rule_application.clone(), combination);
            results.push(new_tree);
        }

        results
    }

    /// Compute all possible node queries and save them to a csv
    pub fn collect_node_queries(&mut self) {
        let all_facts = self.all_facts();
        // let facts = all_facts[1000000..1010000]
        // let facts = all_facts[1000000..1000001]
        //     .iter()
        //     .cloned()
        //     .collect::<Vec<_>>();
        let facts = all_facts;

        let (trace, handles) = self.trace(facts).expect("error while tracing");
        let num_facts = handles.len();

        let mut query_map = HashMap::<TableEntriesForTreeNodesQuery, usize>::default();

        for (index, handle) in handles.into_iter().enumerate() {
            if let Some(tree) = trace.tree(handle) {
                for subtree in Self::all_subtrees(&tree) {
                    let query = subtree.to_node_query();

                    *query_map.entry(query).or_insert(0) += 1;
                }
            }

            if index % 1000 == 0 {
                println!("Progress: {}/{}", index + 1, num_facts);
            }
        }

        let data_path = Path::new(COLLECT_OUTPUT_DIR).join("data.txt");
        let mut data_file = File::create(&data_path).expect("failed to create file");
        data_file
            .write_fmt(format_args!("{}\n\n", query_map.len()))
            .expect("failed to write in file");

        for (index, (query, multiplicity)) in query_map.iter().enumerate() {
            let json = serde_json::to_string_pretty(&query).unwrap();

            let query_path = Path::new(COLLECT_OUTPUT_DIR);
            create_dir_all(query_path).expect("failed to create directory");

            let query_path = query_path.join(format!("query_{}.json", index));
            let mut file = File::create(&query_path).expect("failed to create file");
            file.write_all(json.as_bytes())
                .expect("failed to write file");

            data_file
                .write_fmt(format_args!("{multiplicity}\n"))
                .expect("failed to write in file");
        }
    }
}
