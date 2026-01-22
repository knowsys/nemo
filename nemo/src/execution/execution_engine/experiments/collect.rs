//! This module defines procedures to setup experiments

use std::{
    collections::HashMap,
    fs::{File, create_dir_all},
    io::Write,
    path::Path,
};

use rand::{rngs::ThreadRng, seq::SliceRandom, thread_rng};

use itertools::Itertools;

use crate::{
    execution::{
        ExecutionEngine,
        planning::normalization::atom::ground::GroundAtom,
        selection_strategy::strategy::RuleSelectionStrategy,
        tracing::{node_query::TableEntriesForTreeNodesQuery, trace::ExecutionTraceTree},
    },
    rule_model::components::{fact::Fact, tag::Tag, term::Term},
};

const COLLECT_OUTPUT_DIR: &'static str = "output";
const MAX_TREE_PER_FACT: usize = 10;

impl<Strategy: RuleSelectionStrategy> ExecutionEngine<Strategy> {
    /// Collect all facts derived during the chase.
    async fn all_facts(&mut self) -> Vec<Fact> {
        let mut facts = Vec::default();

        let predicates = self.chase_program().derived_predicates().clone();

        for predicate in predicates {
            if let Ok(Some(row_iterator)) = self.predicate_rows(&predicate).await {
                for row in row_iterator {
                    let subterms = row.into_iter().map(|value| Term::ground(value));
                    facts.push(Fact::new(predicate.clone(), subterms));
                }
            }
        }

        facts
    }

    fn all_subtrees(tree: &ExecutionTraceTree, rng: &mut ThreadRng) -> Vec<ExecutionTraceTree> {
        let tree_empty =
            ExecutionTraceTree::Fact(GroundAtom::new(Tag::new(String::default()), vec![]));

        let (rule_application, subtree_list) = match tree {
            ExecutionTraceTree::Fact(_) => return vec![tree.clone()],
            ExecutionTraceTree::Rule(rule_application, subtrees) => {
                let mut subtree_list = Vec::default();

                for subtree in subtrees {
                    let mut choices = Self::all_subtrees(subtree, rng);
                    choices.shuffle(rng);
                    choices.insert(0, tree_empty.clone());

                    subtree_list.push(choices);
                }

                (rule_application, subtree_list)
            }
        };

        let mut results = Vec::default();

        for combination in subtree_list
            .into_iter()
            .multi_cartesian_product()
            .take(MAX_TREE_PER_FACT)
        {
            let new_tree = ExecutionTraceTree::Rule(rule_application.clone(), combination);
            results.push(new_tree);
        }

        results
    }

    /// Compute all possible node queries and save them to a csv
    pub async fn collect_node_queries(&mut self, num_queries: usize) {
        let mut rng = thread_rng();

        let mut facts = self.all_facts().await;
        facts.shuffle(&mut rng);
        facts = facts
            .into_iter()
            .take(num_queries * MAX_TREE_PER_FACT * 100)
            .collect();

        let (trace, mut handles) = self.trace(facts).await.expect("error while tracing");
        handles.shuffle(&mut rng);

        let num_facts = handles.len();

        let mut query_map = HashMap::<TableEntriesForTreeNodesQuery, usize>::default();

        'fact_loop: for (index, handle) in handles.into_iter().enumerate() {
            if let Some(tree) = trace.tree(handle) {
                for subtree in Self::all_subtrees(&tree, &mut rng) {
                    if subtree.node_count() <= 1 {
                        continue;
                    }

                    let query = subtree.to_node_query();

                    *query_map.entry(query).or_insert(0) += 1;

                    if num_queries > 0 && query_map.len() >= num_queries {
                        break 'fact_loop;
                    }
                }
            }

            if index % 10 == 0 {
                println!("Progress: {}/{}", index + 1, num_facts);
            }
        }

        let path = Path::new(COLLECT_OUTPUT_DIR);
        create_dir_all(path).expect("failed to create directory");

        let data_path = path.join("data.txt");
        let mut data_file = File::create(&data_path).expect("failed to create file");

        for (index, (query, multiplicity)) in query_map.iter().enumerate() {
            let json = serde_json::to_string_pretty(&query).unwrap();

            let query_path = path.join(format!("query_{}.json", index));
            let mut query_file = File::create(&query_path).expect("failed to create file");

            query_file
                .write_all(json.as_bytes())
                .expect("failed to write file");

            data_file
                .write_fmt(format_args!(
                    "{multiplicity},{},{}\n",
                    query.num_nodes(),
                    query.max_depth()
                ))
                .expect("failed to write in file");
        }
    }
}
