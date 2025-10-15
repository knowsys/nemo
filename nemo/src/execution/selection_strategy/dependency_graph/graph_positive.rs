//! Module for defining a graph that checks
//! when the application of a rule might lead to the application of another.

use std::collections::HashMap;

use crate::{
    execution::planning_new::normalization::rule::NormalizedRule, rule_model::components::tag::Tag,
};

use super::graph_constructor::{DependencyGraph, DependencyGraphConstructor};

/// Constructor for a dependency graph that has an edge from one rule to another
/// if the application of the first might lead to an applicatin of the second.
#[derive(Debug, Copy, Clone)]
pub struct GraphConstructorPositive {}

impl DependencyGraphConstructor for GraphConstructorPositive {
    fn build_graph(rules: &[&NormalizedRule]) -> DependencyGraph {
        let rule_count = rules.len();

        let mut predicate_to_rules_body = HashMap::<Tag, Vec<usize>>::new();
        let mut predicate_to_rules_head = HashMap::<Tag, Vec<usize>>::new();

        for (rule_index, rule) in rules.iter().enumerate() {
            for (body_predicate, _) in rule.predicates_positive() {
                let indices = predicate_to_rules_body.entry(body_predicate).or_default();

                indices.push(rule_index);
            }

            for (head_predicate, _) in rule.predicates_head() {
                let indices = predicate_to_rules_head.entry(head_predicate).or_default();

                indices.push(rule_index);
            }
        }

        let mut graph = DependencyGraph::new();
        let mut rule_to_node_index = Vec::new();

        for rule_index in 0..rule_count {
            let node_index = graph.add_node(rule_index);
            rule_to_node_index.push(node_index);
        }

        for (head_predicate, head_rules) in predicate_to_rules_head {
            if let Some(body_rules) = predicate_to_rules_body.get(&head_predicate) {
                for head_index in head_rules {
                    for &body_index in body_rules {
                        let node_head = rule_to_node_index[head_index];
                        let node_body = rule_to_node_index[body_index];

                        graph.add_edge(node_head, node_body, ());
                    }
                }
            }
        }

        graph
    }
}
