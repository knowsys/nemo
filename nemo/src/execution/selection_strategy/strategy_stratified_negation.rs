//! Defines the execution strategy by which each rule is applied in the order it appears.

use std::collections::{BTreeSet, HashMap};

use petgraph::Directed;

use crate::{
    execution::planning::normalization::rule::NormalizedRule, rule_model::components::tag::Tag,
    util::labeled_graph::LabeledGraph,
};

use super::strategy::{RuleSelectionStrategy, SelectionStrategyError};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum EdgeLabel {
    Positive,
    Negative,
    Restrain,
}

type NegationGraph = LabeledGraph<usize, EdgeLabel, Directed>;

/// Defines a strategy where the rules are divided into the strongly connected
/// components of their dependency graph, which are executed in succession.
///
/// The components are ordered such that a component is only entered
/// once every rule it depends on has been applied exhaustively.
/// In particular, the table for every negated atom
/// will not get any new elements after that point.
#[derive(Debug)]
pub struct StrategyStratifiedNegation<SubStrategy: RuleSelectionStrategy> {
    /// The components of the dependency graph, in the order they are executed
    ordered_components: Vec<Vec<usize>>,
    /// For each component, the strategy that is used to select rules within it
    substrategies: Vec<SubStrategy>,

    /// The index of the component that is currently being executed
    current_component: usize,
}

impl<SubStrategy: RuleSelectionStrategy> StrategyStratifiedNegation<SubStrategy> {
    fn build_graph(rules: &Vec<&NormalizedRule>) -> NegationGraph {
        let mut predicate_to_rules_body_positive = HashMap::<Tag, BTreeSet<usize>>::new();
        let mut predicate_to_rules_body_negative = HashMap::<Tag, BTreeSet<usize>>::new();
        let mut predicate_to_rules_head = HashMap::<Tag, BTreeSet<usize>>::new();
        let mut predicate_to_rules_head_existential = HashMap::<Tag, BTreeSet<usize>>::new();

        let rule_count = rules.len();

        for (rule_index, rule) in rules.iter().enumerate() {
            for (body_predicate, _) in rule.predicates_positive() {
                let indices = if rule.contains_aggregates() {
                    // An aggregate in a head means that the head predicates need to be in a higher stratum than the body predicates
                    // This is the same as when all body literals are negative
                    // Therefore, we can easily compute strata for aggregates by acting if all body atoms in the rule were negated
                    predicate_to_rules_body_negative
                        .entry(body_predicate)
                        .or_default()
                } else {
                    // No aggregates in the rule
                    predicate_to_rules_body_positive
                        .entry(body_predicate)
                        .or_default()
                };

                indices.insert(rule_index);
            }

            for (body_predicate, _) in rule.predicates_negative() {
                let indices = predicate_to_rules_body_negative
                    .entry(body_predicate)
                    .or_default();

                indices.insert(rule_index);
            }

            for (head_predicate, _) in rule.predicates_head() {
                let indices = predicate_to_rules_head.entry(head_predicate).or_default();

                indices.insert(rule_index);
            }

            for (head_predicate, _) in rule.predicates_head_existential() {
                let indices = predicate_to_rules_head_existential
                    .entry(head_predicate)
                    .or_default();

                indices.insert(rule_index);
            }
        }

        // Collect the edges before adding them to the graph,
        // to avoid multiple edges between the same pair of nodes.
        let mut edges = BTreeSet::<(usize, usize, EdgeLabel)>::new();

        for (head_predicate, head_rules) in &predicate_to_rules_head {
            if let Some(body_rules) = predicate_to_rules_body_positive.get(head_predicate) {
                for &head_index in head_rules {
                    for &body_index in body_rules {
                        edges.insert((head_index, body_index, EdgeLabel::Positive));
                    }
                }
            }

            if let Some(body_rules) = predicate_to_rules_body_negative.get(head_predicate) {
                for &head_index in head_rules {
                    for &body_index in body_rules {
                        edges.insert((head_index, body_index, EdgeLabel::Negative));
                    }
                }
            }

            if let Some(existential_rules) = predicate_to_rules_head_existential.get(head_predicate)
            {
                for &head_index in head_rules {
                    for &existential_index in existential_rules {
                        if head_index != existential_index {
                            edges.insert((head_index, existential_index, EdgeLabel::Restrain));
                        }
                    }
                }
            }
        }

        let mut graph = NegationGraph::default();

        for rule_index in 0..rule_count {
            graph.add_node(rule_index);
        }

        for (head_index, body_index, label) in edges {
            graph.add_edge(head_index, body_index, label);
        }

        graph
    }
}

impl<SubStrategy: RuleSelectionStrategy> RuleSelectionStrategy
    for StrategyStratifiedNegation<SubStrategy>
{
    /// Create new [StrategyStratifiedNegation].
    fn new(rules: Vec<&NormalizedRule>) -> Result<Self, SelectionStrategyError> {
        let graph = Self::build_graph(&rules);

        let Some(mut components) =
            graph.topological_components(&[EdgeLabel::Negative], &[EdgeLabel::Restrain])
        else {
            return Err(SelectionStrategyError::NonStratifiedProgram);
        };

        let mut substrategies = Vec::new();

        for component in &mut components {
            component.sort();

            let sub_rules = component.iter().map(|i| rules[*i]).collect::<Vec<_>>();

            substrategies.push(SubStrategy::new(sub_rules)?);
        }

        if components.len() > 1 {
            log::info!("Stratified program: {components:?}")
        }

        Ok(Self {
            ordered_components: components,
            substrategies,
            current_component: 0,
        })
    }

    fn next_rule(&mut self, mut new_derivations: Option<bool>) -> Option<usize> {
        while self.current_component < self.ordered_components.len() {
            if let Some(substrategy_next_rule) =
                self.substrategies[self.current_component].next_rule(new_derivations)
            {
                return Some(
                    self.ordered_components[self.current_component][substrategy_next_rule],
                );
            } else {
                self.current_component += 1;
                new_derivations = None;
            }
        }

        None
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use crate::{
        execution::{
            execution_parameters::ExecutionParameters,
            planning::normalization::program::NormalizedProgram,
            selection_strategy::{
                strategy::{RuleSelectionStrategy, SelectionStrategyError},
                strategy_round_robin::StrategyRoundRobin,
            },
        },
        rule_file::RuleFile,
        rule_model::{
            pipeline::transformations::default::TransformationDefault,
            programs::handle::ProgramHandle,
        },
    };

    use super::{EdgeLabel, StrategyStratifiedNegation};

    type Strategy = StrategyStratifiedNegation<StrategyRoundRobin>;

    /// Parse and normalize a program
    fn normalize(program: &str) -> NormalizedProgram {
        let handle =
            ProgramHandle::from_file(&RuleFile::new(program.to_string(), String::default()))
                .expect("program parses")
                .into_object();
        let parameters = ExecutionParameters::default();
        let handle = handle
            .transform(TransformationDefault::new(&parameters))
            .expect("program is valid");

        NormalizedProgram::normalize_program(&handle)
    }

    /// Return the dependency graph built for `program`
    /// as a set of `(from_rule, to_rule, label)` triples.
    fn edges(program: &NormalizedProgram) -> BTreeSet<(usize, usize, EdgeLabel)> {
        let rules = program.rules().iter().collect::<Vec<_>>();
        let graph = Strategy::build_graph(&rules);
        let graph = graph.graph();

        graph
            .edge_indices()
            .map(|edge| {
                let (from, to) = graph
                    .edge_endpoints(edge)
                    .expect("edge index comes from the graph");

                (
                    *graph.node_weight(from).expect("node index is valid"),
                    *graph.node_weight(to).expect("node index is valid"),
                    *graph.edge_weight(edge).expect("edge index is valid"),
                )
            })
            .collect()
    }

    /// Return the components computed for `program` in the order they are executed,
    /// or `None` if the program is not stratifiable.
    fn components(program: &NormalizedProgram) -> Option<Vec<Vec<usize>>> {
        let rules = program.rules().iter().collect::<Vec<_>>();

        match Strategy::new(rules) {
            Ok(strategy) => Some(strategy.ordered_components),
            Err(SelectionStrategyError::NonStratifiedProgram) => None,
        }
    }

    /// Drive the strategy until it reports that no rule is left,
    /// always claiming that the last rule application produced no new derivations.
    fn rule_order(program: &NormalizedProgram) -> Vec<usize> {
        let rules = program.rules().iter().collect::<Vec<_>>();
        let mut strategy = Strategy::new(rules).expect("program is stratifiable");

        let mut result = Vec::new();
        let mut derivations = None;

        while let Some(rule) = strategy.next_rule(derivations) {
            result.push(rule);
            derivations = Some(false);
        }

        result
    }

    const PROGRAM: &str = "
        edge(1, 2) . edge(2, 3) . edge(3, 1) .

        a(?x) :- total(?x) .
        total(#count(?x)) :- acyclic(?x) .

        b(?x) :- a(?x) .
        a(?x) :- b(?x) .

        acyclic(?x) :- source(?x), ~cyclic(?x) .

        source(?x), sink(?y) :- reachable(?x, ?y) .

        cyclic(?x) :- reachable(?x, ?y), reachable(?y, ?x) .

        reachable(?x, ?z) :- reachable(?x, ?y), edge(?y, ?z) .

        reachable(?x, ?y) :- edge(?x, ?y) .

        linked(?x, ?y) :- reachable(?x, ?y) .
        linked(?x, !y) :- source(?x) .
    ";

    #[test]
    fn stratification() {
        let program = normalize(PROGRAM);

        assert_eq!(
            edges(&program),
            BTreeSet::from([
                // rule 1 derives `total`, which rule 0 consumes
                (1, 0, EdgeLabel::Positive),
                // the aggregate of rule 1 turns its body into a negative dependency
                (4, 1, EdgeLabel::Negative),
                // rules 0 and 3 derive `a`, which rule 2 consumes
                (0, 2, EdgeLabel::Positive),
                (3, 2, EdgeLabel::Positive),
                // rule 2 derives `b`, which rule 3 consumes
                (2, 3, EdgeLabel::Positive),
                // rule 5 derives `source`, rule 6 derives the negated `cyclic`
                (5, 4, EdgeLabel::Positive),
                (6, 4, EdgeLabel::Negative),
                // rules 7 and 8 derive `reachable`, which rules 5, 6 and 7 consume;
                // the two body atoms of rule 6 do not make its dependencies count twice
                (7, 5, EdgeLabel::Positive),
                (7, 6, EdgeLabel::Positive),
                (7, 7, EdgeLabel::Positive),
                (8, 5, EdgeLabel::Positive),
                (8, 6, EdgeLabel::Positive),
                (8, 7, EdgeLabel::Positive),
                // rules 7 and 8 also derive what rule 9 consumes
                (7, 9, EdgeLabel::Positive),
                (8, 9, EdgeLabel::Positive),
                // rule 5 derives the `source` that rule 10 consumes
                (5, 10, EdgeLabel::Positive),
                // rule 10 introduces a null for `linked`,
                // which rule 9 may make unnecessary
                (9, 10, EdgeLabel::Restrain),
            ])
        );

        // Rule 9 precedes rule 10 because it restrains it,
        // while rules 5 and 6 do not depend on each other
        // and are therefore applied in the order they appear in the program.
        assert_eq!(
            components(&program),
            Some(vec![
                vec![8],
                vec![7],
                vec![5],
                vec![6],
                vec![4],
                vec![1],
                vec![0],
                vec![2, 3],
                vec![9],
                vec![10]
            ])
        );

        assert_eq!(rule_order(&program), vec![8, 7, 5, 6, 4, 1, 0, 2, 3, 9, 10]);

        let mutual_restraint = normalize(
            "a(?x, !v) :- b(?x) .
             a(?x, !w) :- c(?x) .",
        );

        assert_eq!(components(&mutual_restraint), Some(vec![vec![0], vec![1]]));

        let restrained_negation = normalize(
            "q(?x) :- s(?x), ~p(?x) .
             p(?x), q(!v) :- t(?x) .",
        );

        assert_eq!(
            components(&restrained_negation),
            Some(vec![vec![1], vec![0]])
        );

        let negative_cycle = normalize(
            "a(?x) :- b(?x), ~c(?x) .
             c(?x) :- a(?x) .",
        );
        let self_negation = normalize("a(?x) :- b(?x), ~a(?x) .");

        assert_eq!(components(&negative_cycle), None);
        assert_eq!(components(&self_negation), None);
    }
}
