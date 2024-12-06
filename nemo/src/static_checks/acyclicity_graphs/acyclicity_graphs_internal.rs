use crate::rule_model::components::{rule::Rule, term::primitive::variable::Variable};
use crate::static_checks::acyclicity_graphs::{
    AcyclicityGraphCycle, Cycle, JointlyAcyclicityGraph, WeaklyAcyclicityGraph,
};
use crate::static_checks::collection_traits::InsertAllRet;
use crate::static_checks::positions::{ExtendedPositions, Position, Positions};
use crate::static_checks::rule_set::RuleSet;

use petgraph::algo::is_cyclic_directed;
use petgraph::graphmap::{DiGraphMap, NodeTrait};
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
pub enum WeaklyAcyclicityGraphEdgeType {
    Common,
    CommonAndSpecial,
    Special,
}

pub(crate) trait AcyclicityGraphBuilderInternal<'a> {
    fn add_nodes(&mut self, rule_set: &'a RuleSet);
    fn add_edges(&mut self, rule_set: &'a RuleSet);
    fn build_graph_internal(rule_set: &'a RuleSet) -> Self;
}

impl<'a> AcyclicityGraphBuilderInternal<'a> for JointlyAcyclicityGraph<'a> {
    fn add_edges(&mut self, rule_set: &'a RuleSet) {
        let attacked_pos_by_vars: HashMap<&Variable, Positions> =
            rule_set.attacked_positions_by_variables();
        rule_set.iter().for_each(|rule| {
            self.add_edges_for_rule(rule, &attacked_pos_by_vars);
        })
    }

    fn add_nodes(&mut self, rule_set: &'a RuleSet) {
        let existential_variables: HashSet<&Variable> = rule_set.existential_variables();
        existential_variables.iter().for_each(|var| {
            self.add_node(var);
        })
    }

    fn build_graph_internal(rule_set: &'a RuleSet) -> Self {
        let mut jo_ac_graph: JointlyAcyclicityGraph = JointlyAcyclicityGraph::new();
        jo_ac_graph.add_nodes(rule_set);
        jo_ac_graph.add_edges(rule_set);
        jo_ac_graph
    }
}

impl<'a> AcyclicityGraphBuilderInternal<'a> for WeaklyAcyclicityGraph<'a> {
    fn add_edges(&mut self, rule_set: &'a RuleSet) {
        rule_set.iter().for_each(|rule| {
            self.add_edges_for_rule(rule);
        });
    }

    fn add_nodes(&mut self, rule_set: &'a RuleSet) {
        let all_positive_ex_pos: ExtendedPositions = rule_set.all_positive_extended_positions();
        all_positive_ex_pos.into_iter().for_each(|pos| {
            self.add_node(pos);
        });
    }

    fn build_graph_internal(rule_set: &'a RuleSet) -> Self {
        let mut we_ac_graph: WeaklyAcyclicityGraph = WeaklyAcyclicityGraph::new();
        we_ac_graph.add_nodes(rule_set);
        we_ac_graph.add_edges(rule_set);
        we_ac_graph
    }
}

trait JointlyAcyclicityGraphBuilderInternal<'a>: AcyclicityGraphBuilderInternal<'a> {
    fn add_edges_for_attacked_var(
        &mut self,
        attacked_var: &Variable,
        rule: &Rule,
        ex_vars_of_rule: &HashSet<&'a Variable>,
        attacked_pos_by_vars: &HashMap<&'a Variable, Positions>,
    );
    fn add_edges_for_attacking_var(
        &mut self,
        attacking_var: &'a Variable,
        ex_vars_of_rule: &HashSet<&'a Variable>,
    );
    fn add_edges_for_rule(
        &mut self,
        rule: &'a Rule,
        attacked_pos_by_vars: &HashMap<&'a Variable, Positions>,
    );
}

impl<'a> JointlyAcyclicityGraphBuilderInternal<'a> for JointlyAcyclicityGraph<'a> {
    fn add_edges_for_attacked_var(
        &mut self,
        attacked_var: &Variable,
        rule: &Rule,
        ex_vars_of_rule: &HashSet<&'a Variable>,
        attacked_pos_by_vars: &HashMap<&'a Variable, Positions>,
    ) {
        attacked_pos_by_vars
            .keys()
            .filter(|attacking_var| {
                attacked_var.is_attacked_by_variable(attacking_var, rule, attacked_pos_by_vars)
            })
            .for_each(|attacking_var| {
                self.add_edges_for_attacking_var(attacking_var, ex_vars_of_rule);
            });
    }

    fn add_edges_for_attacking_var(
        &mut self,
        attacking_var: &'a Variable,
        ex_vars_of_rule: &HashSet<&'a Variable>,
    ) {
        ex_vars_of_rule.iter().for_each(|ex_var| {
            self.add_edge(attacking_var, ex_var, ());
        })
    }

    fn add_edges_for_rule(
        &mut self,
        rule: &'a Rule,
        attacked_pos_by_vars: &HashMap<&'a Variable, Positions>,
    ) {
        let ex_vars_of_rule: HashSet<&Variable> = rule.existential_variables();
        let attacked_positive_vars: HashSet<&Variable> =
            rule.attacked_universal_variables(attacked_pos_by_vars);
        attacked_positive_vars.iter().for_each(|var| {
            self.add_edges_for_attacked_var(var, rule, &ex_vars_of_rule, attacked_pos_by_vars);
        })
    }
}

trait WeaklyAcyclicityGraphBuilderInternal<'a>: AcyclicityGraphBuilderInternal<'a> {
    fn add_common_edges_for_pos(
        &mut self,
        body_pos: Position<'a>,
        head_ex_pos_of_var: &ExtendedPositions<'a>,
    );
    fn add_common_edges_for_rule(&mut self, rule: &'a Rule, variable_body: &Variable);
    fn add_edges_for_rule(&mut self, rule: &'a Rule);
    fn add_special_edge(&mut self, body_pos: Position<'a>, existential_pos: Position<'a>);
    fn add_special_edges_for_pos(
        &mut self,
        body_pos: Position<'a>,
        extended_pos_of_ex_vars: &ExtendedPositions<'a>,
    );
    fn add_special_edges_for_rule(&mut self, rule: &'a Rule, variable_body: &Variable);
}

impl<'a> WeaklyAcyclicityGraphBuilderInternal<'a> for WeaklyAcyclicityGraph<'a> {
    fn add_common_edges_for_pos(
        &mut self,
        body_pos: Position<'a>,
        head_ex_pos_of_var: &ExtendedPositions<'a>,
    ) {
        head_ex_pos_of_var.iter().for_each(|head_pos| {
            self.add_edge(body_pos, *head_pos, WeaklyAcyclicityGraphEdgeType::Common);
        })
    }

    fn add_common_edges_for_rule(&mut self, rule: &'a Rule, variable_body: &Variable) {
        let body_ex_pos_of_var: ExtendedPositions =
            variable_body.get_extended_positions_in_positive_body(rule);
        let head_ex_pos_of_var: ExtendedPositions =
            variable_body.get_extended_positions_in_head(rule);
        body_ex_pos_of_var
            .into_iter()
            .for_each(|body_pos| self.add_common_edges_for_pos(body_pos, &head_ex_pos_of_var));
    }

    fn add_edges_for_rule(&mut self, rule: &'a Rule) {
        let positive_variables: HashSet<&Variable> = rule.positive_variables();
        positive_variables.iter().for_each(|variable_body| {
            self.add_common_edges_for_rule(rule, variable_body);
            self.add_special_edges_for_rule(rule, variable_body);
        });
    }

    fn add_special_edge(&mut self, body_pos: Position<'a>, existential_pos: Position<'a>) {
        if self.contains_edge(body_pos, existential_pos) {
            self.add_edge(
                body_pos,
                existential_pos,
                WeaklyAcyclicityGraphEdgeType::CommonAndSpecial,
            );
        } else {
            self.add_edge(
                body_pos,
                existential_pos,
                WeaklyAcyclicityGraphEdgeType::Special,
            );
        }
    }

    fn add_special_edges_for_pos(
        &mut self,
        body_pos: Position<'a>,
        extended_pos_of_ex_vars: &ExtendedPositions<'a>,
    ) {
        extended_pos_of_ex_vars.iter().for_each(|existential_pos| {
            self.add_special_edge(body_pos, *existential_pos);
        })
    }

    fn add_special_edges_for_rule(&mut self, rule: &'a Rule, variable_body: &Variable) {
        let body_ex_pos_of_var: ExtendedPositions =
            variable_body.get_extended_positions_in_positive_body(rule);
        let extended_pos_of_ex_vars: ExtendedPositions =
            rule.extended_positions_of_existential_variables();
        body_ex_pos_of_var.into_iter().for_each(|body_pos| {
            self.add_special_edges_for_pos(body_pos, &extended_pos_of_ex_vars);
        })
    }
}

pub(crate) trait AcyclicityGraphCycleInternal<N> {
    fn cycles_internal(&self) -> HashSet<Cycle<N>>;
    fn cycles_rec(&self, neighbor: &N, cycle: &mut Cycle<N>, cycles: &mut HashSet<Cycle<N>>);
    fn cycles_with_start(&self, node: &N) -> HashSet<Cycle<N>>;
    fn is_cyclic_internal(&self) -> bool;
    fn recursion_stops(
        &self,
        neighbor: &N,
        cycle: &Cycle<N>,
        cycles: &mut HashSet<Cycle<N>>,
    ) -> bool;
    fn recursion_with_node(&self, node: &N, cycle: &mut Cycle<N>, cycles: &mut HashSet<Cycle<N>>);
}

impl<N, E> AcyclicityGraphCycleInternal<N> for DiGraphMap<N, E>
where
    N: NodeTrait,
{
    fn cycles_internal(&self) -> HashSet<Cycle<N>> {
        let cycles: HashSet<Cycle<N>> =
            self.nodes()
                .fold(HashSet::<Cycle<N>>::new(), |cycles, node| {
                    let cycles_with_start: HashSet<Cycle<N>> = self.cycles_with_start(&node);
                    cycles.insert_all_take_ret(cycles_with_start)
                });
        cycles.filter_permutations_ret()
    }

    fn cycles_rec(&self, neighbor: &N, cycle: &mut Cycle<N>, cycles: &mut HashSet<Cycle<N>>) {
        if self.recursion_stops(neighbor, cycle, cycles) {
            return;
        }
        self.recursion_with_node(neighbor, cycle, cycles);
    }

    fn cycles_with_start(&self, node: &N) -> HashSet<Cycle<N>> {
        let mut cycles: HashSet<Cycle<N>> = HashSet::<Cycle<N>>::new();
        let mut cycle: Cycle<N> = Cycle::<N>::new();
        self.recursion_with_node(node, &mut cycle, &mut cycles);
        cycles
    }

    fn is_cyclic_internal(&self) -> bool {
        is_cyclic_directed(self)
    }

    fn recursion_stops(
        &self,
        neighbor: &N,
        cycle: &Cycle<N>,
        cycles: &mut HashSet<Cycle<N>>,
    ) -> bool {
        if !cycle.contains(neighbor) {
            return false;
        }
        let starting_node: &N = cycle.first().unwrap();
        if neighbor == starting_node {
            let cycle_clone: Cycle<N> = cycle.clone();
            cycles.insert(cycle_clone);
        }
        true
    }

    fn recursion_with_node(&self, node: &N, cycle: &mut Cycle<N>, cycles: &mut HashSet<Cycle<N>>) {
        self.neighbors(*node).for_each(|neighbor| {
            cycle.push(*node);
            self.cycles_rec(&neighbor, cycle, cycles);
            cycle.pop();
        });
    }
}

pub(crate) trait WeaklyAcyclicityGraphCycleInternal<N>: AcyclicityGraphCycle<N> {
    fn contains_cycle_with_special_edge_internal(&self) -> bool;
    fn cycle_contains_special_edge(&self, cycle: &Cycle<N>) -> bool;
    fn match_edge(&self, node: &N, next_node: &N) -> bool;
}

impl<'a> WeaklyAcyclicityGraphCycleInternal<Position<'a>> for WeaklyAcyclicityGraph<'a> {
    fn contains_cycle_with_special_edge_internal(&self) -> bool {
        let cycles: HashSet<Cycle<Position<'a>>> = self.cycles();
        cycles
            .iter()
            .any(|cycle| self.cycle_contains_special_edge(cycle))
    }

    fn cycle_contains_special_edge(&self, cycle: &Cycle<Position<'a>>) -> bool {
        let cycle_size: usize = cycle.len();
        cycle.iter().enumerate().any(|(i, pos)| {
            let j: usize = (i + 1) % cycle_size;
            let next_pos: &Position<'a> = cycle.get(j).unwrap();
            self.match_edge(pos, next_pos)
        })
    }

    fn match_edge(&self, node: &Position<'a>, next_node: &Position<'a>) -> bool {
        match self.edge_weight(*node, *next_node).unwrap() {
            WeaklyAcyclicityGraphEdgeType::Common => false,
            WeaklyAcyclicityGraphEdgeType::CommonAndSpecial => true,
            WeaklyAcyclicityGraphEdgeType::Special => true,
        }
    }
}

trait PermutationEq<N> {
    fn permutation_eq(&self, other: &Cycle<N>) -> bool;
}

impl<N> PermutationEq<N> for Cycle<N>
where
    N: NodeTrait,
{
    fn permutation_eq(&self, other: &Cycle<N>) -> bool {
        if self.different_size(other) {
            return false;
        }
        let cycle_size: usize = self.len();
        (0..cycle_size).any(|offset| {
            self.iter().enumerate().all(|(j, node_self)| {
                let node_other: &N = other.get(j + offset).unwrap();
                node_self == node_other
            })
        })
    }
}

trait DifferentSize<N> {
    fn different_size(&self, other: &Cycle<N>) -> bool;
}

impl<N> DifferentSize<N> for Cycle<N>
where
    N: NodeTrait,
{
    fn different_size(&self, other: &Cycle<N>) -> bool {
        let self_length: usize = self.len();
        let other_length: usize = other.len();
        self_length != other_length
    }
}

trait FilterPermutations {
    fn filter_permutations(&mut self);
    fn filter_permutations_ret(self) -> Self;
}

impl<N> FilterPermutations for HashSet<Cycle<N>>
where
    N: NodeTrait,
{
    fn filter_permutations(&mut self) {
        let self_clone: HashSet<Cycle<N>> = self.clone();
        self_clone.iter().for_each(|cycle1| {
            self_clone
                .iter()
                .filter(|cycle2| cycle1 != *cycle2)
                .for_each(|cycle2| {
                    if cycle1.permutation_eq(cycle2) {
                        self.remove(cycle2);
                    }
                })
        })
    }

    fn filter_permutations_ret(mut self) -> Self {
        self.filter_permutations();
        self
    }
}
