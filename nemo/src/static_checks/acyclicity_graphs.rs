//! Functionality that provides methods to build the (JointAcyclicityGraph / WeakAcyclicityGraph) based on
/// a RuleSet.
use crate::static_checks::collection_traits::{InsertAll, RemoveAll};
use crate::static_checks::positions::{
    ExtendedPositions, FromPositionSet, Position, Positions, PositionsByRuleIdxVariables,
};
use crate::static_checks::rule_set::{
    Attacked, AttackedVariables, ExistentialVariables, ExistentialVariablesPositions,
    RuleIdxVariable, RuleIdxVariables, RulePositions, RuleSet, SpecialPositionsConstructor,
    Variables,
};
use petgraph::algo::is_cyclic_directed;
use petgraph::graphmap::{DiGraphMap, NodeTrait};
use std::collections::HashSet;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Cycle<N>(Vec<N>);

impl<N> Cycle<N>
where
    N: NodeTrait,
{
    fn new() -> Self {
        Self(vec![])
    }

    fn different_size(&self, other: &Cycle<N>) -> bool {
        let self_length: usize = self.0.len();
        let other_length: usize = other.0.len();
        self_length != other_length
    }

    fn permutation_eq(&self, other: &Cycle<N>) -> bool {
        if self.different_size(other) {
            return false;
        }
        let cycle_size: usize = self.0.len();
        (0..cycle_size).any(|offset| {
            self.0.iter().enumerate().all(|(j, node_self)| {
                let node_other: &N = other.0.get((j + offset) % cycle_size).unwrap();
                node_self == node_other
            })
        })
    }
}

// TODO: not sure if this should be public, I'm leaning towards thinking that it should not be...
/// Wraps a HashSet meant to be a collection of cycles
#[derive(Clone, Debug)]
pub struct Cycles<N>(HashSet<Cycle<N>>);

impl<N> Cycles<N>
where
    N: NodeTrait,
{
    fn new() -> Self {
        Self(HashSet::new())
    }

    fn filter_permutations(&mut self) {
        let self_clone: Self = self.clone();
        self_clone.0.iter().for_each(|cycle1| {
            self_clone
                .0
                .iter()
                .filter(|cycle2| cycle1 != *cycle2)
                .for_each(|cycle2| {
                    if cycle1.permutation_eq(cycle2) {
                        self.0.remove(cycle2);
                    }
                })
        })
    }
}

/// Type to declare a JointAcyclicityGraph of some RuleSet.
#[derive(Debug)]
pub struct JointAcyclicityGraph<'a>(DiGraphMap<RuleIdxVariable<'a>, ()>);

/// Type to declare a WeakAcyclicityGraph of some RuleSet.
#[derive(Debug)]
pub struct WeakAcyclicityGraph<'a>(DiGraphMap<Position<'a>, WeakAcyclicityGraphEdgeType>);

/// Enum to distinguish between (Common / Special / Both) edge between two Position(s) of the
/// WeakAcyclicityGraph.
#[derive(Clone, Copy, Debug)]
pub enum WeakAcyclicityGraphEdgeType {
    /// There is a common edge between two Position(s) of the WeakAcyclicityGraph.
    Common,
    /// There is a special edge between two Position(s) of the WeakAcyclicityGraph.
    Special,
}

impl<'a> JointAcyclicityGraph<'a> {
    /// Builds the JointAcyclicityGraph of a RuleSet.
    pub fn new(rule_set: &'a RuleSet) -> Self {
        let mut graph: DiGraphMap<RuleIdxVariable<'a>, ()> = DiGraphMap::new();

        let attacked_pos_by_ex_rule_idx_vars: PositionsByRuleIdxVariables =
            rule_set.attacked_positions_by_existential_rule_idx_variables();

        rule_set.iter().enumerate().for_each(|r_idx_rule| {
            // TODO: USE ExistentialRuleIdxVariables INSTEAD OF FOLLOWING 5 LINES OF CODE
            let ex_vars_of_rule: Variables = r_idx_rule.1.existential_variables();
            let ex_rule_idx_vars_of_rule: RuleIdxVariables = ex_vars_of_rule
                .into_iter()
                .map(|var| (r_idx_rule.0, var))
                .collect();

            let attacked_positive_vars: Variables = r_idx_rule
                .1
                .attacked_universal_variables(&attacked_pos_by_ex_rule_idx_vars);

            for source_pos in
                attacked_pos_by_ex_rule_idx_vars
                    .keys()
                    .filter(|attacking_rule_idx_var| {
                        attacked_positive_vars.iter().any(|var| {
                            var.is_attacked_by_variable(
                                attacking_rule_idx_var,
                                r_idx_rule.1,
                                &attacked_pos_by_ex_rule_idx_vars,
                            )
                        })
                    })
            {
                for target_pos in &ex_rule_idx_vars_of_rule {
                    graph.add_edge(*source_pos, *target_pos, ());
                }
            }
        });

        Self(graph)
    }

    /// Returns all RuleIdxVariable(s) appearing in cycles of the JointAcyclicityGraph.
    pub fn rule_idx_variables_in_cycles(&self) -> RuleIdxVariables<'a> {
        let cycles: Cycles<RuleIdxVariable<'a>> = self.0.cycles();
        cycles
            .0
            .iter()
            .fold(RuleIdxVariables::new(), |rule_idx_vars_in_cycles, cycle| {
                rule_idx_vars_in_cycles.insert_all_ret(&cycle.0)
            })
    }

    /// Returns true iff there is a cycle in a JointAcyclicityGraph.
    pub fn is_cyclic(&self) -> bool {
        self.0.is_cyclic()
    }
}

impl<'a> WeakAcyclicityGraph<'a> {
    /// Builds the WeakAcyclicityGraph of a RuleSet.
    pub fn new(rule_set: &'a RuleSet) -> Self {
        let mut graph: DiGraphMap<Position<'a>, WeakAcyclicityGraphEdgeType> = DiGraphMap::new();

        for rule in rule_set {
            let positive_variables: Variables = rule.positive_variables();
            positive_variables.iter().for_each(|variable_body| {
                let body_ex_pos_of_var: ExtendedPositions =
                    variable_body.extended_positions_in_positive_body(rule);
                let head_ex_pos_of_var: ExtendedPositions =
                    variable_body.extended_positions_in_head(rule);
                let extended_pos_of_ex_vars: ExtendedPositions =
                    rule.extended_positions_of_existential_variables();

                for body_pos in body_ex_pos_of_var {
                    for head_pos in &head_ex_pos_of_var {
                        // take care to not override possibly existing special edges!
                        if !graph.contains_edge(body_pos, *head_pos) {
                            graph.add_edge(
                                body_pos,
                                *head_pos,
                                WeakAcyclicityGraphEdgeType::Common,
                            );
                        }
                    }
                    for head_pos in &extended_pos_of_ex_vars {
                        graph.add_edge(body_pos, *head_pos, WeakAcyclicityGraphEdgeType::Special);
                    }
                }
            });
        }

        Self(graph)
    }

    fn edge_is_special(&self, node: &Position<'a>, next_node: &Position<'a>) -> bool {
        matches!(
            self.0.edge_weight(*node, *next_node),
            Some(WeakAcyclicityGraphEdgeType::Special)
        )
    }

    fn cycle_contains_special_edge(&self, cycle: &Cycle<Position<'a>>) -> bool {
        let cycle_size: usize = cycle.0.len();
        cycle.0.iter().enumerate().any(|(i, pos)| {
            let j: usize = (i + 1) % cycle_size;
            let next_pos: &Position<'a> = cycle.0.get(j).unwrap();
            self.edge_is_special(pos, next_pos)
        })
    }

    /// Checks if there exists a Cycle in the WeakAcyclicityGraph that contains a special edge.
    pub fn contains_cycle_with_special_edge(&self) -> bool {
        let cycles: Cycles<Position<'a>> = self.0.cycles();
        cycles
            .0
            .iter()
            .any(|cycle| self.cycle_contains_special_edge(cycle))
    }

    fn cycles_containing_special_edges(&self) -> Cycles<Position<'a>> {
        let cycles: Cycles<Position<'a>> = self.0.cycles();
        Cycles(
            cycles
                .0
                .iter()
                .filter(|cycle| self.cycle_contains_special_edge(cycle))
                .cloned()
                .collect(),
        )
    }

    /// The positions in the WeakAcyclicityGraph with infinite rank,
    /// i.e. the ones reachable from a special edge cycle.
    pub fn infinite_rank_positions(&self) -> Positions {
        let cycs_con_spe_edge: Cycles<Position> = self.cycles_containing_special_edges();
        let repr_of_cycles: HashSet<Position> =
            self.0.represantatives_of_cycles(&cycs_con_spe_edge);
        let mut inf_rank_pos_set: HashSet<Position> = self.0.cycles_into_nodes(cycs_con_spe_edge);

        let mut unreachable_nodes: HashSet<Position> = self.0.all_nodes();
        unreachable_nodes.remove_all(&inf_rank_pos_set);

        for pos in repr_of_cycles.iter() {
            if unreachable_nodes.is_empty() {
                break;
            }
            let rea_nodes_of_node: HashSet<Position> = self.0.reachable_nodes_of_node(pos);
            unreachable_nodes.remove_all(&rea_nodes_of_node);
            inf_rank_pos_set.insert_all_take(rea_nodes_of_node);
        }
        Positions::from_extended_positions(inf_rank_pos_set)
    }
}

/// This Trait provides methods for a Graph in relation with Cycle(s).
pub trait AcyclicityGraphCycle<N> {
    /// Returns all nodes of a Graph.
    fn all_nodes(&self) -> HashSet<N>;
    /// Returns all Cycles of a Graph.
    fn cycles(&self) -> Cycles<N>;
    /// Returns all nodes of all Cycles in a Graph.
    fn cycles_into_nodes(&self, cycles: Cycles<N>) -> HashSet<N>;
    /// Checks if a Graph is cyclic.
    fn is_cyclic(&self) -> bool;
    /// Returns all reachable nodes of some node in the Graph.
    fn reachable_nodes_of_node(&self, node: &N) -> HashSet<N>;
    /// Returns a represantative node of every Cycle in some Cycles.
    fn represantatives_of_cycles(&self, cycles: &Cycles<N>) -> HashSet<N>;
}

impl<N, E> AcyclicityGraphCycle<N> for DiGraphMap<N, E>
where
    N: NodeTrait,
{
    fn all_nodes(&self) -> HashSet<N> {
        self.nodes().collect()
    }

    fn cycles(&self) -> Cycles<N> {
        let mut cycles: Cycles<N> = self.nodes().fold(Cycles::<N>::new(), |cycles, node| {
            let cycles_with_start: Cycles<N> = self.cycles_with_start(&node);
            Cycles(cycles.0.insert_all_take_ret(cycles_with_start.0))
        });
        cycles.filter_permutations();
        cycles
    }

    fn cycles_into_nodes(&self, cycles: Cycles<N>) -> HashSet<N> {
        cycles
            .0
            .into_iter()
            .fold(HashSet::<N>::new(), |nodes, cycle| {
                nodes.insert_all_take_ret(cycle.0)
            })
    }

    fn is_cyclic(&self) -> bool {
        is_cyclic_directed(self)
    }

    fn reachable_nodes_of_node(&self, node: &N) -> HashSet<N> {
        let mut rea_nodes: HashSet<N> = HashSet::from([*node]);
        let mut new_found_rea_nodes: HashSet<N> = rea_nodes.clone();
        while !new_found_rea_nodes.is_empty() {
            let new_con_rea_nodes: HashSet<N> = self.conclude_reachable_nodes(new_found_rea_nodes);
            new_found_rea_nodes = new_con_rea_nodes.remove_all_ret(&rea_nodes);
            rea_nodes.insert_all(&new_found_rea_nodes);
        }
        rea_nodes
    }

    fn represantatives_of_cycles(&self, cycles: &Cycles<N>) -> HashSet<N> {
        cycles
            .0
            .iter()
            .fold(HashSet::<N>::new(), |mut reprs, cycle| {
                let repr_of_cycle: N = *cycle.0.first().unwrap();
                reprs.insert(repr_of_cycle);
                reprs
            })
    }
}

trait AcyclicityGraphCyclePrivate<N> {
    fn conclude_reachable_nodes(&self, nodes: HashSet<N>) -> HashSet<N>;
    fn cycles_rec(&self, neighbor: &N, cycle: &mut Cycle<N>, cycles: &mut Cycles<N>);
    fn cycles_with_start(&self, node: &N) -> Cycles<N>;
    fn recursion_stops(&self, neighbor: &N, cycle: &Cycle<N>, cycles: &mut Cycles<N>) -> bool;
    fn recursion_with_node(&self, node: &N, cycle: &mut Cycle<N>, cycles: &mut Cycles<N>);
}

impl<N, E> AcyclicityGraphCyclePrivate<N> for DiGraphMap<N, E>
where
    N: NodeTrait,
{
    fn conclude_reachable_nodes(&self, nodes: HashSet<N>) -> HashSet<N> {
        nodes
            .into_iter()
            .fold(HashSet::<N>::new(), |neighbors, node| {
                let neighbors_of_node: HashSet<N> = self.neighbors(node).collect();
                neighbors.insert_all_take_ret(neighbors_of_node)
            })
    }

    fn cycles_rec(&self, neighbor: &N, cycle: &mut Cycle<N>, cycles: &mut Cycles<N>) {
        if self.recursion_stops(neighbor, cycle, cycles) {
            return;
        }
        self.recursion_with_node(neighbor, cycle, cycles);
    }

    fn cycles_with_start(&self, node: &N) -> Cycles<N> {
        let mut cycles: Cycles<N> = Cycles::<N>::new();
        let mut cycle: Cycle<N> = Cycle::<N>::new();
        self.recursion_with_node(node, &mut cycle, &mut cycles);
        cycles
    }

    fn recursion_stops(&self, neighbor: &N, cycle: &Cycle<N>, cycles: &mut Cycles<N>) -> bool {
        if !cycle.0.contains(neighbor) {
            return false;
        }
        let starting_node: &N = cycle.0.first().unwrap();
        if neighbor == starting_node {
            let cycle_clone: Cycle<N> = cycle.clone();
            cycles.0.insert(cycle_clone);
        }
        true
    }

    fn recursion_with_node(&self, node: &N, cycle: &mut Cycle<N>, cycles: &mut Cycles<N>) {
        self.neighbors(*node).for_each(|neighbor| {
            cycle.0.push(*node);
            self.cycles_rec(&neighbor, cycle, cycles);
            cycle.0.pop();
        });
    }
}
