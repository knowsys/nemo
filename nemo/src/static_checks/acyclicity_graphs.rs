//! Functionality that provides methods to build the (JointAcyclicityGraph / WeakAcyclicityGraph) based on
/// a RuleSet.
use crate::rule_model::components::{rule::Rule, term::primitive::variable::Variable};
use crate::static_checks::collection_traits::{InsertAll, RemoveAll};
use crate::static_checks::positions::{
    ExtendedPositions, FromPositionSet, Position, Positions, PositionsByRuleIdxVariables,
};
use crate::static_checks::rule_set::{
    AllPositivePositions, Attacked, AttackedVariables, ExistentialVariables,
    ExistentialVariablesPositions, RuleIdxRule, RuleIdxVariable, RuleIdxVariables, RulePositions,
    RuleSet, SpecialPositionsConstructor, Variables,
};
use petgraph::algo::is_cyclic_directed;
use petgraph::graphmap::{DiGraphMap, NodeTrait};
use std::collections::HashSet;

/// Type to declare a JointAcyclicityGraph of some RuleSet.
pub type JointAcyclicityGraph<'a> = DiGraphMap<RuleIdxVariable<'a>, ()>;

/// Type to declare a WeakAcyclicityGraph of some RuleSet.
pub type WeakAcyclicityGraph<'a> = DiGraphMap<Position<'a>, WeakAcyclicityGraphEdgeType>;

type Cycle<N> = Vec<N>;

type Cycles<N> = HashSet<Cycle<N>>;

/// Enum to distinguish between (Common / Special / Both) edge between two Position(s) of the
/// WeakAcyclicityGraph.
#[derive(Clone, Copy, Debug)]
pub enum WeakAcyclicityGraphEdgeType {
    /// There is a common edge between two Position(s) of the WeakAcyclicityGraph.
    Common,
    /// There is a common and a special edge between two Position(s) of the WeakAcyclicityGraph.
    CommonAndSpecial,
    /// There is a special edge between two Position(s) of the WeakAcyclicityGraph.
    Special,
}

/// This Trait provides a method to build the (JointAcyclicityGraph / WeakAcyclicityGraph) of a
/// RuleSet.
pub trait AcyclicityGraphBuilder<'a> {
    /// Builds the (JointAcyclicityGraph / WeakAcyclicityGraph) of a RuleSet.
    fn build_graph(rule_set: &'a RuleSet) -> Self;
}

impl<'a> AcyclicityGraphBuilder<'a> for JointAcyclicityGraph<'a> {
    fn build_graph(rule_set: &'a RuleSet) -> JointAcyclicityGraph<'a> {
        let mut jo_ac_graph: JointAcyclicityGraph = JointAcyclicityGraph::new();
        jo_ac_graph.add_nodes(rule_set);
        jo_ac_graph.add_edges(rule_set);
        jo_ac_graph
    }
}

impl<'a> AcyclicityGraphBuilder<'a> for WeakAcyclicityGraph<'a> {
    fn build_graph(rule_set: &'a RuleSet) -> WeakAcyclicityGraph<'a> {
        let mut we_ac_graph: WeakAcyclicityGraph = WeakAcyclicityGraph::new();
        we_ac_graph.add_nodes(rule_set);
        we_ac_graph.add_edges(rule_set);
        we_ac_graph
    }
}

trait AcyclicityGraphBuilderPrivate<'a> {
    fn add_nodes(&mut self, rule_set: &'a RuleSet);
    fn add_edges(&mut self, rule_set: &'a RuleSet);
}

impl<'a> AcyclicityGraphBuilderPrivate<'a> for JointAcyclicityGraph<'a> {
    fn add_edges(&mut self, rule_set: &'a RuleSet) {
        let attacked_pos_by_ex_rule_idx_vars: PositionsByRuleIdxVariables =
            rule_set.attacked_positions_by_existential_rule_idx_variables();
        rule_set.iter().enumerate().for_each(|idx_rule| {
            self.add_edges_for_rule(idx_rule, &attacked_pos_by_ex_rule_idx_vars);
        })
    }

    fn add_nodes(&mut self, rule_set: &'a RuleSet) {
        rule_set.iter().enumerate().for_each(|idx_rule| {
            self.add_nodes_for_rule(idx_rule);
        })
    }
}

impl<'a> AcyclicityGraphBuilderPrivate<'a> for WeakAcyclicityGraph<'a> {
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
        let cycles: Cycles<N> = self.nodes().fold(Cycles::<N>::new(), |cycles, node| {
            let cycles_with_start: Cycles<N> = self.cycles_with_start(&node);
            cycles.insert_all_take_ret(cycles_with_start)
        });
        cycles.filter_permutations_ret()
    }

    fn cycles_into_nodes(&self, cycles: Cycles<N>) -> HashSet<N> {
        cycles
            .into_iter()
            .fold(HashSet::<N>::new(), |nodes, cycle| {
                nodes.insert_all_take_ret(cycle)
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
        cycles.iter().fold(HashSet::<N>::new(), |mut reprs, cycle| {
            let repr_of_cycle: N = *cycle.first().unwrap();
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

    fn recursion_with_node(&self, node: &N, cycle: &mut Cycle<N>, cycles: &mut Cycles<N>) {
        self.neighbors(*node).for_each(|neighbor| {
            cycle.push(*node);
            self.cycles_rec(&neighbor, cycle, cycles);
            cycle.pop();
        });
    }
}

trait JointAcyclicityGraphBuilderPrivate<'a> {
    fn add_edges_for_attacked_var(
        &mut self,
        attacked_var: &Variable,
        rule: &Rule,
        ex_rule_idx_vars_of_rule: &RuleIdxVariables<'a>,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables<'a, '_>,
    );
    fn add_edges_for_attacking_var(
        &mut self,
        attacking_rule_idx_var: &RuleIdxVariable<'a>,
        ex_rule_idx_vars_of_rule: &RuleIdxVariables<'a>,
    );
    fn add_edges_for_rule(
        &mut self,
        idx_rule: RuleIdxRule<'a>,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables<'a, '_>,
    );
    fn add_nodes_for_rule(&mut self, idx_rule: RuleIdxRule<'a>);
}

impl<'a> JointAcyclicityGraphBuilderPrivate<'a> for JointAcyclicityGraph<'a> {
    fn add_edges_for_attacked_var(
        &mut self,
        attacked_var: &Variable,
        rule: &Rule,
        ex_rule_idx_vars_of_rule: &RuleIdxVariables<'a>,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables<'a, '_>,
    ) {
        attacked_pos_by_rule_idx_vars
            .keys()
            .filter(|attacking_rule_idx_var| {
                attacked_var.is_attacked_by_variable(
                    attacking_rule_idx_var,
                    rule,
                    attacked_pos_by_rule_idx_vars,
                )
            })
            .for_each(|attacking_rule_idx_var| {
                self.add_edges_for_attacking_var(attacking_rule_idx_var, ex_rule_idx_vars_of_rule);
            });
    }

    fn add_edges_for_attacking_var(
        &mut self,
        attacking_rule_idx_var: &RuleIdxVariable<'a>,
        ex_rule_idx_vars_of_rule: &RuleIdxVariables<'a>,
    ) {
        ex_rule_idx_vars_of_rule.iter().for_each(|ex_rule_idx_var| {
            self.add_edge(*attacking_rule_idx_var, *ex_rule_idx_var, ());
        })
    }

    fn add_edges_for_rule(
        &mut self,
        r_idx_rule: RuleIdxRule<'a>,
        attacked_pos_by_rule_idx_vars: &PositionsByRuleIdxVariables<'a, '_>,
    ) {
        // TODO: USE ExistentialRuleIdxVariables INSTEAD OF FOLLOWING 5 LINES OF CODE
        let ex_vars_of_rule: Variables = r_idx_rule.1.existential_variables();
        let ex_rule_idx_vars_of_rule: RuleIdxVariables = ex_vars_of_rule
            .into_iter()
            .map(|var| (r_idx_rule.0, var))
            .collect();
        let attacked_positive_vars: Variables = r_idx_rule
            .1
            .attacked_universal_variables(attacked_pos_by_rule_idx_vars);
        attacked_positive_vars.iter().for_each(|var| {
            self.add_edges_for_attacked_var(
                var,
                r_idx_rule.1,
                &ex_rule_idx_vars_of_rule,
                attacked_pos_by_rule_idx_vars,
            );
        })
    }

    fn add_nodes_for_rule(&mut self, (rule_idx, rule): RuleIdxRule<'a>) {
        let ex_vars_of_rule: Variables = rule.existential_variables();
        ex_vars_of_rule.into_iter().for_each(|var| {
            self.add_node((rule_idx, var));
        })
    }
}

/// This Trait provides methods for a JointAcyclicityGraph in relation with Cycle(s).
pub trait JointAcyclicityGraphCycle<N>: AcyclicityGraphCycle<N> {
    /// Returns all RuleIdxVariable(s) appearing in cycles of the JointAcyclicityGraph.
    fn rule_idx_variables_in_cycles(&self) -> HashSet<N>;
}

impl<'a> JointAcyclicityGraphCycle<RuleIdxVariable<'a>> for JointAcyclicityGraph<'a> {
    fn rule_idx_variables_in_cycles(&self) -> RuleIdxVariables<'a> {
        let cycles: Cycles<RuleIdxVariable<'a>> = self.cycles();
        cycles
            .iter()
            .fold(RuleIdxVariables::new(), |rule_idx_vars_in_cycles, cycle| {
                rule_idx_vars_in_cycles.insert_all_ret(cycle)
            })
    }
}

trait WeakAcyclicityGraphBuilderPrivate<'a> {
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

impl<'a> WeakAcyclicityGraphBuilderPrivate<'a> for WeakAcyclicityGraph<'a> {
    fn add_common_edges_for_pos(
        &mut self,
        body_pos: Position<'a>,
        head_ex_pos_of_var: &ExtendedPositions<'a>,
    ) {
        head_ex_pos_of_var.iter().for_each(|head_pos| {
            self.add_edge(body_pos, *head_pos, WeakAcyclicityGraphEdgeType::Common);
        })
    }

    fn add_common_edges_for_rule(&mut self, rule: &'a Rule, variable_body: &Variable) {
        let body_ex_pos_of_var: ExtendedPositions =
            variable_body.extended_positions_in_positive_body(rule);
        let head_ex_pos_of_var: ExtendedPositions = variable_body.extended_positions_in_head(rule);
        body_ex_pos_of_var
            .into_iter()
            .for_each(|body_pos| self.add_common_edges_for_pos(body_pos, &head_ex_pos_of_var));
    }

    fn add_edges_for_rule(&mut self, rule: &'a Rule) {
        let positive_variables: Variables = rule.positive_variables();
        positive_variables.iter().for_each(|variable_body| {
            self.add_common_edges_for_rule(rule, variable_body);
            self.add_special_edges_for_rule(rule, variable_body);
        });
    }

    fn add_special_edge(&mut self, body_pos: Position<'a>, existential_pos: Position<'a>) {
        match self.contains_edge(body_pos, existential_pos) {
            true => self.add_edge(
                body_pos,
                existential_pos,
                WeakAcyclicityGraphEdgeType::CommonAndSpecial,
            ),
            false => self.add_edge(
                body_pos,
                existential_pos,
                WeakAcyclicityGraphEdgeType::Special,
            ),
        };
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
            variable_body.extended_positions_in_positive_body(rule);
        let extended_pos_of_ex_vars: ExtendedPositions =
            rule.extended_positions_of_existential_variables();
        body_ex_pos_of_var.into_iter().for_each(|body_pos| {
            self.add_special_edges_for_pos(body_pos, &extended_pos_of_ex_vars);
        })
    }
}

/// This Trait provides methods for a WeakAcyclicityGraph in relation with Cycle(s).
pub trait WeakAcyclicityGraphCycle<N>: AcyclicityGraphCycle<N> {
    /// Checks if there exists a Cycle in the WeakAcyclicityGraph that contains a special edge.
    fn contains_cycle_with_special_edge(&self) -> bool;
    /// Returns all Cycles in the WeakAcyclicityGraph that contain a special edge.
    fn cycles_containing_special_edges(&self) -> Cycles<N>;
}

impl<'a> WeakAcyclicityGraphCycle<Position<'a>> for WeakAcyclicityGraph<'a> {
    fn contains_cycle_with_special_edge(&self) -> bool {
        let cycles: Cycles<Position<'a>> = self.cycles();
        cycles
            .iter()
            .any(|cycle| self.cycle_contains_special_edge(cycle))
    }

    fn cycles_containing_special_edges(&self) -> Cycles<Position<'a>> {
        let cycles: Cycles<Position<'a>> = self.cycles();
        cycles
            .iter()
            .filter(|cycle| self.cycle_contains_special_edge(cycle))
            .cloned()
            .collect()
    }
}

trait WeakAcyclicityGraphCyclePrivate<N> {
    fn cycle_contains_special_edge(&self, cycle: &Cycle<N>) -> bool;
    fn edge_is_special(&self, node: &N, next_node: &N) -> bool;
}

impl<'a> WeakAcyclicityGraphCyclePrivate<Position<'a>> for WeakAcyclicityGraph<'a> {
    fn cycle_contains_special_edge(&self, cycle: &Cycle<Position<'a>>) -> bool {
        let cycle_size: usize = cycle.len();
        cycle.iter().enumerate().any(|(i, pos)| {
            let j: usize = (i + 1) % cycle_size;
            let next_pos: &Position<'a> = cycle.get(j).unwrap();
            self.edge_is_special(pos, next_pos)
        })
    }

    fn edge_is_special(&self, node: &Position<'a>, next_node: &Position<'a>) -> bool {
        match self.edge_weight(*node, *next_node).unwrap() {
            WeakAcyclicityGraphEdgeType::Common => false,
            WeakAcyclicityGraphEdgeType::CommonAndSpecial => true,
            WeakAcyclicityGraphEdgeType::Special => true,
        }
    }
}

/// This Trait provides a method to get the inifinite rank Positions of some WeakAcyclicityGraph.
pub trait InfiniteRankPositions {
    /// Builds the infinite rank Positions for some WeakAcyclicityGraph.
    fn infinite_rank_positions(&self) -> Positions;
}

impl InfiniteRankPositions for WeakAcyclicityGraph<'_> {
    fn infinite_rank_positions(&self) -> Positions {
        let cycs_con_spe_edge: Cycles<Position> = self.cycles_containing_special_edges();
        let repr_of_cycles: HashSet<Position> = self.represantatives_of_cycles(&cycs_con_spe_edge);
        let mut inf_rank_pos_set: HashSet<Position> = self.cycles_into_nodes(cycs_con_spe_edge);
        let all_nodes: HashSet<Position> = self.all_nodes();
        let mut unreachable_nodes: HashSet<Position> = all_nodes.remove_all_ret(&inf_rank_pos_set);
        for pos in repr_of_cycles.iter() {
            if unreachable_nodes.is_empty() {
                break;
            }
            let rea_nodes_of_node: HashSet<Position> = self.reachable_nodes_of_node(pos);
            unreachable_nodes.remove_all(&rea_nodes_of_node);
            inf_rank_pos_set.insert_all_take(rea_nodes_of_node);
        }
        Positions::from_extended_positions(inf_rank_pos_set)
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
                let node_other: &N = other.get((j + offset) % cycle_size).unwrap();
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
