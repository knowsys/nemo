//! Functionality that provides methods to build the (JointAcyclicityGraph / WeakAcyclicityGraph) based on
/// a RuleSet.
use crate::rule_model::components::{rule::Rule, term::primitive::variable::Variable};
use crate::static_checks::collection_traits::{InsertAll, RemoveAll};
use crate::static_checks::positions::{Position, Positions, PositionsByRuleAndVariables};
use crate::static_checks::rule_set::{
    AllPositivePositions, ExistentialVariables, RuleAndVariable, RuleSet,
};
use petgraph::algo::is_cyclic_directed;
use petgraph::graphmap::{DiGraphMap, NodeTrait};
use std::collections::HashSet;

use super::rule_set::ExistentialRuleAndVariables;

type Cycle<N> = Vec<N>;

/// Enum to distinguish between (Common / Special) edge between two Position(s) of the
/// WeakAcyclicityGraph.
#[derive(Clone, Copy, Debug)]
pub enum WeakAcyclicityGraphEdgeType {
    /// There is a common edge between two Position(s) of the WeakAcyclicityGraph.
    Common,
    /// There is a special edge between two Position(s) of the WeakAcyclicityGraph.
    Special,
}

/// Type to declare a JointAcyclicityGraph of some RuleSet.
#[derive(Debug)]
pub struct JointAcyclicityGraph<'a>(DiGraphMap<RuleAndVariable<'a>, ()>);

/// This Impl-Block contains the function for creating the JointAcyclicityGraph.
impl<'a> JointAcyclicityGraph<'a> {
    /// Builds the JointAcyclicityGraph of a RuleSet.
    pub fn new(rule_set: &'a RuleSet) -> Self {
        let mut jo_ac_graph_builder: JointAcyclicityGraphBuilder =
            JointAcyclicityGraphBuilder::new();
        jo_ac_graph_builder.add_nodes(rule_set);
        jo_ac_graph_builder.add_edges(rule_set);
        jo_ac_graph_builder.finalize()
    }
}

impl<'a> JointAcyclicityGraph<'a> {
    /// Returns all nodes of a Graph that appear in a Cycle.
    pub fn all_nodes_of_cycles(&self) -> HashSet<RuleAndVariable<'a>> {
        let cycles: HashSet<Cycle<RuleAndVariable>> = self.cycles();
        self.cycles_into_nodes(cycles)
    }

    /// Checks if a Graph is cyclic.
    pub fn is_cyclic(&self) -> bool {
        is_cyclic_directed(&self.0)
    }
}

/// This struct declares a JointAcyclicityGraph of a RuleSet.
#[derive(Debug)]
struct JointAcyclicityGraphBuilder<'a>(DiGraphMap<RuleAndVariable<'a>, ()>);

/// This Impl-Block contains the main methods to Build the JointAcyclicityGraph.
impl<'a> JointAcyclicityGraphBuilder<'a> {
    fn add_edges(&mut self, rule_set: &'a RuleSet) {
        let attacked_pos_by_ex_ruleandvars: PositionsByRuleAndVariables =
            rule_set.attacked_positions_by_existential_ruleandvariables();
        rule_set.0.iter().for_each(|rule| {
            self.add_edges_for_rule(rule, &attacked_pos_by_ex_ruleandvars);
        })
    }

    fn add_nodes(&mut self, rule_set: &'a RuleSet) {
        rule_set.0.iter().for_each(|rule| {
            self.add_nodes_for_rule(rule);
        })
    }

    fn finalize(self) -> JointAcyclicityGraph<'a> {
        JointAcyclicityGraph(self.0)
    }

    fn new() -> Self {
        Self(DiGraphMap::new())
    }
}

/// This Impl-Block contains help methods to Build the JointAcyclicityGraph.
impl<'a> JointAcyclicityGraphBuilder<'a> {
    fn add_edges_for_attacked_var(
        &mut self,
        attacked_ruleandvar: RuleAndVariable,
        ex_ruleandvars: &HashSet<RuleAndVariable<'a>>,
        attacked_pos_by_ruleandvars: &PositionsByRuleAndVariables<'a>,
    ) {
        attacked_pos_by_ruleandvars
            .0
            .keys()
            .filter(|attacking_ruleandvar| {
                attacked_ruleandvar.is_attacked_by_ruleandvariable(
                    attacking_ruleandvar,
                    attacked_pos_by_ruleandvars,
                )
            })
            .for_each(|attacking_ruleandvar| {
                self.add_edges_for_attacking_var(attacking_ruleandvar, ex_ruleandvars);
            });
    }

    fn add_edges_for_attacking_var(
        &mut self,
        attacking_ruleandvar: &RuleAndVariable<'a>,
        ex_ruleandvars: &HashSet<RuleAndVariable<'a>>,
    ) {
        ex_ruleandvars.iter().for_each(|ex_rule_idx_var| {
            self.0.add_edge(*attacking_ruleandvar, *ex_rule_idx_var, ());
        })
    }

    fn add_edges_for_rule(
        &mut self,
        rule: &'a Rule,
        attacked_pos_by_ruleandvars: &PositionsByRuleAndVariables<'a>,
    ) {
        let ex_ruleandvars: HashSet<RuleAndVariable> = rule.existential_ruleandvariables();
        let attacked_positive_vars: HashSet<&Variable> =
            rule.attacked_universal_variables(attacked_pos_by_ruleandvars);
        attacked_positive_vars.iter().for_each(|var| {
            self.add_edges_for_attacked_var(
                RuleAndVariable(rule, var),
                &ex_ruleandvars,
                attacked_pos_by_ruleandvars,
            );
        })
    }

    fn add_nodes_for_rule(&mut self, rule: &'a Rule) {
        let ex_ruleandvars: HashSet<RuleAndVariable> = rule.existential_ruleandvariables();
        ex_ruleandvars.into_iter().for_each(|ruleandvar| {
            self.0.add_node(ruleandvar);
        })
    }
}

#[derive(Debug)]
/// This struct declares a WeakAcyclicityGraph of a RuleSet.
pub struct WeakAcyclicityGraph<'a>(DiGraphMap<Position<'a>, WeakAcyclicityGraphEdgeType>);

/// This Impl-Block contains the function for creating the WeakAcyclicityGraph.
impl<'a> WeakAcyclicityGraph<'a> {
    /// Builds the WeakAcyclicityGraph of a RuleSet.
    pub fn new(rule_set: &'a RuleSet) -> WeakAcyclicityGraph<'a> {
        let mut we_ac_graph_builder: WeakAcyclicityGraphBuilder = WeakAcyclicityGraphBuilder::new();
        we_ac_graph_builder.add_nodes(rule_set);
        we_ac_graph_builder.add_edges(rule_set);
        we_ac_graph_builder.finalize()
    }
}

/// This Impl-Block contains the main cycle methods that are exclusive for the WeakAcyclicityGraph.
impl<'a> WeakAcyclicityGraph<'a> {
    /// Checks if there exists a Cycle in the WeakAcyclicityGraph that contains a special edge.
    pub fn contains_cycle_with_special_edge(&self) -> bool {
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
            self.edge_is_special(pos, next_pos)
        })
    }

    /// Returns all Cycles in the WeakAcyclicityGraph that contain a special edge.
    fn cycles_containing_special_edges(&self) -> HashSet<Cycle<Position<'a>>> {
        let cycles: HashSet<Cycle<Position<'a>>> = self.cycles();
        let special_edge_cycles: HashSet<Cycle<Position<'a>>> = cycles
            .iter()
            .filter(|cycle| self.cycle_contains_special_edge(cycle))
            .cloned()
            .collect();
        special_edge_cycles
    }

    fn edge_is_special(&self, node: &Position<'a>, next_node: &Position<'a>) -> bool {
        matches!(
            self.0.edge_weight(*node, *next_node).unwrap(),
            WeakAcyclicityGraphEdgeType::Special
        )
    }
}

/// This Impl-Block provides a method to get the inifinite rank Positions of some WeakAcyclicityGraph.
impl<'a> WeakAcyclicityGraph<'a> {
    /// Returns all nodes of a Graph.
    fn all_nodes(&self) -> HashSet<Position> {
        self.0.nodes().collect()
    }

    fn conclude_reachable_nodes(&self, nodes: HashSet<Position<'a>>) -> HashSet<Position<'a>> {
        nodes
            .into_iter()
            .fold(HashSet::<Position>::new(), |neighbors, node| {
                let neighbors_of_node: HashSet<Position> = self.0.neighbors(node).collect();
                neighbors.insert_all_take_ret(neighbors_of_node)
            })
    }

    /// Builds the infinite rank Positions for some WeakAcyclicityGraph.
    pub fn infinite_rank_positions(&self) -> Positions {
        let cycs_con_spe_edge: HashSet<Cycle<Position>> = self.cycles_containing_special_edges();
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
        Positions::from(inf_rank_pos_set)
    }

    /// Returns all reachable nodes of some node in the Graph.
    fn reachable_nodes_of_node(&self, node: &Position<'a>) -> HashSet<Position<'a>> {
        let mut rea_nodes: HashSet<Position> = HashSet::from([*node]);
        let mut new_found_rea_nodes: HashSet<Position> = rea_nodes.clone();
        while !new_found_rea_nodes.is_empty() {
            let new_con_rea_nodes: HashSet<Position> =
                self.conclude_reachable_nodes(new_found_rea_nodes);
            new_found_rea_nodes = new_con_rea_nodes.remove_all_ret(&rea_nodes);
            rea_nodes.insert_all(&new_found_rea_nodes);
        }
        rea_nodes
    }

    /// Returns a represantative node of every Cycle in some Cycles.
    fn represantatives_of_cycles(
        &self,
        cycles: &HashSet<Cycle<Position<'a>>>,
    ) -> HashSet<Position<'a>> {
        cycles
            .iter()
            .fold(HashSet::<Position>::new(), |mut reprs, cycle| {
                let repr_of_cycle: Position = *cycle.first().unwrap();
                reprs.insert(repr_of_cycle);
                reprs
            })
    }
}

#[derive(Debug)]
struct WeakAcyclicityGraphBuilder<'a>(DiGraphMap<Position<'a>, WeakAcyclicityGraphEdgeType>);

/// This Impl-Block contains the main methods to Build the WeakAcyclicityGraph.
impl<'a> WeakAcyclicityGraphBuilder<'a> {
    fn add_edges(&mut self, rule_set: &'a RuleSet) {
        rule_set.0.iter().for_each(|rule| {
            self.add_edges_for_rule(rule);
        });
    }

    fn add_nodes(&mut self, rule_set: &'a RuleSet) {
        let all_positive_ex_pos: HashSet<Position> = rule_set.all_positive_extended_positions();
        all_positive_ex_pos.into_iter().for_each(|pos| {
            self.0.add_node(pos);
        });
    }

    fn finalize(self) -> WeakAcyclicityGraph<'a> {
        WeakAcyclicityGraph(self.0)
    }

    fn new() -> Self {
        Self(DiGraphMap::new())
    }
}

/// This Impl-Block contains help methods to Build the WeakAcyclicityGraph.
impl<'a> WeakAcyclicityGraphBuilder<'a> {
    fn add_common_edges_for_pos(
        &mut self,
        body_pos: Position<'a>,
        head_ex_pos_of_var: &HashSet<Position<'a>>,
    ) {
        head_ex_pos_of_var.iter().for_each(|head_pos| {
            self.0
                .add_edge(body_pos, *head_pos, WeakAcyclicityGraphEdgeType::Common);
        })
    }

    // FIXME: I THINK USING VARIABLE_BODY FOR THE HEAD POSITIONS IS WRONG. IT SHOULD BE A
    // GENERALISED VARIABLE
    fn add_common_edges_for_ruleandbodyvariable(&mut self, ruleandvar: RuleAndVariable<'a>) {
        let body_ex_pos_of_var: HashSet<Position> =
            ruleandvar.extended_positions_in_positive_body();
        let head_ex_pos_of_var: HashSet<Position> = ruleandvar.extended_positions_in_head();
        body_ex_pos_of_var
            .into_iter()
            .for_each(|body_pos| self.add_common_edges_for_pos(body_pos, &head_ex_pos_of_var));
    }

    fn add_edges_for_rule(&mut self, rule: &'a Rule) {
        let positive_variables: HashSet<&Variable> = rule.positive_variables();
        positive_variables.iter().for_each(|body_var| {
            let ruleandbodyvar: RuleAndVariable = RuleAndVariable(rule, body_var);
            self.add_common_edges_for_ruleandbodyvariable(ruleandbodyvar);
            self.add_special_edges_for_ruleandbodyvariable(ruleandbodyvar);
        });
    }

    fn add_special_edge(&mut self, body_pos: Position<'a>, existential_pos: Position<'a>) {
        self.0.add_edge(
            body_pos,
            existential_pos,
            WeakAcyclicityGraphEdgeType::Special,
        );
    }

    fn add_special_edges_for_pos(
        &mut self,
        body_pos: Position<'a>,
        extended_pos_of_ex_vars: &HashSet<Position<'a>>,
    ) {
        extended_pos_of_ex_vars.iter().for_each(|existential_pos| {
            self.add_special_edge(body_pos, *existential_pos);
        })
    }

    fn add_special_edges_for_ruleandbodyvariable(&mut self, ruleandvar: RuleAndVariable<'a>) {
        let body_ex_pos_of_var: HashSet<Position> =
            ruleandvar.extended_positions_in_positive_body();
        let extended_pos_of_ex_vars: HashSet<Position> =
            ruleandvar.0.extended_positions_of_existential_variables();
        body_ex_pos_of_var.into_iter().for_each(|body_pos| {
            self.add_special_edges_for_pos(body_pos, &extended_pos_of_ex_vars);
        })
    }
}

/// This Trait provides methods that are used for both Graph(s) in relation with Cycle(s).
trait AcyclicityGraphMethods<N>
where
    N: NodeTrait,
{
    /// Returns all Cycles of a Graph.
    fn cycles(&self) -> HashSet<Cycle<N>>;
    /// Returns all nodes of all Cycles in a Graph.
    fn cycles_into_nodes(&self, cycles: HashSet<Cycle<N>>) -> HashSet<N>;
}

impl<N, E> AcyclicityGraphMethods<N> for DiGraphMap<N, E>
where
    N: NodeTrait,
{
    fn cycles(&self) -> HashSet<Cycle<N>> {
        let mut cycles: HashSet<Cycle<N>> = HashSet::<Cycle<N>>::new();
        let mut sorted_cycles: HashSet<Cycle<N>> = HashSet::<Cycle<N>>::new();
        let mut used_nodes: HashSet<N> = HashSet::<N>::new();
        let mut reused_cycle: Cycle<N> = Cycle::<N>::new();
        for node in self.nodes() {
            if used_nodes.contains(&node) {
                continue;
            }
            self.cycles_rec(
                &node,
                &mut reused_cycle,
                &mut used_nodes,
                &mut cycles,
                &mut sorted_cycles,
            );
        }
        cycles
    }

    fn cycles_into_nodes(&self, cycles: HashSet<Cycle<N>>) -> HashSet<N> {
        cycles
            .into_iter()
            .fold(HashSet::<N>::new(), |nodes, cycle| {
                nodes.insert_all_take_ret(cycle)
            })
    }
}

impl<'a> AcyclicityGraphMethods<RuleAndVariable<'a>> for JointAcyclicityGraph<'a> {
    fn cycles(&self) -> HashSet<Cycle<RuleAndVariable<'a>>> {
        self.0.cycles()
    }

    fn cycles_into_nodes(
        &self,
        cycles: HashSet<Cycle<RuleAndVariable<'a>>>,
    ) -> HashSet<RuleAndVariable<'a>> {
        self.0.cycles_into_nodes(cycles)
    }
}

impl<'a> AcyclicityGraphMethods<Position<'a>> for WeakAcyclicityGraph<'a> {
    fn cycles(&self) -> HashSet<Cycle<Position<'a>>> {
        self.0.cycles()
    }

    fn cycles_into_nodes(&self, cycles: HashSet<Cycle<Position<'a>>>) -> HashSet<Position<'a>> {
        self.0.cycles_into_nodes(cycles)
    }
}

trait AcyclicityGraphMethodsPrivate<N>
where
    N: NodeTrait,
{
    fn cycles_rec(
        &self,
        node: &N,
        reused_cycle: &mut Cycle<N>,
        used_nodes: &mut HashSet<N>,
        cycles: &mut HashSet<Cycle<N>>,
        sorted_cycles: &mut HashSet<Cycle<N>>,
    );
    fn recursion_stops(
        &self,
        node: &N,
        reused_cycle: &mut Cycle<N>,
        cycles: &mut HashSet<Cycle<N>>,
        sorted_cycles: &mut HashSet<Cycle<N>>,
    ) -> bool;
}

impl<N, E> AcyclicityGraphMethodsPrivate<N> for DiGraphMap<N, E>
where
    N: NodeTrait,
{
    fn cycles_rec(
        &self,
        node: &N,
        reused_cycle: &mut Cycle<N>,
        used_nodes: &mut HashSet<N>,
        cycles: &mut HashSet<Cycle<N>>,
        sorted_cycles: &mut HashSet<Cycle<N>>,
    ) {
        if self.recursion_stops(node, reused_cycle, cycles, sorted_cycles) {
            return;
        }
        used_nodes.insert(*node);
        reused_cycle.push(*node);
        self.neighbors(*node).for_each(|neighbor| {
            self.cycles_rec(&neighbor, reused_cycle, used_nodes, cycles, sorted_cycles);
        });
        reused_cycle.pop();
    }

    fn recursion_stops(
        &self,
        node: &N,
        reused_cycle: &mut Cycle<N>,
        cycles: &mut HashSet<Cycle<N>>,
        sorted_cycles: &mut HashSet<Cycle<N>>,
    ) -> bool {
        if !reused_cycle.contains(node) {
            return false;
        }
        let n: usize = reused_cycle
            .iter()
            .position(|cycle_node| cycle_node == node)
            .unwrap();
        let mut cycle: Cycle<N> = reused_cycle.split_off(n);
        let mut sorted_cycle: Cycle<N> = cycle.clone();
        sorted_cycle.sort();
        if !sorted_cycles.contains(&sorted_cycle) {
            cycles.insert(cycle.clone());
            sorted_cycles.insert(sorted_cycle);
        }
        reused_cycle.append(&mut cycle);
        true
    }
}
