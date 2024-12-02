use crate::rule_model::components::{rule::Rule, term::primitive::variable::Variable};
use crate::static_checks::acyclicity_graphs::{JointlyAcyclicityGraph, WeaklyAcyclicityGraph};
use crate::static_checks::collection_traits::InsertAllRet;
use crate::static_checks::positions::{ExtendedPositions, Position, Positions};
use crate::static_checks::rule_set::RuleSet;

use petgraph::algo::is_cyclic_directed;
use petgraph::graphmap::{DiGraphMap, NodeTrait};
use std::collections::{BTreeSet, HashMap, HashSet};

#[derive(Clone)]
pub enum WeaklyAcyclicityGraphEdgeType {
    CommonEdge,
    SpecialEdge,
}

pub(crate) trait AcyclicityGraphCycleInternal<N>
where
    N: NodeTrait,
{
    fn cycles_internal(&self) -> HashSet<BTreeSet<N>>;
    fn cycles_rec(
        &self,
        neighbor: N,
        visited_nodes: &mut BTreeSet<N>,
        cycles: &mut HashSet<BTreeSet<N>>,
    );
    fn cycles_with_start(&self, node: N) -> HashSet<BTreeSet<N>>;
    fn is_cyclic_internal(&self) -> bool;
}

impl<N, E> AcyclicityGraphCycleInternal<N> for DiGraphMap<N, E>
where
    N: NodeTrait,
{
    fn cycles_internal(&self) -> HashSet<BTreeSet<N>> {
        self.nodes()
            .fold(HashSet::<BTreeSet<N>>::new(), |cycles, node| {
                let cycles_with_start: HashSet<BTreeSet<N>> = self.cycles_with_start(node);
                cycles.insert_all_ret(&cycles_with_start)
            })
    }

    fn cycles_rec(
        &self,
        neighbor: N,
        visited_nodes: &mut BTreeSet<N>,
        cycles: &mut HashSet<BTreeSet<N>>,
    ) {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn cycles_with_start(&self, node: N) -> HashSet<BTreeSet<N>> {
        let mut cycles: HashSet<BTreeSet<N>> = HashSet::<BTreeSet<N>>::new();
        let mut visited_nodes: BTreeSet<N> = BTreeSet::from([node]);
        self.neighbors(node).for_each(|neighbor| {
            self.cycles_rec(neighbor, &mut visited_nodes, &mut cycles);
        });
        cycles
    }

    fn is_cyclic_internal(&self) -> bool {
        is_cyclic_directed(self)
    }
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
    fn add_common_edges_for_var(&mut self, rule: &'a Rule, variable_body: &Variable);
    fn add_common_edges_for_pos(
        &mut self,
        body_pos: Position<'a>,
        head_ex_pos_of_var: &ExtendedPositions<'a>,
    );
    fn add_edges_for_rule(&mut self, rule: &'a Rule);
    fn add_special_edges_for_pos(
        &mut self,
        body_pos: Position<'a>,
        extended_pos_of_ex_vars: &ExtendedPositions<'a>,
    );
    fn add_special_edges_for_rule(&mut self, rule: &'a Rule, variable_body: &Variable);
}

impl<'a> WeaklyAcyclicityGraphBuilderInternal<'a> for WeaklyAcyclicityGraph<'a> {
    fn add_common_edges_for_var(&mut self, rule: &'a Rule, variable_body: &Variable) {
        let body_ex_pos_of_var: ExtendedPositions =
            variable_body.get_extended_positions_in_positive_body(rule);
        let head_ex_pos_of_var: ExtendedPositions =
            variable_body.get_extended_positions_in_head(rule);
        body_ex_pos_of_var
            .into_iter()
            .for_each(|body_pos| self.add_common_edges_for_pos(body_pos, &head_ex_pos_of_var));
    }

    fn add_common_edges_for_pos(
        &mut self,
        body_pos: Position<'a>,
        head_ex_pos_of_var: &ExtendedPositions<'a>,
    ) {
        head_ex_pos_of_var.iter().for_each(|head_pos| {
            self.add_edge(
                body_pos,
                *head_pos,
                WeaklyAcyclicityGraphEdgeType::CommonEdge,
            );
        })
    }

    fn add_edges_for_rule(&mut self, rule: &'a Rule) {
        let positive_variables: HashSet<&Variable> = rule.positive_variables();
        positive_variables.iter().for_each(|variable_body| {
            self.add_common_edges_for_var(rule, variable_body);
            self.add_special_edges_for_rule(rule, variable_body);
        });
    }

    fn add_special_edges_for_pos(
        &mut self,
        body_pos: Position<'a>,
        extended_pos_of_ex_vars: &ExtendedPositions<'a>,
    ) {
        extended_pos_of_ex_vars.iter().for_each(|ex_pos| {
            self.add_edge(
                body_pos,
                *ex_pos,
                WeaklyAcyclicityGraphEdgeType::SpecialEdge,
            );
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
