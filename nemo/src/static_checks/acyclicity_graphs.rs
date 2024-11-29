use crate::rule_model::components::{rule::Rule, tag::Tag, term::primitive::variable::Variable};
use crate::static_checks::{positions::Positions, rule_set::RuleSet};
use petgraph::graphmap::DiGraphMap;
use std::collections::{
    hash_set::{IntoIter, Iter},
    HashMap, HashSet,
};

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
struct Position<'a>(&'a Tag, usize);

#[derive(Clone, Debug)]
pub struct ExtendedPositions<'a>(HashSet<Position<'a>>);

impl<'a> ExtendedPositions<'a> {
    pub fn new() -> Self {
        ExtendedPositions(HashSet::<Position>::new())
    }

    fn into_iter(self) -> IntoIter<Position<'a>> {
        self.0.into_iter()
    }

    fn iter(&self) -> Iter<Position<'a>> {
        self.0.iter()
    }
}

impl Default for ExtendedPositions<'_> {
    fn default() -> Self {
        ExtendedPositions::new()
    }
}

#[derive(Clone)]
enum WeaklyAcyclicityGraphEdgeType {
    CommonEdge,
    SpecialEdge,
}

type JointlyAcyclicityGraph<'a> = DiGraphMap<&'a Variable, ()>;

type WeaklyAcyclicityGraph<'a> = DiGraphMap<Position<'a>, WeaklyAcyclicityGraphEdgeType>;

trait AcyclicityGraphBuilder<'a> {
    fn build_graph(rule_set: &'a RuleSet) -> Self;
    fn add_nodes(&mut self, rule_set: &'a RuleSet);
    fn add_edges(&mut self, rule_set: &'a RuleSet);
}

impl<'a> AcyclicityGraphBuilder<'a> for JointlyAcyclicityGraph<'a> {
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

    fn build_graph(rule_set: &'a RuleSet) -> Self {
        let mut jo_ac_graph: JointlyAcyclicityGraph = JointlyAcyclicityGraph::new();
        jo_ac_graph.add_nodes(rule_set);
        jo_ac_graph.add_edges(rule_set);
        jo_ac_graph
    }
}

impl<'a> AcyclicityGraphBuilder<'a> for WeaklyAcyclicityGraph<'a> {
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

    fn build_graph(rule_set: &'a RuleSet) -> Self {
        let mut we_ac_graph: WeaklyAcyclicityGraph = WeaklyAcyclicityGraph::new();
        we_ac_graph.add_nodes(rule_set);
        we_ac_graph.add_edges(rule_set);
        we_ac_graph
    }
}

trait WeaklyAcyclicityGraphBuilder<'a>: AcyclicityGraphBuilder<'a> {
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

impl<'a> WeaklyAcyclicityGraphBuilder<'a> for WeaklyAcyclicityGraph<'a> {
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

trait JointlyAcyclicityGraphBuilder<'a>: AcyclicityGraphBuilder<'a> {
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

impl<'a> JointlyAcyclicityGraphBuilder<'a> for JointlyAcyclicityGraph<'a> {
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

trait AcyclicityGraphConstructor<'a> {
    fn weakly_acyclicity_graph(&'a self) -> WeaklyAcyclicityGraph<'a>;
    fn jointly_acyclicity_graph(&'a self) -> JointlyAcyclicityGraph<'a>;
}

impl<'a> AcyclicityGraphConstructor<'a> for RuleSet {
    fn weakly_acyclicity_graph(&'a self) -> WeaklyAcyclicityGraph<'a> {
        WeaklyAcyclicityGraph::build_graph(self)
    }

    fn jointly_acyclicity_graph(&'a self) -> JointlyAcyclicityGraph<'a> {
        JointlyAcyclicityGraph::build_graph(self)
    }
}

impl<'a> From<Positions<'a>> for ExtendedPositions<'a> {
    fn from(positions: Positions<'a>) -> Self {
        ExtendedPositions::from(positions.into_iter().fold(
            HashSet::<Position>::new(),
            |ex_pos: HashSet<Position>, (pred, indices): (&'a Tag, HashSet<usize>)| {
                ex_pos
                    .union(&indices.iter().map(|index| Position(pred, *index)).collect())
                    .copied()
                    .collect()
            },
        ))
    }
}

impl<'a> From<HashSet<Position<'a>>> for ExtendedPositions<'a> {
    fn from(position_set: HashSet<Position<'a>>) -> Self {
        ExtendedPositions(position_set)
    }
}
