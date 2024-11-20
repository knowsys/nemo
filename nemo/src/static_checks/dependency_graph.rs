use crate::rule_model::components::{rule::Rule, tag::Tag, term::primitive::variable::Variable};
use crate::static_checks::{positions::Positions, rule_set::RuleSet};
use petgraph::{graphmap::GraphMap, Directed};
use std::collections::{
    hash_set::{IntoIter, Iter},
    HashSet,
};

#[derive(Copy, Clone, Eq, Hash, PartialEq, Ord, PartialOrd)]
struct Position<'a>(&'a Tag, usize);

struct ExtendedPositions<'a>(HashSet<Position<'a>>);

impl<'a> ExtendedPositions<'a> {
    pub fn new() -> Self {
        ExtendedPositions(HashSet::<Position>::new())
    }

    fn into_iter(self) -> IntoIter<Position<'a>> {
        self.0.into_iter()
    }

    fn iter(&self) -> Iter<Position> {
        self.0.iter()
    }
}

impl<'a> From<HashSet<Position<'a>>> for ExtendedPositions<'a> {
    fn from(position_set: HashSet<Position<'a>>) -> Self {
        ExtendedPositions(position_set)
    }
}

#[derive(Clone)]
enum WeaklyAcyclicityGraphEdgeType {
    CommonEdge,
    SpeciaEdge,
}

type WeaklyAcyclicityGraph<'a> = GraphMap<Position<'a>, WeaklyAcyclicityGraphEdgeType, Directed>;

trait WeaklyAcyclicityGraphBuilder<'a> {
    fn add_edges(&mut self, rule_set: &'a RuleSet);
    fn add_common_edges_for_var(&mut self, rule: &'a Rule, variable_body: &Variable);
    fn add_common_edges_for_pos(
        &mut self,
        body_pos: Position<'a>,
        head_pos_of_var_ex: ExtendedPositions<'a>,
    );
    fn add_edges_for_rule(&mut self, rule: &'a Rule);
    fn add_special_edges_for_rule_and_body_var(&mut self, rule: &'a Rule, variable_body: &Variable);
    fn add_nodes(&mut self, all_positive_extended_positions: ExtendedPositions<'a>);
}

impl<'a> WeaklyAcyclicityGraphBuilder<'a> for WeaklyAcyclicityGraph<'a> {
    fn add_edges(&mut self, rule_set: &'a RuleSet) {
        rule_set.iter().for_each(|rule| {
            self.add_edges_for_rule(rule);
        });
    }

    fn add_common_edges_for_var(&mut self, rule: &'a Rule, variable_body: &Variable) {
        let body_pos_of_var: Positions =
            variable_body.get_positions_in_atoms(&rule.body_positive_refs());
        let body_pos_of_var_ex: ExtendedPositions = ExtendedPositions::from(body_pos_of_var);
        let head_pos_of_var: Positions = variable_body.get_positions_in_atoms(&rule.head_refs());
        let head_pos_of_var_ex: ExtendedPositions = ExtendedPositions::from(head_pos_of_var);
        body_pos_of_var_ex
            .into_iter()
            .for_each(|body_pos| self.add_common_edges_for_pos(body_pos, head_pos_of_var_ex))
    }

    fn add_common_edges_for_pos(
        &mut self,
        body_pos: Position<'a>,
        head_pos_of_var_ex: ExtendedPositions<'a>,
    ) {
        head_pos_of_var_ex.into_iter().for_each(|head_pos| {
            self.add_edge(
                body_pos,
                head_pos,
                WeaklyAcyclicityGraphEdgeType::CommonEdge,
            );
        })
    }

    fn add_edges_for_rule(&mut self, rule: &'a Rule) {
        let positive_variables: HashSet<&Variable> = rule.positive_variables();
        let existential_variables: HashSet<&Variable> = rule.existential_variables();
        positive_variables.iter().for_each(|variable_body| {
            self.add_common_edges_for_var(rule, variable_body);
            self.add_special_edges_for_rule_and_body_var(rule, variable_body);
        });
    }

    fn add_special_edges_for_rule_and_body_var(
        &mut self,
        rule: &'a Rule,
        variable_body: &Variable,
    ) {
        todo!("IMPLEMENT");
        // TODO: IMPLEMENT
    }

    fn add_nodes(&mut self, all_positive_extended_positions: ExtendedPositions<'a>) {
        all_positive_extended_positions.into_iter().for_each(|pos| {
            self.add_node(pos);
        });
    }
}

trait WeaklyAcyclicityGraphConstructor {
    fn all_positive_extended_positions(&self) -> ExtendedPositions;
    fn build_graph(&self) -> WeaklyAcyclicityGraph;
}

impl WeaklyAcyclicityGraphConstructor for RuleSet {
    fn all_positive_extended_positions(&self) -> ExtendedPositions {
        let all_positive_positions: Positions = self.all_positive_positions();
        ExtendedPositions::from(all_positive_positions)
    }

    fn build_graph(&self) -> WeaklyAcyclicityGraph {
        let mut we_ac_graph: WeaklyAcyclicityGraph = WeaklyAcyclicityGraph::new();
        let all_positive_ex_pos: ExtendedPositions = self.all_positive_extended_positions();
        we_ac_graph.add_nodes(all_positive_ex_pos);
        we_ac_graph.add_edges(self);
        we_ac_graph
    }
}

// TODO: MAYBE SHORTEN FUNCTION
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
