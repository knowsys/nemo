use std::collections::{HashMap, HashSet};

use petgraph::{
    visit::{Dfs, EdgeFiltered},
    Directed,
};

use crate::{
    model::{
        chase_model::{ChaseAtom, ChaseRule},
        types::error::TypeError,
        Identifier, PrimitiveTerm, PrimitiveType, Variable,
    },
    util::labeled_graph::LabeledGraph,
};

use super::type_requirement::PredicateTypeRequirements;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct PredicatePosition {
    pub(super) predicate: Identifier,
    pub(super) position: usize,
}

impl PredicatePosition {
    /// Create new [`PredicatePosition`].
    pub fn new(predicate: Identifier, position: usize) -> Self {
        Self {
            predicate,
            position,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub(super) enum PositionGraphEdge {
    WithinBody,
    BodyToHeadSameVariable,
    /// Describes data flow from an aggregate input variable to and aggregate output variable, where the output variable has a static type (see [`AggregateOperation::static_output_type`]).
    /// This has no impact on type propagation from input to output, but is there for completeness sake, as it still describes some data flow.
    BodyToHeadAggregateStaticOutputType,
    /// Describes data flow from an aggregate input variable to and aggregate output variable, where the aggregate output variable has the same type as the input variable (see [`AggregateOperation::static_output_type`]).
    BodyToHeadAggregateNonStaticOutputType,
}

pub(super) type PositionGraph = LabeledGraph<PredicatePosition, PositionGraphEdge, Directed>;

impl PositionGraph {
    pub(super) fn from_rules(rules: &Vec<ChaseRule>) -> PositionGraph {
        let mut graph = PositionGraph::default();

        for rule in rules {
            let mut variables_to_head_positions =
                HashMap::<Variable, Vec<PredicatePosition>>::new();

            for atom in rule.head() {
                for (term_position, term) in atom.terms().iter().enumerate() {
                    if let PrimitiveTerm::Variable(variable) = term {
                        let predicate_position =
                            PredicatePosition::new(atom.predicate(), term_position);

                        variables_to_head_positions
                            .entry(variable.clone())
                            .and_modify(|e| e.push(predicate_position.clone()))
                            .or_insert(vec![predicate_position]);
                    }
                }
            }

            let mut aggregate_input_to_output_variables =
                HashMap::<Variable, Vec<(Variable, PositionGraphEdge)>>::new();
            for aggregate in rule.aggregates() {
                for input_variable_identifier in &aggregate.input_variables {
                    let edge_label = if aggregate.aggregate_operation.static_output_type().is_some()
                    {
                        PositionGraphEdge::BodyToHeadAggregateStaticOutputType
                    } else {
                        PositionGraphEdge::BodyToHeadAggregateNonStaticOutputType
                    };

                    aggregate_input_to_output_variables
                        .entry(input_variable_identifier.clone())
                        .or_default()
                        .push((aggregate.output_variable.clone(), edge_label));
                }
            }

            let mut variables_to_last_node = HashMap::<Variable, PredicatePosition>::new();

            for atom in rule.all_body() {
                for (term_position, variable) in atom.terms().iter().enumerate() {
                    let predicate_position =
                        PredicatePosition::new(atom.predicate(), term_position);

                    // Add head position edges
                    {
                        // NOTE: we connect each body position to each head position of the same variable
                        for pos in variables_to_head_positions
                            .get(variable)
                            .into_iter()
                            .flatten()
                        {
                            graph.add_edge(
                                predicate_position.clone(),
                                pos.clone(),
                                PositionGraphEdge::BodyToHeadSameVariable,
                            );
                        }

                        // NOTE: we connect every aggregate input variable to it's corresponding output variable in the head
                        for (output_variable_identifier, edge_label) in
                            aggregate_input_to_output_variables
                                .get(variable)
                                .into_iter()
                                .flatten()
                        {
                            for pos in variables_to_head_positions
                                .get(&output_variable_identifier.clone())
                                .unwrap()
                            {
                                graph.add_edge(
                                    predicate_position.clone(),
                                    pos.clone(),
                                    *edge_label,
                                );
                            }
                        }
                    }

                    // NOTE: we do not fully interconnect body positions as we start DFS from
                    // each possible position later covering all possible combinations
                    // nonetheless
                    variables_to_last_node
                        .entry(variable.clone())
                        .and_modify(|entry| {
                            let last_position =
                                std::mem::replace(entry, predicate_position.clone());
                            graph.add_edge(
                                last_position.clone(),
                                predicate_position.clone(),
                                PositionGraphEdge::WithinBody,
                            );
                            graph.add_edge(
                                predicate_position.clone(),
                                last_position,
                                PositionGraphEdge::WithinBody,
                            );
                        })
                        .or_insert(predicate_position);
                }
            }

            for constructor in rule.constructors() {
                for head_position in variables_to_head_positions
                    .get(constructor.variable())
                    .unwrap_or(&vec![])
                {
                    for term in constructor.term().primitive_terms() {
                        if let PrimitiveTerm::Variable(body_variable) = term {
                            let body_position_opt =
                                variables_to_last_node.get(body_variable).cloned();

                            if let Some(body_position) = body_position_opt {
                                graph.add_edge(
                                    body_position,
                                    head_position.clone(),
                                    PositionGraphEdge::BodyToHeadSameVariable,
                                );
                            }
                        }
                    }
                }
            }

            for constraint in rule.all_constraints() {
                let variables = constraint
                    .left()
                    .variables()
                    .chain(constraint.right().variables());
                let next_variables = constraint
                    .left()
                    .variables()
                    .chain(constraint.right().variables())
                    .skip(1);

                for (current_variable, next_variable) in variables.zip(next_variables) {
                    let position_current = variables_to_last_node
                        .get(current_variable)
                        .expect("Variables in filters should also appear in the rule body")
                        .clone();
                    let position_next = variables_to_last_node
                        .get(next_variable)
                        .expect("Variables in filters should also appear in the rule body")
                        .clone();

                    graph.add_edge(
                        position_current.clone(),
                        position_next.clone(),
                        PositionGraphEdge::WithinBody,
                    );
                    graph.add_edge(
                        position_next,
                        position_current,
                        PositionGraphEdge::WithinBody,
                    );
                }
            }
        }

        graph
    }

    fn dfs_for_type_requirements_check(
        &self,
        edge_types: HashSet<PositionGraphEdge>,
        start_position: PredicatePosition,
        mut payload: impl FnMut(&PredicatePosition) -> Result<(), TypeError>,
    ) -> Result<(), TypeError> {
        if let Some(start_node) = self.get_node(&start_position) {
            let edge_filtered_graph =
                EdgeFiltered::from_fn(self.graph(), |e| edge_types.contains(e.weight()));

            let mut dfs = Dfs::new(&edge_filtered_graph, start_node);

            while let Some(next_node) = dfs.next(&edge_filtered_graph) {
                let next_position = self
                    .graph()
                    .node_weight(next_node)
                    .expect("The DFS iterator guarantees that every node exists.");

                payload(next_position)?;
            }
        }

        Ok(())
    }

    pub(super) fn propagate_type_requirements(
        &self,
        reqs: PredicateTypeRequirements,
    ) -> Result<PredicateTypeRequirements, TypeError> {
        let mut propagated_reqs = reqs.clone();

        // Propagate each type from its declaration
        for (predicate, types) in reqs.into_iter() {
            for (position, logical_type_requirement) in types.iter().enumerate() {
                let predicate_position = PredicatePosition::new(predicate.clone(), position);

                self.dfs_for_type_requirements_check(
                    HashSet::from([
                        PositionGraphEdge::BodyToHeadSameVariable,
                        PositionGraphEdge::BodyToHeadAggregateNonStaticOutputType,
                    ]),
                    predicate_position,
                    |next_position| {
                        let current_type_requirement = &mut propagated_reqs
                            .get_mut(&next_position.predicate)
                            .expect("The initialization step inserted every known predicate")
                            [next_position.position];

                        if let Some(replacement) = current_type_requirement
                            .replace_with_max_type_if_compatible(*logical_type_requirement)
                        {
                            *current_type_requirement = replacement;
                            Ok(())
                        } else {
                            Err(TypeError::InvalidRuleConflictingTypes(
                                next_position.predicate.0.clone(),
                                next_position.position + 1,
                                Option::<PrimitiveType>::from(*current_type_requirement)
                                    .expect("if the type requirement is none, there is a maximum"),
                                Option::<PrimitiveType>::from(*logical_type_requirement)
                                    .expect("if the type requirement is none, there is a maximum"),
                            ))
                        }
                    },
                )?;
            }
        }

        Ok(propagated_reqs)
    }

    pub(super) fn check_type_requirement_compatibility(
        &self,
        reqs: &HashMap<Identifier, Vec<PrimitiveType>>,
    ) -> Result<(), TypeError> {
        // Check compatibility of body types without overwriting
        for (predicate, types) in reqs {
            for (position, logical_type) in types.iter().enumerate() {
                let predicate_position = PredicatePosition::new(predicate.clone(), position);

                self.dfs_for_type_requirements_check(
                    HashSet::from([PositionGraphEdge::WithinBody]),
                    predicate_position,
                    |next_position| {
                        let current_type = &reqs
                            .get(&next_position.predicate)
                            .expect("The initialization step inserted every known predicate")
                            [next_position.position];

                        current_type.partial_cmp(logical_type).map(|_| ()).ok_or(
                            // TODO: maybe just throw a warning here? (comparison of incompatible
                            // types can be done but will trivially result in inequality)
                            TypeError::InvalidRuleConflictingTypes(
                                next_position.predicate.0.clone(),
                                next_position.position + 1,
                                *current_type,
                                *logical_type,
                            ),
                        )
                    },
                )?;
            }
        }

        Ok(())
    }
}
