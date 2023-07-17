use std::collections::{HashMap, HashSet};

use nemo_physical::management::database::ColumnOrder;

use crate::{
    error::Error,
    model::chase_model::{ChaseProgram, ChaseRule},
    model::{
        chase_model::ChaseAtom, types::error::TypeError, DataSource, FilterOperation, Identifier,
        PrimitiveType, Term, TermOperation, TypeConstraint, Variable,
    },
    util::labeled_graph::LabeledGraph,
};

use super::{
    normalization::normalize_atom_vector,
    variable_order::{build_preferable_variable_orders, BuilderResultVariants, VariableOrder},
};

use petgraph::{
    visit::{Dfs, EdgeFiltered},
    Directed,
};
use thiserror::Error;

/// Contains useful information for a (existential) rule
#[derive(Debug, Clone)]
pub struct RuleAnalysis {
    /// Whether it uses an existential variable in its head.
    pub is_existential: bool,
    /// Whether an atom in the head also occurs in the body.
    pub is_recursive: bool,
    /// Whether the rule has positive filters that need to be applied.
    pub has_positive_filters: bool,
    /// Whether the rule has negative filters that need to be applied.
    pub has_negative_filters: bool,

    /// Predicates appearing in the positive part of the body.
    pub positive_body_predicates: HashSet<Identifier>,
    /// Predicates appearing in the negative part of the body.
    pub negative_body_predicates: HashSet<Identifier>,
    /// Predicates appearing in the head.
    pub head_predicates: HashSet<Identifier>,

    /// Variables occuring in the positive part of the body.
    pub positive_body_variables: HashSet<Variable>,
    /// Variables occuring in the positive part of the body.
    pub negative_body_variables: HashSet<Variable>,
    /// Variables occuring in the head.
    pub head_variables: HashSet<Variable>,
    /// Number of existential variables.
    pub num_existential: usize,

    /// Rule that represents the calculation of the satisfied matches for an existential rule.
    pub existential_aux_rule: ChaseRule,
    /// The associated variable order for the join of the head atoms
    pub existential_aux_order: VariableOrder,
    /// The types associated with the auxillary rule
    pub existential_aux_types: HashMap<Variable, PrimitiveType>,

    /// Variable orders that are worth considering.
    pub promising_variable_orders: Vec<VariableOrder>,

    /// Logical Type of each Variable
    pub variable_types: HashMap<Variable, PrimitiveType>,
    /// Logical Type of predicates in Rule
    pub predicate_types: HashMap<Identifier, Vec<PrimitiveType>>,
}

/// Errors than can occur during rule analysis
#[derive(Error, Debug, Copy, Clone)]
#[allow(clippy::enum_variant_names)]
pub enum RuleAnalysisError {
    /// Unsupported feature: Overloading of predicate names by arity/type
    #[error("Overloading of predicate names by arity is currently not supported.")]
    UnsupportedFeaturePredicateOverloading,
}

/// Return true if there is a predicate in the positive part of the rule that also appears in the head of the rule.
fn is_recursive(rule: &ChaseRule) -> bool {
    rule.head().iter().any(|h| {
        rule.positive_body()
            .iter()
            .any(|b| h.predicate() == b.predicate())
    })
}

fn count_distinct_existential_variables(rule: &ChaseRule) -> usize {
    let mut existentials = HashSet::<Variable>::new();

    for head_atom in rule.head() {
        for term in head_atom.terms() {
            if let Term::Variable(Variable::Existential(id)) = term {
                existentials.insert(Variable::Existential(id.clone()));
            }
        }
    }

    existentials.len()
}

fn get_variables(atoms: &[ChaseAtom]) -> HashSet<Variable> {
    let mut result = HashSet::new();
    for atom in atoms {
        for term in atom.terms() {
            if let Term::Variable(v) = term {
                result.insert(v.clone());
            }
        }
    }
    result
}

fn get_predicates(atoms: &[ChaseAtom]) -> HashSet<Identifier> {
    atoms.iter().map(|a| a.predicate()).collect()
}

fn get_fresh_rule_predicate(rule_index: usize) -> Identifier {
    Identifier(format!(
        "FRESH_HEAD_MATCHES_IDENTIFIER_FOR_RULE_{rule_index}"
    ))
}

fn construct_existential_aux_rule(
    rule_index: usize,
    head_atoms: &Vec<ChaseAtom>,
    predicate_types: &HashMap<Identifier, Vec<PrimitiveType>>,
    column_orders: &HashMap<Identifier, HashSet<ColumnOrder>>,
) -> (ChaseRule, VariableOrder, HashMap<Variable, PrimitiveType>) {
    let normalized_head = normalize_atom_vector(head_atoms, &[], &mut 0);

    let temp_head_identifier = get_fresh_rule_predicate(rule_index);

    let mut term_vec = Vec::<Term>::new();
    let mut occured_variables = HashSet::<Variable>::new();

    for atom in head_atoms {
        for term in atom.terms() {
            if let Term::Variable(Variable::Universal(variable)) = term {
                if occured_variables.insert(Variable::Universal(variable.clone())) {
                    term_vec.push(Term::Variable(Variable::Universal(variable.clone())));
                }
            }
        }
    }

    let mut variable_types = HashMap::<Variable, PrimitiveType>::new();
    for atom in &normalized_head.atoms {
        let types = predicate_types
            .get(&atom.predicate())
            .expect("Every predicate should have type information at this point");

        for (term_index, term) in atom.terms().iter().enumerate() {
            if let Term::Variable(variable) = term {
                variable_types.insert(variable.clone(), types[term_index]);
            }
        }
    }

    let temp_head_atom = ChaseAtom::new(temp_head_identifier, term_vec);
    let temp_rule = ChaseRule::new(
        vec![temp_head_atom],
        HashMap::default(),
        normalized_head.atoms,
        normalized_head.filters,
        vec![],
        vec![],
    );

    let temp_program = vec![temp_rule.clone()].into();
    let variable_order =
        build_preferable_variable_orders(&temp_program, Some(column_orders.clone()))
            .all_variable_orders
            .pop()
            .and_then(|mut v| v.pop())
            .expect("This functions provides at least one variable order");

    (temp_rule, variable_order, variable_types)
}

fn analyze_rule(
    rule: &ChaseRule,
    promising_variable_orders: Vec<VariableOrder>,
    promising_column_orders: &[HashMap<Identifier, HashSet<ColumnOrder>>],
    rule_index: usize,
    type_declarations: &HashMap<Identifier, Vec<PrimitiveType>>,
) -> RuleAnalysis {
    let num_existential = count_distinct_existential_variables(rule);

    let mut variable_types: HashMap<Variable, PrimitiveType> = HashMap::new();
    for atom in rule.all_atoms() {
        for (term_position, term) in atom.terms().iter().enumerate() {
            if let Term::Variable(variable) = term {
                variable_types.entry(variable.clone()).or_insert(
                    type_declarations
                        .get(&atom.predicate())
                        .expect("Every predicate should have recived type information.")
                        [term_position],
                );
            }
        }
    }

    let rule_all_predicates: Vec<Identifier> = rule
        .all_body()
        .chain(rule.head())
        .map(|a| a.predicate())
        .collect();

    let (existential_aux_rule, existential_aux_order, existential_aux_types) =
        if num_existential > 0 {
            // TODO: We only consider the first variable order
            construct_existential_aux_rule(
                rule_index,
                rule.head(),
                type_declarations,
                &promising_column_orders[0],
            )
        } else {
            (ChaseRule::default(), VariableOrder::new(), HashMap::new())
        };

    RuleAnalysis {
        is_existential: num_existential > 0,
        is_recursive: is_recursive(rule),
        has_positive_filters: !rule.positive_filters().is_empty(),
        has_negative_filters: !rule.negative_filters().is_empty(),
        positive_body_predicates: get_predicates(rule.positive_body()),
        negative_body_predicates: get_predicates(rule.negative_body()),
        head_predicates: get_predicates(rule.head()),
        positive_body_variables: get_variables(rule.positive_body()),
        negative_body_variables: get_variables(rule.negative_body()),
        head_variables: get_variables(rule.head()),
        num_existential,
        existential_aux_rule,
        existential_aux_order,
        existential_aux_types,
        promising_variable_orders,
        variable_types,
        predicate_types: type_declarations
            .iter()
            .filter_map(|(k, v)| {
                rule_all_predicates
                    .contains(k)
                    .then(|| (k.clone(), v.clone()))
            })
            .collect(),
    }
}

/// Identifies a position within a predicate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PredicatePosition {
    predicate: Identifier,
    position: usize,
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

/// Edge Types in Position Graph for Type Analysis
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum PositionGraphEdge {
    WithinBody,
    BodyToHead,
}
/// Graph that represents a prioritization between rules.
pub type PositionGraph = LabeledGraph<PredicatePosition, PositionGraphEdge, Directed>;

#[derive(Clone, Copy, Debug, PartialEq)]
enum TypeRequirement {
    Hard(PrimitiveType),
    Soft(PrimitiveType),
    None,
}

impl TypeRequirement {
    fn stricter_requirement(self, other: Self) -> Option<Self> {
        match self {
            Self::Hard(t1) => match other {
                Self::Hard(t2) => (t1 == t2).then_some(self),
                Self::Soft(t2) => (t1 >= t2).then_some(self),
                Self::None => Some(self),
            },
            Self::Soft(t1) => match other {
                Self::Hard(t2) => (t1 <= t2).then_some(other),
                Self::Soft(t2) => t1.partial_cmp(&t2).map(|ord| match ord {
                    std::cmp::Ordering::Equal => self,
                    std::cmp::Ordering::Greater => self,
                    std::cmp::Ordering::Less => other,
                }),
                Self::None => Some(self),
            },
            Self::None => Some(other),
        }
    }

    fn replacement_type_if_compatible(self, other: Self) -> Option<Self> {
        match self {
            Self::Hard(t1) => match other {
                Self::Hard(t2) => (t1 == t2).then_some(self),
                Self::Soft(t2) => (t1 >= t2).then_some(self),
                Self::None => Some(self),
            },
            Self::Soft(t1) => match other {
                Self::Hard(t2) => t1.partial_cmp(&t2).map(|ord| match ord {
                    std::cmp::Ordering::Equal => self,
                    std::cmp::Ordering::Greater => self,
                    std::cmp::Ordering::Less => Self::Soft(t2),
                }),
                Self::Soft(t2) => t1.partial_cmp(&t2).map(|ord| match ord {
                    std::cmp::Ordering::Equal => self,
                    std::cmp::Ordering::Greater => self,
                    std::cmp::Ordering::Less => other,
                }),
                Self::None => Some(self),
            },
            Self::None => match other {
                Self::Hard(t2) => Some(Self::Soft(t2)),
                Self::Soft(t2) => Some(Self::Soft(t2)),
                Self::None => Some(Self::None),
            },
        }
    }

    fn allowed_to_merge_with(self, other: Self) -> bool {
        match Option::<PrimitiveType>::from(self) {
            Some(t1) => match Option::from(other) {
                Some(t2) => t1.partial_cmp(&t2).is_some(),
                None => true,
            },
            None => true,
        }
    }
}

impl From<TypeConstraint> for TypeRequirement {
    fn from(value: TypeConstraint) -> Self {
        match value {
            TypeConstraint::None => TypeRequirement::None,
            TypeConstraint::Exact(p) => TypeRequirement::Hard(p),
            TypeConstraint::AtLeast(p) => TypeRequirement::Soft(p),
            TypeConstraint::Tuple(_) => {
                unimplemented!("currently nested type checking is not supported")
            }
        }
    }
}

impl From<TypeRequirement> for Option<PrimitiveType> {
    fn from(source: TypeRequirement) -> Self {
        match source {
            TypeRequirement::Hard(t) => Some(t),
            TypeRequirement::Soft(t) => Some(t),
            TypeRequirement::None => None,
        }
    }
}

/// Contains useful information about the
#[derive(Debug)]
pub struct ProgramAnalysis {
    /// Analysis result for each rule.
    pub rule_analysis: Vec<RuleAnalysis>,
    /// Set of all the predicates that are derived in the chase along with their arity.
    pub derived_predicates: HashSet<Identifier>,
    /// Set of all predicates and their arity.
    pub all_predicates: HashSet<(Identifier, usize)>,
    /// Logical Type Declarations for Predicates
    pub predicate_types: HashMap<Identifier, Vec<PrimitiveType>>,
    /// Graph representing the information flow between predicates
    pub position_graph: PositionGraph,
}

impl ChaseProgram {
    /// Collect all predicates that appear in a head atom into a [`HashSet`]
    fn get_head_predicates(&self) -> HashSet<Identifier> {
        let mut result = HashSet::<Identifier>::new();

        for rule in self.rules() {
            for head_atom in rule.head() {
                result.insert(head_atom.predicate());
            }
        }

        result
    }

    /// Collect all predicates occurring in the program.
    fn get_all_predicates(&self) -> HashSet<(Identifier, usize)> {
        let mut result = HashSet::<(Identifier, usize)>::new();

        // Predicates in source statements
        for source in self.sources() {
            result.insert((source.predicate.clone(), source.input_types().arity()));
        }

        // Predicates in rules
        for rule in self.rules() {
            for atom in rule.all_atoms() {
                result.insert((atom.predicate(), atom.terms().len()));
            }
        }

        // Predicates in facts
        for fact in self.facts() {
            result.insert((fact.0.predicate(), fact.0.terms().len()));
        }

        // Additional predicates for existential rules
        for (rule_index, rule) in self.rules().iter().enumerate() {
            let is_existential = count_distinct_existential_variables(rule) > 0;
            if !is_existential {
                continue;
            }

            let body_variables = get_variables(rule.positive_body());
            let head_variables = get_variables(rule.head());

            let predicate = get_fresh_rule_predicate(rule_index);
            let arity = head_variables.difference(&body_variables).count();

            result.insert((predicate, arity));
        }

        result
    }

    fn build_position_graph(&self) -> PositionGraph {
        let mut graph = PositionGraph::default();

        for rule in self.rules() {
            let mut variables_to_head_positions =
                HashMap::<Variable, Vec<PredicatePosition>>::new();

            for atom in rule.head() {
                for (term_position, term) in atom.terms().iter().enumerate() {
                    if let Term::Variable(variable) = term {
                        let predicate_position =
                            PredicatePosition::new(atom.predicate(), term_position);

                        variables_to_head_positions
                            .entry(variable.clone())
                            .and_modify(|e| e.push(predicate_position.clone()))
                            .or_insert(vec![predicate_position]);
                    }
                }
            }

            let mut variables_to_last_node = HashMap::<Variable, PredicatePosition>::new();

            for atom in rule.all_body() {
                for (term_position, term) in atom.terms().iter().enumerate() {
                    if let Term::Variable(variable) = term {
                        let predicate_position =
                            PredicatePosition::new(atom.predicate(), term_position);

                        // NOTE: we connect each body position to each head position of the same variable
                        if let Some(head_positions) = variables_to_head_positions.get(variable) {
                            for pos in head_positions {
                                graph.add_edge(
                                    predicate_position.clone(),
                                    pos.clone(),
                                    PositionGraphEdge::BodyToHead,
                                );
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
            }

            for (head_variable, tree) in rule.constructors() {
                // Note that the head variable for constructors is unique so we can get the first entry of this vector
                let head_position = &variables_to_head_positions
                    .get(head_variable)
                    .expect("The loop at the top went through all head atoms")[0];

                for term in tree.terms() {
                    if let Term::Variable(body_variable) = term {
                        let body_position = variables_to_last_node
                            .get(body_variable)
                            .expect("The iteration above went through all body atoms")
                            .clone();

                        graph.add_edge(
                            body_position,
                            head_position.clone(),
                            PositionGraphEdge::BodyToHead,
                        );
                    }
                }
            }

            for filter in rule.all_filters() {
                let position_left = variables_to_last_node
                    .get(&filter.lhs)
                    .expect("Variables in filters should also appear in the rule body")
                    .clone();

                if let Term::Variable(variable_right) = &filter.rhs {
                    let position_right = variables_to_last_node
                        .get(variable_right)
                        .expect("Variables in filters should also appear in the rule body")
                        .clone();

                    graph.add_edge(
                        position_left.clone(),
                        position_right.clone(),
                        PositionGraphEdge::WithinBody,
                    );
                    graph.add_edge(position_right, position_left, PositionGraphEdge::WithinBody);
                }
            }
        }

        graph
    }

    fn infer_predicate_types(
        &self,
        position_graph: &PositionGraph,
        all_predicates: &HashSet<(Identifier, usize)>,
    ) -> Result<HashMap<Identifier, Vec<PrimitiveType>>, TypeError> {
        let mut predicate_types: HashMap<Identifier, Vec<TypeRequirement>> = {
            let pred_decls = self
                .parsed_predicate_declarations()
                .iter()
                .map(|(pred, types)| {
                    (
                        pred.clone(),
                        types
                            .iter()
                            .copied()
                            .map(TypeRequirement::Hard)
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<HashMap<_, _>>();

            let source_decls: HashMap<Identifier, Vec<TypeRequirement>> = self
                .sources()
                .map(|source| {
                    (
                        source.predicate.clone(),
                        source
                            .input_types()
                            .iter()
                            .cloned()
                            .map(TypeRequirement::from)
                            .collect(),
                    )
                })
                .collect::<HashMap<_, _>>();

            let existential_decls = self
                .rules()
                .iter()
                .flat_map(|r| r.head())
                .map(|a| {
                    (
                        a.predicate(),
                        a.terms()
                            .iter()
                            .map(|t| {
                                if matches!(t, Term::Variable(Variable::Existential(_))) {
                                    TypeRequirement::Hard(PrimitiveType::Any)
                                } else {
                                    TypeRequirement::None
                                }
                            })
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<HashMap<_, _>>();

            let mut predicate_types = source_decls;
            predicate_types.extend(pred_decls);
            for (pred, exis_types) in existential_decls {
                // keep track of error that might occur on conflicting type changes
                let mut types_not_in_conflict: Result<_, _> = Ok(());

                let pred_clone = pred.clone();

                predicate_types
                    .entry(pred)
                    .and_modify(|ts| {
                        ts.iter_mut()
                            .zip(&exis_types)
                            .enumerate()
                            .for_each(|(index, (t, et))| match t.stricter_requirement(*et) {
                                Some(res) => {
                                    *t = res;
                                }
                                None => {
                                    types_not_in_conflict = Err(TypeError::InvalidRuleConflictingTypes(
                                        pred_clone.0.clone(),
                                        index,
                                        Option::<PrimitiveType>::from(*t).expect(
                                            "if the type requirement is none, there is a maximum",
                                        ),
                                        Option::<PrimitiveType>::from(*et).expect(
                                            "if the type requirement is none, there is a maximum",
                                        ),
                                    ));
                                }
                            })
                    })
                    .or_insert(exis_types);

                // abort if there is a type error
                types_not_in_conflict?
            }
            for (predicate, arity) in all_predicates {
                predicate_types
                    .entry(predicate.clone())
                    .or_insert(vec![TypeRequirement::None; *arity]);
            }
            predicate_types
        };

        let initial_types = predicate_types.clone();

        for (predicate, types) in initial_types {
            for (position, logical_type_requirement) in types.into_iter().enumerate() {
                let predicate_position = PredicatePosition::new(predicate.clone(), position);

                if let Some(start_node) = position_graph.get_node(&predicate_position) {
                    // Propagate each type from its declaration
                    let edge_filtered_graph = EdgeFiltered::from_fn(position_graph.graph(), |e| {
                        *e.weight() == PositionGraphEdge::BodyToHead
                    });

                    let mut dfs = Dfs::new(&edge_filtered_graph, start_node);

                    while let Some(next_node) = dfs.next(&edge_filtered_graph) {
                        let next_position = position_graph
                            .graph()
                            .node_weight(next_node)
                            .expect("The DFS iterator guarantees that every node exists.");

                        let current_type_requirement = &mut predicate_types
                            .get_mut(&next_position.predicate)
                            .expect("The initialization step inserted every known predicate")
                            [next_position.position];

                        if let Some(replacement) = current_type_requirement
                            .replacement_type_if_compatible(logical_type_requirement)
                        {
                            *current_type_requirement = replacement;
                        } else {
                            return Err(TypeError::InvalidRuleConflictingTypes(
                                next_position.predicate.0.clone(),
                                next_position.position + 1,
                                Option::<PrimitiveType>::from(*current_type_requirement)
                                    .expect("if the type requirement is none, there is a maximum"),
                                Option::<PrimitiveType>::from(logical_type_requirement)
                                    .expect("if the type requirement is none, there is a maximum"),
                            ));
                        }
                    }

                    // Check compatibility of body types without overwriting
                    let edge_filtered_graph = EdgeFiltered::from_fn(position_graph.graph(), |e| {
                        *e.weight() == PositionGraphEdge::WithinBody
                    });

                    let mut dfs = Dfs::new(&edge_filtered_graph, start_node);

                    while let Some(next_node) = dfs.next(&edge_filtered_graph) {
                        let next_position = position_graph
                            .graph()
                            .node_weight(next_node)
                            .expect("The DFS iterator guarantees that every node exists.");

                        let current_type_requirement = &mut predicate_types
                            .get_mut(&next_position.predicate)
                            .expect("The initialization step inserted every known predicate")
                            [next_position.position];

                        if !current_type_requirement.allowed_to_merge_with(logical_type_requirement)
                        {
                            // TODO: maybe just throw a warning here? (comparison of incompatible
                            // types can be done but will trivially result in inequality)
                            return Err(TypeError::InvalidRuleConflictingTypes(
                                next_position.predicate.0.clone(),
                                next_position.position + 1,
                                Option::<PrimitiveType>::from(*current_type_requirement)
                                    .expect("if the type requirement is none, merging is allowed"),
                                Option::<PrimitiveType>::from(logical_type_requirement)
                                    .expect("if the type requirement is none, merging is allowed"),
                            ));
                        }
                    }
                }
            }
        }

        // All the types that are not set will be mapped to a default type
        let result = predicate_types
            .into_iter()
            .map(|(predicate, types)| {
                (
                    predicate,
                    types
                        .into_iter()
                        .map(|t| Option::<PrimitiveType>::from(t).unwrap_or_default())
                        .collect(),
                )
            })
            .collect();

        Ok(result)
    }

    /// Check if the program contains rules with unsupported features
    pub fn check_for_unsupported_features(&self) -> Result<(), RuleAnalysisError> {
        let mut arities = HashMap::new();

        for source in self.sources() {
            arities.insert(source.predicate.clone(), source.input_types().arity());
        }

        for rule in self.rules() {
            for atom in rule.all_atoms() {
                // check for consistent predicate arities
                let arity = atom.terms().len();
                if arity != *arities.entry(atom.predicate()).or_insert(arity) {
                    return Err(RuleAnalysisError::UnsupportedFeaturePredicateOverloading);
                }
            }
        }

        for fact in self.facts() {
            let arity = fact.0.terms().len();
            if arity != *arities.entry(fact.0.predicate()).or_insert(arity) {
                return Err(RuleAnalysisError::UnsupportedFeaturePredicateOverloading);
            }
        }

        Ok(())
    }

    fn check_for_nonnumeric_arithmetic(&self, analyses: &[RuleAnalysis]) -> Result<(), TypeError> {
        for (rule, analysis) in self.rules().iter().zip(analyses.iter()) {
            for (variable, term_tree) in rule.constructors() {
                if !term_tree.0.is_leaf() {
                    let variable_type = analysis
                        .variable_types
                        .get(variable)
                        .expect("Previous analysis should have assigned a type to each variable.");

                    if !variable_type.allows_numeric_operations() {
                        return Err(TypeError::InvalidRuleNonNumericArithmetic);
                    }

                    for term in term_tree.terms() {
                        if let Term::Variable(variable) = term {
                            let variable_type = analysis.variable_types.get(variable).expect(
                                "Previous analysis should have assigned a type to each variable.",
                            );

                            if !variable_type.allows_numeric_operations() {
                                return Err(TypeError::InvalidRuleNonNumericArithmetic);
                            }
                        } else {
                            variable_type.ground_term_to_data_value_t(term.clone())?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if there is a constant that cannot be converted into the type of
    /// the variable/predicate position it is compared to.
    fn check_for_incompatible_constant_types(
        &self,
        analyses: &[RuleAnalysis],
        predicate_types: &HashMap<Identifier, Vec<PrimitiveType>>,
    ) -> Result<(), TypeError> {
        for fact in self.facts() {
            let predicate_types = predicate_types
                .get(&fact.0.predicate())
                .expect("Previous analysis should have assigned a type vector to each predicate.");

            for (term_index, ground_term_tree) in fact.0.terms().iter().enumerate() {
                if let TermOperation::Term(ground_term) = ground_term_tree.operation() {
                    let logical_type = predicate_types[term_index];
                    logical_type.ground_term_to_data_value_t(ground_term.clone())?;
                } else {
                    unreachable!(
                        "Its assumed that facts do not contain complicated expressions (for now?)"
                    );
                }
            }
        }

        for (rule, analysis) in self.rules().iter().zip(analyses.iter()) {
            for filter in rule.all_filters() {
                let left_variable = &filter.lhs;
                let right_term = if let Term::Variable(_) = filter.rhs {
                    continue;
                } else {
                    &filter.rhs
                };

                let variable_type = analysis
                    .variable_types
                    .get(left_variable)
                    .expect("Previous analysis should have assigned a type to each variable.");

                if filter.operation != FilterOperation::Equals
                    && !variable_type.allows_numeric_operations()
                {
                    return Err(TypeError::InvalidRuleNonNumericComparison);
                }

                variable_type.ground_term_to_data_value_t(right_term.clone())?;
            }

            for atom in rule.head() {
                let predicate_types = predicate_types.get(&atom.predicate()).expect(
                    "Previous analysis should have assigned a type vector to each predicate.",
                );

                for (term_index, term) in atom.terms().iter().enumerate() {
                    if let Term::Variable(head_variable) = term {
                        if rule.constructors().contains_key(head_variable) {
                            let variable_type = analysis.variable_types.get(head_variable).expect(
                                "Previous analysis should have assigned a type to each variable.",
                            );

                            if !variable_type.allows_numeric_operations() {
                                return Err(TypeError::InvalidRuleNonNumericArithmetic);
                            }
                        }
                    } else {
                        let logical_type = predicate_types[term_index];

                        logical_type.ground_term_to_data_value_t(term.clone())?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Analyze itself and return a struct containing the results.
    pub fn analyze(&self) -> Result<ProgramAnalysis, Error> {
        let BuilderResultVariants {
            all_variable_orders,
            all_column_orders,
        } = build_preferable_variable_orders(self, None);

        let all_predicates = self.get_all_predicates();
        let derived_predicates = self.get_head_predicates();

        let position_graph = self.build_position_graph();
        let predicate_types = self.infer_predicate_types(&position_graph, &all_predicates)?;

        let rule_analysis: Vec<RuleAnalysis> = self
            .rules()
            .iter()
            .enumerate()
            .map(|(i, r)| {
                analyze_rule(
                    r,
                    all_variable_orders[i].clone(),
                    &all_column_orders,
                    i,
                    &predicate_types,
                )
            })
            .collect();

        self.check_for_incompatible_constant_types(&rule_analysis, &predicate_types)?;
        self.check_for_nonnumeric_arithmetic(&rule_analysis)?;

        Ok(ProgramAnalysis {
            rule_analysis,
            derived_predicates,
            all_predicates,
            predicate_types,
            position_graph,
        })
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        model::{
            chase_model::{ChaseAtom, ChaseProgram, ChaseRule},
            DataSourceDeclaration, DsvFile, Identifier, NativeDataSource, PrimitiveType, Term,
            TupleConstraint, Variable,
        },
        program_analysis::analysis::get_fresh_rule_predicate,
    };

    fn get_test_rules_and_predicates() -> (
        (ChaseRule, ChaseRule),
        (Identifier, Identifier, Identifier, Identifier),
    ) {
        let a = Identifier("a".to_string());
        let b = Identifier("b".to_string());
        let c = Identifier("c".to_string());
        let r = Identifier("r".to_string());

        let x = Variable::Universal(Identifier("x".to_string()));
        let z = Variable::Existential(Identifier("z".to_string()));

        let tx = Term::Variable(x);
        let tz = Term::Variable(z);

        // A(x) :- B(x), C(x).
        let basic_rule = ChaseRule::new(
            vec![ChaseAtom::new(a.clone(), vec![tx.clone()])],
            HashMap::new(),
            vec![
                ChaseAtom::new(b.clone(), vec![tx.clone()]),
                ChaseAtom::new(c.clone(), vec![tx.clone()]),
            ],
            vec![],
            vec![],
            vec![],
        );

        // R(x, !z) :- A(x).
        let exis_rule = ChaseRule::new(
            vec![ChaseAtom::new(r.clone(), vec![tx.clone(), tz])],
            HashMap::new(),
            vec![ChaseAtom::new(a.clone(), vec![tx])],
            vec![],
            vec![],
            vec![],
        );

        ((basic_rule, exis_rule), (a, b, c, r))
    }

    #[test]
    fn infer_types_no_decl() {
        let ((basic_rule, exis_rule), (a, b, c, r)) = get_test_rules_and_predicates();

        let no_decl = ChaseProgram::new(
            None,
            Default::default(),
            Default::default(),
            vec![basic_rule, exis_rule],
            Default::default(),
            Default::default(),
            Default::default(),
        );

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::Any]),
            (b, vec![PrimitiveType::Any]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::Any, PrimitiveType::Any]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = no_decl
            .infer_predicate_types(
                &no_decl.build_position_graph(),
                &no_decl.get_all_predicates(),
            )
            .unwrap();
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_a_string_decl() {
        let ((basic_rule, exis_rule), (a, b, c, r)) = get_test_rules_and_predicates();

        let a_string_decl = ChaseProgram::new(
            None,
            Default::default(),
            Default::default(),
            vec![basic_rule, exis_rule],
            Default::default(),
            [(a.clone(), vec![PrimitiveType::String])]
                .into_iter()
                .collect(),
            Default::default(),
        );

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::String]),
            (b, vec![PrimitiveType::Any]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::String, PrimitiveType::Any]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = a_string_decl
            .infer_predicate_types(
                &a_string_decl.build_position_graph(),
                &a_string_decl.get_all_predicates(),
            )
            .unwrap();
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_a_int_decl() {
        let ((basic_rule, exis_rule), (a, b, c, r)) = get_test_rules_and_predicates();

        let a_int_decl = ChaseProgram::new(
            None,
            Default::default(),
            Default::default(),
            vec![basic_rule, exis_rule],
            Default::default(),
            [(a.clone(), vec![PrimitiveType::Integer])]
                .into_iter()
                .collect(),
            Default::default(),
        );

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::Integer]),
            (b, vec![PrimitiveType::Any]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::Integer, PrimitiveType::Any]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = a_int_decl
            .infer_predicate_types(
                &a_int_decl.build_position_graph(),
                &a_int_decl.get_all_predicates(),
            )
            .unwrap();
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_b_string_decl() {
        let ((basic_rule, exis_rule), (a, b, c, r)) = get_test_rules_and_predicates();

        let b_string_decl = ChaseProgram::new(
            None,
            Default::default(),
            Default::default(),
            vec![basic_rule, exis_rule],
            Default::default(),
            [(b.clone(), vec![PrimitiveType::String])]
                .into_iter()
                .collect(),
            Default::default(),
        );

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::String]),
            (b, vec![PrimitiveType::String]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::String, PrimitiveType::Any]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = b_string_decl
            .infer_predicate_types(
                &b_string_decl.build_position_graph(),
                &b_string_decl.get_all_predicates(),
            )
            .unwrap();
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_b_int_decl() {
        let ((basic_rule, exis_rule), (a, b, c, r)) = get_test_rules_and_predicates();

        let b_integer_decl = ChaseProgram::new(
            None,
            Default::default(),
            Default::default(),
            vec![basic_rule, exis_rule],
            Default::default(),
            [(b.clone(), vec![PrimitiveType::Integer])]
                .into_iter()
                .collect(),
            Default::default(),
        );

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::Integer]),
            (b, vec![PrimitiveType::Integer]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::Integer, PrimitiveType::Any]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = b_integer_decl
            .infer_predicate_types(
                &b_integer_decl.build_position_graph(),
                &b_integer_decl.get_all_predicates(),
            )
            .unwrap();
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_b_source_decl() {
        let ((basic_rule, exis_rule), (a, b, c, r)) = get_test_rules_and_predicates();

        let b_source_decl = ChaseProgram::new(
            None,
            Default::default(),
            vec![DataSourceDeclaration::new(
                b.clone(),
                NativeDataSource::DsvFile(DsvFile::csv_file("", TupleConstraint::from_arity(1))),
            )],
            vec![basic_rule, exis_rule],
            Default::default(),
            Default::default(),
            Default::default(),
        );

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::String]),
            (b, vec![PrimitiveType::String]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::String, PrimitiveType::Any]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = b_source_decl
            .infer_predicate_types(
                &b_source_decl.build_position_graph(),
                &b_source_decl.get_all_predicates(),
            )
            .unwrap();
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_c_explicit_decl_overrides_source_type() {
        let ((basic_rule, exis_rule), (a, b, c, r)) = get_test_rules_and_predicates();

        let c_explicit_decl_overrides_source_type = ChaseProgram::new(
            None,
            Default::default(),
            vec![DataSourceDeclaration::new(
                c.clone(),
                NativeDataSource::DsvFile(DsvFile::csv_file("", TupleConstraint::from_arity(1))),
            )],
            vec![basic_rule, exis_rule],
            Default::default(),
            [(c.clone(), vec![PrimitiveType::Integer])]
                .into_iter()
                .collect(),
            Default::default(),
        );

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::Integer]),
            (b, vec![PrimitiveType::Any]),
            (c, vec![PrimitiveType::Integer]),
            (r, vec![PrimitiveType::Integer, PrimitiveType::Any]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = c_explicit_decl_overrides_source_type
            .infer_predicate_types(
                &c_explicit_decl_overrides_source_type.build_position_graph(),
                &c_explicit_decl_overrides_source_type.get_all_predicates(),
            )
            .unwrap();
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_a_and_c_conflict_with_implicit_source_decl() {
        let ((basic_rule, exis_rule), (a, _b, c, _r)) = get_test_rules_and_predicates();

        let a_and_c_conflict_with_implicit_source_decl = ChaseProgram::new(
            None,
            Default::default(),
            vec![DataSourceDeclaration::new(
                c,
                NativeDataSource::DsvFile(DsvFile::csv_file("", TupleConstraint::from_arity(1))),
            )],
            vec![basic_rule, exis_rule],
            Default::default(),
            [(a, vec![PrimitiveType::Integer])].into_iter().collect(),
            Default::default(),
        );

        let inferred_types_res = a_and_c_conflict_with_implicit_source_decl.infer_predicate_types(
            &a_and_c_conflict_with_implicit_source_decl.build_position_graph(),
            &a_and_c_conflict_with_implicit_source_decl.get_all_predicates(),
        );
        assert!(inferred_types_res.is_err());
    }

    #[test]
    fn infer_types_a_and_c_conflict_with_explicit_source_decl_that_would_be_compatible_the_other_way_around(
    ) {
        let ((basic_rule, exis_rule), (a, _b, c, _r)) = get_test_rules_and_predicates();

        let a_and_c_conflict_with_explicit_source_decl_that_would_be_compatible_the_other_way_around =
            ChaseProgram::new(
                None,
                Default::default(),
                vec![DataSourceDeclaration::new(
                    c,
                    NativeDataSource::DsvFile(DsvFile::csv_file(
                        "",
                        [PrimitiveType::Any].into_iter().collect(),
                    )),
                )],
                vec![basic_rule, exis_rule],
                Default::default(),
                [(a, vec![PrimitiveType::String])].into_iter().collect(),
                Default::default(),
            );

        let inferred_types_res =
            a_and_c_conflict_with_explicit_source_decl_that_would_be_compatible_the_other_way_around
                .infer_predicate_types(
                &a_and_c_conflict_with_explicit_source_decl_that_would_be_compatible_the_other_way_around
                    .build_position_graph(),
                &a_and_c_conflict_with_explicit_source_decl_that_would_be_compatible_the_other_way_around
                    .get_all_predicates(),
            );
        assert!(inferred_types_res.is_err());
    }

    #[test]
    fn infer_types_a_and_b_source_decl_resolvable_conflict() {
        let ((basic_rule, exis_rule), (a, b, c, r)) = get_test_rules_and_predicates();

        let a_and_b_source_decl_resolvable_conflict = ChaseProgram::new(
            None,
            Default::default(),
            vec![DataSourceDeclaration::new(
                b.clone(),
                NativeDataSource::DsvFile(DsvFile::csv_file("", TupleConstraint::from_arity(1))),
            )],
            vec![basic_rule, exis_rule],
            Default::default(),
            [(a.clone(), vec![PrimitiveType::Any])]
                .into_iter()
                .collect(),
            Default::default(),
        );

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::Any]),
            (b, vec![PrimitiveType::String]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::Any, PrimitiveType::Any]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = a_and_b_source_decl_resolvable_conflict
            .infer_predicate_types(
                &a_and_b_source_decl_resolvable_conflict.build_position_graph(),
                &a_and_b_source_decl_resolvable_conflict.get_all_predicates(),
            )
            .unwrap();
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_r_source_decl_resolvable_conflict_with_exis() {
        let ((basic_rule, exis_rule), (a, b, c, r)) = get_test_rules_and_predicates();

        let r_source_decl_resolvable_conflict_with_exis = ChaseProgram::new(
            None,
            Default::default(),
            vec![DataSourceDeclaration::new(
                r.clone(),
                NativeDataSource::DsvFile(DsvFile::csv_file("", TupleConstraint::from_arity(2))),
            )],
            vec![basic_rule, exis_rule],
            Default::default(),
            Default::default(),
            Default::default(),
        );

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::Any]),
            (b, vec![PrimitiveType::Any]),
            (c, vec![PrimitiveType::Any]),
            (r, vec![PrimitiveType::String, PrimitiveType::Any]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = r_source_decl_resolvable_conflict_with_exis
            .infer_predicate_types(
                &r_source_decl_resolvable_conflict_with_exis.build_position_graph(),
                &r_source_decl_resolvable_conflict_with_exis.get_all_predicates(),
            )
            .unwrap();
        assert_eq!(inferred_types, expected_types);
    }

    #[test]
    fn infer_types_b_and_c_conflict_decl() {
        let ((basic_rule, exis_rule), (_a, b, c, _r)) = get_test_rules_and_predicates();

        let b_and_c_conflict_decl = ChaseProgram::new(
            None,
            Default::default(),
            Default::default(),
            vec![basic_rule, exis_rule],
            Default::default(),
            [
                (b, vec![PrimitiveType::Integer]),
                (c, vec![PrimitiveType::String]),
            ]
            .into_iter()
            .collect(),
            Default::default(),
        );

        let inferred_types_res = b_and_c_conflict_decl.infer_predicate_types(
            &b_and_c_conflict_decl.build_position_graph(),
            &b_and_c_conflict_decl.get_all_predicates(),
        );
        assert!(inferred_types_res.is_err());
    }

    #[test]
    fn infer_types_b_anc_c_conflict_decl_resolvable() {
        let ((basic_rule, exis_rule), (a, b, c, r)) = get_test_rules_and_predicates();

        let b_and_c_conflict_decl_resolvable = ChaseProgram::new(
            None,
            Default::default(),
            Default::default(),
            vec![basic_rule, exis_rule],
            Default::default(),
            [
                (b.clone(), vec![PrimitiveType::Any]),
                (c.clone(), vec![PrimitiveType::String]),
            ]
            .into_iter()
            .collect(),
            Default::default(),
        );

        let expected_types: HashMap<Identifier, Vec<PrimitiveType>> = [
            (a, vec![PrimitiveType::Any]),
            (b, vec![PrimitiveType::Any]),
            (c, vec![PrimitiveType::String]),
            (r, vec![PrimitiveType::Any, PrimitiveType::Any]),
            (get_fresh_rule_predicate(1), vec![PrimitiveType::Any]),
        ]
        .into_iter()
        .collect();

        let inferred_types = b_and_c_conflict_decl_resolvable
            .infer_predicate_types(
                &b_and_c_conflict_decl_resolvable.build_position_graph(),
                &b_and_c_conflict_decl_resolvable.get_all_predicates(),
            )
            .unwrap();
        assert_eq!(inferred_types, expected_types);
    }
}
